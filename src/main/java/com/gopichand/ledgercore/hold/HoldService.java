package com.gopichand.ledgercore.hold;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.account.AccountRepository;
import com.gopichand.ledgercore.domain.exception.AccountNotFoundException;
import com.gopichand.ledgercore.domain.exception.InsufficientFundsException;
import com.gopichand.ledgercore.domain.exception.InvalidTransferException;
import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.Direction;
import com.gopichand.ledgercore.domain.model.EntryStatus;
import com.gopichand.ledgercore.domain.model.EntryType;
import com.gopichand.ledgercore.domain.model.HoldStatus;
import com.gopichand.ledgercore.domain.model.JournalEntry;
import com.gopichand.ledgercore.domain.model.LedgerLine;
import com.gopichand.ledgercore.domain.model.PaymentHold;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class HoldService {

    private static final Logger log = LoggerFactory.getLogger(HoldService.class);

    private final HoldRepository holdRepository;
    private final AccountRepository accountRepository;
    private final ObjectMapper objectMapper;

    public HoldService(HoldRepository holdRepository, AccountRepository accountRepository, ObjectMapper objectMapper) {
        this.holdRepository = holdRepository;
        this.accountRepository = accountRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * AUTHORIZE: Locks account FOR UPDATE and shifts amountPaise from available_balance to held_balance.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public HoldResponse createHold(CreateHoldRequest request) {
        UUID accountId = request.accountId();
        long amountPaise = request.amountPaise();

        if (amountPaise <= 0) {
            throw new InvalidTransferException("Hold amount in paise must be strictly positive (> 0)");
        }

        // Lock source account FOR UPDATE
        Account account = accountRepository.findByIdForUpdate(accountId)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + accountId));

        if (account.status() != AccountStatus.ACTIVE) {
            throw new InvalidTransferException("Account " + accountId + " is not ACTIVE");
        }

        long availableBalance = account.availableBalancePaise();
        if (availableBalance < amountPaise) {
            throw new InsufficientFundsException(accountId, availableBalance, amountPaise);
        }

        // Shift funds from available to held (increase pending_balance_paise)
        long newPendingBalance = account.pendingBalancePaise() + amountPaise;
        holdRepository.updateAccountHeldBalance(accountId, newPendingBalance);

        UUID holdId = UUID.randomUUID();
        Instant now = Instant.now();
        PaymentHold hold = new PaymentHold(
                holdId,
                accountId,
                request.destinationAccountId(),
                amountPaise,
                HoldStatus.HELD,
                request.reference(),
                null,
                now,
                now
        );
        holdRepository.insertHold(hold);

        log.info("Authorized payment hold {} for {} paise on account {}", holdId, amountPaise, accountId);
        return HoldResponse.fromDomain(hold);
    }

    /**
     * CAPTURE: Settles the held funds to the destination merchant account via a balanced double-entry journal entry.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public HoldResponse captureHold(UUID holdId) {
        PaymentHold hold = holdRepository.findByIdForUpdate(holdId)
                .orElseThrow(() -> new IllegalArgumentException("Payment hold not found: " + holdId));

        if (hold.status() != HoldStatus.HELD) {
            throw new InvalidTransferException("Cannot capture hold " + holdId + " in status: " + hold.status());
        }

        if (hold.destinationAccountId() == null) {
            throw new InvalidTransferException("Cannot capture hold without destinationAccountId");
        }

        UUID sourceId = hold.accountId();
        UUID destId = hold.destinationAccountId();
        long amountPaise = hold.amountPaise();

        // Lexicographical ordering for deterministic row locks
        List<UUID> sortedIds = Stream.of(sourceId, destId).sorted().toList();
        Account first = accountRepository.findByIdForUpdate(sortedIds.get(0)).orElseThrow();
        Account second = accountRepository.findByIdForUpdate(sortedIds.get(1)).orElseThrow();

        Account source = sourceId.equals(sortedIds.get(0)) ? first : second;
        Account destination = destId.equals(sortedIds.get(0)) ? first : second;

        // Release held balance and deduct total balance from source
        long sourceBalanceAfter = source.balancePaise() - amountPaise;
        long sourcePendingAfter = Math.max(0, source.pendingBalancePaise() - amountPaise);
        holdRepository.updateAccountBalances(sourceId, sourceBalanceAfter, sourcePendingAfter);

        // Add funds to destination account
        long destBalanceAfter = destination.balancePaise() + amountPaise;
        accountRepository.updateBalance(destId, destBalanceAfter);

        // Insert double-entry journal entry and balanced ledger lines
        UUID journalId = UUID.randomUUID();
        Instant now = Instant.now();
        String idempotencyKey = "CAPTURE_" + holdId;

        JournalEntry journalEntry = new JournalEntry(
                journalId,
                idempotencyKey,
                null,
                EntryType.CAPTURE,
                EntryStatus.POSTED,
                "Capture of hold " + holdId,
                null,
                now,
                now
        );
        accountRepository.insertJournalEntry(journalEntry);

        LedgerLine debitLeg = new LedgerLine(
                UUID.randomUUID(),
                journalId,
                sourceId,
                Direction.DEBIT,
                amountPaise,
                sourceBalanceAfter,
                1,
                now
        );
        LedgerLine creditLeg = new LedgerLine(
                UUID.randomUUID(),
                journalId,
                destId,
                Direction.CREDIT,
                amountPaise,
                destBalanceAfter,
                2,
                now
        );
        accountRepository.insertLedgerLine(debitLeg);
        accountRepository.insertLedgerLine(creditLeg);

        // Update hold status to CAPTURED
        holdRepository.updateHoldStatus(holdId, HoldStatus.CAPTURED, journalId);

        // Emit outbox event
        emitHoldOutboxEvent(holdId, "LEDGER_HOLD_CAPTURED", Map.of(
                "holdId", holdId,
                "journalEntryId", journalId,
                "sourceAccountId", sourceId,
                "destinationAccountId", destId,
                "amountPaise", amountPaise
        ));

        log.info("Captured hold {} into journal entry {} for {} paise", holdId, journalId, amountPaise);
        return new HoldResponse(holdId, sourceId, destId, amountPaise, HoldStatus.CAPTURED, hold.reference(), journalId, hold.createdAt(), now);
    }

    /**
     * VOID: Releases held_balance back to available_balance.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public HoldResponse voidHold(UUID holdId) {
        PaymentHold hold = holdRepository.findByIdForUpdate(holdId)
                .orElseThrow(() -> new IllegalArgumentException("Payment hold not found: " + holdId));

        if (hold.status() != HoldStatus.HELD) {
            throw new InvalidTransferException("Cannot void hold " + holdId + " in status: " + hold.status());
        }

        UUID sourceId = hold.accountId();
        Account account = accountRepository.findByIdForUpdate(sourceId)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + sourceId));

        // Release held balance back to available balance
        long newPendingBalance = Math.max(0, account.pendingBalancePaise() - hold.amountPaise());
        holdRepository.updateAccountHeldBalance(sourceId, newPendingBalance);

        // Update hold status to VOIDED
        holdRepository.updateHoldStatus(holdId, HoldStatus.VOIDED, null);

        emitHoldOutboxEvent(holdId, "LEDGER_HOLD_VOIDED", Map.of(
                "holdId", holdId,
                "accountId", sourceId,
                "amountPaise", hold.amountPaise()
        ));

        log.info("Voided payment hold {} for {} paise, released to available balance on {}",
                holdId, hold.amountPaise(), sourceId);

        Instant now = Instant.now();
        return new HoldResponse(holdId, sourceId, hold.destinationAccountId(), hold.amountPaise(), HoldStatus.VOIDED, hold.reference(), null, hold.createdAt(), now);
    }

    private void emitHoldOutboxEvent(UUID holdId, String eventType, Map<String, Object> data) {
        try {
            String payload = objectMapper.writeValueAsString(data);
            accountRepository.insertOutboxEvent(UUID.randomUUID(), "HOLD", holdId.toString(), eventType, payload);
        } catch (Exception e) {
            log.error("Failed to write hold outbox event for {}: {}", holdId, e.getMessage());
        }
    }
}
