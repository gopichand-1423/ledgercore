package com.gopichand.ledgercore.transfer;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.gopichand.ledgercore.domain.model.JournalEntry;
import com.gopichand.ledgercore.domain.model.LedgerLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class TransferService {

    private static final Logger log = LoggerFactory.getLogger(TransferService.class);

    private final AccountRepository accountRepository;
    private final ObjectMapper objectMapper;

    public TransferService(AccountRepository accountRepository, ObjectMapper objectMapper) {
        this.accountRepository = accountRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Executes double-entry transfer within an ACID transaction boundary.
     * Prevents deadlocks by sorting account IDs lexicographically before acquiring row-level locks.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public TransferResponse executeTransfer(String idempotencyKey, String payloadHash, TransferRequest request) {
        UUID sourceId = request.sourceAccountId();
        UUID destinationId = request.destinationAccountId();
        long amountPaise = request.amountPaise();

        // 1. Invariant: Distinct accounts and positive amount
        if (sourceId.equals(destinationId)) {
            throw new InvalidTransferException("Source and destination accounts must be distinct");
        }
        if (amountPaise <= 0) {
            throw new InvalidTransferException("Transfer amount in paise must be strictly positive (> 0)");
        }

        // 2. Deterministic Lexicographical Sorting to prevent circular wait deadlocks
        List<UUID> sortedIds = Stream.of(sourceId, destinationId)
                .sorted()
                .toList();
        UUID firstId = sortedIds.get(0);
        UUID secondId = sortedIds.get(1);

        log.debug("Acquiring pessimistic row locks in deterministic order: {} then {}", firstId, secondId);
        Account firstAccount = accountRepository.findByIdForUpdate(firstId)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + firstId));
        Account secondAccount = accountRepository.findByIdForUpdate(secondId)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + secondId));

        Account source = sourceId.equals(firstId) ? firstAccount : secondAccount;
        Account destination = destinationId.equals(firstId) ? firstAccount : secondAccount;

        // 3. Status and Currency Verification
        if (source.status() != AccountStatus.ACTIVE) {
            throw new InvalidTransferException("Source account " + sourceId + " is not ACTIVE (status: " + source.status() + ")");
        }
        if (destination.status() != AccountStatus.ACTIVE) {
            throw new InvalidTransferException("Destination account " + destinationId + " is not ACTIVE (status: " + destination.status() + ")");
        }
        if (!source.currency().equals(destination.currency())) {
            throw new InvalidTransferException("Currency mismatch between source (" + source.currency()
                    + ") and destination (" + destination.currency() + ")");
        }

        // 4. Validate Balance Invariant: availableBalance >= amountPaise
        long availableBalance = source.availableBalancePaise();
        if (availableBalance < amountPaise) {
            throw new InsufficientFundsException(sourceId, availableBalance, amountPaise);
        }

        // 5. Calculate balance snapshots
        long sourceBalanceAfter = source.balancePaise() - amountPaise;
        long destinationBalanceAfter = destination.balancePaise() + amountPaise;

        UUID journalId = UUID.randomUUID();
        Instant now = Instant.now();
        String description = request.reference() != null && !request.reference().isBlank()
                ? request.reference()
                : "Transfer from " + sourceId + " to " + destinationId;

        // 6. Insert Journal Entry (status = 'POSTED')
        JournalEntry journalEntry = new JournalEntry(
                journalId,
                idempotencyKey,
                payloadHash,
                EntryType.TRANSFER,
                EntryStatus.POSTED,
                description,
                null,
                now,
                now
        );
        accountRepository.insertJournalEntry(journalEntry);

        // 7. Insert balanced ledger lines (DEBIT source, CREDIT destination)
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
                destinationId,
                Direction.CREDIT,
                amountPaise,
                destinationBalanceAfter,
                2,
                now
        );
        accountRepository.insertLedgerLine(debitLeg);
        accountRepository.insertLedgerLine(creditLeg);

        // 8. Update account balances
        accountRepository.updateBalance(sourceId, sourceBalanceAfter);
        accountRepository.updateBalance(destinationId, destinationBalanceAfter);

        // 9. Write outbox event within the same ACID transaction
        TransferEvent event = new TransferEvent(
                UUID.randomUUID(),
                journalId,
                idempotencyKey,
                sourceId,
                destinationId,
                amountPaise,
                source.currency(),
                description,
                now
        );
        try {
            String payloadJson = objectMapper.writeValueAsString(event);
            accountRepository.insertOutboxEvent(
                    event.eventId(),
                    "TRANSFER",
                    journalId.toString(),
                    "LEDGER_TRANSFER_POSTED",
                    payloadJson
            );
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event payload", e);
        }

        log.info("Successfully posted transfer {} (journal: {}) for {} {} paise from {} to {}",
                idempotencyKey, journalId, amountPaise, source.currency(), sourceId, destinationId);

        return new TransferResponse(
                journalId,
                idempotencyKey,
                sourceId,
                destinationId,
                amountPaise,
                EntryStatus.POSTED.name(),
                description,
                now
        );
    }

    /**
     * Executes an append-only compensating reversal of a previous journal entry.
     * Never mutates or deletes historical ledger lines.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public TransferResponse executeReversal(UUID entryId) {
        JournalEntry original = accountRepository.findJournalEntryById(entryId)
                .orElseThrow(() -> new IllegalArgumentException("Journal entry not found: " + entryId));

        if (original.status() == EntryStatus.REVERSED) {
            throw new InvalidTransferException("Journal entry " + entryId + " is already reversed");
        }

        List<LedgerLine> originalLines = accountRepository.findLedgerLinesByEntryId(entryId);
        if (originalLines.size() < 2) {
            throw new InvalidTransferException("Invalid journal entry: must contain at least 2 legs");
        }

        LedgerLine originalDebit = originalLines.stream()
                .filter(l -> l.direction() == Direction.DEBIT)
                .findFirst()
                .orElseThrow(() -> new InvalidTransferException("Missing DEBIT leg in original journal entry"));
        LedgerLine originalCredit = originalLines.stream()
                .filter(l -> l.direction() == Direction.CREDIT)
                .findFirst()
                .orElseThrow(() -> new InvalidTransferException("Missing CREDIT leg in original journal entry"));

        UUID originalSourceId = originalDebit.accountId();
        UUID originalDestId = originalCredit.accountId();
        long amountPaise = originalDebit.amountPaise();

        // Deterministic lexicographical locking order
        List<UUID> sortedIds = Stream.of(originalSourceId, originalDestId).sorted().toList();
        Account first = accountRepository.findByIdForUpdate(sortedIds.get(0)).orElseThrow();
        Account second = accountRepository.findByIdForUpdate(sortedIds.get(1)).orElseThrow();

        Account currentDest = originalDestId.equals(sortedIds.get(0)) ? first : second;
        Account currentSource = originalSourceId.equals(sortedIds.get(0)) ? first : second;

        // Verify destination has sufficient available funds to be debited for the reversal
        long destAvailable = currentDest.availableBalancePaise();
        if (destAvailable < amountPaise) {
            throw new InsufficientFundsException(originalDestId, destAvailable, amountPaise);
        }

        long destBalanceAfter = currentDest.balancePaise() - amountPaise;
        long sourceBalanceAfter = currentSource.balancePaise() + amountPaise;

        UUID reversalJournalId = UUID.randomUUID();
        Instant now = Instant.now();
        String idempotencyKey = "REVERSAL_" + entryId;

        JournalEntry reversalEntry = new JournalEntry(
                reversalJournalId,
                idempotencyKey,
                null,
                EntryType.REVERSAL,
                EntryStatus.POSTED,
                "Compensating reversal of journal entry " + entryId,
                entryId,
                entryId,
                now,
                now
        );
        accountRepository.insertJournalEntry(reversalEntry);

        // Inverted postings: DEBIT originalDestId, CREDIT originalSourceId
        LedgerLine debitLeg = new LedgerLine(
                UUID.randomUUID(),
                reversalJournalId,
                originalDestId,
                Direction.DEBIT,
                amountPaise,
                destBalanceAfter,
                1,
                now
        );
        LedgerLine creditLeg = new LedgerLine(
                UUID.randomUUID(),
                reversalJournalId,
                originalSourceId,
                Direction.CREDIT,
                amountPaise,
                sourceBalanceAfter,
                2,
                now
        );
        accountRepository.insertLedgerLine(debitLeg);
        accountRepository.insertLedgerLine(creditLeg);

        // Update balances
        accountRepository.updateBalance(originalDestId, destBalanceAfter);
        accountRepository.updateBalance(originalSourceId, sourceBalanceAfter);

        // Mark original entry as REVERSED
        accountRepository.updateJournalEntryStatus(entryId, EntryStatus.REVERSED);

        // Emit outbox event LEDGER_TRANSFER_REVERSED
        TransferEvent reversalEvent = new TransferEvent(
                UUID.randomUUID(),
                reversalJournalId,
                idempotencyKey,
                originalDestId,
                originalSourceId,
                amountPaise,
                currentSource.currency(),
                "Reversal of " + entryId,
                now
        );
        try {
            String payloadJson = objectMapper.writeValueAsString(reversalEvent);
            accountRepository.insertOutboxEvent(
                    reversalEvent.eventId(),
                    "TRANSFER",
                    reversalJournalId.toString(),
                    "LEDGER_TRANSFER_REVERSED",
                    payloadJson
            );
        } catch (Exception e) {
            log.error("Failed to serialize reversal outbox event for {}: {}", entryId, e.getMessage());
        }

        log.info("Posted compensating reversal {} for original entry {}", reversalJournalId, entryId);
        return new TransferResponse(
                reversalJournalId,
                idempotencyKey,
                originalDestId,
                originalSourceId,
                amountPaise,
                EntryStatus.POSTED.name(),
                "Reversal of " + entryId,
                now
        );
    }
}
