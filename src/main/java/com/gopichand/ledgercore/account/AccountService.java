package com.gopichand.ledgercore.account;

import com.gopichand.ledgercore.domain.exception.AccountNotFoundException;
import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;
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

@Service
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    private final AccountRepository accountRepository;

    public AccountService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public AccountResponse createAccount(CreateAccountRequest request) {
        if (accountRepository.findByAccountNumber(request.accountNumber()).isPresent()) {
            throw new IllegalArgumentException("Account with number " + request.accountNumber() + " already exists");
        }

        UUID accountId = UUID.randomUUID();
        Instant now = Instant.now();
        long initialDeposit = request.initialFundingPaise() != null ? request.initialFundingPaise() : 0L;

        Account account = new Account(
                accountId,
                request.accountNumber(),
                request.accountType(),
                request.currency(),
                0L, // Initial balance starts at 0 before funding
                0L,
                AccountStatus.ACTIVE,
                0L,
                now,
                now
        );
        accountRepository.insertAccount(account);
        log.info("Created account id: {}, number: {}, type: {}", accountId, request.accountNumber(), request.accountType());

        // Process initial funding via system mint account if specified
        if (initialDeposit > 0) {
            executeInitialMintFunding(account, initialDeposit);
            account = accountRepository.findById(accountId)
                    .orElseThrow(() -> new AccountNotFoundException(accountId));
        }

        return AccountResponse.fromDomain(account);
    }

    private void executeInitialMintFunding(Account userAccount, long amountPaise) {
        String mintAccountNumber = "SYSTEM_MINT_" + userAccount.currency();
        Account mintAccount = accountRepository.findByAccountNumber(mintAccountNumber)
                .orElseGet(() -> {
                    UUID mintId = UUID.randomUUID();
                    Instant now = Instant.now();
                    Account newMint = new Account(
                            mintId,
                            mintAccountNumber,
                            AccountType.EQUITY,
                            userAccount.currency(),
                            1_000_000_000_000_00L, // 10 trillion paise mint reserve
                            0L,
                            AccountStatus.ACTIVE,
                            0L,
                            now,
                            now
                    );
                    accountRepository.insertAccount(newMint);
                    log.info("Initialized system mint account: {}", mintAccountNumber);
                    return newMint;
                });

        // Acquire lock on mint and user account deterministically
        UUID firstId = mintAccount.id().compareTo(userAccount.id()) < 0 ? mintAccount.id() : userAccount.id();
        UUID secondId = mintAccount.id().compareTo(userAccount.id()) < 0 ? userAccount.id() : mintAccount.id();

        Account lockedFirst = accountRepository.findByIdForUpdate(firstId).orElseThrow();
        Account lockedSecond = accountRepository.findByIdForUpdate(secondId).orElseThrow();

        Account lockedMint = mintAccount.id().equals(firstId) ? lockedFirst : lockedSecond;
        Account lockedUser = userAccount.id().equals(firstId) ? lockedFirst : lockedSecond;

        long mintBalanceAfter = lockedMint.balancePaise() - amountPaise;
        long userBalanceAfter = lockedUser.balancePaise() + amountPaise;

        UUID journalId = UUID.randomUUID();
        Instant now = Instant.now();

        JournalEntry journalEntry = new JournalEntry(
                journalId,
                "INITIAL_FUNDING_" + userAccount.id(),
                null,
                EntryType.CLEARING,
                EntryStatus.POSTED,
                "Initial funding for account " + userAccount.accountNumber(),
                null,
                now,
                now
        );
        accountRepository.insertJournalEntry(journalEntry);

        // Balanced double-entry legs: DEBIT mint, CREDIT user
        LedgerLine debitLeg = new LedgerLine(
                UUID.randomUUID(),
                journalId,
                lockedMint.id(),
                Direction.DEBIT,
                amountPaise,
                mintBalanceAfter,
                1,
                now
        );
        LedgerLine creditLeg = new LedgerLine(
                UUID.randomUUID(),
                journalId,
                lockedUser.id(),
                Direction.CREDIT,
                amountPaise,
                userBalanceAfter,
                2,
                now
        );
        accountRepository.insertLedgerLine(debitLeg);
        accountRepository.insertLedgerLine(creditLeg);

        accountRepository.updateBalance(lockedMint.id(), mintBalanceAfter);
        accountRepository.updateBalance(lockedUser.id(), userBalanceAfter);

        log.info("Successfully funded account {} with {} paise from {}",
                userAccount.id(), amountPaise, lockedMint.accountNumber());
    }

    @Transactional(readOnly = true)
    public AccountStatementResponse getStatement(UUID accountId, int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("Page index cannot be negative");
        }
        if (size <= 0 || size > 100) {
            throw new IllegalArgumentException("Page size must be between 1 and 100");
        }

        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));

        int offset = page * size;
        List<LedgerLine> lines = accountRepository.findLedgerLinesByAccountId(accountId, offset, size);
        long totalElements = accountRepository.countLedgerLinesByAccountId(accountId);

        List<LedgerLineResponse> lineResponses = lines.stream()
                .map(LedgerLineResponse::fromDomain)
                .toList();

        return new AccountStatementResponse(
                account.id(),
                account.accountNumber(),
                account.currency(),
                account.availableBalancePaise(),
                account.pendingBalancePaise(),
                account.balancePaise(),
                page,
                size,
                totalElements,
                lineResponses
        );
    }
}
