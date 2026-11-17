package com.gopichand.ledgercore.account;

import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;

import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
    UUID id,
    String accountNumber,
    AccountType accountType,
    String currency,
    long balancePaise,
    long pendingBalancePaise,
    long availableBalancePaise,
    AccountStatus status,
    Instant createdAt,
    Instant updatedAt
) {
    public static AccountResponse fromDomain(Account account) {
        return new AccountResponse(
            account.id(),
            account.accountNumber(),
            account.accountType(),
            account.currency(),
            account.balancePaise(),
            account.pendingBalancePaise(),
            account.availableBalancePaise(),
            account.status(),
            account.createdAt(),
            account.updatedAt()
        );
    }
}
