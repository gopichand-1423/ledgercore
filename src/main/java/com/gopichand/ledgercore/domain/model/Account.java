package com.gopichand.ledgercore.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record Account(
    UUID id,
    String accountNumber,
    AccountType accountType,
    String currency,
    long balancePaise,
    long pendingBalancePaise,
    AccountStatus status,
    long version,
    Instant createdAt,
    Instant updatedAt
) {
    public Account {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(accountNumber, "accountNumber must not be null");
        Objects.requireNonNull(accountType, "accountType must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    public long availableBalancePaise() {
        return balancePaise - pendingBalancePaise;
    }
}
