package com.gopichand.ledgercore.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record LedgerLine(
    UUID id,
    UUID entryId,
    UUID accountId,
    Direction direction,
    long amountPaise,
    long balanceAfterPaise,
    int sequenceNumber,
    Instant createdAt
) {
    public LedgerLine {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(entryId, "entryId must not be null");
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(direction, "direction must not be null");
        if (amountPaise <= 0) {
            throw new IllegalArgumentException("amountPaise must be strictly positive (> 0)");
        }
    }
}
