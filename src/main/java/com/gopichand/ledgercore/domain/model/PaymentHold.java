package com.gopichand.ledgercore.domain.model;

import java.time.Instant;
import java.util.UUID;

public record PaymentHold(
    UUID id,
    UUID accountId,
    UUID destinationAccountId,
    long amountPaise,
    HoldStatus status,
    String reference,
    UUID journalEntryId,
    Instant createdAt,
    Instant updatedAt
) {
}
