package com.gopichand.ledgercore.transfer;

import java.time.Instant;
import java.util.UUID;

public record TransferEvent(
    UUID eventId,
    UUID journalEntryId,
    String idempotencyKey,
    UUID sourceAccountId,
    UUID destinationAccountId,
    long amountPaise,
    String currency,
    String reference,
    Instant timestamp
) {
}
