package com.gopichand.ledgercore.transfer;

import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
    UUID journalEntryId,
    String idempotencyKey,
    UUID sourceAccountId,
    UUID destinationAccountId,
    long amountPaise,
    String status,
    String reference,
    Instant postedAt
) {
}
