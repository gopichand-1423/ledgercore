package com.gopichand.ledgercore.transfer;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.Objects;
import java.util.UUID;

public record TransferRequest(
    @NotNull(message = "sourceAccountId must not be null")
    UUID sourceAccountId,

    @NotNull(message = "destinationAccountId must not be null")
    UUID destinationAccountId,

    @Positive(message = "amountPaise must be strictly positive (> 0)")
    long amountPaise,

    String reference
) {
    public TransferRequest {
        Objects.requireNonNull(sourceAccountId, "sourceAccountId must not be null");
        Objects.requireNonNull(destinationAccountId, "destinationAccountId must not be null");
        if (sourceAccountId.equals(destinationAccountId)) {
            throw new IllegalArgumentException("Source and destination accounts must be distinct");
        }
        if (amountPaise <= 0) {
            throw new IllegalArgumentException("Transaction amount in paise must be strictly positive (> 0)");
        }
    }
}
