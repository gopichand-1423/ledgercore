package com.gopichand.ledgercore.hold;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

public record CreateHoldRequest(
    @NotNull(message = "accountId must not be null")
    UUID accountId,

    UUID destinationAccountId,

    @Positive(message = "amountPaise must be strictly positive (> 0)")
    long amountPaise,

    String reference
) {
    public CreateHoldRequest {
        if (amountPaise <= 0) {
            throw new IllegalArgumentException("amountPaise must be strictly positive (> 0)");
        }
    }
}
