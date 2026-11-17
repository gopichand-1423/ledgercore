package com.gopichand.ledgercore.account;

import com.gopichand.ledgercore.domain.model.AccountType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreateAccountRequest(
    @NotBlank(message = "accountNumber is required")
    String accountNumber,

    @NotNull(message = "accountType is required")
    AccountType accountType,

    @NotBlank(message = "currency is required")
    @Size(min = 3, max = 3, message = "currency must be a valid 3-letter ISO code")
    String currency,

    Long initialFundingPaise
) {
    public CreateAccountRequest {
        if (currency != null) {
            currency = currency.trim().toUpperCase();
        }
        if (initialFundingPaise != null && initialFundingPaise < 0) {
            throw new IllegalArgumentException("initialFundingPaise must not be negative");
        }
    }
}
