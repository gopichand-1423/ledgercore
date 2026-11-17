package com.gopichand.ledgercore.domain.exception;

import java.util.UUID;

public class InsufficientFundsException extends RuntimeException {

    private final UUID accountId;
    private final long availableBalancePaise;
    private final long requestedAmountPaise;

    public InsufficientFundsException(UUID accountId, long availableBalancePaise, long requestedAmountPaise) {
        super(String.format("Insufficient funds in account %s: available balance %d paise, requested %d paise",
                accountId, availableBalancePaise, requestedAmountPaise));
        this.accountId = accountId;
        this.availableBalancePaise = availableBalancePaise;
        this.requestedAmountPaise = requestedAmountPaise;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public long getAvailableBalancePaise() {
        return availableBalancePaise;
    }

    public long getRequestedAmountPaise() {
        return requestedAmountPaise;
    }
}
