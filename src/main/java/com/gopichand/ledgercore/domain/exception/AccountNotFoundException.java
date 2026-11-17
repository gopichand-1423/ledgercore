package com.gopichand.ledgercore.domain.exception;

import java.util.UUID;

public class AccountNotFoundException extends RuntimeException {

    private final UUID accountId;

    public AccountNotFoundException(UUID accountId) {
        super("Account not found with id: " + accountId);
        this.accountId = accountId;
    }

    public AccountNotFoundException(String message) {
        super(message);
        this.accountId = null;
    }

    public UUID getAccountId() {
        return accountId;
    }
}
