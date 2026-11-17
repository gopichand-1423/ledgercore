package com.gopichand.ledgercore.account;

import com.gopichand.ledgercore.domain.model.Direction;
import com.gopichand.ledgercore.domain.model.LedgerLine;

import java.time.Instant;
import java.util.UUID;

public record LedgerLineResponse(
    UUID id,
    UUID entryId,
    UUID accountId,
    Direction direction,
    long amountPaise,
    long balanceAfterPaise,
    int sequenceNumber,
    Instant createdAt
) {
    public static LedgerLineResponse fromDomain(LedgerLine line) {
        return new LedgerLineResponse(
            line.id(),
            line.entryId(),
            line.accountId(),
            line.direction(),
            line.amountPaise(),
            line.balanceAfterPaise(),
            line.sequenceNumber(),
            line.createdAt()
        );
    }
}
