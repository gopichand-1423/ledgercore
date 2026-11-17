package com.gopichand.ledgercore.hold;

import com.gopichand.ledgercore.domain.model.HoldStatus;
import com.gopichand.ledgercore.domain.model.PaymentHold;

import java.time.Instant;
import java.util.UUID;

public record HoldResponse(
    UUID holdId,
    UUID accountId,
    UUID destinationAccountId,
    long amountPaise,
    HoldStatus status,
    String reference,
    UUID journalEntryId,
    Instant createdAt,
    Instant updatedAt
) {
    public static HoldResponse fromDomain(PaymentHold hold) {
        return new HoldResponse(
            hold.id(),
            hold.accountId(),
            hold.destinationAccountId(),
            hold.amountPaise(),
            hold.status(),
            hold.reference(),
            hold.journalEntryId(),
            hold.createdAt(),
            hold.updatedAt()
        );
    }
}
