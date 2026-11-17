package com.gopichand.ledgercore.account;

import java.util.List;
import java.util.UUID;

public record AccountStatementResponse(
    UUID accountId,
    String accountNumber,
    String currency,
    long availableBalancePaise,
    long heldBalancePaise,
    long totalBalancePaise,
    int page,
    int size,
    long totalElements,
    List<LedgerLineResponse> ledgerLines
) {
}
