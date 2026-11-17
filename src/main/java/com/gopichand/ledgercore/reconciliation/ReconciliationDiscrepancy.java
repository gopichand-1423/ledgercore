package com.gopichand.ledgercore.reconciliation;

import java.util.UUID;

public record ReconciliationDiscrepancy(
    UUID accountId,
    String accountNumber,
    long accountBalancePaise,
    long ledgerBalancePaise,
    long driftPaise
) {
}
