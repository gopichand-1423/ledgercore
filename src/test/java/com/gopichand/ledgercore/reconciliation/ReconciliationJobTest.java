package com.gopichand.ledgercore.reconciliation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconciliationJobTest {

    @Mock
    private ReconciliationRepository reconciliationRepository;

    private SimpleMeterRegistry meterRegistry;
    private ReconciliationJob reconciliationJob;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        reconciliationJob = new ReconciliationJob(reconciliationRepository, meterRegistry);
    }

    @Test
    @DisplayName("Should report zero drift count when ledger and stateful balances match perfectly")
    void testZeroDriftAudit() {
        when(reconciliationRepository.findDriftDiscrepancies()).thenReturn(List.of());

        reconciliationJob.executeReconciliationAudit();

        assertEquals(0L, reconciliationJob.getCurrentDriftCount());
        assertEquals(0.0, meterRegistry.get("ledger_reconciliation_drift_count").gauge().value());
    }

    @Test
    @DisplayName("Should detect and update drift count gauge when accounts have discrepancies")
    void testNonZeroDriftAudit() {
        ReconciliationDiscrepancy discrepancy = new ReconciliationDiscrepancy(
                UUID.randomUUID(),
                "ACC-DRIFT-1",
                50000L,
                48000L,
                2000L
        );
        when(reconciliationRepository.findDriftDiscrepancies()).thenReturn(List.of(discrepancy));

        reconciliationJob.executeReconciliationAudit();

        assertEquals(1L, reconciliationJob.getCurrentDriftCount());
        assertEquals(1.0, meterRegistry.get("ledger_reconciliation_drift_count").gauge().value());
    }
}
