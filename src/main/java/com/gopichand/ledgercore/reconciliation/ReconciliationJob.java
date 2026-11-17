package com.gopichand.ledgercore.reconciliation;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationJob.class);

    private final ReconciliationRepository reconciliationRepository;
    private final AtomicLong driftGauge;

    public ReconciliationJob(ReconciliationRepository reconciliationRepository, MeterRegistry meterRegistry) {
        this.reconciliationRepository = reconciliationRepository;
        this.driftGauge = meterRegistry.gauge("ledger_reconciliation_drift_count", new AtomicLong(0));
    }

    /**
     * Executes automated periodic reconciliation audit comparing stateful balances to immutable ledger lines.
     * Reports drift count to Micrometer Prometheus gauge and logs alerts.
     */
    @Scheduled(fixedRate = 60000)
    public void executeReconciliationAudit() {
        log.debug("Initiating scheduled double-entry reconciliation audit...");
        List<ReconciliationDiscrepancy> discrepancies = reconciliationRepository.findDriftDiscrepancies();
        long driftCount = discrepancies.size();
        driftGauge.set(driftCount);

        if (driftCount > 0) {
            log.error("CRITICAL AUDIT ALERT: Ledger balance drift detected across {} accounts! Details: {}",
                    driftCount, discrepancies);
        } else {
            log.info("Automated reconciliation passed: 0 account drifts detected across ledger.");
        }
    }

    public long getCurrentDriftCount() {
        return driftGauge.get();
    }
}
