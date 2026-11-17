package com.gopichand.ledgercore.settlement;

import com.gopichand.ledgercore.transfer.TransferEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Service
public class PayoutGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(PayoutGatewayClient.class);

    /**
     * Executes payout settlement with simulated external bank/UPI gateway.
     * Protected by Resilience4j Circuit Breaker and Spring Retry with exponential backoff & jitter.
     */
    @CircuitBreaker(name = "payoutGateway")
    @Retryable(
            retryFor = { PayoutGatewayException.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2.0, random = true)
    )
    public void executePayout(TransferEvent event) {
        log.info("Calling external banking/UPI payout gateway for eventId: {}, journal: {}, amount: {} paise",
                event.eventId(), event.journalEntryId(), event.amountPaise());

        // Simulation hook: if reference contains "FAIL_PAYOUT", simulate gateway downtime
        if (event.reference() != null && event.reference().contains("FAIL_PAYOUT")) {
            log.warn("Simulated banking network error for eventId: {}", event.eventId());
            throw new PayoutGatewayException("Simulated external banking switch timeout for event: " + event.eventId());
        }

        log.info("External payout confirmed by banking partner for eventId: {}", event.eventId());
    }
}
