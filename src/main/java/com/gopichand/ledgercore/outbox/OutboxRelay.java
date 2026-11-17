package com.gopichand.ledgercore.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    public static final String TOPIC_LEDGER_TRANSFER_POSTED = "ledger.transfer.posted";

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Periodically queries unpublished outbox events using SKIP LOCKED and publishes to Kafka.
     * Executes on Java 24 Virtual Threads with non-blocking concurrency.
     */
    @Scheduled(fixedDelay = 200)
    @Transactional
    public void relayUnpublishedEvents() {
        List<OutboxEvent> events = outboxRepository.findUnpublishedForUpdate(50);
        if (events.isEmpty()) {
            return;
        }

        log.debug("Found {} unpublished outbox events to relay to Kafka topic {}",
                events.size(), TOPIC_LEDGER_TRANSFER_POSTED);

        for (OutboxEvent event : events) {
            try {
                // Publish synchronously to ensure partition offset acknowledgement before committing DB state
                kafkaTemplate.send(TOPIC_LEDGER_TRANSFER_POSTED, event.aggregateId(), event.payload())
                        .get(5, TimeUnit.SECONDS);

                outboxRepository.markPublished(event.id());
                log.info("Relayed outbox event id {} (aggregate: {}) to topic {}",
                        event.id(), event.aggregateId(), TOPIC_LEDGER_TRANSFER_POSTED);
            } catch (Exception e) {
                log.error("Failed to relay outbox event id {}: {}", event.id(), e.getMessage());
                outboxRepository.recordFailure(event.id(), e.getMessage());
            }
        }
    }
}
