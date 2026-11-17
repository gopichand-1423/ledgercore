package com.gopichand.ledgercore.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.transfer.TransferEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class DLQAdminService {

    private static final Logger log = LoggerFactory.getLogger(DLQAdminService.class);
    public static final String TOPIC_RETRY_DESTINATION = "ledger.transfer.posted";

    private final InboxRepository inboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DLQAdminService(InboxRepository inboxRepository, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.inboxRepository = inboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Safely replays a dead-lettered event by removing any inbox deduplication locks
     * and re-publishing the event back into the main settlement queue.
     */
    public DLQReplayResponse replayMessage(DLQReplayRequest request) {
        UUID eventId = request.eventId();
        String payload = request.payload();

        log.info("Initiating DLQ replay for eventId: {}", eventId);

        try {
            TransferEvent event = objectMapper.readValue(payload, TransferEvent.class);
            String partitionKey = event.journalEntryId().toString();

            // Clear inbox deduplication record to allow re-processing by SettlementConsumer
            inboxRepository.removeInboxEvent(eventId, SettlementConsumer.CONSUMER_GROUP);

            // Re-publish to the original ledger.transfer.posted topic
            kafkaTemplate.send(TOPIC_RETRY_DESTINATION, partitionKey, payload);

            log.info("Successfully re-queued DLQ event {} to topic {}", eventId, TOPIC_RETRY_DESTINATION);

            return new DLQReplayResponse(
                    "REPLAYED",
                    "Event successfully re-published to " + TOPIC_RETRY_DESTINATION,
                    eventId,
                    Instant.now()
            );
        } catch (Exception e) {
            log.error("Failed to replay DLQ message for event {}: {}", eventId, e.getMessage(), e);
            throw new IllegalArgumentException("Invalid payload or failed replay: " + e.getMessage(), e);
        }
    }
}
