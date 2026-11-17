package com.gopichand.ledgercore.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.transfer.TransferEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class SettlementConsumer {

    private static final Logger log = LoggerFactory.getLogger(SettlementConsumer.class);
    public static final String CONSUMER_GROUP = "ledgercore-settlement-group";
    public static final String TOPIC_DLQ = "ledger.transfer.dlq";

    private final InboxRepository inboxRepository;
    private final PayoutGatewayClient payoutGatewayClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public SettlementConsumer(
            InboxRepository inboxRepository,
            PayoutGatewayClient payoutGatewayClient,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.inboxRepository = inboxRepository;
        this.payoutGatewayClient = payoutGatewayClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "ledger.transfer.posted", groupId = CONSUMER_GROUP)
    public void consumeTransferPosted(ConsumerRecord<String, String> record) {
        String payload = record.value();
        TransferEvent event;
        try {
            event = objectMapper.readValue(payload, TransferEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse Kafka record to TransferEvent: payload={}", payload, e);
            sendToDLQ(record.key(), payload, "JSON parsing error: " + e.getMessage());
            return;
        }

        UUID eventId = event.eventId();
        String messageId = record.topic() + "-" + record.partition() + "-" + record.offset();

        // 1. Inbox pattern deduplication check and insert
        boolean isNew = inboxRepository.tryInsertInboxEvent(eventId, messageId, CONSUMER_GROUP, "LEDGER_TRANSFER_POSTED");
        if (!isNew) {
            log.info("Duplicate event {} detected by consumer group {}. Idempotently skipped.",
                    eventId, CONSUMER_GROUP);
            return;
        }

        // 2. Invoke simulated banking/UPI payout gateway
        try {
            payoutGatewayClient.executePayout(event);
        } catch (Exception e) {
            log.error("Payout failed for event {} after all retries. Routing to DLQ: {}", eventId, e.getMessage());
            // Clear inbox lock so that subsequent admin replay can re-execute settlement
            inboxRepository.removeInboxEvent(eventId, CONSUMER_GROUP);
            sendToDLQ(event.journalEntryId().toString(), payload, e.getMessage());
        }
    }

    public void sendToDLQ(String key, String payload, String reason) {
        try {
            log.warn("Forwarding message to Dead-Letter Queue {} (key: {}): reason={}", TOPIC_DLQ, key, reason);
            kafkaTemplate.send(TOPIC_DLQ, key, payload);
        } catch (Exception e) {
            log.error("Failed to publish to DLQ topic {}: {}", TOPIC_DLQ, e.getMessage(), e);
        }
    }
}
