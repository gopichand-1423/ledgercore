package com.gopichand.ledgercore.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gopichand.ledgercore.transfer.TransferEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SettlementConsumerTest {

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private PayoutGatewayClient payoutGatewayClient;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SettlementConsumer settlementConsumer;

    @BeforeEach
    void setUp() {
        settlementConsumer = new SettlementConsumer(inboxRepository, payoutGatewayClient, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("Should process new transfer event via payout gateway and register in inbox")
    void testProcessNewEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID journalId = UUID.randomUUID();
        UUID sourceId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();

        TransferEvent event = new TransferEvent(
                eventId,
                journalId,
                "idemp-1",
                sourceId,
                destId,
                5000L,
                "INR",
                "Settlement test",
                Instant.now()
        );
        String json = objectMapper.writeValueAsString(event);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ledger.transfer.posted", 0, 100L, journalId.toString(), json);

        when(inboxRepository.tryInsertInboxEvent(eq(eventId), any(), eq(SettlementConsumer.CONSUMER_GROUP), eq("LEDGER_TRANSFER_POSTED")))
                .thenReturn(true);

        settlementConsumer.consumeTransferPosted(record);

        verify(payoutGatewayClient).executePayout(any(TransferEvent.class));
        verify(kafkaTemplate, never()).send(eq(SettlementConsumer.TOPIC_DLQ), any(), any());
    }

    @Test
    @DisplayName("Should idempotently skip event if already processed according to inbox table")
    void testSkipDuplicateEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID journalId = UUID.randomUUID();
        UUID sourceId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();

        TransferEvent event = new TransferEvent(
                eventId,
                journalId,
                "idemp-duplicate",
                sourceId,
                destId,
                5000L,
                "INR",
                "Duplicate test",
                Instant.now()
        );
        String json = objectMapper.writeValueAsString(event);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("ledger.transfer.posted", 0, 101L, journalId.toString(), json);

        when(inboxRepository.tryInsertInboxEvent(eq(eventId), any(), eq(SettlementConsumer.CONSUMER_GROUP), eq("LEDGER_TRANSFER_POSTED")))
                .thenReturn(false);

        settlementConsumer.consumeTransferPosted(record);

        verify(payoutGatewayClient, never()).executePayout(any());
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
