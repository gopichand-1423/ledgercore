package com.gopichand.ledgercore.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxRelay outboxRelay;

    @BeforeEach
    void setUp() {
        outboxRelay = new OutboxRelay(outboxRepository, kafkaTemplate);
    }

    @Test
    @DisplayName("Should successfully publish unpublished outbox events and mark them as published")
    void testRelayUnpublishedEvents() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = new OutboxEvent(
                eventId,
                "TRANSFER",
                "agg-123",
                "LEDGER_TRANSFER_POSTED",
                "{\"amount\":1000}",
                false,
                "PENDING",
                0,
                Instant.now(),
                null
        );

        when(outboxRepository.findUnpublishedForUpdate(50)).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq("ledger.transfer.posted"), eq("agg-123"), eq("{\"amount\":1000}")))
                .thenReturn(future);

        outboxRelay.relayUnpublishedEvents();

        verify(kafkaTemplate).send("ledger.transfer.posted", "agg-123", "{\"amount\":1000}");
        verify(outboxRepository).markPublished(eventId);
    }

    @Test
    @DisplayName("Should do nothing when no unpublished outbox events exist")
    void testNoEventsToRelay() {
        when(outboxRepository.findUnpublishedForUpdate(50)).thenReturn(List.of());

        outboxRelay.relayUnpublishedEvents();

        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
