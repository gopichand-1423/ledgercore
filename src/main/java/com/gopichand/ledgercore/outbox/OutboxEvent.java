package com.gopichand.ledgercore.outbox;

import java.time.Instant;
import java.util.UUID;

public record OutboxEvent(
    UUID id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String payload,
    boolean published,
    String status,
    int retryCount,
    Instant createdAt,
    Instant publishedAt
) {
}
