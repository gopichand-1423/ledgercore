package com.gopichand.ledgercore.settlement;

import java.time.Instant;
import java.util.UUID;

public record DLQReplayResponse(
    String status,
    String message,
    UUID eventId,
    Instant replayedAt
) {
}
