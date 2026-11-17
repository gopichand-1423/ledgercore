package com.gopichand.ledgercore.settlement;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record DLQReplayRequest(
    @NotNull(message = "eventId must not be null")
    UUID eventId,

    @NotBlank(message = "payload must not be blank")
    String payload
) {
}
