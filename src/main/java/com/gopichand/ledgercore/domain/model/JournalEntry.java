package com.gopichand.ledgercore.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record JournalEntry(
    UUID id,
    String idempotencyKey,
    String payloadHash,
    EntryType entryType,
    EntryStatus status,
    String description,
    UUID referenceId,
    UUID reversesEntryId,
    Instant postedAt,
    Instant createdAt
) {
    public JournalEntry {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(entryType, "entryType must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    public JournalEntry(UUID id, String idempotencyKey, String payloadHash, EntryType entryType, EntryStatus status, String description, UUID referenceId, Instant postedAt, Instant createdAt) {
        this(id, idempotencyKey, payloadHash, entryType, status, description, referenceId, null, postedAt, createdAt);
    }
}
