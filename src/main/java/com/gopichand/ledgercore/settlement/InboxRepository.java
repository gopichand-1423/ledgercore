package com.gopichand.ledgercore.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class InboxRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public InboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Atomically records an event into inbox_processed_events using ON CONFLICT DO NOTHING.
     * Returns true if newly inserted (not processed before); false if duplicate.
     */
    public boolean tryInsertInboxEvent(UUID eventId, String messageId, String consumerGroup, String eventType) {
        String sql = """
            INSERT INTO inbox_processed_events (id, event_id, message_id, consumer_group, event_type, processed_at)
            VALUES (gen_random_uuid(), :eventId, :messageId, :consumerGroup, :eventType, :processedAt)
            ON CONFLICT (event_id, consumer_group) DO NOTHING
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("eventId", eventId)
                .addValue("messageId", messageId)
                .addValue("consumerGroup", consumerGroup)
                .addValue("eventType", eventType)
                .addValue("processedAt", Timestamp.from(Instant.now()));
        int updated = jdbc.update(sql, params);
        return updated > 0;
    }

    public boolean isEventProcessed(UUID eventId, String consumerGroup) {
        String sql = "SELECT COUNT(*) FROM inbox_processed_events WHERE event_id = :eventId AND consumer_group = :consumerGroup";
        Long count = jdbc.queryForObject(sql, new MapSqlParameterSource("eventId", eventId).addValue("consumerGroup", consumerGroup), Long.class);
        return count != null && count > 0;
    }

    public void removeInboxEvent(UUID eventId, String consumerGroup) {
        String sql = "DELETE FROM inbox_processed_events WHERE event_id = :eventId AND consumer_group = :consumerGroup";
        jdbc.update(sql, new MapSqlParameterSource("eventId", eventId).addValue("consumerGroup", consumerGroup));
    }
}
