package com.gopichand.ledgercore.outbox;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<OutboxEvent> OUTBOX_ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getObject("id", UUID.class),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getString("event_type"),
            rs.getString("payload"),
            rs.getBoolean("published"),
            rs.getString("status"),
            rs.getInt("retry_count"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("published_at") != null ? rs.getTimestamp("published_at").toInstant() : null
    );

    public List<OutboxEvent> findUnpublishedForUpdate(int limit) {
        String sql = """
            SELECT * FROM outbox_events 
            WHERE published = FALSE 
            ORDER BY created_at 
            LIMIT :limit 
            FOR UPDATE SKIP LOCKED
            """;
        return jdbc.query(sql, new MapSqlParameterSource("limit", limit), OUTBOX_ROW_MAPPER);
    }

    public void markPublished(UUID id) {
        String sql = """
            UPDATE outbox_events 
            SET published = TRUE, status = 'PUBLISHED', published_at = :publishedAt 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("publishedAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }

    public void recordFailure(UUID id, String errorMessage) {
        String sql = """
            UPDATE outbox_events 
            SET retry_count = retry_count + 1, error_message = :errorMessage 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("errorMessage", errorMessage);
        jdbc.update(sql, params);
    }
}
