package com.gopichand.ledgercore.hold;

import com.gopichand.ledgercore.domain.model.HoldStatus;
import com.gopichand.ledgercore.domain.model.PaymentHold;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class HoldRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public HoldRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<PaymentHold> HOLD_ROW_MAPPER = (rs, rowNum) -> new PaymentHold(
            rs.getObject("id", UUID.class),
            rs.getObject("account_id", UUID.class),
            rs.getObject("destination_account_id", UUID.class),
            rs.getLong("amount_paise"),
            HoldStatus.valueOf(rs.getString("status")),
            rs.getString("reference"),
            rs.getObject("journal_entry_id", UUID.class),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    public void insertHold(PaymentHold hold) {
        String sql = """
            INSERT INTO payment_holds (id, account_id, destination_account_id, amount_paise, status, reference, journal_entry_id, created_at, updated_at)
            VALUES (:id, :accountId, :destinationAccountId, :amountPaise, :status, :reference, :journalEntryId, :createdAt, :updatedAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", hold.id())
                .addValue("accountId", hold.accountId())
                .addValue("destinationAccountId", hold.destinationAccountId())
                .addValue("amountPaise", hold.amountPaise())
                .addValue("status", hold.status().name())
                .addValue("reference", hold.reference())
                .addValue("journalEntryId", hold.journalEntryId())
                .addValue("createdAt", Timestamp.from(hold.createdAt()))
                .addValue("updatedAt", Timestamp.from(hold.updatedAt()));
        jdbc.update(sql, params);
    }

    public Optional<PaymentHold> findById(UUID holdId) {
        String sql = "SELECT * FROM payment_holds WHERE id = :id";
        List<PaymentHold> results = jdbc.query(sql, new MapSqlParameterSource("id", holdId), HOLD_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public Optional<PaymentHold> findByIdForUpdate(UUID holdId) {
        String sql = "SELECT * FROM payment_holds WHERE id = :id FOR UPDATE";
        List<PaymentHold> results = jdbc.query(sql, new MapSqlParameterSource("id", holdId), HOLD_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public void updateHoldStatus(UUID holdId, HoldStatus status, UUID journalEntryId) {
        String sql = """
            UPDATE payment_holds 
            SET status = :status, journal_entry_id = :journalEntryId, updated_at = :updatedAt 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", holdId)
                .addValue("status", status.name())
                .addValue("journalEntryId", journalEntryId)
                .addValue("updatedAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }

    public void updateAccountHeldBalance(UUID accountId, long newPendingBalancePaise) {
        String sql = """
            UPDATE accounts 
            SET pending_balance_paise = :pendingBalancePaise, version = version + 1, updated_at = :updatedAt 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", accountId)
                .addValue("pendingBalancePaise", newPendingBalancePaise)
                .addValue("updatedAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }

    public void updateAccountBalances(UUID accountId, long newBalancePaise, long newPendingBalancePaise) {
        String sql = """
            UPDATE accounts 
            SET balance_paise = :balancePaise, pending_balance_paise = :pendingBalancePaise, version = version + 1, updated_at = :updatedAt 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", accountId)
                .addValue("balancePaise", newBalancePaise)
                .addValue("pendingBalancePaise", newPendingBalancePaise)
                .addValue("updatedAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }
}
