package com.gopichand.ledgercore.account;

import com.gopichand.ledgercore.domain.model.Account;
import com.gopichand.ledgercore.domain.model.AccountStatus;
import com.gopichand.ledgercore.domain.model.AccountType;
import com.gopichand.ledgercore.domain.model.Direction;
import com.gopichand.ledgercore.domain.model.EntryStatus;
import com.gopichand.ledgercore.domain.model.EntryType;
import com.gopichand.ledgercore.domain.model.JournalEntry;
import com.gopichand.ledgercore.domain.model.LedgerLine;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class AccountRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public AccountRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<Account> ACCOUNT_ROW_MAPPER = (rs, rowNum) -> new Account(
            rs.getObject("id", UUID.class),
            rs.getString("account_number"),
            AccountType.valueOf(rs.getString("account_type")),
            rs.getString("currency"),
            rs.getLong("balance_paise"),
            rs.getLong("pending_balance_paise"),
            AccountStatus.valueOf(rs.getString("status")),
            rs.getLong("version"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    private static final RowMapper<LedgerLine> LEDGER_LINE_ROW_MAPPER = (rs, rowNum) -> new LedgerLine(
            rs.getObject("id", UUID.class),
            rs.getObject("entry_id", UUID.class),
            rs.getObject("account_id", UUID.class),
            Direction.valueOf(rs.getString("direction")),
            rs.getLong("amount_paise"),
            rs.getLong("balance_after_paise"),
            rs.getInt("sequence_number"),
            rs.getTimestamp("created_at").toInstant()
    );

    private static final RowMapper<JournalEntry> JOURNAL_ENTRY_ROW_MAPPER = (rs, rowNum) -> new JournalEntry(
            rs.getObject("id", UUID.class),
            rs.getString("idempotency_key"),
            rs.getString("payload_hash"),
            EntryType.valueOf(rs.getString("entry_type")),
            EntryStatus.valueOf(rs.getString("status")),
            rs.getString("description"),
            rs.getObject("reference_id", UUID.class),
            rs.getObject("reverses_entry_id", UUID.class),
            rs.getTimestamp("posted_at").toInstant(),
            rs.getTimestamp("created_at").toInstant()
    );

    public Optional<Account> findById(UUID id) {
        String sql = "SELECT * FROM accounts WHERE id = :id";
        List<Account> results = jdbc.query(sql, new MapSqlParameterSource("id", id), ACCOUNT_ROW_MAPPER);
        return results.stream().findFirst();
    }

    /**
     * Acquires a pessimistic row-level lock on the specified account.
     * Note: Both id and account_id point to the same account identifier.
     */
    public Optional<Account> findByIdForUpdate(UUID accountId) {
        String sql = "SELECT * FROM accounts WHERE id = :id FOR UPDATE";
        List<Account> results = jdbc.query(sql, new MapSqlParameterSource("id", accountId), ACCOUNT_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public Optional<Account> findByAccountNumber(String accountNumber) {
        String sql = "SELECT * FROM accounts WHERE account_number = :accountNumber";
        List<Account> results = jdbc.query(sql, new MapSqlParameterSource("accountNumber", accountNumber), ACCOUNT_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public void insertAccount(Account account) {
        String sql = """
            INSERT INTO accounts (id, account_number, account_type, currency, balance_paise, pending_balance_paise, status, version, created_at, updated_at)
            VALUES (:id, :accountNumber, :accountType, :currency, :balancePaise, :pendingBalancePaise, :status, :version, :createdAt, :updatedAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", account.id())
                .addValue("accountNumber", account.accountNumber())
                .addValue("accountType", account.accountType().name())
                .addValue("currency", account.currency())
                .addValue("balancePaise", account.balancePaise())
                .addValue("pendingBalancePaise", account.pendingBalancePaise())
                .addValue("status", account.status().name())
                .addValue("version", account.version())
                .addValue("createdAt", Timestamp.from(account.createdAt()))
                .addValue("updatedAt", Timestamp.from(account.updatedAt()));
        jdbc.update(sql, params);
    }

    public void updateBalance(UUID accountId, long newBalancePaise) {
        String sql = """
            UPDATE accounts 
            SET balance_paise = :balancePaise, version = version + 1, updated_at = :updatedAt 
            WHERE id = :id
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", accountId)
                .addValue("balancePaise", newBalancePaise)
                .addValue("updatedAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }

    public List<LedgerLine> findLedgerLinesByAccountId(UUID accountId, int offset, int limit) {
        String sql = """
            SELECT * FROM ledger_lines 
            WHERE account_id = :accountId 
            ORDER BY created_at DESC, sequence_number ASC 
            LIMIT :limit OFFSET :offset
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("limit", limit)
                .addValue("offset", offset);
        return jdbc.query(sql, params, LEDGER_LINE_ROW_MAPPER);
    }

    public long countLedgerLinesByAccountId(UUID accountId) {
        String sql = "SELECT COUNT(*) FROM ledger_lines WHERE account_id = :accountId";
        Long count = jdbc.queryForObject(sql, new MapSqlParameterSource("accountId", accountId), Long.class);
        return count != null ? count : 0L;
    }

    public void insertJournalEntry(JournalEntry entry) {
        String sql = """
            INSERT INTO journal_entries (id, idempotency_key, payload_hash, entry_type, status, description, reference_id, reverses_entry_id, posted_at, created_at)
            VALUES (:id, :idempotencyKey, :payloadHash, :entryType, :status, :description, :referenceId, :reversesEntryId, :postedAt, :createdAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", entry.id())
                .addValue("idempotencyKey", entry.idempotencyKey())
                .addValue("payloadHash", entry.payloadHash())
                .addValue("entryType", entry.entryType().name())
                .addValue("status", entry.status().name())
                .addValue("description", entry.description())
                .addValue("referenceId", entry.referenceId())
                .addValue("reversesEntryId", entry.reversesEntryId())
                .addValue("postedAt", Timestamp.from(entry.postedAt()))
                .addValue("createdAt", Timestamp.from(entry.createdAt()));
        jdbc.update(sql, params);
    }

    public Optional<JournalEntry> findJournalEntryById(UUID id) {
        String sql = "SELECT * FROM journal_entries WHERE id = :id";
        List<JournalEntry> results = jdbc.query(sql, new MapSqlParameterSource("id", id), JOURNAL_ENTRY_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public void updateJournalEntryStatus(UUID id, EntryStatus status) {
        String sql = "UPDATE journal_entries SET status = :status WHERE id = :id";
        jdbc.update(sql, new MapSqlParameterSource("id", id).addValue("status", status.name()));
    }

    public void insertLedgerLine(LedgerLine line) {
        String sql = """
            INSERT INTO ledger_lines (id, entry_id, account_id, direction, amount_paise, balance_after_paise, sequence_number, created_at)
            VALUES (:id, :entryId, :accountId, :direction, :amountPaise, :balanceAfterPaise, :sequenceNumber, :createdAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", line.id())
                .addValue("entryId", line.entryId())
                .addValue("accountId", line.accountId())
                .addValue("direction", line.direction().name())
                .addValue("amountPaise", line.amountPaise())
                .addValue("balanceAfterPaise", line.balanceAfterPaise())
                .addValue("sequenceNumber", line.sequenceNumber())
                .addValue("createdAt", Timestamp.from(line.createdAt()));
        jdbc.update(sql, params);
    }

    public void insertOutboxEvent(UUID id, String aggregateType, String aggregateId, String eventType, String payloadJson) {
        String sql = """
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, published, status, retry_count, created_at)
            VALUES (:id, :aggregateType, :aggregateId, :eventType, CAST(:payload AS jsonb), FALSE, 'PENDING', 0, :createdAt)
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("aggregateType", aggregateType)
                .addValue("aggregateId", aggregateId)
                .addValue("eventType", eventType)
                .addValue("payload", payloadJson)
                .addValue("createdAt", Timestamp.from(Instant.now()));
        jdbc.update(sql, params);
    }

    public Optional<JournalEntry> findJournalEntryByIdempotencyKey(String idempotencyKey) {
        String sql = "SELECT * FROM journal_entries WHERE idempotency_key = :idempotencyKey";
        List<JournalEntry> results = jdbc.query(sql, new MapSqlParameterSource("idempotencyKey", idempotencyKey), JOURNAL_ENTRY_ROW_MAPPER);
        return results.stream().findFirst();
    }

    public List<LedgerLine> findLedgerLinesByEntryId(UUID entryId) {
        String sql = "SELECT * FROM ledger_lines WHERE entry_id = :entryId ORDER BY sequence_number ASC";
        return jdbc.query(sql, new MapSqlParameterSource("entryId", entryId), LEDGER_LINE_ROW_MAPPER);
    }
}
