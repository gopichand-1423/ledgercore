package com.gopichand.ledgercore.reconciliation;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Repository
public class ReconciliationRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ReconciliationRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<ReconciliationDiscrepancy> DISCREPANCY_ROW_MAPPER = (rs, rowNum) -> new ReconciliationDiscrepancy(
            rs.getObject("account_id", UUID.class),
            rs.getString("account_number"),
            rs.getLong("account_balance"),
            rs.getLong("ledger_balance"),
            rs.getLong("drift_paise")
    );

    /**
     * Executes the double-entry accounting audit query.
     * Compares every account's available_balance + held_balance against
     * the historical ledger line aggregation: COALESCE(SUM(CREDIT), 0) - COALESCE(SUM(DEBIT), 0).
     */
    public List<ReconciliationDiscrepancy> findDriftDiscrepancies() {
        String sql = """
            SELECT 
                a.id AS account_id,
                a.account_number,
                (a.available_balance + a.held_balance) AS account_balance,
                (COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_paise ELSE 0 END), 0) - 
                 COALESCE(SUM(CASE WHEN l.direction = 'DEBIT' THEN l.amount_paise ELSE 0 END), 0)) AS ledger_balance,
                ((a.available_balance + a.held_balance) - 
                 (COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_paise ELSE 0 END), 0) - 
                  COALESCE(SUM(CASE WHEN l.direction = 'DEBIT' THEN l.amount_paise ELSE 0 END), 0))) AS drift_paise
            FROM accounts a
            LEFT JOIN ledger_lines l ON a.id = l.account_id
            GROUP BY a.id, a.account_number, a.available_balance, a.held_balance
            HAVING (a.available_balance + a.held_balance) != 
                   (COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_paise ELSE 0 END), 0) - 
                    COALESCE(SUM(CASE WHEN l.direction = 'DEBIT' THEN l.amount_paise ELSE 0 END), 0))
            """;
        return jdbc.query(sql, Collections.emptyMap(), DISCREPANCY_ROW_MAPPER);
    }
}
