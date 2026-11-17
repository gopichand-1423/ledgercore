-- LedgerCore Initial Schema Migration
-- Flyway V1: Core Double-Entry Ledger, Idempotency, Transactional Outbox, and Inbox

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. ACCOUNTS TABLE
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID GENERATED ALWAYS AS (id) STORED,
    account_number VARCHAR(64) NOT NULL UNIQUE,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE')),
    currency CHAR(3) NOT NULL,
    balance_paise BIGINT NOT NULL DEFAULT 0,
    pending_balance_paise BIGINT NOT NULL DEFAULT 0,
    held_balance BIGINT GENERATED ALWAYS AS (pending_balance_paise) STORED,
    available_balance BIGINT GENERATED ALWAYS AS (balance_paise - pending_balance_paise) STORED,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_positive_balance CHECK (account_type != 'ASSET' OR balance_paise >= 0)
);

CREATE INDEX idx_accounts_account_id ON accounts (account_id);
CREATE INDEX idx_accounts_status ON accounts (status);
CREATE INDEX idx_accounts_currency ON accounts (currency);

-- 2. JOURNAL ENTRIES TABLE
CREATE TABLE journal_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    payload_hash VARCHAR(64),
    entry_type VARCHAR(32) NOT NULL CHECK (entry_type IN ('TRANSFER', 'CLEARING', 'SETTLEMENT', 'FEE', 'REVERSAL', 'HOLD', 'CAPTURE')),
    status VARCHAR(20) NOT NULL DEFAULT 'POSTED' CHECK (status IN ('PENDING', 'POSTED', 'COMMITTED', 'REVERSED')),
    description TEXT,
    reference_id UUID REFERENCES journal_entries(id),
    reverses_entry_id UUID REFERENCES journal_entries(id),
    posted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_journal_entries_idempotency ON journal_entries (idempotency_key);
CREATE INDEX idx_journal_entries_reference_id ON journal_entries (reference_id) WHERE reference_id IS NOT NULL;
CREATE INDEX idx_journal_entries_reverses_id ON journal_entries (reverses_entry_id) WHERE reverses_entry_id IS NOT NULL;
CREATE INDEX idx_journal_entries_posted_at ON journal_entries (posted_at DESC);

-- 3. LEDGER LINES TABLE (Individual Debit and Credit Legs)
CREATE TABLE ledger_lines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE RESTRICT,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    direction VARCHAR(6) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_paise BIGINT NOT NULL CHECK (amount_paise > 0),
    balance_after_paise BIGINT NOT NULL,
    sequence_number INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ledger_lines_entry_id ON ledger_lines (entry_id);
CREATE INDEX idx_ledger_lines_account_created ON ledger_lines (account_id, created_at DESC) 
    INCLUDE (direction, amount_paise, balance_after_paise);

-- 4. OUTBOX EVENTS TABLE (Transactional Outbox Pattern for Kafka Streaming)
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED', 'PROCESSED', 'FAILED')),
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_events_unpublished ON outbox_events (created_at ASC) WHERE published = FALSE;
CREATE INDEX idx_outbox_events_pending ON outbox_events (created_at ASC) WHERE status = 'PENDING';

-- 5. INBOX PROCESSED EVENTS TABLE (Idempotent Consumer Deduplication)
CREATE TABLE inbox_processed_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    message_id VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_inbox_event UNIQUE (event_id, consumer_group)
);

CREATE INDEX idx_inbox_events_event_id ON inbox_processed_events (event_id);
CREATE INDEX idx_inbox_events_lookup ON inbox_processed_events (message_id, consumer_group);

-- 6. PAYMENT HOLDS TABLE (Two-Phase Authorization & Capture Engine)
CREATE TABLE payment_holds (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    destination_account_id UUID REFERENCES accounts(id) ON DELETE RESTRICT,
    amount_paise BIGINT NOT NULL CHECK (amount_paise > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'HELD' CHECK (status IN ('HELD', 'CAPTURED', 'VOIDED')),
    reference VARCHAR(256),
    journal_entry_id UUID REFERENCES journal_entries(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payment_holds_account_id ON payment_holds (account_id);
CREATE INDEX idx_payment_holds_status ON payment_holds (status);

-- ============================================================================
-- DOUBLE-ENTRY CONSTRAINT TRIGGER FUNCTION
-- Rejects any transaction where SUM(DEBIT) != SUM(CREDIT) for an entry_id.
-- Configured as DEFERRABLE INITIALLY DEFERRED so all lines can be inserted
-- before validating the balanced ledger invariant upon COMMIT.
-- ============================================================================
CREATE OR REPLACE FUNCTION fn_verify_double_entry_balance()
RETURNS TRIGGER AS $$
DECLARE
    v_entry_id UUID;
    v_debit_sum BIGINT;
    v_credit_sum BIGINT;
    v_line_count INT;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        v_entry_id := OLD.entry_id;
    ELSE
        v_entry_id := NEW.entry_id;
    END IF;

    -- Aggregate debits and credits for this entry_id
    SELECT 
        COUNT(*),
        COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_paise ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_paise ELSE 0 END), 0)
    INTO v_line_count, v_debit_sum, v_credit_sum
    FROM ledger_lines
    WHERE entry_id = v_entry_id;

    -- If lines exist, enforce double-entry invariants
    IF v_line_count > 0 THEN
        IF v_line_count < 2 THEN
            RAISE EXCEPTION 'Double-entry violation: entry_id % has only % line(s); minimum 2 required.',
                v_entry_id, v_line_count
                USING ERRCODE = 'check_violation';
        END IF;

        IF v_debit_sum <> v_credit_sum THEN
            RAISE EXCEPTION 'Double-entry balance violation: entry_id % total debits (% paise) != total credits (% paise).',
                v_entry_id, v_debit_sum, v_credit_sum
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_verify_double_entry
AFTER INSERT OR UPDATE OR DELETE ON ledger_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION fn_verify_double_entry_balance();
