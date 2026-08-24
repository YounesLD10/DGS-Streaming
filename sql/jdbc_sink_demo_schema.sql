-- ============================================================================
-- JDBC Sink demo table (database: datamart, host: postgres-datamart)
-- ============================================================================
-- Standalone demo proving the Debezium JDBC Sink connector can write
-- successfully against a flattened topic (payments.gold.flat, produced by
-- scripts/gold_flattener.py). Completely isolated from the real Star Schema:
-- no FKs, no dimension tables, no trigger, and NEVER touched by gold-sink.
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_transactions_jdbc_demo (
    authorization_code   TEXT PRIMARY KEY,
    message_type         TEXT,
    transaction_amount   NUMERIC(18,2),
    currency_code        TEXT,
    currency_alpha       TEXT,
    issuing_bank         TEXT,
    card_type            TEXT,
    card_scheme          TEXT,
    payment_channel       TEXT,
    risk_score            TEXT,
    mti_name              TEXT,
    mcc_description       TEXT,
    matching_status       TEXT,
    reject_code           TEXT,
    processed_at          TEXT,
    source_system         TEXT,
    pipeline_version      TEXT
);
