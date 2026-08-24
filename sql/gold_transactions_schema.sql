-- ============================================================================
-- gold_transactions  (database: datamart, host: postgres-datamart)
-- ============================================================================
-- Single flat table fed by the real Kafka Connect JDBC Sink connector
-- "gold-transactions-sink" (payments.gold.flat -> gold_transactions).
-- This is the primary, production path from payments.gold into Postgres,
-- replacing the Star Schema / gold-sink path (see datamart_schema.sql,
-- now retired/frozen). No FKs, no dimension tables — one column per
-- field in the flattened record produced by scripts/gold_flattener.py.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold_transactions (
    authorization_code   TEXT PRIMARY KEY,
    message_type         TEXT,
    transaction_amount   NUMERIC(18,2),
    currency_code        TEXT,
    currency_alpha       TEXT,
    issuing_bank         TEXT,
    card_type            TEXT,
    card_scheme          TEXT,
    payment_channel      TEXT,
    risk_score           TEXT,
    mti_name             TEXT,
    mcc_description      TEXT,
    matching_status      TEXT,
    reject_code          TEXT,
    processed_at         TEXT,
    source_system        TEXT,
    pipeline_version     TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
