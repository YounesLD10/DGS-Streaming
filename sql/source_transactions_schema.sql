-- ============================================================================
-- source.transactions  (database: hps_db, host: postgres-hps)
-- ============================================================================
-- OLTP-style source table for the Debezium CDC source connector
-- "debezium-hps-source" (table.include.list=public.transactions,
-- topic.prefix=hps -> topic "hps.public.transactions").
--
-- wal_level=logical and publication "debezium_pub" (FOR ALL TABLES) are
-- already configured on this instance, so creating this table activates
-- the pre-registered connector automatically.
--
-- Columns mirror the transaction fields produced by Job4 (see
-- flink-jobs/job4_optimize.py) so this table can serve as an alternative
-- CDC-based ingestion path alongside the existing Kafka producer.
-- ============================================================================

CREATE TABLE IF NOT EXISTS transactions (
    authorization_code  TEXT PRIMARY KEY,
    message_type        TEXT,
    product_code        TEXT,
    transaction_amount  NUMERIC(18,2),
    transaction_currency TEXT,
    issuing_bank        TEXT,
    card_type           TEXT,
    matching_status     TEXT,
    reject_code         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO transactions (
    authorization_code, message_type, product_code, transaction_amount,
    transaction_currency, issuing_bank, card_type, matching_status, reject_code
) VALUES
    ('AUTH9001', '0200', '6', 250.00,  'MAD', 'BANK_ALPHA', 'VISA',       'U', ''),
    ('AUTH9002', '0200', '1', 15000.00,'MAD', 'BANK_BETA',  'MASTERCARD', 'U', ''),
    ('AUTH9003', '0200', '6', 0.00,    'MAD', 'BANK_GAMMA', 'VISA',       'X', '05')
ON CONFLICT (authorization_code) DO NOTHING;
