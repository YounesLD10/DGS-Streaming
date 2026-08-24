-- ============================================================================
-- PostgreSQL Star Schema Data Mart  (database: datamart, host: postgres-datamart)
-- ============================================================================
-- RETIRED / FROZEN: superseded by gold_transactions (see
-- gold_transactions_schema.sql). Kept for reference/rollback only — the
-- gold-sink Python bridge that fed this schema is paused (0 replicas), not
-- deleted, and these tables no longer receive new data.
--
-- Fully normalized: dimension resolution happens in the gold-sink Python code
-- before the INSERT. The trigger fn_set_dims() has been removed; the Python
-- bridge resolves all FKs via atomic upserts per dimension.
-- ============================================================================

-- ── Dimension: risk ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_risk (
    risk_id    SERIAL PRIMARY KEY,
    risk_score TEXT UNIQUE NOT NULL
);

INSERT INTO dim_risk (risk_score) VALUES
    ('HIGH'), ('MEDIUM'), ('LOW')
ON CONFLICT (risk_score) DO NOTHING;

-- ── Dimension: payment channel ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_canal (
    canal_id        SERIAL PRIMARY KEY,
    payment_channel TEXT UNIQUE NOT NULL
);

INSERT INTO dim_canal (payment_channel) VALUES
    ('SO_CARTE'), ('SO_MOBILE')
ON CONFLICT (payment_channel) DO NOTHING;

-- ── Dimension: issuing bank ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_banque (
    banque_id    SERIAL PRIMARY KEY,
    issuing_bank TEXT UNIQUE NOT NULL
);

-- ── Dimension: date ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INT PRIMARY KEY,        -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year      INT NOT NULL,
    month     INT NOT NULL,
    day       INT NOT NULL,
    quarter   INT NOT NULL,
    day_name  TEXT NOT NULL
);

-- ── Fact table (fully normalized — no denormalized text columns) ─────────────
-- issuing_bank, risk_score, payment_channel have been removed from fact_transactions.
-- They now exist exclusively in their respective dimension tables.
-- Dimension FKs (risk_id, canal_id, banque_id, date_id) are resolved in the
-- gold-sink Python code via atomic upserts before each INSERT.
CREATE TABLE IF NOT EXISTS fact_transactions (
    authorization_code   TEXT PRIMARY KEY,
    message_type         TEXT,
    transaction_amount   NUMERIC(18,2),
    currency_code        TEXT,
    currency_alpha       TEXT,
    card_type            TEXT,
    card_scheme          TEXT,
    mti_name             TEXT,
    mcc_description      TEXT,
    matching_status      TEXT,
    reject_code          TEXT,
    processed_at         TIMESTAMPTZ,
    source_system        TEXT,
    pipeline_version     TEXT,
    risk_id              INT REFERENCES dim_risk(risk_id),
    canal_id             INT REFERENCES dim_canal(canal_id),
    banque_id            INT REFERENCES dim_banque(banque_id),
    date_id              INT REFERENCES dim_date(date_id),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_risk_id   ON fact_transactions (risk_id);
CREATE INDEX IF NOT EXISTS idx_fact_canal_id  ON fact_transactions (canal_id);
CREATE INDEX IF NOT EXISTS idx_fact_banque_id ON fact_transactions (banque_id);
CREATE INDEX IF NOT EXISTS idx_fact_date_id   ON fact_transactions (date_id);

-- Trigger fn_set_dims() removed. Dimension resolution is now handled in
-- the gold-sink Python bridge before INSERT for atomicity and per-row isolation.

-- ── View: risk summary (uses JOIN — no dependency on dropped denormalized columns) ─
CREATE OR REPLACE VIEW v_risk_summary AS
SELECT
    dr.risk_score,
    COUNT(*)                   AS transaction_count,
    SUM(ft.transaction_amount) AS total_amount,
    AVG(ft.transaction_amount) AS avg_amount
FROM fact_transactions ft
JOIN dim_risk dr ON dr.risk_id = ft.risk_id
GROUP BY dr.risk_score
ORDER BY dr.risk_score;
