-- ============================================================================
-- SWAM Real-Time Payments PoC — Business Analytics Queries
-- ============================================================================
-- Run against: postgres-datamart / database "datamart"
--   minikube kubectl -- exec -i -n kafka-connect <postgres-datamart-pod> \
--       -- psql -U hps -d datamart < sql/analytics.sql
--
-- All queries read from the star schema populated by gold-sink:
--   fact_transactions  ->  dim_risk, dim_canal, dim_banque, dim_date
--
-- RETIRED / FROZEN: this star schema is superseded by the flat
-- gold_transactions table (see gold_transactions_schema.sql), fed by the
-- gold-transactions-sink Kafka Connect JDBC connector. gold-sink (which fed
-- this schema) is paused, not deleted — these queries are kept for
-- reference/rollback and will return stale results.
-- ============================================================================


-- ── 1. Transaction count, total and average amount per risk score ───────────
SELECT
    dr.risk_score,
    COUNT(*)                   AS transaction_count,
    SUM(ft.transaction_amount) AS total_amount,
    AVG(ft.transaction_amount) AS avg_amount
FROM fact_transactions ft
JOIN dim_risk dr ON dr.risk_id = ft.risk_id
GROUP BY dr.risk_score
ORDER BY dr.risk_score;


-- ── 2. Transaction count and total amount per payment channel ───────────────
SELECT
    dc.payment_channel,
    COUNT(*)                   AS transaction_count,
    SUM(ft.transaction_amount) AS total_amount,
    AVG(ft.transaction_amount) AS avg_amount
FROM fact_transactions ft
JOIN dim_canal dc ON dc.canal_id = ft.canal_id
GROUP BY dc.payment_channel
ORDER BY transaction_count DESC;


-- ── 3. Issuing banks ranked by total transaction amount (descending) ────────
SELECT
    db.issuing_bank,
    COUNT(*)                   AS transaction_count,
    SUM(ft.transaction_amount) AS total_amount
FROM fact_transactions ft
JOIN dim_banque db ON db.banque_id = ft.banque_id
GROUP BY db.issuing_bank
ORDER BY total_amount DESC;


-- ── 4. Issuing banks with the most HIGH-risk transactions ────────────────────
SELECT
    db.issuing_bank,
    COUNT(*) AS high_risk_count
FROM fact_transactions ft
JOIN dim_banque db ON db.banque_id = ft.banque_id
JOIN dim_risk   dr ON dr.risk_id   = ft.risk_id
WHERE dr.risk_score = 'HIGH'
GROUP BY db.issuing_bank
ORDER BY high_risk_count DESC;


-- ── 5. Risk score distribution as a percentage of all transactions ──────────
SELECT
    dr.risk_score,
    COUNT(*) AS transaction_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2
    ) AS pct_of_total
FROM fact_transactions ft
JOIN dim_risk dr ON dr.risk_id = ft.risk_id
GROUP BY dr.risk_score
ORDER BY pct_of_total DESC;


-- ── 6. Pre-built risk summary view ───────────────────────────────────────────
SELECT * FROM v_risk_summary;
