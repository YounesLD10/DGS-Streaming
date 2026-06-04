-- Suppression des tables si elles existent (pour réinitialisation)
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_risk CASCADE;
DROP TABLE IF EXISTS dim_canal CASCADE;
DROP TABLE IF EXISTS dim_banque CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS staging_transactions CASCADE;

-- 1. Création de la Dimension Risque
CREATE TABLE dim_risk (
    risk_id SERIAL PRIMARY KEY,
    pan_masked VARCHAR(19) UNIQUE NOT NULL,
    pan_bin VARCHAR(6) NOT NULL,
    card_scheme VARCHAR(50) NOT NULL,
    luhn_status VARCHAR(5) NOT NULL
);

-- 2. Création de la Dimension Canal
CREATE TABLE dim_canal (
    canal_id SERIAL PRIMARY KEY,
    mti VARCHAR(4) UNIQUE NOT NULL,
    mti_name VARCHAR(100) NOT NULL,
    terminal_id VARCHAR(50) NOT NULL
);

-- 3. Création de la Dimension Banque / Marchand
CREATE TABLE dim_banque (
    banque_id SERIAL PRIMARY KEY,
    mcc VARCHAR(4) NOT NULL,
    mcc_description VARCHAR(255) NOT NULL,
    merchant_name VARCHAR(100) NOT NULL,
    acquirer_bank VARCHAR(100) NOT NULL,
    issuer_bank VARCHAR(100) NOT NULL,
    CONSTRAINT unique_merchant UNIQUE (mcc, merchant_name)
);

-- 4. Création de la Dimension Date
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY, -- Format YYYYMMDD
    full_timestamp TIMESTAMP NOT NULL,
    calendar_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

-- 5. Création de la Table de Fait
CREATE TABLE fact_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    risk_id INT NOT NULL REFERENCES dim_risk(risk_id),
    canal_id INT NOT NULL REFERENCES dim_canal(canal_id),
    banque_id INT NOT NULL REFERENCES dim_banque(banque_id),
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    transaction_amount NUMERIC(18,2) NOT NULL,
    currency_alpha VARCHAR(3) NOT NULL,
    status VARCHAR(50) NOT NULL
);