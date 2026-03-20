-- ============================================================
-- Korea Market Data Pipeline - Database Schema
-- 3-layer architecture: raw → staging → mart
-- ============================================================

-- ── RAW Layer ────────────────────────────────────────────────
-- Raw data as-is from source systems, append-only with ingestion timestamp

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.naver_market_summary (
    id              SERIAL PRIMARY KEY,
    rank_no         INTEGER,
    stock_name      VARCHAR(100),
    current_price   VARCHAR(20),
    price_change    VARCHAR(20),
    change_rate     VARCHAR(20),
    volume          VARCHAR(30),
    market_cap      VARCHAR(30),
    sales           VARCHAR(30),
    sales_growth    VARCHAR(20),
    operating_profit VARCHAR(30),
    roe             VARCHAR(20),
    per             VARCHAR(20),
    pbr             VARCHAR(20),
    market_type     VARCHAR(10),  -- KOSPI / KOSDAQ
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.daily_price (
    id          SERIAL PRIMARY KEY,
    stock_code  VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,
    open        BIGINT,
    high        BIGINT,
    low         BIGINT,
    close       BIGINT,
    volume      BIGINT,
    source      VARCHAR(20) DEFAULT 'yfinance',
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.sector_info (
    id          SERIAL PRIMARY KEY,
    sector_name VARCHAR(100),
    stock_code  VARCHAR(10),
    stock_name  VARCHAR(100),
    market_type VARCHAR(10),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- ── MART Layer ───────────────────────────────────────────────
-- Cleaned, transformed, business-ready tables

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_stock (
    stock_code      VARCHAR(10) PRIMARY KEY,
    stock_name      VARCHAR(100) NOT NULL,
    market_type     VARCHAR(10),       -- KOSPI / KOSDAQ
    sector          VARCHAR(100),
    roe             NUMERIC(10, 2),
    per             NUMERIC(10, 2),
    pbr             NUMERIC(10, 2),
    market_cap      BIGINT,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.fact_daily_price (
    stock_code  VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,
    open        BIGINT,
    high        BIGINT,
    low         BIGINT,
    close       BIGINT,
    volume      BIGINT,
    change_rate NUMERIC(10, 4),
    PRIMARY KEY (stock_code, trade_date)
);

CREATE TABLE IF NOT EXISTS mart.agg_market_summary (
    trade_date      DATE NOT NULL,
    market_type     VARCHAR(10) NOT NULL,
    total_stocks    INTEGER,
    avg_per         NUMERIC(10, 2),
    avg_pbr         NUMERIC(10, 2),
    avg_roe         NUMERIC(10, 2),
    total_volume    BIGINT,
    total_market_cap BIGINT,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (trade_date, market_type)
);

-- ── Data Quality Log ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.data_quality_log (
    id              SERIAL PRIMARY KEY,
    check_name      VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    check_type      VARCHAR(50),     -- row_count, null_ratio, value_range, freshness
    status          VARCHAR(10),     -- PASS / FAIL / WARN
    expected_value  VARCHAR(100),
    actual_value    VARCHAR(100),
    details         TEXT,
    checked_at      TIMESTAMP DEFAULT NOW()
);

-- ── Indexes ──────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_raw_daily_price_code_date
    ON raw.daily_price (stock_code, trade_date);

CREATE INDEX IF NOT EXISTS idx_mart_fact_daily_price_date
    ON mart.fact_daily_price (trade_date);

CREATE INDEX IF NOT EXISTS idx_data_quality_log_table
    ON public.data_quality_log (table_name, checked_at);
