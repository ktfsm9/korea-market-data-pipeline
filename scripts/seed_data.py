"""
Seed script for populating mart tables with realistic Korean stock market data.

Use this when external crawling (Naver Finance) and market data APIs (yfinance)
are unavailable, or for local development/testing.

Usage:
    POSTGRES_HOST=localhost python scripts/seed_data.py
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    user = os.getenv("POSTGRES_USER", "pipeline")
    password = os.getenv("POSTGRES_PASSWORD", "pipeline123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "market_data")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


STOCKS = [
    # KOSPI
    ("005930", "삼성전자", "KOSPI", 9.53, 12.45, 1.35, 410_000_000_000_000),
    ("000660", "SK하이닉스", "KOSPI", 4.12, 28.90, 2.10, 120_000_000_000_000),
    ("035420", "NAVER", "KOSPI", 10.21, 25.30, 1.85, 42_000_000_000_000),
    ("035720", "카카오", "KOSPI", 3.45, 45.20, 1.92, 20_000_000_000_000),
    ("051910", "LG화학", "KOSPI", 5.67, 18.40, 1.15, 28_000_000_000_000),
    ("006400", "삼성SDI", "KOSPI", 6.23, 22.10, 1.68, 25_000_000_000_000),
    ("068270", "셀트리온", "KOSPI", 12.34, 30.50, 3.45, 22_000_000_000_000),
    ("028260", "삼성물산", "KOSPI", 4.56, 15.20, 0.85, 18_000_000_000_000),
    ("105560", "KB금융", "KOSPI", 8.90, 6.80, 0.52, 25_000_000_000_000),
    ("055550", "신한지주", "KOSPI", 7.65, 5.90, 0.45, 20_000_000_000_000),
    ("003670", "포스코퓨처엠", "KOSPI", 2.10, 85.30, 4.20, 12_000_000_000_000),
    ("207940", "삼성바이오로직스", "KOSPI", 5.40, 78.90, 8.50, 55_000_000_000_000),
    ("373220", "LG에너지솔루션", "KOSPI", 3.20, 120.50, 5.60, 90_000_000_000_000),
    ("000270", "기아", "KOSPI", 15.60, 5.20, 0.78, 35_000_000_000_000),
    ("012330", "현대모비스", "KOSPI", 6.80, 8.90, 0.65, 22_000_000_000_000),
    ("066570", "LG전자", "KOSPI", 5.90, 12.30, 0.95, 15_000_000_000_000),
    ("034730", "SK", "KOSPI", 4.50, 9.80, 0.72, 14_000_000_000_000),
    ("003550", "LG", "KOSPI", 7.20, 10.50, 0.88, 12_000_000_000_000),
    ("096770", "SK이노베이션", "KOSPI", 3.80, 15.60, 0.92, 10_000_000_000_000),
    ("069500", "KODEX 200", "KOSPI", None, None, None, 8_000_000_000_000),
    # KOSDAQ
    ("247540", "에코프로비엠", "KOSDAQ", 2.50, 180.20, 12.30, 15_000_000_000_000),
    ("091990", "셀트리온헬스케어", "KOSDAQ", 8.90, 35.40, 2.80, 10_000_000_000_000),
    ("086520", "에코프로", "KOSDAQ", 1.80, 250.30, 15.60, 12_000_000_000_000),
    ("196170", "알테오젠", "KOSDAQ", 15.20, 42.10, 8.90, 8_000_000_000_000),
    ("041510", "SM", "KOSDAQ", 12.30, 18.50, 3.20, 3_000_000_000_000),
    ("263750", "펄어비스", "KOSDAQ", 8.50, 22.30, 2.10, 4_000_000_000_000),
    ("293490", "카카오게임즈", "KOSDAQ", 4.20, 35.60, 1.80, 3_500_000_000_000),
    ("328130", "루닛", "KOSDAQ", -5.60, None, 15.20, 5_000_000_000_000),
    ("377300", "카카오페이", "KOSDAQ", 1.20, 120.80, 5.40, 6_000_000_000_000),
    ("352820", "하이브", "KOSDAQ", 9.80, 28.90, 4.50, 9_000_000_000_000),
]

BASE_PRICES = {
    "005930": 72000, "000660": 180000, "035420": 210000, "035720": 52000,
    "051910": 380000, "006400": 350000, "068270": 190000, "028260": 130000,
    "105560": 72000, "055550": 48000, "003670": 250000, "207940": 850000,
    "373220": 380000, "000270": 120000, "012330": 230000, "066570": 95000,
    "034730": 165000, "003550": 82000, "096770": 120000, "069500": 35000,
    "247540": 180000, "091990": 75000, "086520": 95000, "196170": 85000,
    "041510": 95000, "263750": 42000, "293490": 22000, "328130": 65000,
    "377300": 35000, "352820": 220000,
}


def seed_dim_stock(engine):
    """Insert stock master data into mart.dim_stock."""
    df = pd.DataFrame(
        STOCKS,
        columns=["stock_code", "stock_name", "market_type", "roe", "per", "pbr", "market_cap"],
    )
    df["updated_at"] = datetime.now()

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM mart.dim_stock"))

    df.to_sql("dim_stock", engine, schema="mart", if_exists="append", index=False)
    print(f"Seeded {len(df)} stocks into mart.dim_stock")


def seed_fact_daily_price(engine, num_days=10):
    """Generate and insert daily OHLCV price data."""
    np.random.seed(42)
    rows = []
    today = datetime.now()

    for stock_code, base_price in BASE_PRICES.items():
        price = base_price
        for i in range(num_days):
            d = today - timedelta(days=(num_days + 4) - i)
            if d.weekday() >= 5:  # skip weekends
                continue
            change = np.random.normal(0, 0.02)
            price = int(price * (1 + change))
            o = int(price * (1 + np.random.normal(0, 0.005)))
            h = int(max(o, price) * (1 + abs(np.random.normal(0, 0.01))))
            l = int(min(o, price) * (1 - abs(np.random.normal(0, 0.01))))
            vol = int(np.random.lognormal(15, 1))
            rows.append((stock_code, d.date(), o, h, l, price, vol, round(change, 4)))

    df = pd.DataFrame(
        rows,
        columns=["stock_code", "trade_date", "open", "high", "low", "close", "volume", "change_rate"],
    )

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM mart.fact_daily_price"))

    df.to_sql("fact_daily_price", engine, schema="mart", if_exists="append", index=False)
    print(f"Seeded {len(df)} price records into mart.fact_daily_price")


def build_agg_market_summary(engine):
    """Aggregate market summary from dim_stock and fact_daily_price."""
    query = """
        SELECT
            f.trade_date,
            d.market_type,
            COUNT(DISTINCT f.stock_code) AS total_stocks,
            ROUND(AVG(d.per)::numeric, 2) AS avg_per,
            ROUND(AVG(d.pbr)::numeric, 2) AS avg_pbr,
            ROUND(AVG(d.roe)::numeric, 2) AS avg_roe,
            SUM(f.volume) AS total_volume,
            SUM(d.market_cap) AS total_market_cap
        FROM mart.fact_daily_price f
        JOIN mart.dim_stock d ON f.stock_code = d.stock_code
        WHERE d.market_type IS NOT NULL
        GROUP BY f.trade_date, d.market_type
        ORDER BY f.trade_date DESC
    """
    df = pd.read_sql(query, engine)
    df["updated_at"] = datetime.now()

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM mart.agg_market_summary"))

    df.to_sql("agg_market_summary", engine, schema="mart", if_exists="append", index=False)
    print(f"Seeded {len(df)} records into mart.agg_market_summary")


def main():
    engine = get_engine()
    seed_dim_stock(engine)
    seed_fact_daily_price(engine)
    build_agg_market_summary(engine)
    print("Database seeding complete!")


if __name__ == "__main__":
    main()
