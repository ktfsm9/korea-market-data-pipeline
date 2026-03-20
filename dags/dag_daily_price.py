"""
DAG: Daily Stock Price Collector

yfinance를 사용하여 주요 종목의 OHLCV 일봉 데이터를 수집하고
raw 테이블에 적재한 후 mart 테이블로 변환합니다.

Schedule: 매일 오후 5시 (장 마감 후, 시장 데이터 확정 후)
"""

from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from airflow import DAG
from sqlalchemy import text as sa_text
from airflow.operators.python import PythonOperator

from common.db import get_engine
from common.quality import check_freshness, check_null_ratio, check_row_count

# 주요 ETF 및 대형주 (종목코드 → yfinance 티커)
TARGET_STOCKS = {
    "069500": "069500.KS",  # KODEX 200
    "005930": "005930.KS",  # 삼성전자
    "000660": "000660.KS",  # SK하이닉스
    "035420": "035420.KS",  # NAVER
    "035720": "035720.KS",  # 카카오
    "051910": "051910.KS",  # LG화학
    "006400": "006400.KS",  # 삼성SDI
    "068270": "068270.KS",  # 셀트리온
    "028260": "028260.KS",  # 삼성물산
    "105560": "105560.KS",  # KB금융
    "055550": "055550.KS",  # 신한지주
    "003670": "003670.KS",  # 포스코퓨처엠
    "207940": "207940.KS",  # 삼성바이오로직스
    "373220": "373220.KS",  # LG에너지솔루션
    "000270": "000270.KS",  # 기아
    "012330": "012330.KS",  # 현대모비스
    "066570": "066570.KS",  # LG전자
    "034730": "034730.KS",  # SK
    "003550": "003550.KS",  # LG
    "096770": "096770.KS",  # SK이노베이션
}

default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}


def extract_daily_prices(**context):
    """Extract: Download daily OHLCV data from yfinance."""
    engine = get_engine()
    execution_date = context["ds"]
    total_inserted = 0

    for stock_code, ticker in TARGET_STOCKS.items():
        try:
            df = yf.download(
                tickers=ticker,
                period="5d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )

            if df is None or df.empty:
                print(f"No data for {ticker}")
                continue

            # Handle MultiIndex columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                cols = {}
                for field in ["Open", "High", "Low", "Close", "Volume"]:
                    key = (field, ticker)
                    if key in df.columns:
                        cols[field.lower()] = df[key]
                df_clean = pd.DataFrame(cols)
            else:
                df_clean = df[["Open", "High", "Low", "Close", "Volume"]].copy()
                df_clean.columns = ["open", "high", "low", "close", "volume"]

            df_clean = df_clean.dropna()

            if df_clean.index.tz is not None:
                df_clean.index = df_clean.index.tz_localize(None)

            df_clean["stock_code"] = stock_code
            df_clean["trade_date"] = df_clean.index.date
            df_clean["source"] = "yfinance"

            # Cast to int
            for col in ["open", "high", "low", "close", "volume"]:
                df_clean[col] = df_clean[col].astype(int)

            raw_df = df_clean[
                ["stock_code", "trade_date", "open", "high", "low", "close", "volume", "source"]
            ]

            raw_df.to_sql(
                "daily_price",
                engine,
                schema="raw",
                if_exists="append",
                index=False,
            )
            total_inserted += len(raw_df)

        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    context["ti"].xcom_push(key="rows_inserted", value=total_inserted)
    print(f"Extracted {total_inserted} price records")


def transform_to_fact_daily_price(**context):
    """Transform: Deduplicate and load into mart.fact_daily_price."""
    engine = get_engine()

    query = """
        WITH deduped AS (
            SELECT DISTINCT ON (stock_code, trade_date)
                stock_code, trade_date, open, high, low, close, volume
            FROM raw.daily_price
            ORDER BY stock_code, trade_date, ingested_at DESC
        )
        SELECT
            d.*,
            CASE
                WHEN LAG(d.close) OVER (PARTITION BY d.stock_code ORDER BY d.trade_date) > 0
                THEN ROUND(
                    (d.close::NUMERIC - LAG(d.close) OVER (PARTITION BY d.stock_code ORDER BY d.trade_date))
                    / LAG(d.close) OVER (PARTITION BY d.stock_code ORDER BY d.trade_date),
                    4
                )
                ELSE NULL
            END AS change_rate
        FROM deduped d
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data to transform")
        return

    # Upsert: delete existing then insert
    with engine.begin() as conn:
        dates = df["trade_date"].unique()
        for date in dates:
            conn.execute(
                sa_text(
                    "DELETE FROM mart.fact_daily_price WHERE trade_date = :dt"
                ),
                {"dt": str(date)},
            )

    df.to_sql(
        "fact_daily_price",
        engine,
        schema="mart",
        if_exists="append",
        index=False,
    )
    print(f"Transformed {len(df)} records to mart.fact_daily_price")


def run_quality_checks(**context):
    """Run data quality checks on price data."""
    engine = get_engine()

    check_row_count(engine, "raw.daily_price", min_rows=10)
    check_null_ratio(engine, "raw.daily_price", "close")
    check_freshness(engine, "raw.daily_price", "ingested_at", max_age_hours=48)

    print("Price data quality checks completed")


with DAG(
    dag_id="daily_stock_price",
    default_args=default_args,
    description="yfinance OHLCV 일봉 수집 → raw 적재 → mart 변환",
    schedule="0 17 * * 1-5",  # 평일 오후 5시
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["extract", "yfinance", "price"],
) as dag:

    extract = PythonOperator(
        task_id="extract_daily_prices",
        python_callable=extract_daily_prices,
    )

    transform = PythonOperator(
        task_id="transform_to_fact_daily_price",
        python_callable=transform_to_fact_daily_price,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    extract >> transform >> quality
