"""
DAG: Market Aggregation

raw/mart 데이터를 기반으로 시장별 집계 데이터를 생성합니다.
naver_market_summary DAG과 daily_stock_price DAG 이후에 실행됩니다.

Schedule: 매일 오후 6시
"""

from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from sqlalchemy import text as sa_text
from airflow.operators.python import PythonOperator

from common.db import get_engine
from common.quality import check_row_count

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def build_market_summary(**context):
    """Aggregate market-level statistics from dim_stock and fact_daily_price."""
    engine = get_engine()

    query = """
        SELECT
            f.trade_date,
            d.market_type,
            COUNT(DISTINCT f.stock_code) AS total_stocks,
            ROUND(AVG(d.per), 2) AS avg_per,
            ROUND(AVG(d.pbr), 2) AS avg_pbr,
            ROUND(AVG(d.roe), 2) AS avg_roe,
            SUM(f.volume) AS total_volume,
            SUM(d.market_cap) AS total_market_cap
        FROM mart.fact_daily_price f
        JOIN mart.dim_stock d ON f.stock_code = d.stock_code
        WHERE d.market_type IS NOT NULL
        GROUP BY f.trade_date, d.market_type
        ORDER BY f.trade_date DESC
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data available for aggregation")
        return

    df["updated_at"] = datetime.now()

    # Upsert
    with engine.begin() as conn:
        dates = df["trade_date"].unique()
        for date in dates:
            conn.execute(
                sa_text(
                    "DELETE FROM mart.agg_market_summary WHERE trade_date = :dt"
                ),
                {"dt": str(date)},
            )

    df.to_sql(
        "agg_market_summary",
        engine,
        schema="mart",
        if_exists="append",
        index=False,
    )
    print(f"Aggregated {len(df)} market summary records")


def run_quality_checks(**context):
    """Validate aggregation results."""
    engine = get_engine()
    check_row_count(engine, "mart.agg_market_summary", min_rows=1)
    print("Aggregation quality checks completed")


with DAG(
    dag_id="market_aggregation",
    default_args=default_args,
    description="시장별 집계 데이터 생성 (dim_stock + fact_daily_price → agg_market_summary)",
    schedule="0 18 * * 1-5",  # 평일 오후 6시
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transform", "aggregation", "mart"],
) as dag:

    aggregate = PythonOperator(
        task_id="build_market_summary",
        python_callable=build_market_summary,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    aggregate >> quality
