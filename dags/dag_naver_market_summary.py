"""
DAG: Naver Finance Market Summary Crawler

네이버 금융에서 KOSPI/KOSDAQ 전 종목의 시가총액, ROE, PER 등
재무 데이터를 크롤링하여 raw 테이블에 적재하고 mart 테이블로 변환합니다.

Schedule: 매일 오후 4시 (장 마감 후)
"""

from datetime import datetime, timedelta

import re

import numpy as np
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup

from common.db import get_engine
from common.quality import check_null_ratio, check_row_count

BASE_URL = "https://finance.naver.com/sise/sise_market_sum.nhn?sosok="
MARKET_CODES = {"0": "KOSPI", "1": "KOSDAQ"}

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_total_pages(market_code: str) -> int:
    """Get total number of pages for a market."""
    res = requests.get(f"{BASE_URL}{market_code}", timeout=30)
    soup = BeautifulSoup(res.text, "lxml")
    last_page_tag = soup.select_one("td.pgRR > a")
    if last_page_tag:
        return int(last_page_tag.get("href").split("=")[-1])
    return 1


def _get_field_ids(market_code: str) -> list[str]:
    """Get available field IDs from Naver Finance."""
    res = requests.get(f"{BASE_URL}{market_code}", timeout=30)
    soup = BeautifulSoup(res.text, "lxml")
    ipt_html = soup.select_one("div.subcnt_sise_item_top")
    return [item.get("value") for item in ipt_html.select("input")]


def _crawl_page(market_code: str, page: int, fields: list[str]) -> pd.DataFrame:
    """Crawl a single page of market summary data."""
    data = {
        "menu": "market_sum",
        "fieldIds": fields,
        "returnUrl": f"{BASE_URL}{market_code}&page={page}",
    }
    res = requests.post(
        "https://finance.naver.com/sise/field_submit.nhn",
        data=data,
        timeout=30,
    )
    soup = BeautifulSoup(res.text, "lxml")
    table_html = soup.select_one("div.box_type_l")

    if not table_html:
        return pd.DataFrame()

    headers = [
        item.get_text().strip() for item in table_html.select("thead th")
    ][1:-1]

    # Extract stock codes from <a class="tltle" href="/item/main.naver?code=XXXXXX">
    stock_codes = []
    for a_tag in table_html.find_all("a", class_="tltle"):
        href = a_tag.get("href", "")
        match = re.search(r"code=(\d+)", href)
        if match:
            stock_codes.append(match.group(1))

    inner_data = [
        item.get_text().strip()
        for item in table_html.find_all(
            lambda x: (x.name == "a" and "tltle" in x.get("class", []))
            or (x.name == "td" and "number" in x.get("class", []))
        )
    ]

    no_data = [item.get_text().strip() for item in table_html.select("td.no")]
    arr = np.array(inner_data)
    arr.resize(len(no_data), len(headers))

    df = pd.DataFrame(data=arr, columns=headers)

    # Add stock_code column from extracted href codes
    if stock_codes and len(stock_codes) == len(df):
        df["stock_code"] = stock_codes
    else:
        # Fallback: use row index (should not happen with valid HTML)
        df["stock_code"] = df.index.astype(str).str.zfill(6)

    return df


def extract_naver_market(**context):
    """Extract: Crawl all KOSPI/KOSDAQ stocks from Naver Finance."""
    engine = get_engine()
    total_inserted = 0

    for code, market_name in MARKET_CODES.items():
        total_pages = _get_total_pages(code)
        fields = _get_field_ids(code)

        frames = []
        for page in range(1, total_pages + 1):
            df = _crawl_page(code, page, fields)
            if not df.empty:
                frames.append(df)

        if not frames:
            continue

        df_market = pd.concat(frames, ignore_index=True)
        df_market["market_type"] = market_name

        # Map Korean column names to English for raw table
        column_map = {
            "종목명": "stock_name",
            "현재가": "current_price",
            "전일비": "price_change",
            "등락률": "change_rate",
            "거래량": "volume",
            "시가총액": "market_cap",
            "매출액": "sales",
            "매출액증가율": "sales_growth",
            "영업이익": "operating_profit",
            "ROE": "roe",
            "PER": "per",
            "PBR": "pbr",
        }

        df_raw = df_market.rename(columns=column_map)
        # Keep only columns that exist in our raw table
        valid_cols = [c for c in column_map.values() if c in df_raw.columns]
        valid_cols.append("market_type")
        # Include stock_code from crawled data
        if "stock_code" in df_raw.columns:
            valid_cols.append("stock_code")
        df_raw = df_raw[valid_cols]

        df_raw.to_sql(
            "naver_market_summary",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        total_inserted += len(df_raw)

    context["ti"].xcom_push(key="rows_inserted", value=total_inserted)
    print(f"Extracted {total_inserted} rows from Naver Finance")


def transform_to_dim_stock(**context):
    """Transform: Build dim_stock from latest raw data."""
    engine = get_engine()

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (stock_code)
                stock_code, stock_name, market_type, roe, per, pbr, market_cap, ingested_at
            FROM raw.naver_market_summary
            WHERE stock_code IS NOT NULL
            ORDER BY stock_code, ingested_at DESC
        )
        SELECT * FROM latest
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data to transform")
        return

    # Clean numeric columns
    for col in ["roe", "per", "pbr", "market_cap"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("N/A", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["updated_at"] = datetime.now()

    result = df[["stock_code", "stock_name", "market_type", "roe", "per", "pbr", "market_cap", "updated_at"]]

    # Upsert to mart.dim_stock
    result.to_sql(
        "dim_stock",
        engine,
        schema="mart",
        if_exists="replace",
        index=False,
    )
    print(f"Transformed {len(result)} stocks to mart.dim_stock")


def run_quality_checks(**context):
    """Run data quality checks after pipeline completion."""
    engine = get_engine()

    check_row_count(engine, "raw.naver_market_summary", min_rows=100)
    check_null_ratio(engine, "raw.naver_market_summary", "stock_name")
    check_null_ratio(engine, "raw.naver_market_summary", "market_type")

    print("Data quality checks completed")


with DAG(
    dag_id="naver_market_summary",
    default_args=default_args,
    description="네이버 금융 시가총액 데이터 크롤링 → raw 적재 → mart 변환",
    schedule="0 16 * * 1-5",  # 평일 오후 4시
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["extract", "naver", "market"],
) as dag:

    extract = PythonOperator(
        task_id="extract_naver_market",
        python_callable=extract_naver_market,
    )

    transform = PythonOperator(
        task_id="transform_to_dim_stock",
        python_callable=transform_to_dim_stock,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    extract >> transform >> quality
