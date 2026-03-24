"""
DAG: Naver Finance Market Summary Crawler

네이버 금융에서 KOSPI/KOSDAQ 전 종목의 시가총액, ROE, PER 등
재무 데이터를 크롤링하여 raw 테이블에 적재하고 mart 테이블로 변환합니다.

Schedule: 매일 오후 4시 (장 마감 후)
"""

from datetime import datetime, timedelta

import re
import time

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from sqlalchemy import text as sa_text

from common.db import get_engine
from common.quality import check_null_ratio, check_row_count

BASE_URL = "https://finance.naver.com/sise/sise_market_sum.nhn?sosok="
SECTOR_LIST_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
SECTOR_DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no="
MARKET_CODES = {"0": "KOSPI", "1": "KOSDAQ"}
CRAWL_DELAY = 0.3  # seconds between requests to avoid IP ban
RAW_RETENTION_DAYS = 7  # keep raw.naver_market_summary for this many days

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_total_pages(session: requests.Session, market_code: str) -> int:
    """Get total number of pages for a market."""
    res = session.get(f"{BASE_URL}{market_code}", timeout=30)
    soup = BeautifulSoup(res.text, "lxml")
    last_page_tag = soup.select_one("td.pgRR > a")
    if last_page_tag:
        return int(last_page_tag.get("href").split("=")[-1])
    return 1


def _get_field_ids(session: requests.Session, market_code: str) -> list[str]:
    """Get available field IDs from Naver Finance."""
    res = session.get(f"{BASE_URL}{market_code}", timeout=30)
    soup = BeautifulSoup(res.text, "lxml")
    ipt_html = soup.select_one("div.subcnt_sise_item_top")
    return [item.get("value") for item in ipt_html.select("input")]


def _crawl_page(session: requests.Session, market_code: str, page: int, fields: list[str]) -> pd.DataFrame:
    """Crawl a single page of market summary data."""
    data = {
        "menu": "market_sum",
        "fieldIds": fields,
        "returnUrl": f"{BASE_URL}{market_code}&page={page}",
    }
    time.sleep(CRAWL_DELAY)
    res = session.post(
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

    # Parse row-by-row to keep stock_code aligned with data
    rows = []
    for tr in table_html.select("tbody tr"):
        if not tr.select("td.no"):
            continue

        a_tag = tr.select_one("a.tltle")
        if not a_tag:
            continue

        href = a_tag.get("href", "")
        match = re.search(r"code=(\d+)", href)
        if not match:
            continue
        stock_code = match.group(1)

        # Build cell values: stock name + numeric columns
        cells = [a_tag.get_text().strip()]
        for td in tr.select("td.number"):
            cells.append(td.get_text().strip())

        if len(cells) == len(headers):
            row_data = dict(zip(headers, cells))
            row_data["stock_code"] = stock_code
            rows.append(row_data)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def extract_naver_market(**context):
    """Extract: Crawl all KOSPI/KOSDAQ stocks from Naver Finance."""
    engine = get_engine()
    total_inserted = 0

    # Cleanup old raw data beyond retention window
    with engine.begin() as conn:
        conn.execute(sa_text(
            f"DELETE FROM raw.naver_market_summary "
            f"WHERE ingested_at < NOW() - INTERVAL '{RAW_RETENTION_DAYS} days'"
        ))

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for code, market_name in MARKET_CODES.items():
        total_pages = _get_total_pages(session, code)
        fields = _get_field_ids(session, code)

        frames = []
        for page in range(1, total_pages + 1):
            df = _crawl_page(session, code, page, fields)
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


def extract_sector_info(**context):
    """Extract: Crawl sector (업종) info from Naver Finance."""
    engine = get_engine()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    res = session.get(SECTOR_LIST_URL, timeout=30)
    soup = BeautifulSoup(res.text, "lxml")

    # Collect sector IDs and names from the sector list page
    sectors = []
    for a_tag in soup.select("a[href*='sise_group_detail']"):
        href = a_tag.get("href", "")
        match = re.search(r"no=(\d+)", href)
        if match:
            sectors.append((match.group(1), a_tag.get_text().strip()))

    rows = []
    for sector_id, sector_name in sectors:
        try:
            time.sleep(CRAWL_DELAY)
            detail_res = session.get(
                f"{SECTOR_DETAIL_URL}{sector_id}", timeout=30
            )
            detail_soup = BeautifulSoup(detail_res.text, "lxml")

            for a_tag in detail_soup.find_all("a", href=re.compile(r"/item/main\.naver\?code=\d+")):
                href = a_tag.get("href", "")
                code_match = re.search(r"code=(\d+)", href)
                if code_match and a_tag.get_text().strip():
                    rows.append({
                        "sector_name": sector_name,
                        "stock_code": code_match.group(1),
                        "stock_name": a_tag.get_text().strip(),
                    })
        except Exception as e:
            print(f"Error fetching sector {sector_name}: {e}")

    if not rows:
        print("No sector data extracted")
        return

    df = pd.DataFrame(rows)

    # Truncate + append to preserve schema defined in init.sql
    with engine.begin() as conn:
        conn.execute(sa_text("TRUNCATE TABLE raw.sector_info"))

    df.to_sql(
        "sector_info",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
    )
    print(f"Extracted {len(df)} sector-stock mappings")


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
        ),
        sector AS (
            SELECT DISTINCT ON (stock_code)
                stock_code, sector_name
            FROM raw.sector_info
            ORDER BY stock_code
        )
        SELECT l.*, s.sector_name AS sector
        FROM latest l
        LEFT JOIN sector s ON l.stock_code = s.stock_code
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

    result = df[["stock_code", "stock_name", "market_type", "sector", "roe", "per", "pbr", "market_cap", "updated_at"]]

    # Upsert to mart.dim_stock (truncate + insert to preserve PK/indexes)
    with engine.begin() as conn:
        conn.execute(sa_text("TRUNCATE TABLE mart.dim_stock"))

    result.to_sql(
        "dim_stock",
        engine,
        schema="mart",
        if_exists="append",
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

    extract_sector = PythonOperator(
        task_id="extract_sector_info",
        python_callable=extract_sector_info,
    )

    transform = PythonOperator(
        task_id="transform_to_dim_stock",
        python_callable=transform_to_dim_stock,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    [extract, extract_sector] >> transform >> quality
