"""
Korea Market Data Pipeline - FastAPI Data Serving API

mart 계층의 데이터를 REST API로 제공합니다.
"""

from datetime import date, datetime
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import get_engine as _get_engine
from schemas import (
    DailyPriceResponse,
    DataQualityResponse,
    MarketSummaryResponse,
    StockResponse,
)

app = FastAPI(
    title="Korea Market Data API",
    description="한국 주식 시장 데이터 파이프라인 API",
    version="1.0.0",
)


def get_db() -> Engine:
    return _get_engine()


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/stocks", response_model=list[StockResponse])
def list_stocks(
    market_type: Optional[str] = Query(None, description="KOSPI or KOSDAQ"),
    limit: int = Query(50, ge=1, le=500),
    engine: Engine = Depends(get_db),
):
    """종목 마스터 목록 조회."""
    query = "SELECT * FROM mart.dim_stock WHERE 1=1"
    params = {}

    if market_type:
        query += " AND market_type = :market_type"
        params["market_type"] = market_type

    query += " ORDER BY market_cap DESC NULLS LAST LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = result.mappings().all()

    if not rows:
        return []
    return [StockResponse(**dict(row)) for row in rows]


@app.get("/stocks/{stock_code}", response_model=StockResponse)
def get_stock(stock_code: str, engine: Engine = Depends(get_db)):
    """종목 상세 정보 조회."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM mart.dim_stock WHERE stock_code = :code"),
            {"code": stock_code},
        )
        row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Stock {stock_code} not found")
    return StockResponse(**dict(row))


@app.get("/stocks/{stock_code}/prices", response_model=list[DailyPriceResponse])
def get_stock_prices(
    stock_code: str,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    limit: int = Query(30, ge=1, le=365),
    engine: Engine = Depends(get_db),
):
    """종목 일봉 가격 데이터 조회."""
    query = "SELECT * FROM mart.fact_daily_price WHERE stock_code = :code"
    params: dict = {"code": stock_code}

    if start_date:
        query += " AND trade_date >= :start_date"
        params["start_date"] = start_date
    if end_date:
        query += " AND trade_date <= :end_date"
        params["end_date"] = end_date

    query += " ORDER BY trade_date DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = result.mappings().all()

    return [DailyPriceResponse(**dict(row)) for row in rows]


@app.get("/markets/summary", response_model=list[MarketSummaryResponse])
def get_market_summary(
    market_type: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100),
    engine: Engine = Depends(get_db),
):
    """시장별 집계 데이터 조회."""
    query = "SELECT * FROM mart.agg_market_summary WHERE 1=1"
    params = {}

    if market_type:
        query += " AND market_type = :market_type"
        params["market_type"] = market_type

    query += " ORDER BY trade_date DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = result.mappings().all()

    return [MarketSummaryResponse(**dict(row)) for row in rows]


@app.get("/quality/logs", response_model=list[DataQualityResponse])
def get_quality_logs(
    status: Optional[str] = Query(None, description="PASS, FAIL, or WARN"),
    limit: int = Query(20, ge=1, le=100),
    engine: Engine = Depends(get_db),
):
    """데이터 품질 검사 로그 조회."""
    query = "SELECT * FROM public.data_quality_log WHERE 1=1"
    params = {}

    if status:
        query += " AND status = :status"
        params["status"] = status

    query += " ORDER BY checked_at DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = result.mappings().all()

    return [DataQualityResponse(**dict(row)) for row in rows]
