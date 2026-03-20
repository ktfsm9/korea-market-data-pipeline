"""Pydantic response models."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class StockResponse(BaseModel):
    stock_code: str
    stock_name: str
    market_type: Optional[str] = None
    sector: Optional[str] = None
    roe: Optional[float] = None
    per: Optional[float] = None
    pbr: Optional[float] = None
    market_cap: Optional[int] = None
    updated_at: Optional[datetime] = None


class DailyPriceResponse(BaseModel):
    stock_code: str
    trade_date: date
    open: Optional[int] = None
    high: Optional[int] = None
    low: Optional[int] = None
    close: Optional[int] = None
    volume: Optional[int] = None
    change_rate: Optional[float] = None


class MarketSummaryResponse(BaseModel):
    trade_date: date
    market_type: str
    total_stocks: Optional[int] = None
    avg_per: Optional[float] = None
    avg_pbr: Optional[float] = None
    avg_roe: Optional[float] = None
    total_volume: Optional[int] = None
    total_market_cap: Optional[int] = None
    updated_at: Optional[datetime] = None


class DataQualityResponse(BaseModel):
    id: int
    check_name: str
    table_name: str
    check_type: Optional[str] = None
    status: Optional[str] = None
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    details: Optional[str] = None
    checked_at: Optional[datetime] = None
