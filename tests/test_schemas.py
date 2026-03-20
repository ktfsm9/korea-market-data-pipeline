"""Tests for Pydantic schemas."""

from datetime import date, datetime

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from schemas import DailyPriceResponse, StockResponse


class TestStockResponse:
    def test_valid_stock(self):
        stock = StockResponse(
            stock_code="005930",
            stock_name="삼성전자",
            market_type="KOSPI",
            roe=10.5,
            per=12.3,
            pbr=1.5,
            market_cap=400000000,
        )
        assert stock.stock_code == "005930"
        assert stock.stock_name == "삼성전자"
        assert stock.market_type == "KOSPI"

    def test_stock_with_nulls(self):
        stock = StockResponse(
            stock_code="999999",
            stock_name="테스트종목",
        )
        assert stock.roe is None
        assert stock.per is None
        assert stock.market_type is None


class TestDailyPriceResponse:
    def test_valid_price(self):
        price = DailyPriceResponse(
            stock_code="005930",
            trade_date=date(2024, 1, 15),
            open=73000,
            high=74000,
            low=72500,
            close=73500,
            volume=15000000,
            change_rate=0.0068,
        )
        assert price.close == 73500
        assert price.trade_date == date(2024, 1, 15)

    def test_price_with_nulls(self):
        price = DailyPriceResponse(
            stock_code="005930",
            trade_date=date(2024, 1, 15),
        )
        assert price.open is None
        assert price.change_rate is None
