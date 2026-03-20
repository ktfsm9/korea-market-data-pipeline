"""Tests for Pydantic schemas."""

from datetime import date, datetime

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from schemas import DailyPriceResponse, MarketSummaryResponse, DataQualityResponse, StockResponse


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

    def test_stock_all_fields(self):
        now = datetime.now()
        stock = StockResponse(
            stock_code="005930",
            stock_name="삼성전자",
            market_type="KOSPI",
            sector="반도체",
            roe=10.5,
            per=12.3,
            pbr=1.5,
            market_cap=400000000,
            updated_at=now,
        )
        assert stock.sector == "반도체"
        assert stock.updated_at == now

    def test_stock_requires_code_and_name(self):
        with pytest.raises(Exception):
            StockResponse(stock_code="005930")
        with pytest.raises(Exception):
            StockResponse(stock_name="삼성전자")


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

    def test_price_requires_code_and_date(self):
        with pytest.raises(Exception):
            DailyPriceResponse(stock_code="005930")
        with pytest.raises(Exception):
            DailyPriceResponse(trade_date=date(2024, 1, 15))

    def test_negative_change_rate(self):
        price = DailyPriceResponse(
            stock_code="005930",
            trade_date=date(2024, 1, 15),
            change_rate=-0.05,
        )
        assert price.change_rate == -0.05


class TestMarketSummaryResponse:
    def test_valid_summary(self):
        summary = MarketSummaryResponse(
            trade_date=date(2024, 1, 15),
            market_type="KOSPI",
            total_stocks=900,
            avg_per=12.5,
            avg_pbr=1.1,
            avg_roe=8.3,
            total_volume=500000000,
            total_market_cap=2000000000000,
        )
        assert summary.market_type == "KOSPI"
        assert summary.total_stocks == 900

    def test_summary_with_nulls(self):
        summary = MarketSummaryResponse(
            trade_date=date(2024, 1, 15),
            market_type="KOSDAQ",
        )
        assert summary.total_stocks is None
        assert summary.avg_per is None
        assert summary.updated_at is None

    def test_summary_requires_date_and_market(self):
        with pytest.raises(Exception):
            MarketSummaryResponse(trade_date=date(2024, 1, 15))
        with pytest.raises(Exception):
            MarketSummaryResponse(market_type="KOSPI")


class TestDataQualityResponse:
    def test_valid_quality_log(self):
        now = datetime.now()
        log = DataQualityResponse(
            id=1,
            check_name="raw.daily_price_row_count",
            table_name="raw.daily_price",
            check_type="row_count",
            status="PASS",
            expected_value=">= 10",
            actual_value="150",
            checked_at=now,
        )
        assert log.status == "PASS"
        assert log.table_name == "raw.daily_price"

    def test_quality_log_with_nulls(self):
        log = DataQualityResponse(
            id=1,
            check_name="test_check",
            table_name="test_table",
        )
        assert log.status is None
        assert log.details is None

    def test_quality_log_requires_id_name_table(self):
        with pytest.raises(Exception):
            DataQualityResponse(check_name="test", table_name="test")
        with pytest.raises(Exception):
            DataQualityResponse(id=1, table_name="test")
