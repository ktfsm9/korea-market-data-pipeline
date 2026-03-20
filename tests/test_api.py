"""Tests for FastAPI endpoints."""

import os
import sys
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from fastapi.testclient import TestClient
from main import app, get_db


# Mock database engine for all tests
@pytest.fixture
def client():
    mock_engine = MagicMock()
    app.dependency_overrides[get_db] = lambda: mock_engine
    yield TestClient(app), mock_engine
    app.dependency_overrides.clear()


def _mock_query_result(engine, rows):
    """Helper to set up mock query results."""
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    result = MagicMock()
    result.mappings.return_value.all.return_value = rows
    result.mappings.return_value.first.return_value = rows[0] if rows else None
    conn.execute.return_value = result
    return conn


class TestHealthCheck:
    def test_health_returns_ok(self, client):
        test_client, _ = client
        resp = test_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestListStocks:
    def test_list_stocks_returns_data(self, client):
        test_client, engine = client
        rows = [
            {
                "stock_code": "005930", "stock_name": "삼성전자",
                "market_type": "KOSPI", "sector": None,
                "roe": 10.5, "per": 12.3, "pbr": 1.5,
                "market_cap": 400000000, "updated_at": None,
            }
        ]
        _mock_query_result(engine, rows)
        resp = test_client.get("/stocks")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["stock_code"] == "005930"

    def test_list_stocks_empty(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/stocks")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_stocks_with_market_filter(self, client):
        test_client, engine = client
        rows = [
            {
                "stock_code": "005930", "stock_name": "삼성전자",
                "market_type": "KOSPI", "sector": None,
                "roe": 10.5, "per": 12.3, "pbr": 1.5,
                "market_cap": 400000000, "updated_at": None,
            }
        ]
        _mock_query_result(engine, rows)
        resp = test_client.get("/stocks?market_type=KOSPI")
        assert resp.status_code == 200

    def test_list_stocks_with_limit(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/stocks?limit=10")
        assert resp.status_code == 200

    def test_list_stocks_invalid_limit(self, client):
        test_client, _ = client
        resp = test_client.get("/stocks?limit=0")
        assert resp.status_code == 422

        resp = test_client.get("/stocks?limit=1000")
        assert resp.status_code == 422


class TestGetStock:
    def test_get_stock_found(self, client):
        test_client, engine = client
        row = {
            "stock_code": "005930", "stock_name": "삼성전자",
            "market_type": "KOSPI", "sector": None,
            "roe": 10.5, "per": 12.3, "pbr": 1.5,
            "market_cap": 400000000, "updated_at": None,
        }
        _mock_query_result(engine, [row])
        resp = test_client.get("/stocks/005930")
        assert resp.status_code == 200
        assert resp.json()["stock_code"] == "005930"

    def test_get_stock_not_found(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/stocks/999999")
        assert resp.status_code == 404


class TestGetStockPrices:
    def test_get_prices(self, client):
        test_client, engine = client
        rows = [
            {
                "stock_code": "005930", "trade_date": date(2024, 1, 15),
                "open": 73000, "high": 74000, "low": 72500,
                "close": 73500, "volume": 15000000, "change_rate": 0.0068,
            }
        ]
        _mock_query_result(engine, rows)
        resp = test_client.get("/stocks/005930/prices")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["close"] == 73500

    def test_get_prices_with_date_range(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/stocks/005930/prices?start_date=2024-01-01&end_date=2024-01-31")
        assert resp.status_code == 200

    def test_get_prices_empty(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/stocks/005930/prices")
        assert resp.status_code == 200
        assert resp.json() == []


class TestGetMarketSummary:
    def test_get_summary(self, client):
        test_client, engine = client
        rows = [
            {
                "trade_date": date(2024, 1, 15), "market_type": "KOSPI",
                "total_stocks": 900, "avg_per": 12.5, "avg_pbr": 1.1,
                "avg_roe": 8.3, "total_volume": 500000000,
                "total_market_cap": 2000000000000, "updated_at": None,
            }
        ]
        _mock_query_result(engine, rows)
        resp = test_client.get("/markets/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["market_type"] == "KOSPI"

    def test_get_summary_with_filter(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/markets/summary?market_type=KOSDAQ")
        assert resp.status_code == 200


class TestGetQualityLogs:
    def test_get_logs(self, client):
        test_client, engine = client
        rows = [
            {
                "id": 1, "check_name": "row_count",
                "table_name": "raw.daily_price", "check_type": "row_count",
                "status": "PASS", "expected_value": ">= 10",
                "actual_value": "150", "details": None,
                "checked_at": datetime(2024, 1, 15, 12, 0),
            }
        ]
        _mock_query_result(engine, rows)
        resp = test_client.get("/quality/logs")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["status"] == "PASS"

    def test_get_logs_with_status_filter(self, client):
        test_client, engine = client
        _mock_query_result(engine, [])
        resp = test_client.get("/quality/logs?status=FAIL")
        assert resp.status_code == 200
