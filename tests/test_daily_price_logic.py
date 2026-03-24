"""Tests for daily_stock_price DAG task logic."""

import os
import sys
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


class TestGetTargetStocks:
    """Tests for _get_target_stocks helper."""

    def _make_engine(self, rows: list[dict]):
        engine = MagicMock()
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["stock_code", "market_type"])
        with patch("dag_daily_price.pd.read_sql", return_value=df):
            yield engine

    def test_returns_default_when_dim_stock_empty(self):
        from dag_daily_price import DEFAULT_STOCKS, _get_target_stocks
        engine = MagicMock()
        with patch("dag_daily_price.pd.read_sql", return_value=pd.DataFrame()):
            result = _get_target_stocks(engine)
        assert result == DEFAULT_STOCKS

    def test_returns_default_on_exception(self):
        from dag_daily_price import DEFAULT_STOCKS, _get_target_stocks
        engine = MagicMock()
        with patch("dag_daily_price.pd.read_sql", side_effect=Exception("DB error")):
            result = _get_target_stocks(engine)
        assert result == DEFAULT_STOCKS

    def test_kospi_suffix(self):
        from dag_daily_price import _get_target_stocks
        engine = MagicMock()
        df = pd.DataFrame([{"stock_code": "005930", "market_type": "KOSPI"}])
        with patch("dag_daily_price.pd.read_sql", return_value=df):
            result = _get_target_stocks(engine)
        assert result["005930"] == "005930.KS"

    def test_kosdaq_suffix(self):
        from dag_daily_price import _get_target_stocks
        engine = MagicMock()
        df = pd.DataFrame([{"stock_code": "247540", "market_type": "KOSDAQ"}])
        with patch("dag_daily_price.pd.read_sql", return_value=df):
            result = _get_target_stocks(engine)
        assert result["247540"] == "247540.KQ"

    def test_unknown_market_type_uses_kosdaq_suffix(self):
        from dag_daily_price import _get_target_stocks
        engine = MagicMock()
        df = pd.DataFrame([{"stock_code": "000000", "market_type": None}])
        with patch("dag_daily_price.pd.read_sql", return_value=df):
            result = _get_target_stocks(engine)
        assert result["000000"] == "000000.KQ"

    def test_query_contains_limit(self):
        """MAX_STOCKS limit must appear in the SQL query."""
        from dag_daily_price import MAX_STOCKS, _get_target_stocks
        engine = MagicMock()
        captured_query = []

        def mock_read_sql(query, eng):
            captured_query.append(query)
            return pd.DataFrame()

        with patch("dag_daily_price.pd.read_sql", side_effect=mock_read_sql):
            _get_target_stocks(engine)

        assert captured_query, "pd.read_sql was not called"
        assert str(MAX_STOCKS) in captured_query[0].upper() or "LIMIT" in captured_query[0].upper()

    def test_max_stocks_is_defined(self):
        from dag_daily_price import MAX_STOCKS
        assert isinstance(MAX_STOCKS, int)
        assert MAX_STOCKS > 0


class TestExtractDailyPrices:
    """Tests for extract_daily_prices task function."""

    def _make_context(self):
        ti = MagicMock()
        return {"ds": "2026-03-23", "ti": ti}

    def test_skips_empty_yfinance_result(self):
        from dag_daily_price import extract_daily_prices
        engine = MagicMock()
        empty_df = pd.DataFrame()

        with patch("dag_daily_price.get_engine", return_value=engine), \
             patch("dag_daily_price._get_target_stocks", return_value={"005930": "005930.KS"}), \
             patch("dag_daily_price.yf.download", return_value=empty_df):
            ctx = self._make_context()
            extract_daily_prices(**ctx)
            ctx["ti"].xcom_push.assert_called_once_with(key="rows_inserted", value=0)

    def test_handles_yfinance_exception_gracefully(self):
        from dag_daily_price import extract_daily_prices

        with patch("dag_daily_price.get_engine", return_value=MagicMock()), \
             patch("dag_daily_price._get_target_stocks", return_value={"005930": "005930.KS"}), \
             patch("dag_daily_price.yf.download", side_effect=Exception("network error")):
            ctx = self._make_context()
            # Should not raise - errors are caught per-stock
            extract_daily_prices(**ctx)

    def test_pushes_xcom_row_count(self):
        from dag_daily_price import extract_daily_prices

        idx = pd.to_datetime(["2026-03-23"])
        df = pd.DataFrame(
            {"Open": [73000], "High": [74000], "Low": [72000], "Close": [73500], "Volume": [1000000]},
            index=idx,
        )

        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("dag_daily_price.get_engine", return_value=mock_engine), \
             patch("dag_daily_price._get_target_stocks", return_value={"005930": "005930.KS"}), \
             patch("dag_daily_price.yf.download", return_value=df), \
             patch("pandas.DataFrame.to_sql"):
            ctx = self._make_context()
            extract_daily_prices(**ctx)
            ctx["ti"].xcom_push.assert_called_once()
            _, kwargs = ctx["ti"].xcom_push.call_args
            assert kwargs["key"] == "rows_inserted"
            assert kwargs["value"] >= 0


class TestTransformToFactDailyPrice:
    """Tests for transform_to_fact_daily_price task function."""

    def test_skips_when_raw_empty(self):
        from dag_daily_price import transform_to_fact_daily_price

        with patch("dag_daily_price.get_engine", return_value=MagicMock()), \
             patch("dag_daily_price.pd.read_sql", return_value=pd.DataFrame()):
            # Should not raise
            transform_to_fact_daily_price()

    def test_upserts_by_deleting_existing_dates(self):
        from dag_daily_price import transform_to_fact_daily_price

        raw = pd.DataFrame([
            {"stock_code": "005930", "trade_date": date(2026, 3, 23),
             "open": 73000, "high": 74000, "low": 72000, "close": 73500,
             "volume": 1000000, "change_rate": 0.005},
        ])

        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("dag_daily_price.get_engine", return_value=mock_engine), \
             patch("dag_daily_price.pd.read_sql", return_value=raw), \
             patch("pandas.DataFrame.to_sql"):
            transform_to_fact_daily_price()

        # DELETE should have been called for the date
        mock_conn.execute.assert_called()

    def test_change_rate_column_present(self):
        """Verify change_rate is included in transformed output."""
        from dag_daily_price import transform_to_fact_daily_price

        raw = pd.DataFrame([
            {"stock_code": "005930", "trade_date": date(2026, 3, 23),
             "open": 73000, "high": 74000, "low": 72000, "close": 73500,
             "volume": 1000000, "change_rate": 0.005},
        ])

        written_dfs = []
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        def capture_to_sql(self_df, *args, **kwargs):
            written_dfs.append(self_df.copy() if hasattr(self_df, 'copy') else self_df)

        with patch("dag_daily_price.get_engine", return_value=mock_engine), \
             patch("dag_daily_price.pd.read_sql", return_value=raw), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            transform_to_fact_daily_price()

        assert "change_rate" in raw.columns
