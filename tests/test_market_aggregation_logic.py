"""Tests for market_aggregation DAG task logic."""

import os
import sys
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


class FakeEngine:
    """Simple engine mock with real context managers."""

    def __init__(self, connect_conn, begin_conn):
        self._connect_conn = connect_conn
        self._begin_conn = begin_conn

    @contextmanager
    def connect(self):
        yield self._connect_conn

    @contextmanager
    def begin(self):
        yield self._begin_conn


class TestBuildMarketSummary:
    """Tests for build_market_summary task."""

    def _mock_engine(self, fact_count=10, dim_count=10):
        """Create a fake engine with configurable source table counts."""
        mock_conn = MagicMock()
        counts = iter([fact_count, dim_count])
        mock_conn.execute.return_value.scalar.side_effect = lambda: next(counts)

        mock_begin_conn = MagicMock()

        return FakeEngine(mock_conn, mock_begin_conn), mock_begin_conn

    def test_skips_when_fact_daily_price_empty(self):
        from dag_market_aggregation import build_market_summary

        engine, _ = self._mock_engine(fact_count=0, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql") as mock_read:
            build_market_summary(**{"ti": MagicMock()})

        mock_read.assert_not_called()

    def test_skips_when_dim_stock_empty(self):
        from dag_market_aggregation import build_market_summary

        engine, _ = self._mock_engine(fact_count=10, dim_count=0)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql") as mock_read:
            build_market_summary(**{"ti": MagicMock()})

        mock_read.assert_not_called()

    def test_skips_when_aggregation_result_empty(self):
        from dag_market_aggregation import build_market_summary

        engine, _ = self._mock_engine(fact_count=10, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql", return_value=pd.DataFrame()):
            build_market_summary(**{"ti": MagicMock()})

    def test_deletes_existing_dates_before_insert(self):
        from dag_market_aggregation import build_market_summary

        agg_df = pd.DataFrame([{
            "trade_date": date(2026, 3, 23),
            "market_type": "KOSPI",
            "total_stocks": 100,
            "avg_per": 11.5,
            "avg_pbr": 1.5,
            "avg_roe": 4.5,
            "total_volume": 5000000,
            "total_market_cap": 47000000,
        }])

        engine, _ = self._mock_engine(fact_count=10, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql", return_value=agg_df), \
             patch("dag_market_aggregation.sa_text") as mock_sa_text, \
             patch.object(pd.DataFrame, "to_sql"):
            build_market_summary(**{"ti": MagicMock()})

        delete_calls = [c for c in mock_sa_text.call_args_list if "DELETE" in str(c)]
        assert len(delete_calls) >= 1

    def test_adds_updated_at_column(self):
        from dag_market_aggregation import build_market_summary

        agg_df = pd.DataFrame([{
            "trade_date": date(2026, 3, 23),
            "market_type": "KOSPI",
            "total_stocks": 100,
            "avg_per": 11.5,
            "avg_pbr": 1.5,
            "avg_roe": 4.5,
            "total_volume": 5000000,
            "total_market_cap": 47000000,
        }])

        written_dfs = []

        def capture_to_sql(self, *args, **kwargs):
            written_dfs.append(self.copy())

        engine, _ = self._mock_engine(fact_count=10, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql", return_value=agg_df), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            build_market_summary(**{"ti": MagicMock()})

        assert len(written_dfs) > 0
        assert "updated_at" in written_dfs[0].columns

    def test_writes_to_mart_agg_market_summary(self):
        from dag_market_aggregation import build_market_summary

        agg_df = pd.DataFrame([{
            "trade_date": date(2026, 3, 23),
            "market_type": "KOSPI",
            "total_stocks": 100,
            "avg_per": 11.5,
            "avg_pbr": 1.5,
            "avg_roe": 4.5,
            "total_volume": 5000000,
            "total_market_cap": 47000000,
        }])

        to_sql_calls = []

        def capture_to_sql(self, name, *args, **kwargs):
            to_sql_calls.append({"name": name, "schema": kwargs.get("schema")})

        engine, _ = self._mock_engine(fact_count=10, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql", return_value=agg_df), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            build_market_summary(**{"ti": MagicMock()})

        assert len(to_sql_calls) == 1
        assert to_sql_calls[0]["name"] == "agg_market_summary"
        assert to_sql_calls[0]["schema"] == "mart"

    def test_handles_multiple_dates(self):
        from dag_market_aggregation import build_market_summary

        agg_df = pd.DataFrame([
            {"trade_date": date(2026, 3, 23), "market_type": "KOSPI",
             "total_stocks": 100, "avg_per": 11.5, "avg_pbr": 1.5,
             "avg_roe": 4.5, "total_volume": 5000000, "total_market_cap": 47000000},
            {"trade_date": date(2026, 3, 22), "market_type": "KOSPI",
             "total_stocks": 100, "avg_per": 11.3, "avg_pbr": 1.4,
             "avg_roe": 4.3, "total_volume": 4000000, "total_market_cap": 46000000},
        ])

        engine, _ = self._mock_engine(fact_count=10, dim_count=10)

        with patch("dag_market_aggregation.get_engine", return_value=engine), \
             patch("dag_market_aggregation.pd.read_sql", return_value=agg_df), \
             patch("dag_market_aggregation.sa_text") as mock_sa_text, \
             patch.object(pd.DataFrame, "to_sql"):
            build_market_summary(**{"ti": MagicMock()})

        delete_calls = [c for c in mock_sa_text.call_args_list if "DELETE" in str(c)]
        assert len(delete_calls) == 2


class TestRunQualityChecks:
    """Tests for run_quality_checks in market_aggregation."""

    def test_raises_when_check_fails(self):
        from dag_market_aggregation import run_quality_checks

        mock_check = MagicMock(return_value=False)

        with patch("dag_market_aggregation.get_engine", return_value=MagicMock()), \
             patch("dag_market_aggregation.check_row_count", mock_check):
            with pytest.raises(ValueError, match="Quality check failed"):
                run_quality_checks(**{"ti": MagicMock()})

    def test_passes_when_check_succeeds(self):
        from dag_market_aggregation import run_quality_checks

        mock_check = MagicMock(return_value=True)

        with patch("dag_market_aggregation.get_engine", return_value=MagicMock()), \
             patch("dag_market_aggregation.check_row_count", mock_check):
            run_quality_checks(**{"ti": MagicMock()})
