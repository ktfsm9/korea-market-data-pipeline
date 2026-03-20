"""Tests for data quality check utilities."""

import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from common.quality import check_freshness, check_null_ratio, check_row_count, log_quality_check


def test_imports():
    """Verify that quality check functions are importable."""
    assert callable(log_quality_check)
    assert callable(check_row_count)
    assert callable(check_null_ratio)
    assert callable(check_freshness)


class TestCheckRowCount:
    def _make_engine(self, count):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        result = MagicMock()
        result.scalar.return_value = count
        conn.execute.return_value = result
        return engine

    @patch("common.quality.log_quality_check")
    def test_pass_when_enough_rows(self, mock_log):
        engine = self._make_engine(100)
        assert check_row_count(engine, "raw.daily_price", min_rows=10) is True
        mock_log.assert_called_once()
        args = mock_log.call_args
        assert args.kwargs["status"] == "PASS"

    @patch("common.quality.log_quality_check")
    def test_fail_when_too_few_rows(self, mock_log):
        engine = self._make_engine(5)
        assert check_row_count(engine, "raw.daily_price", min_rows=10) is False
        args = mock_log.call_args
        assert args.kwargs["status"] == "FAIL"

    @patch("common.quality.log_quality_check")
    def test_pass_exact_threshold(self, mock_log):
        engine = self._make_engine(10)
        assert check_row_count(engine, "raw.daily_price", min_rows=10) is True

    @patch("common.quality.log_quality_check")
    def test_zero_rows(self, mock_log):
        engine = self._make_engine(0)
        assert check_row_count(engine, "raw.daily_price", min_rows=1) is False


class TestCheckNullRatio:
    def _make_engine(self, total, nulls):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        result = MagicMock()
        result.fetchone.return_value = (total, nulls)
        conn.execute.return_value = result
        return engine

    @patch("common.quality.log_quality_check")
    def test_pass_low_null_ratio(self, mock_log):
        engine = self._make_engine(100, 5)
        assert check_null_ratio(engine, "raw.daily_price", "close") is True
        args = mock_log.call_args
        assert args.kwargs["status"] == "PASS"

    @patch("common.quality.log_quality_check")
    def test_fail_high_null_ratio(self, mock_log):
        engine = self._make_engine(100, 50)
        assert check_null_ratio(engine, "raw.daily_price", "close") is False
        args = mock_log.call_args
        assert args.kwargs["status"] == "FAIL"

    @patch("common.quality.log_quality_check")
    def test_pass_no_nulls(self, mock_log):
        engine = self._make_engine(100, 0)
        assert check_null_ratio(engine, "raw.daily_price", "close") is True

    @patch("common.quality.log_quality_check")
    def test_empty_table(self, mock_log):
        engine = self._make_engine(0, 0)
        # ratio = 0/0 -> 0, should pass
        assert check_null_ratio(engine, "raw.daily_price", "close") is True

    @patch("common.quality.log_quality_check")
    def test_custom_threshold(self, mock_log):
        engine = self._make_engine(100, 20)
        # 20% nulls, threshold 0.25 -> pass
        assert check_null_ratio(engine, "raw.daily_price", "close", max_ratio=0.25) is True
        # 20% nulls, threshold 0.15 -> fail
        engine = self._make_engine(100, 20)
        assert check_null_ratio(engine, "raw.daily_price", "close", max_ratio=0.15) is False


class TestCheckFreshness:
    def _make_engine(self, max_date):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        result = MagicMock()
        result.scalar.return_value = max_date
        conn.execute.return_value = result
        return engine

    @patch("common.quality.log_quality_check")
    def test_pass_fresh_data(self, mock_log):
        recent = datetime.now() - timedelta(hours=1)
        engine = self._make_engine(recent)
        assert check_freshness(engine, "raw.daily_price", "ingested_at") is True
        args = mock_log.call_args
        assert args.kwargs["status"] == "PASS"

    @patch("common.quality.log_quality_check")
    def test_warn_stale_data(self, mock_log):
        old = datetime.now() - timedelta(hours=72)
        engine = self._make_engine(old)
        assert check_freshness(engine, "raw.daily_price", "ingested_at") is False
        args = mock_log.call_args
        assert args.kwargs["status"] == "WARN"

    @patch("common.quality.log_quality_check")
    def test_fail_no_data(self, mock_log):
        engine = self._make_engine(None)
        assert check_freshness(engine, "raw.daily_price", "ingested_at") is False
        args = mock_log.call_args
        assert args.kwargs["status"] == "FAIL"
        assert args.kwargs["actual"] == "no data"

    @patch("common.quality.log_quality_check")
    def test_custom_max_age(self, mock_log):
        old = datetime.now() - timedelta(hours=5)
        engine = self._make_engine(old)
        # 5 hours old, threshold 2 hours -> WARN
        assert check_freshness(engine, "raw.daily_price", "ingested_at", max_age_hours=2) is False
