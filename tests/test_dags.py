"""Tests for Airflow DAG definitions and task logic."""

import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


class TestDagDailyPrice:
    """Tests for the daily stock price DAG."""

    def test_dag_loads(self):
        from dag_daily_price import dag
        assert dag.dag_id == "daily_stock_price"

    def test_dag_schedule(self):
        from dag_daily_price import dag
        assert dag.schedule_interval == "0 17 * * 1-5"

    def test_dag_has_correct_tasks(self):
        from dag_daily_price import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "extract_daily_prices",
            "transform_to_fact_daily_price",
            "run_quality_checks",
        }

    def test_dag_task_order(self):
        from dag_daily_price import dag
        extract = dag.get_task("extract_daily_prices")
        transform = dag.get_task("transform_to_fact_daily_price")
        quality = dag.get_task("run_quality_checks")
        assert "transform_to_fact_daily_price" in [t.task_id for t in extract.downstream_list]
        assert "run_quality_checks" in [t.task_id for t in transform.downstream_list]

    def test_target_stocks_not_empty(self):
        from dag_daily_price import TARGET_STOCKS
        assert len(TARGET_STOCKS) == 20

    def test_target_stocks_format(self):
        from dag_daily_price import TARGET_STOCKS
        for code, ticker in TARGET_STOCKS.items():
            assert len(code) == 6
            assert code.isdigit()
            assert ticker.endswith(".KS")

    def test_default_args(self):
        from dag_daily_price import default_args
        assert default_args["retries"] == 3
        assert default_args["retry_delay"] == timedelta(minutes=3)

    def test_dag_no_catchup(self):
        from dag_daily_price import dag
        assert dag.catchup is False

    def test_dag_tags(self):
        from dag_daily_price import dag
        assert "extract" in dag.tags
        assert "yfinance" in dag.tags


class TestDagNaverMarketSummary:
    """Tests for the Naver Finance market summary DAG."""

    def test_dag_loads(self):
        from dag_naver_market_summary import dag
        assert dag.dag_id == "naver_market_summary"

    def test_dag_schedule(self):
        from dag_naver_market_summary import dag
        assert dag.schedule_interval == "0 16 * * 1-5"

    def test_dag_has_correct_tasks(self):
        from dag_naver_market_summary import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "extract_naver_market",
            "extract_sector_info",
            "transform_to_dim_stock",
            "run_quality_checks",
        }

    def test_dag_task_order(self):
        from dag_naver_market_summary import dag
        extract = dag.get_task("extract_naver_market")
        extract_sector = dag.get_task("extract_sector_info")
        transform = dag.get_task("transform_to_dim_stock")
        quality = dag.get_task("run_quality_checks")
        assert "transform_to_dim_stock" in [t.task_id for t in extract.downstream_list]
        assert "transform_to_dim_stock" in [t.task_id for t in extract_sector.downstream_list]
        assert "run_quality_checks" in [t.task_id for t in transform.downstream_list]

    def test_market_codes(self):
        from dag_naver_market_summary import MARKET_CODES
        assert MARKET_CODES == {"0": "KOSPI", "1": "KOSDAQ"}

    def test_base_url(self):
        from dag_naver_market_summary import BASE_URL
        assert "finance.naver.com" in BASE_URL

    def test_default_args(self):
        from dag_naver_market_summary import default_args
        assert default_args["retries"] == 2
        assert default_args["retry_delay"] == timedelta(minutes=5)


class TestDagMarketAggregation:
    """Tests for the market aggregation DAG."""

    def test_dag_loads(self):
        from dag_market_aggregation import dag
        assert dag.dag_id == "market_aggregation"

    def test_dag_schedule(self):
        from dag_market_aggregation import dag
        assert dag.schedule_interval == "0 18 * * 1-5"

    def test_dag_has_correct_tasks(self):
        from dag_market_aggregation import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"build_market_summary", "run_quality_checks"}

    def test_dag_task_order(self):
        from dag_market_aggregation import dag
        aggregate = dag.get_task("build_market_summary")
        quality = dag.get_task("run_quality_checks")
        assert "run_quality_checks" in [t.task_id for t in aggregate.downstream_list]

    def test_dag_tags(self):
        from dag_market_aggregation import dag
        assert "transform" in dag.tags
        assert "aggregation" in dag.tags


class TestDagScheduleOrder:
    """Test that DAGs run in the correct order throughout the day."""

    def test_naver_runs_before_price(self):
        from dag_naver_market_summary import dag as naver_dag
        from dag_daily_price import dag as price_dag
        # Naver: 16:00, Price: 17:00
        assert naver_dag.schedule_interval < price_dag.schedule_interval

    def test_price_runs_before_aggregation(self):
        from dag_daily_price import dag as price_dag
        from dag_market_aggregation import dag as agg_dag
        # Price: 17:00, Aggregation: 18:00
        assert price_dag.schedule_interval < agg_dag.schedule_interval
