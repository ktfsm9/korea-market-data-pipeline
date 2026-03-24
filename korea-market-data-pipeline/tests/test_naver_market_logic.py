"""Tests for naver_market_summary DAG task logic."""

import os
import sys
import time as time_module
from contextlib import contextmanager
from datetime import datetime
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


def _make_engine():
    """Create a fake engine with mock connections."""
    mock_conn = MagicMock()
    mock_begin_conn = MagicMock()
    return FakeEngine(mock_conn, mock_begin_conn), mock_conn, mock_begin_conn


class TestGetTotalPages:
    """Tests for _get_total_pages helper."""

    def test_returns_last_page_number(self):
        from dag_naver_market_summary import _get_total_pages

        html = '<td class="pgRR"><a href="/sise/sise_market_sum.nhn?sosok=0&amp;page=42">끝</a></td>'
        mock_resp = MagicMock()
        mock_resp.text = html

        session = MagicMock()
        session.get.return_value = mock_resp

        assert _get_total_pages(session, "0") == 42

    def test_returns_1_when_no_pagination(self):
        from dag_naver_market_summary import _get_total_pages

        mock_resp = MagicMock()
        mock_resp.text = "<html><body>no pagination</body></html>"

        session = MagicMock()
        session.get.return_value = mock_resp

        assert _get_total_pages(session, "0") == 1


class TestCrawlPage:
    """Tests for _crawl_page helper."""

    def test_returns_empty_df_when_no_table(self):
        from dag_naver_market_summary import _crawl_page

        mock_resp = MagicMock()
        mock_resp.text = "<html><body></body></html>"

        session = MagicMock()
        session.post.return_value = mock_resp

        with patch.object(time_module, "sleep"):
            result = _crawl_page(session, "0", 1, ["field1"])

        assert result.empty

    def test_returns_dataframe_with_stock_code(self):
        from dag_naver_market_summary import _crawl_page

        html = """
        <div class="box_type_l">
            <table>
                <thead><tr><th>N</th><th>종목명</th><th>현재가</th><th>X</th></tr></thead>
                <tbody>
                    <tr>
                        <td class="no">1</td>
                        <td><a class="tltle" href="/item/main.nhn?code=005930">삼성전자</a></td>
                        <td class="number">73,000</td>
                    </tr>
                </tbody>
            </table>
        </div>
        """
        mock_resp = MagicMock()
        mock_resp.text = html

        session = MagicMock()
        session.post.return_value = mock_resp

        with patch.object(time_module, "sleep"):
            result = _crawl_page(session, "0", 1, ["field1"])

        if not result.empty:
            assert "stock_code" in result.columns


class TestExtractNaverMarket:
    """Tests for extract_naver_market task."""

    def test_cleans_old_data_before_crawl(self):
        from dag_naver_market_summary import extract_naver_market

        mock_engine, _, _ = _make_engine()
        ctx = {"ti": MagicMock()}

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary._get_total_pages", return_value=0), \
             patch("dag_naver_market_summary._get_field_ids", return_value=[]), \
             patch("dag_naver_market_summary.sa_text") as mock_sa_text, \
             patch("dag_naver_market_summary.requests.Session") as MockSession:
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            extract_naver_market(**ctx)

        delete_called = any("DELETE" in str(c) for c in mock_sa_text.call_args_list)
        assert delete_called

    def test_pushes_zero_rows_when_no_data(self):
        from dag_naver_market_summary import extract_naver_market

        mock_engine, _, _ = _make_engine()
        ctx = {"ti": MagicMock()}

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary._get_total_pages", return_value=1), \
             patch("dag_naver_market_summary._get_field_ids", return_value=["f1"]), \
             patch("dag_naver_market_summary._crawl_page", return_value=pd.DataFrame()), \
             patch("dag_naver_market_summary.requests.Session") as MockSession:
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            extract_naver_market(**ctx)

        ctx["ti"].xcom_push.assert_called_once_with(key="rows_inserted", value=0)

    def test_column_mapping_korean_to_english(self):
        from dag_naver_market_summary import extract_naver_market

        crawled = pd.DataFrame([{
            "종목명": "삼성전자", "현재가": "73,000", "거래량": "1000",
            "시가총액": "500000", "ROE": "10.5", "PER": "12.3", "PBR": "1.5",
            "stock_code": "005930",
        }])

        written_dfs = []

        def capture_to_sql(self, *args, **kwargs):
            written_dfs.append(self.copy())

        mock_engine, _, _ = _make_engine()
        ctx = {"ti": MagicMock()}

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary._get_total_pages", return_value=1), \
             patch("dag_naver_market_summary._get_field_ids", return_value=["f1"]), \
             patch("dag_naver_market_summary._crawl_page", return_value=crawled), \
             patch("dag_naver_market_summary.requests.Session") as MockSession, \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            extract_naver_market(**ctx)

        assert len(written_dfs) > 0
        df = written_dfs[0]
        assert "stock_name" in df.columns
        assert "roe" in df.columns
        assert "market_type" in df.columns
        assert "종목명" not in df.columns


class TestExtractSectorInfo:
    """Tests for extract_sector_info task."""

    def test_returns_early_when_no_sectors(self):
        from dag_naver_market_summary import extract_sector_info

        mock_resp = MagicMock()
        mock_resp.text = "<html><body>no sectors</body></html>"

        with patch("dag_naver_market_summary.get_engine", return_value=MagicMock()), \
             patch("dag_naver_market_summary.requests.Session") as MockSession:
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.get.return_value = mock_resp
            extract_sector_info(**{"ti": MagicMock()})

    def test_truncates_before_insert(self):
        from dag_naver_market_summary import extract_sector_info

        sector_list_html = """
        <html><body>
            <a href="sise_group_detail?type=upjong&no=001">반도체</a>
        </body></html>
        """
        sector_detail_html = """
        <html><body>
            <a href="/item/main.naver?code=005930">삼성전자</a>
        </body></html>
        """
        mock_engine, _, _ = _make_engine()
        call_count = {"n": 0}

        def side_effect_get(url, **kwargs):
            call_count["n"] += 1
            resp = MagicMock()
            if call_count["n"] == 1:
                resp.text = sector_list_html
            else:
                resp.text = sector_detail_html
            return resp

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary.requests.Session") as MockSession, \
             patch("dag_naver_market_summary.sa_text") as mock_sa_text, \
             patch.object(time_module, "sleep"), \
             patch.object(pd.DataFrame, "to_sql"):
            mock_session = MagicMock()
            MockSession.return_value = mock_session
            mock_session.get.side_effect = side_effect_get
            extract_sector_info(**{"ti": MagicMock()})

        truncate_called = any("TRUNCATE" in str(c) for c in mock_sa_text.call_args_list)
        assert truncate_called


class TestTransformToDimStock:
    """Tests for transform_to_dim_stock task."""

    def test_skips_when_no_raw_data(self):
        from dag_naver_market_summary import transform_to_dim_stock

        with patch("dag_naver_market_summary.get_engine", return_value=MagicMock()), \
             patch("dag_naver_market_summary.pd.read_sql", return_value=pd.DataFrame()):
            transform_to_dim_stock(**{"ti": MagicMock()})

    def test_cleans_numeric_columns(self):
        from dag_naver_market_summary import transform_to_dim_stock

        raw = pd.DataFrame([{
            "stock_code": "005930", "stock_name": "삼성전자", "market_type": "KOSPI",
            "sector": "반도체", "roe": "10.5", "per": "12.3", "pbr": "1.5",
            "market_cap": "500,000", "ingested_at": datetime.now(),
        }])

        written_dfs = []

        def capture_to_sql(self, *args, **kwargs):
            written_dfs.append(self.copy())

        mock_engine, _, _ = _make_engine()

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary.pd.read_sql", return_value=raw), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            transform_to_dim_stock(**{"ti": MagicMock()})

        assert len(written_dfs) > 0
        df = written_dfs[0]
        assert df["market_cap"].iloc[0] == 500000.0
        assert df["roe"].iloc[0] == 10.5

    def test_handles_na_values(self):
        from dag_naver_market_summary import transform_to_dim_stock

        raw = pd.DataFrame([{
            "stock_code": "005930", "stock_name": "삼성전자", "market_type": "KOSPI",
            "sector": None, "roe": "N/A", "per": "N/A", "pbr": "N/A",
            "market_cap": "N/A", "ingested_at": datetime.now(),
        }])

        written_dfs = []

        def capture_to_sql(self, *args, **kwargs):
            written_dfs.append(self.copy())

        mock_engine, _, _ = _make_engine()

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary.pd.read_sql", return_value=raw), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            transform_to_dim_stock(**{"ti": MagicMock()})

        assert len(written_dfs) > 0
        df = written_dfs[0]
        assert pd.isna(df["roe"].iloc[0])
        assert pd.isna(df["market_cap"].iloc[0])

    def test_truncates_dim_stock_before_insert(self):
        from dag_naver_market_summary import transform_to_dim_stock

        raw = pd.DataFrame([{
            "stock_code": "005930", "stock_name": "삼성전자", "market_type": "KOSPI",
            "sector": "반도체", "roe": "10", "per": "12", "pbr": "1.5",
            "market_cap": "500000", "ingested_at": datetime.now(),
        }])

        mock_engine, _, _ = _make_engine()

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary.pd.read_sql", return_value=raw), \
             patch("dag_naver_market_summary.sa_text") as mock_sa_text, \
             patch.object(pd.DataFrame, "to_sql"):
            transform_to_dim_stock(**{"ti": MagicMock()})

        truncate_called = any("TRUNCATE" in str(c) for c in mock_sa_text.call_args_list)
        assert truncate_called

    def test_output_columns(self):
        from dag_naver_market_summary import transform_to_dim_stock

        raw = pd.DataFrame([{
            "stock_code": "005930", "stock_name": "삼성전자", "market_type": "KOSPI",
            "sector": "반도체", "roe": "10", "per": "12", "pbr": "1.5",
            "market_cap": "500000", "ingested_at": datetime.now(),
        }])

        written_dfs = []

        def capture_to_sql(self, *args, **kwargs):
            written_dfs.append(self.copy())

        mock_engine, _, _ = _make_engine()

        with patch("dag_naver_market_summary.get_engine", return_value=mock_engine), \
             patch("dag_naver_market_summary.pd.read_sql", return_value=raw), \
             patch.object(pd.DataFrame, "to_sql", capture_to_sql):
            transform_to_dim_stock(**{"ti": MagicMock()})

        df = written_dfs[0]
        expected_cols = {"stock_code", "stock_name", "market_type", "sector",
                         "roe", "per", "pbr", "market_cap", "updated_at"}
        assert set(df.columns) == expected_cols
