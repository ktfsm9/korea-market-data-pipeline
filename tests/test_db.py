"""Tests for database connection configuration."""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


class TestDagDbConfig:
    """Test DAG database connection utility."""

    @patch("common.db.create_engine")
    def test_get_engine_default_values(self, mock_create_engine):
        from common.db import get_engine
        with patch.dict(os.environ, {}, clear=True):
            get_engine()
            mock_create_engine.assert_called_once()
            url = mock_create_engine.call_args[0][0]
            assert "pipeline:pipeline123" in url
            assert "postgres:5432" in url
            assert "market_data" in url

    @patch("common.db.create_engine")
    def test_get_engine_custom_env(self, mock_create_engine):
        from common.db import get_engine
        env = {
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "testdb",
        }
        with patch.dict(os.environ, env, clear=True):
            get_engine()
            url = mock_create_engine.call_args[0][0]
            assert "testuser:testpass" in url
            assert "localhost:5433" in url
            assert "testdb" in url

    @patch("common.db.create_engine")
    def test_get_engine_returns_engine(self, mock_create_engine):
        from common.db import get_engine
        result = get_engine()
        assert result == mock_create_engine.return_value


class TestAppConfig:
    """Test FastAPI app database connection configuration."""

    @patch("config.create_engine")
    def test_get_engine_default_values(self, mock_create_engine):
        from config import get_engine
        with patch.dict(os.environ, {}, clear=True):
            get_engine()
            url = mock_create_engine.call_args[0][0]
            assert "postgresql+psycopg2://" in url

    @patch("config.create_engine")
    def test_connection_string_format(self, mock_create_engine):
        from config import get_engine
        env = {
            "POSTGRES_USER": "myuser",
            "POSTGRES_PASSWORD": "mypass",
            "POSTGRES_HOST": "myhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "mydb",
        }
        with patch.dict(os.environ, env, clear=True):
            get_engine()
            url = mock_create_engine.call_args[0][0]
            assert url == "postgresql+psycopg2://myuser:mypass@myhost:5432/mydb"
