"""Database connection utilities for Airflow DAGs."""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """Create a SQLAlchemy engine from environment variables."""
    user = os.getenv("POSTGRES_USER", "pipeline")
    password = os.getenv("POSTGRES_PASSWORD", "pipeline123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "market_data")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
