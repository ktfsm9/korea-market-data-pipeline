"""Data quality check utilities."""

from datetime import datetime

import pandas as pd
from sqlalchemy.engine import Engine


def log_quality_check(
    engine: Engine,
    check_name: str,
    table_name: str,
    check_type: str,
    status: str,
    expected: str,
    actual: str,
    details: str = "",
) -> None:
    """Insert a data quality check result into the log table."""
    df = pd.DataFrame([{
        "check_name": check_name,
        "table_name": table_name,
        "check_type": check_type,
        "status": status,
        "expected_value": expected,
        "actual_value": actual,
        "details": details,
        "checked_at": datetime.now(),
    }])
    df.to_sql("data_quality_log", engine, if_exists="append", index=False)


def check_row_count(engine: Engine, table_name: str, min_rows: int) -> bool:
    """Check if a table has at least min_rows rows."""
    with engine.connect() as conn:
        result = conn.execute(
            pd.io.sql.sqlalchemy.text(f"SELECT COUNT(*) FROM {table_name}")
        )
        count = result.scalar()

    status = "PASS" if count >= min_rows else "FAIL"
    log_quality_check(
        engine,
        check_name=f"{table_name}_row_count",
        table_name=table_name,
        check_type="row_count",
        status=status,
        expected=f">= {min_rows}",
        actual=str(count),
    )
    return status == "PASS"


def check_null_ratio(
    engine: Engine, table_name: str, column: str, max_ratio: float = 0.1
) -> bool:
    """Check if null ratio for a column is below threshold."""
    with engine.connect() as conn:
        result = conn.execute(
            pd.io.sql.sqlalchemy.text(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS nulls
                FROM {table_name}
                """
            )
        )
        row = result.fetchone()
        total, nulls = row[0], row[1]

    ratio = nulls / total if total > 0 else 0
    status = "PASS" if ratio <= max_ratio else "FAIL"
    log_quality_check(
        engine,
        check_name=f"{table_name}_{column}_null_ratio",
        table_name=table_name,
        check_type="null_ratio",
        status=status,
        expected=f"<= {max_ratio}",
        actual=f"{ratio:.4f}",
    )
    return status == "PASS"


def check_freshness(
    engine: Engine, table_name: str, date_column: str, max_age_hours: int = 48
) -> bool:
    """Check if the most recent data is within max_age_hours."""
    with engine.connect() as conn:
        result = conn.execute(
            pd.io.sql.sqlalchemy.text(
                f"SELECT MAX({date_column}) FROM {table_name}"
            )
        )
        max_date = result.scalar()

    if max_date is None:
        status = "FAIL"
        actual = "no data"
    else:
        age_hours = (datetime.now() - pd.Timestamp(max_date).to_pydatetime()).total_seconds() / 3600
        status = "PASS" if age_hours <= max_age_hours else "WARN"
        actual = f"{age_hours:.1f}h"

    log_quality_check(
        engine,
        check_name=f"{table_name}_freshness",
        table_name=table_name,
        check_type="freshness",
        status=status,
        expected=f"<= {max_age_hours}h",
        actual=actual,
    )
    return status == "PASS"
