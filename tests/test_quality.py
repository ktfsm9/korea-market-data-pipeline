"""Tests for data quality check utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

# Test that quality module can be imported
from common.quality import check_freshness, check_null_ratio, check_row_count, log_quality_check


def test_imports():
    """Verify that quality check functions are importable."""
    assert callable(log_quality_check)
    assert callable(check_row_count)
    assert callable(check_null_ratio)
    assert callable(check_freshness)
