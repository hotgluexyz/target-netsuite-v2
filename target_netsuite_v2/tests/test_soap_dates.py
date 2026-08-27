"""Tests for SOAP journal entry date normalization."""

from datetime import datetime

from target_netsuite_v2.utils import format_date, format_date_as_naive_datetime


def test_format_date_plain_calendar_string():
    """Plain YYYY-MM-DD inputs should stay on the same calendar day."""
    assert format_date("2025-08-15") == "2025-08-15"


def test_format_date_midnight_with_timezone_offset():
    """Timezone-aware midnight inputs should not shift the calendar day."""
    assert format_date("2025-08-15T00:00:00-05:00") == "2025-08-15"


def test_format_date_as_naive_datetime_plain_string():
    """SOAP tranDate should be a naive datetime at source calendar midnight."""
    result = format_date_as_naive_datetime("2025-08-15")
    assert result == datetime(2025, 8, 15)
    assert result.tzinfo is None


def test_format_date_as_naive_datetime_with_timezone_offset():
    """Timezone-aware inputs should keep the wall-clock calendar day."""
    result = format_date_as_naive_datetime("2025-08-15T00:00:00-05:00")
    assert result == datetime(2025, 8, 15)
    assert result.tzinfo is None


def test_format_date_as_naive_datetime_utc_midnight_string():
    """UTC midnight inputs should not shift to the previous calendar day."""
    result = format_date_as_naive_datetime("2025-08-15T00:00:00")
    assert result == datetime(2025, 8, 15)
    assert result.tzinfo is None
