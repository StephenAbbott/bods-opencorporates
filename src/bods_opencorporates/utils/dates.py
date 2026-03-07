"""Date parsing and normalization utilities for BODS output."""

from __future__ import annotations

import re
from datetime import date, datetime

from dateutil import parser as dateutil_parser


def normalize_date(date_str: str | None) -> str | None:
    """Normalize various date formats to ISO 8601.

    Returns:
        YYYY-MM-DD for full dates, YYYY-MM for partial dates, or None.

    Handles formats commonly found in OpenCorporates data:
        - "2020-01-15" (ISO)
        - "15/01/2020" (DD/MM/YYYY)
        - "01/15/2020" (MM/DD/YYYY - ambiguous, treated as ISO parse)
        - "2020-01" (partial, month precision)
        - "2020" (partial, year only)
        - "January 2020", "Jan 2020"
        - None or empty string
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Already YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str

    # Already YYYY-MM (partial)
    if re.match(r"^\d{4}-\d{2}$", date_str):
        return date_str

    # Year only
    if re.match(r"^\d{4}$", date_str):
        return date_str

    # DD/MM/YYYY format (common in UK/EU data)
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", date_str)
    if match:
        day, month, year = match.groups()
        day_int, month_int = int(day), int(month)
        # If day > 12, it's definitely DD/MM/YYYY
        # If both <= 12, assume DD/MM/YYYY (European convention)
        if day_int > 12:
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # Try dateutil parser as fallback
    try:
        parsed = dateutil_parser.parse(date_str, dayfirst=True)
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass

    return None


def normalize_partial_date(date_str: str | None) -> str | None:
    """Normalize a partial date (month/year precision) for birth dates.

    OpenCorporates provides partial_date_of_birth as "YYYY-MM".
    This function ensures consistent formatting.

    Returns:
        YYYY-MM or YYYY format, or None.
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Already YYYY-MM
    if re.match(r"^\d{4}-\d{2}$", date_str):
        return date_str

    # Year only
    if re.match(r"^\d{4}$", date_str):
        return date_str

    # YYYY-MM-DD - truncate to YYYY-MM for privacy
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str[:7]

    # Try parsing
    try:
        parsed = dateutil_parser.parse(date_str, dayfirst=True)
        return parsed.strftime("%Y-%m")
    except (ValueError, TypeError):
        pass

    return None


def current_date_iso() -> str:
    """Return today's date in ISO 8601 format (YYYY-MM-DD)."""
    return date.today().isoformat()


def current_datetime_iso() -> str:
    """Return current UTC datetime in ISO 8601 format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
