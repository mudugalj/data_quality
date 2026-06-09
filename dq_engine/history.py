"""Read prior DqResults from output/results.csv.

The results CSV is the MVP's stand-in for a result store: it is the history
that both month-on-month consistency and issue classification look back into.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import List, Optional

from .domain import RESULT_COLUMNS


def _parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_float(value: str) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


class ResultHistory:
    """In-memory view over the results CSV, queried by check identity."""

    def __init__(self, rows: List[dict]):
        self._rows = rows

    @classmethod
    def load(cls, path: str) -> "ResultHistory":
        rows: List[dict] = []
        if path and os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    row["_ts"] = _parse_ts(row.get("run_timestamp", ""))
                    row["_current_value"] = _parse_float(row.get("current_value", ""))
                    rows.append(row)
        return cls(rows)

    def _matching(self, table_name, column_name, check_type, app_code):
        app = app_code or ""
        for row in self._rows:
            if (row.get("table_name") == table_name
                    and row.get("column_name") == column_name
                    and row.get("check_type") == check_type
                    and (row.get("app_code") or "") == app):
                yield row

    def most_recent_before(
        self, table_name, column_name, check_type, app_code, before_ts: datetime
    ) -> Optional[dict]:
        """Most recent prior result strictly before before_ts (any status)."""
        candidates = [
            r for r in self._matching(table_name, column_name, check_type, app_code)
            if r.get("_ts") is not None and r["_ts"] < before_ts
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r["_ts"])

    def mom_baseline(
        self, table_name, column_name, check_type, app_code,
        window_low: datetime, window_high: datetime,
    ) -> Optional[dict]:
        """Most recent consistency_mom result with run_timestamp in
        [window_low, window_high]. window_low/high bracket ~30 days prior."""
        candidates = [
            r for r in self._matching(table_name, column_name, check_type, app_code)
            if r.get("evaluation_mode") == "consistency_mom"
            and r.get("_ts") is not None
            and window_low <= r["_ts"] <= window_high
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r["_ts"])
