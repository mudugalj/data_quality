"""Issue classification: new / recurring / resolved (or none).

Keyed on (table_name, column_name, check_type, app_code) using the most recent
prior result within the retention window. A result is "open" when its status is
failed or errored.

    current open  + prior open            -> recurring
    current open  + (no prior | passed)   -> new
    current passed + prior open           -> resolved
    current passed + (no prior | passed)  -> None (no case)
    prior lookup unavailable              -> None (cannot classify)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from .domain import OPEN_STATUSES
from .history import ResultHistory

RETENTION_DAYS = 183  # ~6 months


def classify(
    history: ResultHistory,
    table_name: str,
    column_name: str,
    check_type: str,
    app_code: Optional[str],
    current_status: str,
    run_ts: datetime,
) -> Optional[str]:
    window_start = run_ts - timedelta(days=RETENTION_DAYS)
    prior = history.most_recent_before(
        table_name, column_name, check_type, app_code, run_ts
    )
    if prior is not None and prior.get("_ts") is not None and prior["_ts"] < window_start:
        prior = None  # outside retention window

    current_open = current_status in OPEN_STATUSES
    prior_open = prior is not None and prior.get("status") in OPEN_STATUSES

    if current_open and prior_open:
        return "recurring"
    if current_open and not prior_open:
        return "new"
    if not current_open and prior_open:
        return "resolved"
    return None
