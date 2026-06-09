"""Month-on-month consistency evaluation.

A consistency_mom check_sql returns a single numeric aggregate. We compare it
to the value from ~30 days earlier (most recent prior result whose
run_timestamp falls in [run-31d, run-28d]) and judge by percentage deviation.

    pct_deviation = |current - prior| / |prior| * 100
    pct_deviation <= deviation_tolerance   -> passed
    pct_deviation >  deviation_tolerance   -> failed
    no baseline in 28-31d window           -> failed ("no prior MoM baseline")
    prior value == 0                       -> failed ("prior MoM value is zero")
    current value missing / non-numeric    -> errored
"""

from __future__ import annotations

from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional

from .history import ResultHistory

MOM_WINDOW_LOW_DAYS = 31
MOM_WINDOW_HIGH_DAYS = 28


@dataclass
class MomOutcome:
    status: str                       # passed | failed | errored
    current_value: Optional[float]
    prior_value: Optional[float]
    deviation: Optional[float]        # percentage
    description: str


def evaluate_mom(
    history: ResultHistory,
    table_name: str,
    column_name: str,
    check_type: str,
    app_code: Optional[str],
    current_value: Optional[float],
    deviation_tolerance: float,
    run_ts: datetime,
) -> MomOutcome:
    if current_value is None:
        return MomOutcome("errored", None, None, None,
                          "check_sql returned no numeric value for MoM aggregate")

    window_low = run_ts - timedelta(days=MOM_WINDOW_LOW_DAYS)
    window_high = run_ts - timedelta(days=MOM_WINDOW_HIGH_DAYS)
    baseline = history.mom_baseline(
        table_name, column_name, check_type, app_code, window_low, window_high
    )

    if baseline is None:
        return MomOutcome("failed", current_value, None, None,
                          "no prior MoM baseline (28-31 day window)")

    prior_value = baseline.get("_current_value")
    if prior_value is None:
        return MomOutcome("errored", current_value, None, None,
                          "prior MoM result has no numeric value")
    if prior_value == 0:
        return MomOutcome("failed", current_value, prior_value, None,
                          "prior MoM value is zero; cannot compute %")

    pct = abs(current_value - prior_value) / abs(prior_value) * 100.0
    if pct <= deviation_tolerance:
        status = "passed"
        desc = f"MoM deviation {pct:.2f}% within tolerance {deviation_tolerance}%"
    else:
        status = "failed"
        desc = f"MoM deviation {pct:.2f}% exceeds tolerance {deviation_tolerance}%"
    return MomOutcome(status, current_value, prior_value, round(pct, 6), desc)
