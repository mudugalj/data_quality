"""Execute one check's check_sql against a Parquet partition via dask-sql.

Single execution path for all check types — check_type is a classification
label only. evaluation_mode decides how the SQL result is judged:

    violation        : check_sql returns violating rows; 0 -> passed, >=1 -> failed
    consistency_mom  : check_sql returns one numeric aggregate; judged by MomEvaluator
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import dask.dataframe as dd
from dask_sql import Context

from .domain import CheckDefinition
from .history import ResultHistory
from .sql_safety import validate_read_only, validate_filter, SqlSafetyError
from .mom_evaluator import evaluate_mom


@dataclass
class ExecOutcome:
    status: str
    violation_count: Optional[int] = None
    current_value: Optional[float] = None
    prior_value: Optional[float] = None
    deviation: Optional[float] = None
    description: str = ""


def _scoped_context(table_name: str, ddf: dd.DataFrame,
                    business_rule_filter: Optional[str]) -> Context:
    """Register the (optionally filtered) partition under table_name."""
    ctx = Context()
    ctx.create_table(table_name, ddf)
    if business_rule_filter:
        predicate = validate_filter(business_rule_filter)
        scoped = ctx.sql(f'SELECT * FROM "{table_name}" WHERE {predicate}')
        ctx = Context()
        ctx.create_table(table_name, scoped)
    return ctx


def _single_numeric(result_df) -> Optional[float]:
    if result_df is None or result_df.shape[0] == 0 or result_df.shape[1] == 0:
        return None
    value = result_df.iloc[0, 0]
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def execute_check(
    check: CheckDefinition,
    ddf: dd.DataFrame,
    history: ResultHistory,
    run_ts: datetime,
) -> ExecOutcome:
    # Step 1 — read-only safety gate.
    try:
        safe_sql = validate_read_only(check.check_sql)
    except SqlSafetyError as exc:
        return ExecOutcome("errored", description=f"safety check failed: {exc}")

    # Step 2 — scope by business_rule_filter, then Step 3 — execute.
    try:
        ctx = _scoped_context(check.table_name, ddf, check.business_rule_filter)
        result_df = ctx.sql(safe_sql).compute()
    except SqlSafetyError as exc:
        return ExecOutcome("errored", description=f"filter rejected: {exc}")
    except Exception as exc:  # noqa: BLE001 — any SQL/runtime error -> errored
        return ExecOutcome("errored", description=f"check_sql execution failed: {exc}")

    # Step 4 — judge by evaluation_mode.
    if check.evaluation_mode == "violation":
        count = int(result_df.shape[0])
        if count == 0:
            return ExecOutcome("passed", violation_count=0,
                               description="no violating rows")
        return ExecOutcome("failed", violation_count=count,
                           description=f"{count} violating row(s)")

    if check.evaluation_mode == "consistency_mom":
        current_value = _single_numeric(result_df)
        outcome = evaluate_mom(
            history, check.table_name, check.column_name, check.check_type,
            check.app_code, current_value, check.deviation_tolerance, run_ts,
        )
        return ExecOutcome(
            outcome.status,
            current_value=outcome.current_value,
            prior_value=outcome.prior_value,
            deviation=outcome.deviation,
            description=outcome.description,
        )

    return ExecOutcome("errored",
                       description=f"unsupported evaluation_mode '{check.evaluation_mode}'")
