"""DQ Engine orchestration.

Run lifecycle:
  1. Load + validate config (checks + source catalog).
  2. Filter checks by selected check types and optional app_code.
  3. Load results history (for MoM baselines + issue classification).
  4. Per source: resolve + read the Parquet partition once.
  5. Per check: execute -> judge -> classify -> build DqResult (per-check isolation).
  6. Append results to history CSV; write exceptions CSV; return a report.
"""

from __future__ import annotations

import calendar
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Sequence

from .domain import CheckDefinition, DqResult, SourceEntry, CHECK_TYPES
from . import config_loader, source_reader, result_writer
from .check_executor import execute_check
from .history import ResultHistory
from .issue_classifier import classify


@dataclass
class RunConfig:
    checks_csv: str
    catalog_csv: str
    results_csv: str
    exceptions_csv: str
    selected_check_types: Sequence[str]
    partition: Optional[str] = None        # specific value | "latest" | None
    app_code: Optional[str] = None
    run_timestamp: Optional[datetime] = None


@dataclass
class DqReport:
    run_id: str
    run_timestamp: str
    month_end_date: str
    executed: int = 0
    passed: int = 0
    failed: int = 0
    errored: int = 0
    new_issues: int = 0
    recurring_issues: int = 0
    resolved_issues: int = 0
    results: List[DqResult] = field(default_factory=list)
    config_rejects: List[str] = field(default_factory=list)

    @property
    def exception_results(self) -> List[DqResult]:
        return [r for r in self.results if r.is_open()]


class TerminatedRun(Exception):
    """Fatal precondition failure — the run produces no report."""


def _month_end(d: datetime) -> str:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return f"{d.year:04d}-{d.month:02d}-{last_day:02d}"


def _validate_check_types(selected: Sequence[str]) -> List[str]:
    if not selected:
        raise TerminatedRun("no check types selected")
    deduped = list(dict.fromkeys(selected))
    bad = [c for c in deduped if c not in CHECK_TYPES]
    if bad:
        raise TerminatedRun(
            f"unrecognised check type(s): {', '.join(bad)} "
            f"(expected subset of {', '.join(CHECK_TYPES)})"
        )
    return deduped


def run(cfg: RunConfig) -> DqReport:
    run_id = str(uuid.uuid4())
    run_ts = cfg.run_timestamp or datetime.now()
    month_end = _month_end(run_ts)

    selected = _validate_check_types(cfg.selected_check_types)

    checks, check_rejects = config_loader.load_checks(cfg.checks_csv)
    catalog, catalog_rejects = config_loader.load_catalog(cfg.catalog_csv)

    report = DqReport(
        run_id=run_id,
        run_timestamp=run_ts.isoformat(timespec="seconds"),
        month_end_date=month_end,
        config_rejects=check_rejects + catalog_rejects,
    )

    # Filter by selected check types and optional app_code (Run Control).
    selected_checks = [c for c in checks if c.check_type in selected]
    if cfg.app_code is not None:
        selected_checks = [c for c in selected_checks if c.app_code == cfg.app_code]

    history = ResultHistory.load(cfg.results_csv)

    # Resolve + read each referenced partition once (cache by table_name).
    source_cache: Dict[str, object] = {}

    for check in selected_checks:
        outcome = _execute_one(
            check, catalog, source_cache, history, run_ts, cfg.partition
        )
        issue_status = classify(
            history, check.table_name, check.column_name, check.check_type,
            check.app_code, outcome.status, run_ts,
        )
        partition_value = source_cache.get(f"{check.table_name}::partition")
        entry: Optional[SourceEntry] = catalog.get(check.table_name)
        result = DqResult(
            dq_ref_id=str(uuid.uuid4()),
            check_id=check.check_id,
            app_code=check.app_code,
            run_id=run_id,
            run_timestamp=report.run_timestamp,
            month_end_date=month_end,
            table_name=check.table_name,
            source_type=entry.source_type if entry else "parquet",
            column_name=check.column_name,
            partition_value=partition_value,
            check_type=check.check_type,
            evaluation_mode=check.evaluation_mode,
            status=outcome.status,
            issue_status=issue_status,
            violation_count=outcome.violation_count,
            current_value=outcome.current_value,
            prior_value=outcome.prior_value,
            deviation=outcome.deviation,
            description=outcome.description,
        )
        _tally(report, result)
        report.results.append(result)

    result_writer.append_results(cfg.results_csv, report.results)
    result_writer.write_exceptions(cfg.exceptions_csv, report.results)
    return report


def _execute_one(check, catalog, source_cache, history, run_ts, requested_partition):
    """Execute a single check with full error isolation."""
    from .check_executor import ExecOutcome

    entry = catalog.get(check.table_name)
    if entry is None:
        return ExecOutcome(
            "errored",
            description=f"no source catalog entry for table '{check.table_name}'",
        )

    cache_key = f"{check.table_name}::ddf"
    part_key = f"{check.table_name}::partition"
    if cache_key not in source_cache:
        try:
            ddf, partition_value = source_reader.resolve_and_read(
                entry, requested_partition
            )
        except source_reader.SourceError as exc:
            source_cache[cache_key] = None
            source_cache[part_key] = None
            source_cache[f"{check.table_name}::error"] = str(exc)
        else:
            source_cache[cache_key] = ddf
            source_cache[part_key] = partition_value

    ddf = source_cache.get(cache_key)
    if ddf is None:
        msg = source_cache.get(f"{check.table_name}::error", "source unavailable")
        return ExecOutcome("errored", description=f"source read failed: {msg}")

    try:
        return execute_check(check, ddf, history, run_ts)
    except Exception as exc:  # noqa: BLE001 — isolate per-check failures
        return ExecOutcome(
            "errored",
            description=f"check '{check.check_id}' on '{check.table_name}' raised: {exc}",
        )


def _tally(report: DqReport, result: DqResult) -> None:
    report.executed += 1
    if result.status == "passed":
        report.passed += 1
    elif result.status == "failed":
        report.failed += 1
    elif result.status == "errored":
        report.errored += 1
    if result.issue_status == "new":
        report.new_issues += 1
    elif result.issue_status == "recurring":
        report.recurring_issues += 1
    elif result.issue_status == "resolved":
        report.resolved_issues += 1
