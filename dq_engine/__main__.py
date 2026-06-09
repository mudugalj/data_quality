"""Command-line entrypoint for the DQ Engine MVP.

Example:
    python -m dq_engine \
        --check-types completeness,validity,consistency \
        --partition latest \
        --checks config/dq_checks.csv \
        --catalog config/source_catalog.csv \
        --results output/results.csv \
        --exceptions output/exceptions.csv
"""

from __future__ import annotations

import argparse
import sys
import warnings

from .engine import RunConfig, run, TerminatedRun, DqReport

warnings.filterwarnings("ignore")


def _parse_args(argv):
    p = argparse.ArgumentParser(prog="dq_engine", description="Parquet DQ Engine (Dask)")
    p.add_argument("--check-types", default="completeness,validity,consistency",
                   help="comma-separated: completeness,validity,consistency")
    p.add_argument("--partition", default="latest",
                   help="partition value to process, or 'latest'")
    p.add_argument("--app-code", default=None, help="restrict run to one app_code")
    p.add_argument("--checks", default="config/dq_checks.csv")
    p.add_argument("--catalog", default="config/source_catalog.csv")
    p.add_argument("--results", default="output/results.csv")
    p.add_argument("--exceptions", default="output/exceptions.csv")
    return p.parse_args(argv)


def _print_report(report: DqReport) -> None:
    print("=" * 64)
    print("DQ RUN REPORT")
    print("=" * 64)
    print(f"  run_id          : {report.run_id}")
    print(f"  run_timestamp   : {report.run_timestamp}")
    print(f"  month_end_date  : {report.month_end_date}")
    print("-" * 64)
    print(f"  executed        : {report.executed}")
    print(f"    passed        : {report.passed}")
    print(f"    failed        : {report.failed}")
    print(f"    errored       : {report.errored}")
    print("-" * 64)
    print(f"  new issues      : {report.new_issues}")
    print(f"  recurring issues: {report.recurring_issues}")
    print(f"  resolved issues : {report.resolved_issues}")
    if report.config_rejects:
        print("-" * 64)
        print(f"  config rejects  : {len(report.config_rejects)}")
        for msg in report.config_rejects:
            print(f"    - {msg}")
    if report.exception_results:
        print("-" * 64)
        print("  EXCEPTIONS (failed / errored):")
        for r in report.exception_results:
            tag = f"[{r.issue_status}]" if r.issue_status else ""
            print(f"    - {r.check_id} ({r.table_name}.{r.column_name}) "
                  f"{r.status} {tag} :: {r.description}")
    print("=" * 64)


def main(argv=None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    check_types = [c.strip() for c in args.check_types.split(",") if c.strip()]
    cfg = RunConfig(
        checks_csv=args.checks,
        catalog_csv=args.catalog,
        results_csv=args.results,
        exceptions_csv=args.exceptions,
        selected_check_types=check_types,
        partition=args.partition,
        app_code=args.app_code,
    )
    try:
        report = run(cfg)
    except TerminatedRun as exc:
        print(f"RUN TERMINATED: {exc}", file=sys.stderr)
        return 2
    _print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
