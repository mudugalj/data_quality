"""Seed output/results.csv with a prior run (~30 days ago).

This gives the current run a history to look back into, so that:
  - month-on-month consistency checks find a baseline in the 28-31 day window, and
  - issue classification can produce 'recurring' and 'resolved' (not just 'new').

The prior run is stamped 2026-05-10 (inside the 28-31 day MoM window relative to
a 2026-06-09 current run). Re-run this before re-running the engine to reset the
demo to a known starting history. Run with the project venv:

    .venv/bin/python data/seed_history.py
"""

from __future__ import annotations

import csv
import os

from dq_engine.domain import RESULT_COLUMNS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "output", "results.csv")

PRIOR_TS = "2026-05-10T06:00:00"
PRIOR_MONTH_END = "2026-05-31"

# Prior-run results. Together with the current run these demonstrate:
#   C1 (email completeness)   prior FAILED  -> current FAILED  => recurring
#   C2 (status validity)      prior FAILED  -> current PASSED  => resolved
#   C5 (avg-age MoM)          prior PASSED, baseline 62.0      => MoM baseline
#   O3 (sum-amount MoM)       prior PASSED, baseline 1200.0    => MoM baseline + new
PRIOR_ROWS = [
    {
        "dq_ref_id": "seed-c1", "check_id": "C1_customers_email_not_null",
        "app_code": "CRM", "run_id": "seed-run", "run_timestamp": PRIOR_TS,
        "month_end_date": PRIOR_MONTH_END, "table_name": "customers",
        "source_type": "parquet", "column_name": "email", "partition_value": PRIOR_TS[:10],
        "check_type": "completeness", "evaluation_mode": "violation",
        "status": "failed", "issue_status": "new", "violation_count": 1,
        "description": "seed: prior email completeness failure",
    },
    {
        "dq_ref_id": "seed-c2", "check_id": "C2_customers_status_allowed",
        "app_code": "CRM", "run_id": "seed-run", "run_timestamp": PRIOR_TS,
        "month_end_date": PRIOR_MONTH_END, "table_name": "customers",
        "source_type": "parquet", "column_name": "status", "partition_value": PRIOR_TS[:10],
        "check_type": "validity", "evaluation_mode": "violation",
        "status": "failed", "issue_status": "new", "violation_count": 2,
        "description": "seed: prior status validity failure",
    },
    {
        "dq_ref_id": "seed-c5", "check_id": "C5_customers_avg_age_mom",
        "app_code": "CRM", "run_id": "seed-run", "run_timestamp": PRIOR_TS,
        "month_end_date": PRIOR_MONTH_END, "table_name": "customers",
        "source_type": "parquet", "column_name": "age", "partition_value": PRIOR_TS[:10],
        "check_type": "consistency", "evaluation_mode": "consistency_mom",
        "status": "passed", "issue_status": "", "current_value": 62.0,
        "description": "seed: prior avg(age) baseline",
    },
    {
        "dq_ref_id": "seed-o3", "check_id": "O3_orders_sum_amount_mom",
        "app_code": "ORDER_MGMT", "run_id": "seed-run", "run_timestamp": PRIOR_TS,
        "month_end_date": PRIOR_MONTH_END, "table_name": "orders",
        "source_type": "parquet", "column_name": "amount", "partition_value": PRIOR_TS[:10],
        "check_type": "consistency", "evaluation_mode": "consistency_mom",
        "status": "passed", "issue_status": "", "current_value": 1200.0,
        "description": "seed: prior sum(amount) baseline",
    },
]


def main() -> None:
    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    with open(RESULTS, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        for row in PRIOR_ROWS:
            writer.writerow({col: row.get(col, "") for col in RESULT_COLUMNS})
    print(f"seeded {len(PRIOR_ROWS)} prior result(s) -> {os.path.relpath(RESULTS, ROOT)}")


if __name__ == "__main__":
    main()
