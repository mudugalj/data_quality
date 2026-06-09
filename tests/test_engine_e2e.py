import csv
import os
from datetime import datetime

import pandas as pd

from dq_engine.engine import RunConfig, run, TerminatedRun
from dq_engine.config_loader import CHECK_FIELDS, SOURCE_FIELDS
from dq_engine.domain import RESULT_COLUMNS

import pytest

RUN_TS = datetime(2026, 6, 9, 9, 0, 0)


def _build_parquet(root):
    part = os.path.join(root, "customers", "date=2026-06-09")
    os.makedirs(part, exist_ok=True)
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@x.com", None, "c@x.com", "d@x.com"],   # 1 null -> completeness fail
        "status": ["active", "active", "inactive", "active"],
        "age": [30, 45, 28, 200],                            # 200 -> range fail; avg 75.75
    })
    df.to_parquet(os.path.join(part, "part.parquet"), index=False)


def _write(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def _setup(tmp_path):
    data_root = tmp_path / "data"
    _build_parquet(str(data_root))

    checks = tmp_path / "dq_checks.csv"
    _write(checks, CHECK_FIELDS, [
        ["k_email", "customers", "email", "", "completeness", "", "", "0",
         "SELECT * FROM customers WHERE email IS NULL", "violation", "CRM"],
        ["k_age", "customers", "age", "", "validity", "", "", "0",
         "SELECT * FROM customers WHERE age < 0 OR age > 120", "violation", "CRM"],
        ["k_avg", "customers", "age", "", "consistency", "", "", "10",
         "SELECT AVG(age) AS v FROM customers", "consistency_mom", "CRM"],
        ["k_bad", "customers", "nope", "", "validity", "", "", "0",
         "SELECT * FROM customers WHERE nope IS NULL", "violation", "CRM"],
    ])

    catalog = tmp_path / "source_catalog.csv"
    _write(catalog, SOURCE_FIELDS, [
        ["customers", "parquet", str(data_root / "customers"), "date"],
    ])

    results = tmp_path / "results.csv"
    _write(results, RESULT_COLUMNS, [])
    # seed: prior email failure (-> recurring) + MoM baseline avg 70 (-> within 10%)
    with open(results, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        w.writerow({**{c: "" for c in RESULT_COLUMNS},
                    "check_id": "k_email", "table_name": "customers",
                    "column_name": "email", "check_type": "completeness",
                    "app_code": "CRM", "evaluation_mode": "violation",
                    "status": "failed", "run_timestamp": "2026-05-10T06:00:00"})
        w.writerow({**{c: "" for c in RESULT_COLUMNS},
                    "check_id": "k_avg", "table_name": "customers",
                    "column_name": "age", "check_type": "consistency",
                    "app_code": "CRM", "evaluation_mode": "consistency_mom",
                    "status": "passed", "current_value": "70",
                    "run_timestamp": "2026-05-10T06:00:00"})

    return RunConfig(
        checks_csv=str(checks), catalog_csv=str(catalog),
        results_csv=str(results), exceptions_csv=str(tmp_path / "exceptions.csv"),
        selected_check_types=["completeness", "validity", "consistency"],
        partition="latest", run_timestamp=RUN_TS,
    )


def test_end_to_end_statuses_and_classification(tmp_path):
    cfg = _setup(tmp_path)
    report = run(cfg)

    by_id = {r.check_id: r for r in report.results}
    assert report.executed == 4
    assert by_id["k_email"].status == "failed"
    assert by_id["k_email"].issue_status == "recurring"
    assert by_id["k_age"].status == "failed"
    assert by_id["k_age"].issue_status == "new"
    assert by_id["k_avg"].status == "passed"          # avg 75.75 vs 70 -> 8.2% <= 10%
    assert by_id["k_bad"].status == "errored"          # missing column, isolated
    assert by_id["k_bad"].issue_status == "new"
    assert report.month_end_date == "2026-06-30"


def test_outputs_written(tmp_path):
    cfg = _setup(tmp_path)
    run(cfg)
    assert os.path.exists(cfg.exceptions_csv)
    with open(cfg.exceptions_csv, newline="", encoding="utf-8") as fh:
        exc = list(csv.DictReader(fh))
    # failed (email, age) + errored (bad) = 3 exceptions; passed avg excluded
    assert {r["check_id"] for r in exc} == {"k_email", "k_age", "k_bad"}


def test_app_code_filter(tmp_path):
    cfg = _setup(tmp_path)
    cfg.app_code = "NO_SUCH_APP"
    report = run(cfg)
    assert report.executed == 0


def test_invalid_check_type_terminates(tmp_path):
    cfg = _setup(tmp_path)
    cfg.selected_check_types = ["bogus"]
    with pytest.raises(TerminatedRun):
        run(cfg)
