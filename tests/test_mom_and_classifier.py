from datetime import datetime

from dq_engine.history import ResultHistory
from dq_engine.mom_evaluator import evaluate_mom
from dq_engine.issue_classifier import classify

RUN = datetime(2026, 6, 9, 6, 0, 0)


def _hist(rows):
    h = ResultHistory(rows=[])
    parsed = []
    for r in rows:
        r = dict(r)
        r["_ts"] = datetime.fromisoformat(r["run_timestamp"])
        r["_current_value"] = float(r["current_value"]) if r.get("current_value") not in (None, "") else None
        parsed.append(r)
    return ResultHistory(parsed)


def _mom_row(value, ts="2026-05-10T06:00:00", status="passed"):
    return {
        "table_name": "customers", "column_name": "age", "check_type": "consistency",
        "app_code": "CRM", "evaluation_mode": "consistency_mom",
        "status": status, "run_timestamp": ts, "current_value": value,
    }


def test_mom_within_tolerance_passes():
    h = _hist([_mom_row(62.0)])
    out = evaluate_mom(h, "customers", "age", "consistency", "CRM", 64.67, 10, RUN)
    assert out.status == "passed"
    assert out.prior_value == 62.0


def test_mom_over_tolerance_fails():
    h = _hist([_mom_row(1200.0)])
    out = evaluate_mom(h, "customers", "age", "consistency", "CRM", 954.5, 15, RUN)
    assert out.status == "failed"
    assert round(out.deviation, 2) == 20.46


def test_mom_no_baseline_fails():
    out = evaluate_mom(_hist([]), "customers", "age", "consistency", "CRM", 50.0, 10, RUN)
    assert out.status == "failed"
    assert "no prior MoM baseline" in out.description


def test_mom_prior_zero_fails():
    h = _hist([_mom_row(0.0)])
    out = evaluate_mom(h, "customers", "age", "consistency", "CRM", 5.0, 10, RUN)
    assert out.status == "failed"
    assert "zero" in out.description


def test_mom_non_numeric_current_errors():
    h = _hist([_mom_row(62.0)])
    out = evaluate_mom(h, "customers", "age", "consistency", "CRM", None, 10, RUN)
    assert out.status == "errored"


def test_mom_outside_window_is_no_baseline():
    # prior result 60 days ago is outside the 28-31 day window
    h = _hist([_mom_row(62.0, ts="2026-04-10T06:00:00")])
    out = evaluate_mom(h, "customers", "age", "consistency", "CRM", 64.0, 10, RUN)
    assert out.status == "failed"
    assert "no prior MoM baseline" in out.description


def _prior(status, ts="2026-05-10T06:00:00"):
    return {
        "table_name": "customers", "column_name": "email", "check_type": "completeness",
        "app_code": "CRM", "evaluation_mode": "violation",
        "status": status, "run_timestamp": ts, "current_value": "",
    }


def test_classify_recurring():
    h = _hist([_prior("failed")])
    assert classify(h, "customers", "email", "completeness", "CRM", "failed", RUN) == "recurring"


def test_classify_new_no_prior():
    assert classify(_hist([]), "customers", "email", "completeness", "CRM", "failed", RUN) == "new"


def test_classify_new_prior_passed():
    h = _hist([_prior("passed")])
    assert classify(h, "customers", "email", "completeness", "CRM", "failed", RUN) == "new"


def test_classify_resolved():
    h = _hist([_prior("failed")])
    assert classify(h, "customers", "email", "completeness", "CRM", "passed", RUN) == "resolved"


def test_classify_none_when_passed_clean():
    assert classify(_hist([]), "customers", "email", "completeness", "CRM", "passed", RUN) is None


def test_classify_errored_is_open():
    h = _hist([_prior("failed")])
    assert classify(h, "customers", "email", "completeness", "CRM", "errored", RUN) == "recurring"
