# Implementation Plan — DQ Engine MVP (Parquet · Dask · CSV)

> All `Req N.x` references point to the numbered acceptance criteria in
> `requirements.md`. `[x]` = implemented and verified; `[ ]` = not started.

## Key invariants
- All checks are SQL-driven; `check_type` is a label only.
- `evaluation_mode ∈ {violation, consistency_mom}`.
- Results CSV is the single history source for MoM and classification.
- Issue classification keys on `(table_name, column_name, check_type, app_code)`.
- Config errors are fatal; per-check and per-source errors are isolated.

---

### Task 1 — Project Scaffold & Environment `[x]`

**Design refs:** Technology Stack
**Requirements:** —

- `requirements.txt` with the pinned compatible stack (dask 2024.5.1, dask-expr
  1.1.1, dask-sql 2024.5.0, pandas 2.2.2, pyarrow, pytest).
- `.venv` virtual environment; `.gitignore`.
- Package layout: `dq_engine/` (engine modules), `config/`, `data/`, `output/`,
  `tests/`.
- **Verified:** dask-sql smoke test runs a SELECT filter and an aggregate.

---

### Task 2 — Domain Model `[x]`

**Design refs:** Data Model
**Requirements:** Req 2.2–2.3, Req 3, Req 9.3, Req 10

- `dq_engine/domain.py`: controlled vocabularies (`CHECK_TYPES`,
  `EVALUATION_MODES`, `SOURCE_TYPES`, `STATUSES`, `ISSUE_STATUSES`,
  `OPEN_STATUSES`); dataclasses `CheckDefinition`, `SourceEntry`, `DqResult`
  (with `month_end_date`, `issue_status`); `RESULT_COLUMNS` (CSV schema).

---

### Task 3 — Config Loading (CSV) `[x]`

**Design refs:** config_loader
**Requirements:** Req 2, Req 1.1, Req 3.1, Req 12.1

- `load_checks(path)` — validates required fields, `check_type`,
  `evaluation_mode`, numeric `deviation_tolerance`, duplicate `check_id`;
  per-row rejection messages; blanks → None; tolerance defaults to 0.
- `load_catalog(path)` — validates `source_type == parquet`, parquet fields;
  drops duplicate tables; missing header column → `ConfigError`.
- **Tests:** `tests/test_config_loader.py` — valid + optional fields; missing
  required; bad enums (incl. `consistency_delta` rejected); duplicate `check_id`;
  catalog rejections (unsupported source_type, missing parquet fields).

---

### Task 4 — Parquet Source Reading (Dask) `[x]`

**Design refs:** source_reader
**Requirements:** Req 1, Req 12.2

- Partition discovery; specific + `latest` selection; `SourceError` on missing /
  empty partitions.
- Reads the partition **directory** (string path) into a Dask DataFrame; drops
  the redundant Hive partition column; casts `category` columns to strings (both
  needed for dask-sql compatibility).
- **Verified:** via the end-to-end engine test and the demo run (`latest`
  resolves to `date=2026-06-09`).

---

### Task 5 — Check Execution: All Modes + Classification `[x]`

**Design refs:** sql_safety, check_executor, mom_evaluator, history,
issue_classifier
**Requirements:** Req 4, 5, 6, 7, 8

- `sql_safety` — read-only gate (single SELECT/CTE; denylist; no multi-statement)
  for `check_sql` and `business_rule_filter`.
- `check_executor` — safety gate → optional filter scoping → `ctx.sql(...)`
  → judge by `evaluation_mode`; per-check errors isolated as errored.
- `mom_evaluator` — 28–31 day baseline lookup; pct deviation vs tolerance;
  no-baseline / prior-zero → failed; non-numeric → errored.
- `history` — `ResultHistory` over `results.csv`; `most_recent_before`,
  `mom_baseline`.
- `issue_classifier` — new / recurring / resolved / None, keyed on
  `(table, column, check_type, app_code)`, bounded to the retention window.
- **Tests:** `tests/test_sql_safety.py`, `tests/test_mom_and_classifier.py`
  (within/over tolerance, no baseline, prior zero, non-numeric, outside window;
  recurring/new/resolved/None; errored is open).

---

### Task 6 — Result Persistence & Reporting (CSV) `[x]`

**Design refs:** result_writer, engine (DqReport)
**Requirements:** Req 9, Req 10

- `result_writer.append_results` (header when new/empty) +
  `write_exceptions` (overwrite with failed/errored).
- `DqReport` with status counts, new/recurring/resolved counts, and
  `month_end_date`; `executed = passed + failed + errored`.
- **Verified:** demo run writes `output/results.csv` (appended) and
  `output/exceptions.csv` (5 exception rows) with `month_end_date` populated.

---

### Task 7 — Engine Orchestration, CLI & Mock Data `[x]`

**Design refs:** engine, __main__
**Requirements:** Req 10, 11, 12, Req 3.2–3.3

- `engine.run(RunConfig)` — full lifecycle; per-table partition caching;
  per-check error isolation; `TerminatedRun` on fatal config / bad check-types.
- `dq_engine/__main__.py` — CLI (`--check-types`, `--partition`, `--app-code`,
  `--checks`, `--catalog`, `--results`, `--exceptions`); prints the run report.
- Mock data: `data/create_parquet.py` (customers + orders, two partitions each),
  `data/seed_history.py` (prior run ~30 days ago for MoM + recurring/resolved).
- Sample config: `config/dq_checks.csv` (10 checks across 3 check_types, both
  evaluation_modes, 2 app codes, plus one intentionally-broken check),
  `config/source_catalog.csv`.
- **Tests:** `tests/test_engine_e2e.py` — statuses + classification
  (failed/recurring, failed/new, MoM passed, errored/new), exceptions file
  contents, app-code filter excludes all, invalid check-type terminates.
- **End-to-end demo result:** 10 executed → 5 passed / 4 failed / 1 errored;
  1 recurring, 1 resolved, 4 new; MoM pass (C5, 4.3%) and MoM fail (O3, 20.46%).

---

## Verification

```bash
PYTHONPATH=. .venv/bin/python -m pytest -q          # 36 passed
.venv/bin/python data/create_parquet.py
PYTHONPATH=. .venv/bin/python data/seed_history.py
PYTHONPATH=. .venv/bin/python -m dq_engine \
    --check-types completeness,validity,consistency --partition latest
```

---

## Out of Scope (Post-MVP) — not implemented

- **Additional source types** — MariaDB/MySQL (JDBC), Hive (HiveServer2).
- **Database result store** and **config-versioning store** — CSV used instead.
- **Scheduling** — external cron/Airflow invokes the stateless CLI.
- **Case management** — removed from scope.
