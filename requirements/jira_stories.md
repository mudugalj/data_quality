# Jira Stories — DQ Engine MVP (Parquet · Dask · CSV)

> Derived from `requirements.md` and `tasks.md`. Each story traces back to
> requirement acceptance criteria (`Req N.x`) and implementation tasks.
> **Status legend:** `To Do` · `In Progress` · `Done`.
> The MVP is implemented and verified, so stories are marked `Done` where the
> corresponding task is complete.

## Story-point scale

Fibonacci, relative sizing: `1` trivial · `2` small · `3` moderate · `5` sizeable ·
`8` large/uncertain.

---

## Epic DQ-E1 — Foundation & Configuration

**Goal:** Stand up the project and read/validate all configuration from CSV.

---

### DQ-1 — Project scaffold and pinned environment
- **Type:** Story · **Points:** 2 · **Status:** Done
- **Epic:** DQ-E1 · **Task:** Task 1

**As a** data engineer
**I want** a reproducible Python project with a pinned dependency stack
**so that** the Dask + dask-sql engine runs the same way on every machine.

**Acceptance criteria**
- [ ] `requirements.txt` pins a mutually-compatible stack (dask 2024.5.1,
      dask-expr 1.1.1, dask-sql 2024.5.0, pandas 2.2.2, pyarrow, pytest).
- [ ] A virtual environment installs cleanly from `requirements.txt`.
- [ ] Package layout exists: `dq_engine/`, `config/`, `data/`, `output/`, `tests/`.
- [ ] A dask-sql smoke test runs a `SELECT` filter and an aggregate successfully.

**Notes:** dask-sql 2024.5.0 requires the `dask_expr` module removed from newer
dask cores — pinning is mandatory, not optional.

---

### DQ-2 — Domain model
- **Type:** Story · **Points:** 2 · **Status:** Done
- **Epic:** DQ-E1 · **Task:** Task 2 · **Req:** 2.2–2.3, 3, 9.3, 10

**As a** developer
**I want** typed domain objects and a single result schema
**so that** checks, sources, and results are consistent across the engine and CSV.

**Acceptance criteria**
- [ ] Controlled vocabularies defined: `check_type`, `evaluation_mode`,
      `source_type`, `status`, `issue_status`, and the open set `{failed, errored}`.
- [ ] `CheckDefinition`, `SourceEntry`, and `DqResult` dataclasses exist.
- [ ] `DqResult` includes `month_end_date` and an optional `issue_status`.
- [ ] `RESULT_COLUMNS` defines the exact results/exceptions CSV column order.

---

### DQ-3 — Load and validate check definitions from CSV
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E1 · **Task:** Task 3 · **Req:** 2.1–2.7, 3.1, 12.1

**As a** data engineer
**I want** `config/dq_checks.csv` parsed and validated row by row
**so that** bad check definitions are rejected with clear messages without
stopping the run.

**Acceptance criteria**
- [ ] Header row is excluded; the 11 documented columns are recognised.
- [ ] Required fields enforced: `check_id, table_name, column_name, check_type,
      evaluation_mode, check_sql`; missing fields named in the rejection message.
- [ ] `check_type ∈ {completeness, validity, consistency}` (case-sensitive).
- [ ] `evaluation_mode ∈ {violation, consistency_mom}` (case-sensitive);
      `consistency_delta` and other legacy values are rejected.
- [ ] Non-numeric `deviation_tolerance` rejected; blank defaults to 0.
- [ ] Duplicate `check_id` rejected.
- [ ] A missing required header column raises a fatal `ConfigError`.
- [ ] Covered by `tests/test_config_loader.py`.

---

### DQ-4 — Load and validate the source catalog from CSV
- **Type:** Story · **Points:** 2 · **Status:** Done
- **Epic:** DQ-E1 · **Task:** Task 3 · **Req:** 1.1, 12.1

**As a** data engineer
**I want** `config/source_catalog.csv` validated for Parquet sources
**so that** only well-formed source mappings reach the reader.

**Acceptance criteria**
- [ ] Columns recognised: `table_name, source_type, parquet_location, partition_column`.
- [ ] `source_type` must equal `parquet`; any other value rejects that row.
- [ ] Parquet rows require `parquet_location` and `partition_column`.
- [ ] Duplicate `table_name` entries cause that table to be dropped (ambiguous).
- [ ] Covered by `tests/test_config_loader.py`.

---

## Epic DQ-E2 — Parquet Ingestion (Dask)

**Goal:** Resolve and read the correct Parquet partition into a Dask DataFrame.

---

### DQ-5 — Resolve and read a Parquet partition
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E2 · **Task:** Task 4 · **Req:** 1.1–1.8, 12.2

**As a** data engineer
**I want** the engine to pick a partition and load it with Dask
**so that** checks run against the right slice of data.

**Acceptance criteria**
- [ ] Discovers `partition_column=value` directories under `parquet_location`.
- [ ] A specific partition value reads only that directory.
- [ ] `latest` / absent selects the lexicographically greatest partition.
- [ ] The partition loads into a Dask DataFrame registered under `table_name`.
- [ ] The redundant Hive partition column is dropped; `category` columns cast to
      string (dask-sql compatibility).
- [ ] Missing or empty partitions raise `SourceError` → affected checks errored,
      run continues.
- [ ] `partition_value` recorded on every result.

---

## Epic DQ-E3 — Check Execution & Evaluation

**Goal:** Execute every `check_sql` read-only and judge it by evaluation mode.

---

### DQ-6 — Read-only SQL safety gate
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E3 · **Task:** Task 5 · **Req:** 7.2–7.3, 5.3.3

**As a** data engineer
**I want** `check_sql` and `business_rule_filter` validated as read-only
**so that** no check can modify data.

**Acceptance criteria**
- [ ] Accepts a single `SELECT`/`WITH` query (with optional trailing `;`).
- [ ] Rejects DML/DDL (INSERT/UPDATE/DELETE/MERGE/CREATE/ALTER/DROP/TRUNCATE/…).
- [ ] Rejects multi-statement batches and empty/blank SQL.
- [ ] `business_rule_filter` rejected if it contains `;` or a denylisted keyword.
- [ ] A rejected check is marked `errored`, not executed.
- [ ] Covered by `tests/test_sql_safety.py`.

**Notes:** Lightweight keyword/structural gate for the MVP; a structural parser
(e.g. sqlglot) is the documented post-MVP hardening step.

---

### DQ-7 — Completeness and validity checks (violation mode)
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E3 · **Task:** Task 5 · **Req:** 4, 5, 7.1, 7.4

**As a** data quality analyst
**I want** completeness and validity checks to flag violating rows
**so that** nulls, out-of-domain values, and out-of-range values are detected.

**Acceptance criteria**
- [ ] `check_sql` runs via dask-sql against the registered partition.
- [ ] Violation mode: 0 rows → `passed`; ≥1 rows → `failed`; SQL/safety/filter
      error → `errored`.
- [ ] `violation_count` recorded.
- [ ] `business_rule_filter`, when present, scopes rows before evaluation
      (e.g., nulls only where `status = 'active'`).
- [ ] A failing check does not abort the run (per-check isolation).

---

### DQ-8 — Month-on-month consistency checks
- **Type:** Story · **Points:** 5 · **Status:** Done
- **Epic:** DQ-E3 · **Task:** Task 5 · **Req:** 6, 12.4

**As a** data quality analyst
**I want** consistency checks to compare an aggregate to ~30 days prior
**so that** unexpected month-on-month shifts are caught.

**Acceptance criteria**
- [ ] `check_sql` returns a single numeric aggregate.
- [ ] Baseline is the most recent `consistency_mom` result in
      `[run − 31d, run − 28d]`, read from `output/results.csv`.
- [ ] `pct_deviation = |current − prior| / |prior| × 100`.
- [ ] `≤ tolerance` → passed; `> tolerance` → failed.
- [ ] No baseline in window → failed with reason; prior value 0 → failed with reason.
- [ ] Non-numeric / missing current value → errored; unreadable history → errored.
- [ ] `current_value`, `prior_value`, `deviation` recorded.
- [ ] Covered by `tests/test_mom_and_classifier.py`.

---

### DQ-9 — Issue classification (new / recurring / resolved)
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E3 · **Task:** Task 5 · **Req:** 8

**As a** data quality analyst
**I want** each result classified against history, keyed per application
**so that** I can tell persistent problems from newly introduced ones.

**Acceptance criteria**
- [ ] Keyed on `(table_name, column_name, check_type, app_code)` within the
      retention window; open = `{failed, errored}`.
- [ ] open + prior open → `recurring`.
- [ ] open + (no prior | passed) → `new`.
- [ ] passed + prior open → `resolved`.
- [ ] passed + (no prior | passed) → no `issue_status` (blank).
- [ ] Covered by `tests/test_mom_and_classifier.py`.

---

## Epic DQ-E4 — Persistence, Reporting & Orchestration

**Goal:** Write results/exceptions to CSV, report the run, and drive it from a CLI.

---

### DQ-10 — Write results and exceptions to CSV
- **Type:** Story · **Points:** 2 · **Status:** Done
- **Epic:** DQ-E4 · **Task:** Task 6 · **Req:** 9

**As a** data engineer
**I want** every result appended to a history CSV and exceptions written separately
**so that** results persist and the failed/errored subset is easy to review.

**Acceptance criteria**
- [ ] One row per executed check appended to `output/results.csv` (header when new).
- [ ] Each row carries a UUID `dq_ref_id` and all documented result columns.
- [ ] `output/exceptions.csv` overwritten each run with failed/errored rows only.
- [ ] `output/results.csv` is the history source for MoM (DQ-8) and classification
      (DQ-9).

---

### DQ-11 — Run report with month-end date
- **Type:** Story · **Points:** 2 · **Status:** Done
- **Epic:** DQ-E4 · **Task:** Task 6, Task 7 · **Req:** 10

**As a** data quality analyst
**I want** a run report with status and issue counts and the month-end date
**so that** I can see the run outcome at a glance for the reporting period.

**Acceptance criteria**
- [ ] Report includes `run_id`, `run_timestamp`, `month_end_date`.
- [ ] Counts: executed / passed / failed / errored, with
      `executed = passed + failed + errored`.
- [ ] Counts: new / recurring / resolved issues.
- [ ] Skipped checks (by type or app code) are not counted.
- [ ] No checks executed → all counts zero.

---

### DQ-12 — Engine orchestration, CLI, run control & error isolation
- **Type:** Story · **Points:** 5 · **Status:** Done
- **Epic:** DQ-E4 · **Task:** Task 7 · **Req:** 11, 12, 3.2–3.3

**As a** data engineer
**I want** a CLI that runs the full lifecycle with run-control parameters
**so that** I can run targeted checks and trust that one failure won't abort the run.

**Acceptance criteria**
- [ ] CLI flags: `--check-types`, `--partition`, `--app-code`, `--checks`,
      `--catalog`, `--results`, `--exceptions`.
- [ ] `--check-types` validated; empty/unknown value terminates before execution.
- [ ] `--app-code` filters to one application (exact, case-sensitive).
- [ ] Config CSV / header errors terminate the run with no report.
- [ ] Per-check and per-source errors are isolated as `errored`; the run continues.
- [ ] Covered by `tests/test_engine_e2e.py`.

---

### DQ-13 — Mock data and seeded history for the demo
- **Type:** Story · **Points:** 3 · **Status:** Done
- **Epic:** DQ-E4 · **Task:** Task 7

**As a** developer
**I want** mock Parquet datasets and a seeded prior run
**so that** the full set of outcomes (incl. MoM, recurring, resolved) is
demonstrable end-to-end.

**Acceptance criteria**
- [ ] `data/create_parquet.py` generates `customers` and `orders`, two partitions each.
- [ ] `data/seed_history.py` seeds a prior run (~30 days ago) for MoM baselines and
      recurring/resolved classification.
- [ ] `config/dq_checks.csv` includes all 3 check types, both evaluation modes,
      ≥2 app codes, and one intentionally-broken check (error-isolation demo).
- [ ] Demo run: 10 executed → 5 passed / 4 failed / 1 errored;
      1 recurring, 1 resolved, 4 new; MoM pass and MoM fail both shown.

---

## Epic DQ-E5 — Post-MVP (Backlog, not in MVP scope)

> Captured for planning only. **Not implemented** in the MVP.

| ID | Story | Points (est.) | Status |
|----|-------|---------------|--------|
| DQ-14 | Add MariaDB/MySQL source reader (JDBC) | 8 | To Do |
| DQ-15 | Add Hive (HiveServer2) source reader | 8 | To Do |
| DQ-16 | Replace CSV stores with a database result/config store | 8 | To Do |
| DQ-17 | Config versioning store and version selection | 5 | To Do |
| DQ-18 | Structural SQL parser for the safety gate (sqlglot) | 3 | To Do |
| DQ-19 | Scheduler integration (cron/Airflow invoking the CLI) | 3 | To Do |
| DQ-20 | Upgrade to current dask + dask-sql (drop dask_expr pin) | 5 | To Do |

> **Removed from scope:** case management.

---

## Traceability summary

| Story | Requirement | Task |
|-------|-------------|------|
| DQ-1  | — | Task 1 |
| DQ-2  | Req 2.2–2.3, 3, 9.3, 10 | Task 2 |
| DQ-3  | Req 2.1–2.7, 3.1, 12.1 | Task 3 |
| DQ-4  | Req 1.1, 12.1 | Task 3 |
| DQ-5  | Req 1, 12.2 | Task 4 |
| DQ-6  | Req 7.2–7.3, 5.3.3 | Task 5 |
| DQ-7  | Req 4, 5, 7.1, 7.4 | Task 5 |
| DQ-8  | Req 6, 12.4 | Task 5 |
| DQ-9  | Req 8 | Task 5 |
| DQ-10 | Req 9 | Task 6 |
| DQ-11 | Req 10 | Task 6, Task 7 |
| DQ-12 | Req 11, 12, 3.2–3.3 | Task 7 |
| DQ-13 | — | Task 7 |
