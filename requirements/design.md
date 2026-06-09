# Design Document — DQ Engine MVP (Parquet · Dask · CSV)

> All `Req N.x` references point to the numbered acceptance criteria in
> `requirements.md`.

## Overview

The DQ Engine MVP is a Python application that executes user-supplied SQL checks
(`check_sql`) against **partitioned Parquet datasets** using **Dask** and
**dask-sql**, and writes outcomes to **CSV**. There is no database.

Each Parquet partition is loaded into a Dask DataFrame and registered as a
dask-sql table named after the logical `table_name`; the user's `check_sql`
references it directly. Results are judged in two modes — `violation` (returns
bad rows) and `consistency_mom` (compares an aggregate to ~30 days prior, read
from the results history CSV). Every result is classified **new / recurring /
resolved** and the run report carries the **month-end date**.

**Key invariants**

- All checks are SQL-driven; `check_type` is a classification label only.
- `evaluation_mode` (`violation` | `consistency_mom`) decides how the SQL result
  is judged.
- The results CSV is the single source of history for both MoM baselines and
  issue classification.
- Issue classification keys on `(table_name, column_name, check_type, app_code)`.
- Config errors terminate the run; per-check and per-source errors are isolated.

## Architecture

```
CLI:  python -m dq_engine --check-types ... --partition ... --app-code ...
  └─ engine.run(RunConfig)
       ├─ config_loader     — read + validate dq_checks.csv and source_catalog.csv
       ├─ history           — load prior results from output/results.csv
       ├─ source_reader     — resolve + read a Parquet partition into a Dask DataFrame
       ├─ check_executor    — register table in a dask-sql Context, run check_sql
       │    ├─ sql_safety        — read-only gate (single SELECT/CTE)
       │    └─ mom_evaluator     — month-on-month judgement from history
       ├─ issue_classifier  — new / recurring / resolved from history
       └─ result_writer     — append results.csv; write exceptions.csv
```

Data flow (the requested MVP slice): **read config → process with Dask →
write results/exceptions to CSV.**

## Components

### config_loader _(Req 2, 1.1, 3.1)_

- `load_checks(path)` → `(list[CheckDefinition], rejects)`. Validates required
  fields, `check_type ∈ {completeness, validity, consistency}`,
  `evaluation_mode ∈ {violation, consistency_mom}`, numeric
  `deviation_tolerance`, and duplicate `check_id`. Per-row isolation: a bad row
  is rejected with a descriptive message and skipped.
- `load_catalog(path)` → `(dict[table_name, SourceEntry], rejects)`. Validates
  `source_type == parquet` and the parquet-specific required fields; drops tables
  with duplicate entries.
- A missing required **header** column raises `ConfigError` → run terminates
  _(Req 12.1)_.

### source_reader _(Req 1)_

- `list_partitions(entry)` — discover `partition_column=value` directories.
- `resolve_partition(entry, requested)` — specific value, or
  lexicographically-greatest for `latest`; `SourceError` when none exist.
- `read_partition(entry, value)` — `dask.dataframe.read_parquet` on the partition
  **directory** (a single path string — dask-sql's parquet pushdown rejects a
  file *list*). The redundant Hive partition column is dropped and any leftover
  `category` columns are cast to strings, because dask-sql cannot map the
  `category` dtype.
- `resolve_and_read(entry, requested)` → `(ddf, partition_value)`.

### sql_safety _(Req 7.2–7.3, 5.3.3)_

- `validate_read_only(sql)` — strips comments, rejects multi-statement batches,
  requires a leading `SELECT`/`WITH`, and rejects a denylist of data-modifying
  keywords (INSERT/UPDATE/DELETE/MERGE/CREATE/ALTER/DROP/TRUNCATE/…). Returns the
  cleaned SQL or raises `SqlSafetyError`.
- `validate_filter(predicate)` — same denylist + no `;`, for `business_rule_filter`.
- Lightweight by design; a structural parser (e.g. sqlglot) is the documented
  post-MVP hardening step.

### check_executor _(Req 4, 5, 6, 7)_

Single execution path for all checks:

1. **Safety gate** — `validate_read_only`; rejection → errored.
2. **Row scoping** — if `business_rule_filter` present, build a scoped table via
   `SELECT * FROM <table> WHERE <filter>` and register that under `table_name` in
   a fresh `dask_sql.Context` _(Req 4.4, 5.x.4, 6.11)_.
3. **Execute** — `ctx.sql(check_sql).compute()`. Any exception → errored
   _(Req 7.4)_.
4. **Judge by `evaluation_mode`:**

   | Mode | Outcome | Status |
   |------|---------|--------|
   | `violation` | 0 rows | passed |
   | `violation` | ≥1 rows | failed |
   | `violation` | SQL/safety/filter error | errored |
   | `consistency_mom` | `pct ≤ tolerance` | passed |
   | `consistency_mom` | `pct > tolerance` | failed |
   | `consistency_mom` | no 28–31d baseline | failed |
   | `consistency_mom` | prior value == 0 | failed |
   | `consistency_mom` | no value / non-numeric | errored |

### mom_evaluator _(Req 6)_

`evaluate_mom(history, key…, current_value, tolerance, run_ts)` →
`MomOutcome(status, current_value, prior_value, deviation, description)`. Reads
the most recent `consistency_mom` result in
`[run_ts − 31d, run_ts − 28d]` from history; computes percentage deviation.

### history _(Req 6.3, 8.1, 9.5)_

`ResultHistory.load(results_csv)` builds an in-memory view over the results CSV.
Provides:
- `most_recent_before(key…, before_ts)` — most recent prior result (any status),
  for classification.
- `mom_baseline(key…, window_low, window_high)` — most recent `consistency_mom`
  result in the MoM window.

### issue_classifier _(Req 8)_

`classify(history, key…, current_status, run_ts)` → `new | recurring | resolved |
None`, using the most recent prior result inside the Retention_Window (~183
days). open = status ∈ `{failed, errored}`.

| Current | Prior | issue_status |
|---------|-------|--------------|
| open | open | `recurring` |
| open | no prior / passed | `new` |
| passed | open | `resolved` |
| passed | no prior / passed | None |

### result_writer _(Req 9)_

- `append_results(results_csv, results)` — append a row per result; write the
  header when the file is new/empty.
- `write_exceptions(exceptions_csv, results)` — overwrite with the failed/errored
  subset; returns the count.

### engine _(Req 10, 11, 12)_

`run(RunConfig)` lifecycle:
1. Generate `run_id` + `run_timestamp`; compute `month_end_date`.
2. Validate `--check-types` _(Req 11.1–11.2)_ → `TerminatedRun` on bad input.
3. Load + validate config _(Req 2, 12.1)_.
4. Filter checks by selected types and optional `app_code` _(Req 3, 11)_.
5. Load history once _(Req 6, 8)_.
6. Per check: resolve+read partition (cached per table) → execute → classify →
   build `DqResult`; isolate any failure as errored _(Req 7.4, 12.2–12.3)_.
7. Append results CSV; write exceptions CSV; return `DqReport` _(Req 9, 10)_.

## Data Model

### Controlled vocabularies (validated as plain strings)
| Field | Values |
|-------|--------|
| `check_type` | `completeness`, `validity`, `consistency` |
| `evaluation_mode` | `violation`, `consistency_mom` |
| `source_type` | `parquet` |
| `status` | `passed`, `failed`, `errored` |
| `issue_status` | `new`, `recurring`, `resolved` |

### CheckDefinition
`check_id, table_name, column_name` (required), `check_type, evaluation_mode,
check_sql` (required), `data_type, business_glossary, business_rule_filter,
app_code` (optional), `deviation_tolerance` (default 0).

### SourceEntry
`table_name, source_type, parquet_location, partition_column`.

### DqResult (also the results.csv row schema)
`dq_ref_id, check_id, app_code, run_id, run_timestamp, month_end_date,
table_name, source_type, column_name, partition_value, check_type,
evaluation_mode, status, issue_status, violation_count, current_value,
prior_value, deviation, description`.

### DqReport
`run_id, run_timestamp, month_end_date, executed, passed, failed, errored,
new_issues, recurring_issues, resolved_issues, results, config_rejects`.
`executed = passed + failed + errored`; `exception_results` = failed + errored.

## File Formats

### config/dq_checks.csv (header + one row per check)
```
check_id,table_name,column_name,data_type,check_type,business_glossary,
business_rule_filter,deviation_tolerance,check_sql,evaluation_mode,app_code
```

### config/source_catalog.csv
```
table_name,source_type,parquet_location,partition_column
```

### output/results.csv (appended each run) / output/exceptions.csv (overwritten)
Columns = the DqResult fields above, in that order.

## Error Handling

| Failure | Behaviour | Req |
|---------|-----------|-----|
| Config CSV missing / bad header | Terminate run, no report | 12.1 |
| Invalid `--check-types` | Terminate run, no report | 11.2 |
| Partition missing / unreadable | Affected checks errored; continue | 1.6, 1.7, 12.2 |
| `check_sql` fails safety or execution | That check errored; continue | 7.2–7.4, 12.3 |
| MoM baseline missing in 28–31d window | That check **failed** (not errored) | 6.7 |
| MoM history unreadable | That consistency check errored | 12.4 |

## Technology Stack

| Concern | Choice |
|---------|--------|
| Language | Python 3.12 |
| Compute | Dask 2024.5.1 (dataframe) |
| SQL engine | dask-sql 2024.5.0 (+ dask-expr 1.1.1) |
| DataFrames / IO | pandas 2.2.2, pyarrow |
| Config & results | CSV (stdlib `csv`) |
| Tests | pytest |

> **Dependency note.** dask-sql 2024.5.0 imports `dask_expr`, a module removed
> from dask core after 2025. The stack is pinned to a compatible set
> (`requirements.txt`); newer dask + dask-sql is a post-MVP upgrade.

## Out of Scope (Post-MVP)

MariaDB/Hive source readers, a database result store, a config-versioning store,
scheduling, and **case management** are not part of the MVP. The CLI is already
stateless and scheduler-ready (an external cron/Airflow job can invoke it).
