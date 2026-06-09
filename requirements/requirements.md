# Requirements Document — DQ Engine MVP (Parquet · Dask · CSV)

## Introduction

The Data Quality (DQ) Engine is a source-agnostic, **SQL-driven** data quality
system. Every check is expressed as a user-supplied SQL statement (`check_sql`)
that the engine executes read-only against a resolved data source. The engine
never computes data quality metrics internally — it only executes and evaluates
the SQL the user provides.

**This document specifies the MVP.** The MVP is deliberately narrowed from the
broader vision to the smallest end-to-end slice that delivers value:

- **One source type — Parquet.** Hive-style partitioned datasets on the local
  filesystem. (MariaDB and Hive sources are out of scope for the MVP.)
- **Python + Dask + dask-sql** for compute. Each Parquet partition is loaded
  into a Dask DataFrame and registered as a dask-sql table; `check_sql` runs
  against it. (The earlier Scala/Spark design is superseded for the MVP.)
- **CSV in, CSV out.** Check definitions and the source catalog are read from
  CSV files. Results and exceptions are written to CSV files. There is no
  database in the MVP.

The end-to-end flow is: **Read config (DQ checks with `check_sql`) → process
using Dask → write results to CSV (exception output).** Every result is
classified as a data quality issue (**new / recurring / resolved**) by comparing
against retained history, and the run report carries a **month-end date**.

Checks are classified by a `check_type` label and evaluated by an
`evaluation_mode`. Together these define what aspect of quality is measured and
how the SQL result is judged. Each check is optionally tagged with an `app_code`
identifying the owning application; a run can be filtered to one app code.

## Glossary

- **DQ Engine**: The Python/Dask system that reads configuration, resolves
  Parquet sources, executes SQL checks, classifies results, and writes outcomes
  to CSV.
- **check_sql**: The required, user-supplied, read-only SQL statement the engine
  executes against the resolved source via dask-sql. For violation-mode checks
  it returns the rows that violate the rule. For consistency-mode checks it
  returns a single numeric aggregate.
- **check_type**: The classification label for the quality dimension a check
  addresses. One of: `completeness`, `validity`, `consistency`. Does not select
  a code path — all checks run through the same executor.
- **evaluation_mode**: Determines how the check_sql result is judged. One of:
  `violation`, `consistency_mom`.
- **violation mode**: check_sql returns the rows that violate the rule. Zero
  rows = passed; one or more rows = failed; a SQL/safety/filter error = errored.
- **consistency_mom mode**: check_sql returns a single numeric aggregate. The
  engine finds the most recent prior result for the same check whose
  run_timestamp falls **28–31 days** before the current run. Passed when
  `|current − prior| / |prior| × 100 ≤ deviation_tolerance`; failed when it
  exceeds tolerance; failed when no prior baseline exists in the window or the
  prior value is zero; errored when check_sql returns no value / a non-numeric
  value. Applies to continuous metrics (AVG, SUM) and categorical aggregates
  (COUNT DISTINCT) alike.
- **app_code**: Optional label identifying the application that owns a check.
- **business_rule_filter**: Optional WHERE-clause predicate scoping the rows the
  check_sql evaluates. Applied to the source before check_sql runs.
- **deviation_tolerance**: For consistency_mom, the acceptable percentage
  deviation (e.g., 10 means ±10%). Defaults to 0.
- **source_type**: The kind of physical source. MVP supports exactly `parquet`.
- **parquet**: A Hive-style partitioned Parquet dataset
  (`partition_column=value/` directories). Processed one partition per run.
- **DQ_Checks_File**: `config/dq_checks.csv` — the CSV holding check definitions.
- **Source_Catalog_File**: `config/source_catalog.csv` — maps logical
  `table_name` values to Parquet locations.
- **Results_File**: `output/results.csv` — one row per executed check, appended
  every run. This is the MVP's result store and the history source for MoM and
  classification.
- **Exceptions_File**: `output/exceptions.csv` — the failed/errored subset of the
  current run.
- **run_id / run_timestamp**: Identifier and start time of one engine execution.
- **month_end_date**: The last calendar day of the run's month, carried in every
  result and in the run report.
- **dq_ref_id**: Unique identifier (UUID) for one persisted result row.
- **Status**: The outcome of a check: `passed`, `failed`, or `errored`. The open
  / "did not pass" set is `{failed, errored}`.
- **issue_status**: Classification of a result as `new`, `recurring`, or
  `resolved`; absent (blank) when there is no case to track.
- **Retention_Window**: Six months from the current run_timestamp; bounds
  history lookups.

---

## Requirement 1: Parquet Source

**User Story:** As a data engineer, I want the engine to resolve logical table
names to partitioned Parquet datasets and read the correct partition, so checks
run against the right data without changing check definitions.

1. The engine SHALL resolve a `parquet` source by reading the dataset at
   `parquet_location` using the Hive-style `partition_column=value` directory
   layout.
2. When a specific partition value is requested, the engine SHALL read only the
   matching `partition_column=value` directory.
3. When no specific partition is requested (absent or `latest`), the engine SHALL
   select the partition whose value is lexicographically greatest.
4. The engine SHALL load the selected partition into a Dask DataFrame and
   register it as a dask-sql table named exactly the logical `table_name`, so
   `check_sql` can reference the source by that name.
5. The redundant Hive partition column SHALL be dropped from the registered table
   (its value is tracked separately as `partition_value`).
6. IF the requested partition does not exist, the engine SHALL mark all checks
   for that source as errored and continue.
7. IF the `parquet_location` contains no partitions, the engine SHALL mark all
   checks for that source as errored and continue.
8. The partition value processed SHALL be recorded in every result for
   traceability.

### 1.1 Source Catalog Validation

1. Every Source_Catalog row SHALL carry `table_name` and `source_type` as
   required fields.
2. `source_type` SHALL be exactly `parquet` (case-sensitive) in the MVP; any
   other value causes that row to be rejected.
3. A `parquet` row SHALL carry `parquet_location` and `partition_column` as
   required fields.
4. IF a `table_name` referenced by a check has no catalog entry, all checks for
   that source SHALL be marked errored and the run continues.
5. IF a `table_name` has duplicate catalog entries, that table SHALL be dropped
   (ambiguous) and its checks marked errored; the run continues.

---

## Requirement 2: Configuration (CSV)

**User Story:** As a data engineer, I want check definitions and the source
catalog stored in CSV so configuration is easy to edit and version in git,
without standing up a database.

1. The engine SHALL load check definitions from `config/dq_checks.csv`. The
   first row is a header and is excluded.
2. Each check row SHALL carry these columns:
   `check_id, table_name, column_name, data_type, check_type, business_glossary,
   business_rule_filter, deviation_tolerance, check_sql, evaluation_mode, app_code`.
3. Required fields: `check_id`, `table_name`, `column_name`, `check_type`,
   `evaluation_mode`, `check_sql`. The rest are optional; blank → not configured;
   `deviation_tolerance` defaults to 0.
4. IF a check row is missing any required field, it SHALL be rejected with a
   descriptive message naming each missing field, and loading continues for the
   remaining rows.
5. IF `check_type` is not one of `completeness`, `validity`, `consistency`
   (case-sensitive), the row SHALL be rejected with a descriptive message.
6. IF `evaluation_mode` is not one of `violation`, `consistency_mom`
   (case-sensitive), the row SHALL be rejected with a descriptive message.
7. IF two rows share the same `check_id`, the duplicate SHALL be rejected with a
   descriptive message.
8. The engine SHALL load the source catalog from `config/source_catalog.csv`
   with columns `table_name, source_type, parquet_location, partition_column`.
9. An empty or missing required header column in either CSV SHALL be a fatal
   configuration error that terminates the run.

---

## Requirement 3: App Code Organisation

**User Story:** As a data quality analyst, I want each check tagged with an
application code so checks can be grouped, the same column checked differently by
different applications, and a run restricted to one application's checks.

1. `app_code` SHALL be an optional field on every check. When absent, the check
   participates in all app-code-unfiltered runs.
2. When an `--app-code` parameter is supplied, the engine SHALL execute only
   checks whose `app_code` equals it (case-sensitive exact match); checks with no
   `app_code` SHALL be skipped.
3. When no app code is supplied, the engine SHALL execute all checks regardless
   of `app_code`.
4. Multiple checks MAY reference the same `table_name` and `column_name` with
   different `app_code` and different `business_rule_filter` values.
5. `app_code` SHALL be carried through to every result.
6. Issue classification (Req 8) SHALL key on
   `(table_name, column_name, check_type, app_code)`.

---

## Requirement 4: Completeness Checks

**User Story:** As a data quality analyst, I want to detect missing or null
values, with optional business-context filtering.

1. A completeness check SHALL have `check_type = completeness` and
   `evaluation_mode = violation`.
2. The `check_sql` SHALL return the rows where the column value is null or missing.
3. Zero rows = passed; one or more rows = failed.
4. When a `business_rule_filter` is present, it SHALL scope the rows evaluated to
   the subset satisfying the filter (e.g., check nulls only where `status =
   'active'`). Excluded rows SHALL NOT be evaluated.
5. When no `business_rule_filter` is present, all rows in the partition SHALL be
   evaluated.
6. The `violation_count` SHALL be recorded in every completeness result.

---

## Requirement 5: Validity Checks

**User Story:** As a data quality analyst, I want to verify values conform to an
allowed set or an acceptable range.

### 5.1 Categorical Values Check
1. `check_type = validity`, `evaluation_mode = violation`.
2. The `check_sql` SHALL return rows where the value is NOT in the allowed set.
3. Zero rows = passed; one or more rows = failed.
4. `business_rule_filter` MAY scope the check.
5. `violation_count` SHALL be recorded.

### 5.2 Continuous / Threshold Check
1. `check_type = validity`, `evaluation_mode = violation`.
2. The `check_sql` SHALL return rows outside the acceptable range/threshold
   (e.g., `value < 0 OR value > 100`).
3. Zero rows = passed; one or more rows = failed.
4. `business_rule_filter` MAY scope the check.
5. `violation_count` SHALL be recorded.

### 5.3 Common
1. IF `check_sql` fails to execute (parse error, missing column, runtime error),
   the result SHALL be marked errored and the run continues.
2. IF the `business_rule_filter` cannot be applied, the result SHALL be errored.
3. The engine SHALL execute `check_sql` read-only and SHALL reject any
   data-modifying SQL (INSERT/UPDATE/DELETE/DDL/etc.), marking it errored.

---

## Requirement 6: Consistency Checks (Month-on-Month)

**User Story:** As a data quality analyst, I want to detect unexpected
month-on-month changes in column-level aggregates.

1. A consistency check SHALL have `check_type = consistency` and
   `evaluation_mode = consistency_mom`.
2. The `check_sql` SHALL return exactly one numeric aggregate, applicable to any
   column type — continuous or categorical.
3. The engine SHALL search the Results_File for the most recent prior
   consistency_mom result for the same `(table_name, column_name, check_type,
   app_code)` whose `run_timestamp` falls in
   **[current_run − 31 days, current_run − 28 days]**.
4. The engine SHALL compute
   `pct_deviation = |current − prior| / |prior| × 100`.
5. WHEN `pct_deviation ≤ deviation_tolerance` → passed.
6. WHEN `pct_deviation > deviation_tolerance` → failed.
7. WHEN no result exists in the 28–31-day window → failed, reason
   `"no prior MoM baseline (28-31 day window)"`. A MoM check's first run does not
   pass and is classified `new`.
8. WHEN the prior value is zero → failed, reason
   `"prior MoM value is zero; cannot compute %"`.
9. WHEN `check_sql` returns no value or a non-numeric value → errored.
10. The result SHALL carry `current_value`, `prior_value`, and `deviation`.
11. The `business_rule_filter`, when present, SHALL scope the rows before the
    aggregate is computed.
12. All consistency lookups SHALL be bounded to the Retention_Window.

---

## Requirement 7: Check Execution Engine

**User Story:** As a data engineer, I want every check to execute its SQL
read-only against the correct partition, with per-check error isolation, so one
failing check never aborts the run.

1. Every check SHALL execute its `check_sql` read-only via dask-sql against the
   resolved partition. `check_type` is a label only — not a code path.
2. `check_sql` SHALL be validated as a single read-only SELECT/CTE before
   execution. DML, DDL, and multi-statement batches SHALL be rejected, producing
   an errored result without aborting the run.
3. Validation SHALL strip comments, reject multiple statements, require a leading
   SELECT/WITH, and reject a denylist of data-modifying keywords. (A structural
   parser is a documented post-MVP hardening step.)
4. IF executing one check raises an error, the engine SHALL mark that result
   errored with a descriptive message identifying `check_id` and `table_name`,
   and SHALL continue with the remaining checks.

---

## Requirement 8: Issue Classification (New / Recurring / Resolved)

**User Story:** As a data quality analyst, I want each result classified as new,
recurring, or resolved based on retained history, keyed per application.

1. Classification SHALL use the most recent prior result within the
   Retention_Window for the same `(table_name, column_name, check_type,
   app_code)`. A result is "open" when its status ∈ `{failed, errored}`.
2. Current open AND prior open → `recurring`.
3. Current open AND (no prior OR prior passed) → `new`.
4. Current passed AND prior open → `resolved`.
5. Current passed AND (no prior OR prior passed) → no `issue_status` (blank).
6. `issue_status` SHALL be written with every result when present and blank
   otherwise; it is one of `new`, `recurring`, `resolved`.

---

## Requirement 9: Result Persistence (CSV)

**User Story:** As a data engineer, I want every result written to CSV with full
traceability, and the failed/errored subset written separately for review.

1. The engine SHALL append one row per executed check to the Results_File.
2. Each result SHALL carry a unique `dq_ref_id` (UUID).
3. Each result SHALL carry: `dq_ref_id, check_id, app_code, run_id,
   run_timestamp, month_end_date, table_name, source_type, column_name,
   partition_value, check_type, evaluation_mode, status, issue_status,
   violation_count, current_value, prior_value, deviation, description`.
4. The engine SHALL write the failed/errored subset of the current run to the
   Exceptions_File, overwriting it each run.
5. The Results_File SHALL be the history source for MoM (Req 6) and
   classification (Req 8).
6. The Results_File header SHALL be written when the file is new or empty.

---

## Requirement 10: DQ Reporting

**User Story:** As a data quality analyst, I want a run report showing counts by
status and by issue classification, stamped with the month-end date.

1. After all selected checks complete, the engine SHALL produce a report with one
   result per attempted check.
2. The report SHALL include: `run_id`, `run_timestamp`, `month_end_date`, counts
   of executed / passed / failed / errored, and counts of new / recurring /
   resolved issues.
3. `executed = passed + failed + errored`.
4. Checks skipped by `check_type` or `app_code` filtering SHALL NOT be counted.
5. The exception list (failed + errored) SHALL be available in the report and in
   the Exceptions_File.
6. When no checks were executed, all counts SHALL be zero.

---

## Requirement 11: Run Control

**User Story:** As a data engineer, I want to control which checks a run executes.

1. `--check-types` SHALL accept one or more of `completeness`, `validity`,
   `consistency` (comma-separated; duplicates deduplicated).
2. IF `--check-types` is empty or contains an unrecognised value, the engine
   SHALL terminate the run with a descriptive error before any check executes.
3. `--app-code` (optional) SHALL filter the run to one app code (exact,
   case-sensitive).
4. `--partition` (optional) SHALL identify the partition value to process; absent
   or `latest` selects the lexicographically greatest partition.
5. A selected `check_type` with no matching checks SHALL execute zero checks
   without error.

---

## Requirement 12: Error Handling and Resilience

**User Story:** As a data engineer, I want config errors to be fatal but data and
per-check errors to be isolated, so one bad check or unreadable partition does
not abort the whole run.

1. IF the config CSVs cannot be read or are missing a required header column, the
   engine SHALL terminate the run before any check executes, producing no report.
2. IF a Parquet source/partition cannot be read, all checks for that source SHALL
   be marked errored and the run continues with other sources.
3. IF a single `check_sql` fails safety validation or execution, that check SHALL
   be errored and the run continues.
4. IF the prior-result history cannot be read for a consistency check, that check
   SHALL be errored. (A consistency check with no baseline in the 28–31-day
   window is distinct: failed, not errored.)

---

## Out of Scope (Post-MVP)

The following are explicitly **not** part of the MVP and are not implemented:

- **Additional source types** — MariaDB/MySQL (JDBC) and Hive (HiveServer2).
- **Database result store** — the MVP uses CSV files in place of a metadata DB.
- **Config versioning store** — CSV config is git-versioned instead.
- **Scheduling** — runs are launched manually or by an external scheduler (cron /
  Airflow) invoking the CLI. No scheduling logic lives in the engine.
- **Case management** — removed from scope.

---

## Design and Task Traceability

| Requirement | Primary Design Components | Task |
|-------------|---------------------------|------|
| **Req 1** — Parquet Source | `source_reader` | Task 4 |
| **Req 2** — Configuration (CSV) | `config_loader` | Task 3 |
| **Req 3** — App Code | `config_loader` · `engine` (filter) | Task 3 · Task 7 |
| **Req 4** — Completeness | `check_executor` (violation) | Task 5 |
| **Req 5** — Validity | `check_executor` (violation) | Task 5 |
| **Req 6** — Consistency (MoM) | `check_executor` · `mom_evaluator` · `history` | Task 5 |
| **Req 7** — Execution Engine | `sql_safety` · `check_executor` | Task 5 |
| **Req 8** — Classification | `issue_classifier` · `history` | Task 5 |
| **Req 9** — Persistence (CSV) | `result_writer` | Task 6 |
| **Req 10** — Reporting | `engine` (DqReport) · CLI | Task 6 · Task 7 |
| **Req 11** — Run Control | `engine` · `__main__` | Task 7 |
| **Req 12** — Error Handling | `engine` · `config_loader` · `source_reader` | Task 3 · Task 4 · Task 7 |
