# Requirements Document

## Introduction

The Data Quality (DQ) Engine is a source-agnostic, SQL-driven data quality system built on Apache Spark and Scala. Every check is expressed as a user-supplied SQL statement (`check_sql`) that the engine executes read-only against a resolved data source. The engine never computes data quality metrics internally — it only executes and evaluates the SQL the user provides.

The engine supports three source types: **Parquet datasets** (partitioned files on local FS or HDFS), **MariaDB/MySQL tables** (JDBC), and **Hive tables** (HiveServer2 JDBC). All sources are resolved through a source catalog and share the same check execution path.

Checks are classified by a `check_type` label and evaluated by an `evaluation_mode`. Together these two fields define what aspect of data quality is being measured and how the SQL result is judged.

Each check is optionally tagged with an `app_code` identifying the application that owns it. The same column can be checked by multiple applications with different business rule filters and different app codes. Runs can be filtered to execute only checks for a specific app code.

Check outcomes are persisted to a MariaDB result store and classified as new, recurring, or resolved (or untracked when there is no issue) by comparing against retained history. Results are retained for six months to enable month-on-month consistency comparisons.

## Glossary

- **DQ Engine**: The complete data quality system — Scala on Spark — that loads configuration, resolves sources, executes SQL checks, classifies results, and persists outcomes.
- **check_sql**: The required, user-supplied, read-only SQL statement that the engine executes against the resolved source. For violation-mode checks it returns the rows that violate the rule. For consistency-mode checks it returns a single numeric aggregate.
- **check_type**: The classification label for the data quality dimension a check addresses. One of: `completeness`, `validity`, `consistency`. Does not select a code path — all checks run through the same executor.
- **evaluation_mode**: Determines how the check_sql result is judged. One of: `violation`, `consistency_delta`, `consistency_mom`.
- **violation mode**: check_sql returns the rows that violate the rule. Zero rows = passed; one or more rows = failed; a SQL/safety/filter error = errored.
- **consistency_delta mode**: check_sql returns a single numeric value. The engine compares it against the most recent prior value for the same check within the six-month retention window. Passed when `|current − prior| ≤ deviation_tolerance`; failed when the deviation exceeds tolerance; failed when no prior baseline exists in the window or the prior has no usable measure (cannot compare); errored when check_sql returns no value / a non-numeric value, or the prior-result lookup fails.
- **consistency_mom mode**: check_sql returns a single numeric value. The engine finds the most recent prior result for the same check whose run_timestamp falls between 28 and 31 days before the current run. Passed when `|current − prior_30d| / prior_30d × 100 ≤ deviation_tolerance` (percentage deviation); failed when it exceeds tolerance; failed when no prior baseline exists in the 28–31-day window or the prior value is zero (percentage undefined); errored when check_sql returns no value / a non-numeric value, or the prior-result lookup fails.
- **app_code**: An optional label on a CheckDefinition identifying the application that owns the check. Enables filtering a run to execute only checks for a specific application. The same table/column combination can have checks from multiple app codes with different business_rule_filters.
- **business_rule_filter**: An optional WHERE-clause predicate that scopes the rows the check_sql evaluates. Applied to the source before check_sql execution. Rows excluded by the filter are not evaluated.
- **deviation_tolerance**: For consistency_delta: the absolute numeric deviation that is acceptable. For consistency_mom: the percentage deviation that is acceptable (e.g., 10 means ±10%). Defaults to 0.
- **source_type**: The kind of physical source a table_name maps to. One of: `parquet`, `mariadb_table`, `hive_table`.
- **parquet**: A Hive-style partitioned Parquet dataset on a local filesystem or HDFS path. Processed one partition per run. The selected partition is registered as a Spark SQL temp view named after the logical table_name.
- **mariadb_table**: A relational table in a MariaDB/MySQL database accessed via JDBC. Read as a full table or as a filter-column slice. check_sql is executed via a direct read-only JDBC connection.
- **hive_table**: A table in a Hive metastore accessed via HiveServer2 JDBC. check_sql is executed directly against HiveServer2.
- **Config_Store**: MariaDB table holding CheckDefinition records, versioned by config_version.
- **Source_Catalog**: MariaDB table mapping logical table_name values to physical sources.
- **Result_Store**: MariaDB table holding DqResult records, retained for at least six months.
- **MariaDB_Store**: The engine's own metadata and results database (Config_Store + Source_Catalog + Result_Store). Distinct from any mariadb_table data source.
- **Retention_Window**: Six months measured from a DqResult's run_timestamp. All history lookups (consistency comparison, recurrence classification) are bounded to this window.
- **run_id**: Unique identifier for one execution of the engine.
- **run_timestamp**: Timestamp marking when a run started.
- **dq_ref_id**: Unique identifier for one persisted DqResult record.
- **Status**: The outcome of a check, one of exactly three values: `passed`, `failed`, `errored`. "Did not pass" (the open-exception set) is `{failed, errored}`.
- **issue_status**: Optional classification of a non-passing result as `new`, `recurring`, or `resolved`. It is absent (NULL) when there is no case to track — i.e., a passing result with no open prior. The non-passing, classified results feed a downstream case-management workflow.
- **config_version**: Identifier of a specific versioned set of CheckDefinition records.
- **config_version_date**: Timestamp when a config_version was established.

---

## Requirement 1: Source Types

**User Story:** As a data engineer, I want the engine to resolve logical table names and read data from Parquet files, MariaDB/MySQL tables, and Hive tables using a single, consistent source catalog, so that checks can run against any of these sources without changing the check definitions.

### 1.1 Parquet Source

1. The engine SHALL resolve a `parquet` source by reading the Parquet dataset at `parquet_location` using the Hive-style `partition_column=value` directory layout.
2. When the Partition_Parameter identifies a specific value, the engine SHALL read only the partition directory matching `partition_column=value`.
3. When no specific partition is requested, the engine SHALL select the Latest_Partition — the directory whose partition value is lexicographically greatest.
4. The engine SHALL register the selected partition's DataFrame as a Spark SQL temp view named exactly the logical `table_name`, so that check_sql can reference the source by that name directly.
5. IF the requested partition does not exist, the engine SHALL mark all checks for that source as errored and continue.
6. IF the `parquet_location` contains no partitions, the engine SHALL mark all checks for that source as errored and continue.
7. The engine SHALL support `parquet_location` values that are local filesystem paths or HDFS paths/URIs, applying the same load logic to both.
8. The partition value processed SHALL be recorded in every DqResult for traceability.

### 1.2 MariaDB/MySQL Table Source

1. The engine SHALL resolve a `mariadb_table` source by connecting to the database identified by `connection_ref` via JDBC and reading `physical_table_name`.
2. When no `filter_column` is configured, the engine SHALL read the full table.
3. When `filter_column` is configured and a specific Partition_Parameter value is supplied, the engine SHALL read only rows where `filter_column = Partition_Parameter_value`.
4. The engine SHALL execute check_sql for a `mariadb_table` source via a direct read-only JDBC connection (`setReadOnly(true)`, read-only transaction, least-privilege account).
5. IF the MariaDB/MySQL data source cannot be reached (unavailable, authentication failure, timeout), the engine SHALL mark all checks for that source as errored and continue processing other sources.
6. The applied filter value SHALL be recorded as the partition_value in DqResult; otherwise partition_value is recorded as not-applicable.

### 1.3 Hive Table Source

1. The engine SHALL resolve a `hive_table` source by connecting to HiveServer2 via JDBC using the `connection_ref` configuration.
2. The engine SHALL execute check_sql for a `hive_table` source by sending it directly to HiveServer2 via a read-only JDBC connection.
3. The `physical_table_name` for a Hive source SHALL be the fully-qualified Hive table name in `database.table` format (e.g., `default.customers`).
4. The engine SHALL support a `filter_column` for Hive sources with the same semantics as for `mariadb_table` sources (Req 1.2.3).
5. IF HiveServer2 cannot be reached, the engine SHALL mark all checks for that source as errored and continue.

### 1.4 Source Catalog Validation

1. Every Source_Catalog record SHALL carry `table_name` and `source_type` as required fields.
2. `source_type` SHALL be exactly one of `parquet`, `mariadb_table`, `hive_table` (case-sensitive).
3. A `parquet` record SHALL carry `parquet_location` and `partition_column` as required fields.
4. A `mariadb_table` record SHALL carry `connection_ref` and `physical_table_name` as required fields.
5. A `hive_table` record SHALL carry `connection_ref` and `physical_table_name` as required fields.
6. `filter_column` is optional for `mariadb_table` and `hive_table` sources.
7. IF a `table_name` referenced by a check has no catalog entry, all checks for that source SHALL be marked errored and the run continues.
8. IF a `table_name` has duplicate catalog entries, all checks for that source SHALL be marked errored and the run continues.

---

## Requirement 2: Configuration Management and Versioning

**User Story:** As a data engineer, I want DQ check definitions to be stored in a versioned configuration store so that every run is traceable to the exact configuration version that produced it, and I can amend configuration without losing history.

1. The engine SHALL load CheckDefinition records from the Config_Store for the config_version being loaded.
2. Each CheckDefinition SHALL carry: `check_id`, `table_name`, `column_name`, `check_type`, `evaluation_mode`, `check_sql` as required fields. All other fields are optional.
3. The optional fields `data_type`, `business_glossary`, `business_rule_filter`, `deviation_tolerance`, `app_code` SHALL be treated as not configured when absent or blank. `deviation_tolerance` defaults to 0.
4. IF a CheckDefinition is missing any required field, it SHALL be rejected with a descriptive error naming each missing field, and loading continues for the remaining records.
5. IF `check_type` is not one of `completeness`, `validity`, `consistency` (case-sensitive), the record SHALL be rejected with a descriptive error.
6. IF `evaluation_mode` is not one of `violation`, `consistency_delta`, `consistency_mom` (case-sensitive), the record SHALL be rejected with a descriptive error.
7. IF two CheckDefinitions share the same `check_id` within a `config_version`, all duplicates SHALL be rejected with a descriptive error.
8. An empty Config_Store for the loaded version SHALL produce an empty valid set with no error.
9. Every upload, full re-upload, or amendment of the Config_Store SHALL assign a new `config_version` and `config_version_date`.
10. When `Config_Version_Parameter` is supplied, the engine SHALL load that specific version. When absent, it SHALL load the version with the most recent `config_version_date`.
11. IF the requested `config_version` does not exist, the engine SHALL terminate the run before any check executes and return a descriptive error, producing no DQ_Report.
12. Every DqResult and the DQ_Report SHALL carry the `config_version` and `config_version_date` of the run that produced them.
13. The engine SHALL support secondary ingestion of CheckDefinitions from a delimited CSV file (`DQ_CHECKS.csv`). Each CSV row SHALL have exactly **14 fields**: `check_id, table_name, column_name, data_type, check_type, business_glossary, business_rule_filter, deviation_tolerance, check_sql, evaluation_mode, app_code, [3 reserved empty fields]`. The first row is treated as a header and excluded.
14. Writing a set of CheckDefinitions to the Config_Store then reading them back SHALL return a set equal to the written set (round-trip property).

---

## Requirement 3: App Code Organisation

**User Story:** As a data quality analyst, I want each DQ check tagged with an application code, so that checks can be grouped by owning application, the same column can be checked differently by different applications, and a run can be restricted to a single application's checks.

1. `app_code` SHALL be an optional field on every CheckDefinition. When absent, the check belongs to no application and participates in all app-code-unfiltered runs.
2. When an `App_Code_Parameter` is supplied to a run, the engine SHALL execute only CheckDefinitions whose `app_code` equals the supplied value (case-sensitive exact match). CheckDefinitions with no `app_code` SHALL be skipped.
3. When no `App_Code_Parameter` is supplied, the engine SHALL execute all CheckDefinitions regardless of `app_code`.
4. Multiple CheckDefinitions may reference the same `table_name` and `column_name` with different `app_code` values and different `business_rule_filter` values, allowing the same column to be checked from multiple application perspectives in the same run.
5. `app_code` SHALL be carried through to every DqResult so results can be filtered by application.
6. Issue classification (Req 8) SHALL key on `(table_name, column_name, check_type, app_code)` so that each application's issue history is tracked independently.

---

## Requirement 4: Completeness Checks

**User Story:** As a data quality analyst, I want to detect missing or null values in columns, with optional business-context filtering, so that I can measure data presence against the business rules that apply to each column.

1. A completeness check SHALL have `check_type = completeness` and `evaluation_mode = violation`.
2. The `check_sql` for a completeness check SHALL return the rows where the column value is null or missing.
3. Zero rows returned SHALL indicate passed; one or more rows SHALL indicate failed.
4. When a `business_rule_filter` is present, it SHALL scope the rows evaluated by `check_sql` to the subset satisfying the filter. Rows excluded by the filter SHALL NOT be evaluated. This enables conditional null checks (e.g., check nulls only for rows where `status = 'active'`).
5. When no `business_rule_filter` is present, the check SHALL evaluate all rows in the resolved source.
6. The `violation_count` (number of rows returned by `check_sql`) SHALL be recorded in every completeness DqResult.

---

## Requirement 5: Validity Checks

**User Story:** As a data quality analyst, I want to verify that column values conform to their expected domain — either a defined set of allowed categories or an acceptable numeric range — so that invalid values are detected and reported.

### 5.1 Categorical Values Check

1. A categorical validity check SHALL have `check_type = validity` and `evaluation_mode = violation`.
2. The `check_sql` SHALL return rows where the column value is NOT in the allowed set of values.
3. Zero rows = passed; one or more rows = failed.
4. The `business_rule_filter` MAY be used to scope the check to a subset of rows (e.g., check category values only for a specific region).
5. The `violation_count` SHALL be recorded in every categorical validity DqResult.

### 5.2 Continuous / Threshold Check

1. A continuous validity check SHALL have `check_type = validity` and `evaluation_mode = violation`.
2. The `check_sql` SHALL return rows where the column value falls outside the acceptable range or threshold (e.g., `value < 0 OR value > 100`).
3. Zero rows = passed; one or more rows = failed.
4. The `business_rule_filter` MAY scope the rows evaluated.
5. The `violation_count` SHALL be recorded in every continuous validity DqResult.

### 5.3 Common validity requirements

1. IF `check_sql` fails to execute (parse error, missing column/table, runtime error), the engine SHALL mark the result as errored and continue.
2. IF the `business_rule_filter` cannot be applied, the engine SHALL mark the result as errored and continue.
3. The engine SHALL execute `check_sql` in read-only mode and SHALL reject any `check_sql` that contains data-modifying operations (INSERT, UPDATE, DELETE, DDL, etc.), marking the result as errored.

---

## Requirement 6: Consistency Checks

**User Story:** As a data quality analyst, I want to detect unexpected changes in column-level aggregates over time — either compared to the most recent prior run or compared to the same period last month — so that I can identify trend breaks, sudden drops, or seasonally unexpected changes.

### 6.1 Delta Consistency (Run-over-Run)

1. A delta consistency check SHALL have `check_type = consistency` and `evaluation_mode = consistency_delta`.
2. The `check_sql` SHALL return exactly one numeric value (an aggregate such as COUNT, SUM, AVG).
3. The engine SHALL retrieve the most recent prior result for the same `(table_name, column_name, check_type, app_code)` whose `run_timestamp` is strictly before the current run and within the Retention_Window.
4. The engine SHALL compute `deviation = |current_value − prior_value|`.
5. WHEN `deviation ≤ deviation_tolerance`, the result SHALL be marked passed.
6. WHEN `deviation > deviation_tolerance`, the result SHALL be marked failed.
7. WHEN no prior baseline exists within the Retention_Window, the result SHALL be marked failed with the reason recorded ("no prior baseline within retention window"). The first run of a consistency check therefore does not pass and is classified as a new issue.
8. WHEN a prior result exists but has no usable measure to compare against, the result SHALL be marked failed with the reason recorded ("cannot compare; no prior measure").
9. WHEN the prior-result lookup fails (Result_Store unreachable), the result SHALL be marked errored.
10. WHEN the `check_sql` returns no value or a non-numeric value, the result SHALL be marked errored.
11. The result SHALL carry `current_value`, `prior_value`, and `deviation`.

### 6.2 Month-on-Month Consistency

1. A month-on-month consistency check SHALL have `check_type = consistency` and `evaluation_mode = consistency_mom`.
2. The `check_sql` SHALL return exactly one numeric value.
3. The engine SHALL search for the most recent prior result for the same `(table_name, column_name, check_type, app_code)` whose `run_timestamp` falls within the window **[current_run_timestamp − 31 days, current_run_timestamp − 28 days]**.
4. The engine SHALL compute `pct_deviation = |current_value − prior_30d_value| / |prior_30d_value| × 100` (percentage deviation from the 30-day-prior value).
5. WHEN `pct_deviation ≤ deviation_tolerance` (where `deviation_tolerance` is interpreted as a percentage, e.g., 10 = ±10%), the result SHALL be marked passed.
6. WHEN `pct_deviation > deviation_tolerance`, the result SHALL be marked failed.
7. WHEN no result exists in the 28–31-day window, the result SHALL be marked failed with the reason recorded ("no prior MoM baseline (28–31 day window)"). The first run of a month-on-month check therefore does not pass and is classified as a new issue.
8. WHEN `prior_30d_value` is zero (percentage undefined), the engine SHALL mark the result failed with the reason recorded ("prior MoM value is zero; cannot compute %").
9. WHEN the prior-result lookup fails (Result_Store unreachable), the result SHALL be marked errored.
10. WHEN the `check_sql` returns no value or a non-numeric value, the result SHALL be marked errored.
11. The result SHALL carry `current_value`, `prior_value` (the 30-day-prior value), and `deviation` (the computed percentage deviation).

### 6.3 Common Consistency Requirements

1. The `business_rule_filter`, when present, SHALL scope the rows evaluated by `check_sql` before the aggregate is computed.
2. All consistency lookups SHALL be bounded to the Retention_Window (six months from the current run_timestamp).
3. A consistency check's first run (no baseline yet) is reported as failed (did not pass) and classified `new`. Once a baseline exists and the value is stable, the check flips to passed and resolved, then carries no issue_status thereafter. Failure can happen across any check type.

---

## Requirement 7: Check Execution Engine

**User Story:** As a data engineer, I want every check to execute its SQL against the correct source in read-only mode, with per-check error isolation, so that a single failing check never aborts the whole run.

1. Every check SHALL be executed by running its `check_sql` read-only against the resolved source. `check_type` does not select a code path — it is a classification label only.
2. `check_sql` SHALL be validated as a single read-only statement (SELECT or CTE) before execution. DML (INSERT/UPDATE/DELETE/MERGE), DDL (CREATE/ALTER/DROP/TRUNCATE), multi-statement batches, and utility commands SHALL be rejected, producing an errored result without aborting the run.
3. Validation SHALL be structural (parsed plan or statement structure) rather than a simple keyword denylist, to be robust against comment embedding and casing tricks.
4. IF the execution of one check raises an error, the engine SHALL mark that check's result as errored with a descriptive message identifying `table_name` and `check_id`, and SHALL continue executing remaining checks.
5. IF the DQ configuration cannot be read from the Config_Store, the engine SHALL terminate the run before any check executes and return a descriptive error, producing no DQ_Report.

---

## Requirement 8: Issue Classification

**User Story:** As a data quality analyst, I want each result classified as a new, recurring, or resolved issue based on retained history, keyed per application, so that I can distinguish persistent problems from newly introduced ones.

1. Issue classification SHALL use the most recent prior DqResult within the Retention_Window for the same `(table_name, column_name, check_type, app_code)` as the prior-result lookup key. A result is "open" when its status ∈ `{failed, errored}`.
2. WHEN the current result is open (failed or errored) AND the prior result was also open → `issue_status = recurring`.
3. WHEN the current result is open (failed or errored) AND (no prior result exists within the window OR the prior result was passed) → `issue_status = new`.
4. WHEN the current result is passed AND the prior result was open → `issue_status = resolved`.
5. WHEN the current result is passed AND (no prior result exists OR the prior result was passed) → no `issue_status` is assigned (NULL); there is no case to track.
6. WHEN the prior-result lookup fails → no `issue_status` is assigned (NULL); the result cannot be classified.
7. `issue_status` SHALL be persisted with every DqResult when present, and SHALL be NULL/absent when no case is tracked. `issue_status` is one of exactly three values: `new`, `recurring`, `resolved`.
8. These non-passing, classified results (failed/errored, carrying new/recurring/resolved) are the inputs to a downstream case-management workflow.

---

## Requirement 9: Result Persistence and Retention

**User Story:** As a data engineer, I want every check result persisted to the Result_Store with full traceability, retained for six months for trend and MOM analysis, so that consistency checks and recurrence classification always have sufficient history.

1. The engine SHALL write one DqResult per executed check to the Result_Store.
2. Each DqResult SHALL carry a unique `dq_ref_id` (UUID).
3. Each DqResult SHALL carry: `check_id`, `app_code`, `config_version`, `config_version_date`, `run_id`, `run_timestamp`, `table_name`, `source_type`, `column_name`, `partition_value`, `check_type`, `evaluation_mode`, `status`, `issue_status` (nullable — NULL when no case is tracked), `violation_count` (violation mode), `current_value` / `prior_value` / `deviation` (consistency modes), `description`.
4. IF writing a DqResult fails, the engine SHALL log the error and continue writing remaining results.
5. The Result_Store SHALL retain every DqResult for at least six months from its `run_timestamp`.
6. DqResult records older than six months SHALL be eligible for purge. A purge operation SHALL delete only out-of-window records.
7. ALL engine-controlled DB access SHALL use parameterized PreparedStatements (no string interpolation of values).
8. Failed/errored results SHALL be queryable as the Exception_Report from the Result_Store.

---

## Requirement 10: DQ Reporting

**User Story:** As a data quality analyst, I want a structured DQ Report after every run showing counts by status and a full exception list, traceable to the configuration version that produced it.

1. After all selected checks complete, the engine SHALL produce a DQ_Report containing one DqResult per attempted check.
2. The DQ_Report SHALL include: `run_id`, `run_timestamp`, `config_version`, `config_version_date`, counts of executed / passed / failed / errored checks, and the full list of DqResults.
3. `executed = passed + failed + errored`.
4. Checks skipped due to `check_type` or `app_code` filtering SHALL NOT be counted as executed.
5. The `exceptionReport` (failed + errored results) SHALL be queryable from the Result_Store after every run.
6. When no checks were executed, the DQ_Report SHALL report all counts as zero.

---

## Requirement 11: Run Control

**User Story:** As a data engineer, I want to control which checks a run executes by specifying check type dimensions, an application code, a partition, and an optional config version, so that I can run targeted quality checks without re-running the entire configuration.

1. The `Selected_Check_Types` parameter SHALL accept one or more of `completeness`, `validity`, `consistency` (case-sensitive, comma-separated). Repeated values are deduplicated.
2. IF `Selected_Check_Types` is empty or contains an unrecognised value, the engine SHALL terminate the run with a descriptive error before any check executes.
3. The `App_Code_Parameter` (optional) SHALL filter the run to execute only checks matching that app_code (exact, case-sensitive). Without this parameter, all checks run.
4. The `Partition_Parameter` (optional) SHALL identify the specific partition value to process. When absent or set to `latest`, the Latest_Partition is used for Parquet sources, and Hive/MariaDB sources use their default scope.
5. The `Config_Version_Parameter` (optional) SHALL identify the config_version to load. When absent, the latest version is used.
6. A selected `check_type` with no matching CheckDefinitions SHALL execute zero checks without error.

---

## Requirement 12: Error Handling and Resilience

**User Story:** As a data engineer, I want the engine to distinguish between its own metadata store failures (which are fatal) and data source failures (which are isolated), so that a single unreachable source does not abort quality checks on all other sources.

1. IF the Config_Store or Source_Catalog cannot be read (MariaDB_Store unavailable, auth failure, timeout), the engine SHALL terminate the run before any check executes and return a descriptive error, producing no DQ_Report.
2. IF a `mariadb_table` or `hive_table` DATA SOURCE cannot be reached, the engine SHALL mark all checks for that source as errored and continue processing other sources and checks.
3. IF the Result_Store cannot be reached when writing a DqResult, the engine SHALL log the error and continue writing remaining results.
4. IF the Result_Store cannot be reached when looking up a prior result for a consistency check, the engine SHALL mark that consistency check as errored with the failure reason and continue. (A consistency check with no prior baseline within the window is distinct: it is marked failed, not errored.)

---

## Design and Task Traceability

| Requirement | Primary Design Components | Implementation Task |
|-------------|--------------------------|---------------------|
| **Req 1** — Source Types | SourceReader (trait) · ParquetSourceReader · MariaDbTableReader · HiveTableReader · SourceResolver | Task 5 |
| **Req 2** — Config Management | DqStore · ConfigLoader · ConfigParser | Task 3 · Task 4 |
| **Req 3** — App Code | CheckDefinition · DqResult · DqEngine (filter) | Task 2 · Task 8 |
| **Req 4** — Completeness Checks | SqlCheckExecutor (violation mode) | Task 6 |
| **Req 5** — Validity Checks | SqlCheckExecutor (violation mode) | Task 6 |
| **Req 6** — Consistency Checks | SqlCheckExecutor (consistency_delta · consistency_mom) · DqStore (history lookup) | Task 6 · Task 3 |
| **Req 7** — Execution Engine | SqlSafetyValidator · SqlCheckExecutor | Task 6 |
| **Req 8** — Issue Classification | IssueClassifier (shared) · SqlCheckExecutor · DqStore | Task 6 · Task 3 |
| **Req 9** — Result Persistence | DqStore · ResultWriter | Task 3 · Task 7 |
| **Req 10** — DQ Reporting | ReportGenerator · DqReport | Task 7 |
| **Req 11** — Run Control | DqEngine · DqEngineMain | Task 8 |
| **Req 12** — Error Handling | DqStore · DqEngine · SourceReaders | Task 3 · Task 5 · Task 8 |
