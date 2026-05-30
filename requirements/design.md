# Design Document

> All `Req N.x` references point to the numbered acceptance criteria in `requirements.md`.

## Overview

The DQ Engine is a Scala/Spark application that executes user-supplied SQL checks (`check_sql`) against three source types — Parquet files, MariaDB/MySQL tables, and Hive tables — and evaluates results in three modes: violation (returns bad rows), consistency_delta (compares to last run), and consistency_mom (compares to 30 days prior). Every check carries an optional `app_code` for application-level ownership. There are no hardcoded expected values: all thresholds are expressed in the check_sql itself.

**Key invariants:**
- All checks are SQL-driven. `check_type` (completeness | validity | consistency) is a classification label only.
- `evaluation_mode` (violation | consistency_delta | consistency_mom) determines how the SQL result is judged.
- Config is versioned; every result carries config_version and config_version_date.
- Issue classification keys on `(table_name, column_name, check_type, app_code)`.
- All MariaDB_Store access uses parameterized PreparedStatements.
- MariaDB_Store read failures terminate; data source failures are isolated per-check.

## Architecture

```
CLI (spark-submit)
  └─ DqEngine
       ├─ ConfigLoader      — load + validate checks and catalog from DqStore
       │    └─ ConfigParser  — optional CSV secondary path
       ├─ SourceResolver    — dispatch by source_type
       │    ├─ ParquetSourceReader   — partitioned Parquet → Spark temp view
       │    ├─ MariaDbTableReader    — JDBC to MySQL/MariaDB data source
       │    └─ HiveTableReader       — JDBC to HiveServer2
       ├─ SqlCheckExecutor  — single execution path for all checks + classification
       │    └─ SqlSafetyValidator    — read-only gate
       └─ DqStore           — all MariaDB_Store I/O (config, catalog, results)
```

## Components

### DqStore _(Req 2, 6.1–6.3, 8, 9.1–9.7, 12.1–12.4)_

All MariaDB_Store I/O via parameterized PreparedStatements. Key methods:

| Method | Satisfies |
|--------|-----------|
| `resolveVersion(param)` | Req 2.10, 2.11 |
| `registerVersion(info)` | Req 2.9 |
| `loadCheckDefinitions(version)` → raw rows | Req 2.1 |
| `writeCheckDefinitions(version, defs)` | Req 2.14 |
| `loadSourceCatalog()` → raw rows | Req 1.4.1–1.4.8 |
| `writeSourceCatalog(entries)` | Req 1.4 |
| `writeResult(result)` | Req 9.1–9.3 |
| `readResultById(dqRefId)` | Req 9 |
| `findPreviousResult(tableName, columnName, checkType, appCode, beforeTs, windowStart)` | Req 6.1.3, 8.1 |
| `findMomResult(tableName, columnName, checkType, appCode, windowLow, windowHigh)` | Req 6.2.3 |
| `purgeOlderThan(cutoff)` | Req 9.5–9.6 |

`MariaDbStoreException` terminates the run _(Req 12.1)_. `ResultStoreUnavailable` is isolated per-result _(Req 9.4, 12.3)_.

### ConfigLoader _(Req 2.1–2.14, 12.1)_

- `resolveVersion(param)` → `Either[String, ConfigVersionInfo]` — `Left` terminates run _(Req 2.11)_
- `loadChecks(version)` → validates fields, emits descriptive per-field rejections, continues _(Req 2.2–2.8)_
- `loadCatalog()` → validates source type and source-type-specific required fields _(Req 1.4)_
- `initialUpload / reupload / amend` → each assigns fresh `config_version + config_version_date` _(Req 2.9)_

Validation rules:

| Rule | Req |
|------|-----|
| Required: check_id, table_name, column_name, check_type, evaluation_mode, check_sql | 2.2, 2.4 |
| Optional absent/blank → not configured; deviation_tolerance defaults to 0 | 2.3 |
| check_type ∈ {completeness, validity, consistency} case-sensitive | 2.5 |
| evaluation_mode ∈ {violation, consistency_delta, consistency_mom} case-sensitive | 2.6 |
| Duplicate check_id within config_version → all duplicates rejected | 2.7 |
| Empty Config_Store → empty valid set, no error | 2.8 |
| Source catalog: table_name + source_type required; source_type ∈ {parquet, mariadb_table, hive_table} | 1.4.1–1.4.5 |

### ConfigParser _(Req 2.13)_

Parses optional `DQ_CHECKS.csv`: header row excluded; each row must have exactly **14 fields**; same validation rules as ConfigLoader.

### SourceReader (trait) + implementations _(Req 1)_

```scala
trait SourceReader {
  def sourceType: SourceType
  def resolveScope(entry: SourceCatalogEntry, param: PartitionParameter, config: EngineConfig): Either[SourceError, (ReadScope, ScopeLabel)]
  def read(spark: SparkSession, entry: SourceCatalogEntry, scope: ReadScope, config: EngineConfig): Either[SourceError, ResolvedSource]
}
```

**ParquetSourceReader** _(Req 1.1)_: Hadoop FileSystem partition discovery; registers temp view as `entry.tableName` so check_sql references it directly; records `ScopeLabel.PartitionValue`.

**MariaDbTableReader** _(Req 1.2)_: Reads via direct JDBC connection (not Spark JDBC); full table or filter-column slice; connectivity failures → `Left(SourceError)`.

**HiveTableReader** _(Req 1.3)_: Connects to HiveServer2 via JDBC; check_sql executes directly against Hive. `physical_table_name` is the fully-qualified Hive table name.

### SourceResolver _(Req 1.4.7–1.4.8)_

Dispatches by `match` on `source_type`. Errors for missing/duplicate `table_name`, unsupported `source_type` → all checks for that source marked errored, run continues.

### SqlSafetyValidator _(Req 7.2–7.3)_

- Parquet path: Spark `SparkSqlParser` → reject non-query LogicalPlan nodes
- JDBC path (MariaDB + Hive): structural keyword + semicolon check
- Rejection → errored result; run continues _(Req 7.4)_

### SqlCheckExecutor _(Req 4, 5, 6, 7, 8)_

Single execution path for all checks. Five steps:

**Step 1 — Safety gate** _(Req 7.2)_: validateReadOnly → errored on rejection.

**Step 2 — Row scoping** _(Req 4.4, 5.1.4, 6.3.1)_: Apply `business_rule_filter` when present:
- Parquet: overwrite the temp view with `CREATE OR REPLACE TEMP VIEW \`name\` AS SELECT * FROM \`name\` WHERE filter`
- JDBC (MariaDB/Hive): filter is composed into a WHERE clause wrapping the SQL or embedded in the check_sql

**Step 3 — Execute** on SqlTarget: Spark SQL for Parquet, direct JDBC for MariaDB/Hive.

**Step 4 — Judge by evaluation_mode**:

| Mode | Logic | Req |
|------|-------|-----|
| `violation` | Count rows returned; 0=passed, >0=failed | 4.3, 5.1.2, 5.2.2 |
| `consistency_delta` | Single numeric value vs most recent prior within Retention_Window; `\|current−prior\| ≤ deviation_tolerance` | 6.1.3–6.1.9 |
| `consistency_mom` | Single numeric value vs value in [−31d, −28d] window; `\|current−prior\|/\|prior\|×100 ≤ deviation_tolerance` | 6.2.3–6.2.9 |

**Step 5 — Issue classification** _(Req 8)_: keyed on `(table_name, column_name, check_type, app_code)`.

### DqEngine _(Req 11, 12)_

Run lifecycle:
1. Generate `run_id` + `run_timestamp`
2. Validate `Selected_Check_Types` _(Req 11.1–11.2)_
3. Resolve `config_version` — terminate on missing or store failure _(Req 2.10–2.11, 12.1)_
4. Load + validate checks and catalog — terminate on store failure _(Req 12.1)_
5. Filter by `Selected_Check_Types` _(Req 11.1)_ and `App_Code_Parameter` _(Req 3.2–3.3)_
6. Per-check: resolve → read → execute → classify → DqResult; continue on any failure _(Req 7.4)_
7. Aggregate DqReport _(Req 10)_
8. Persist results — per-result isolation _(Req 9.4)_
9. Return `CompletedRun` or `TerminatedRun`

## Data Model

### Sealed enums

| Trait | Values |
|-------|--------|
| `CheckType` | `completeness`, `validity`, `consistency` |
| `EvaluationMode` | `violation`, `consistency_delta`, `consistency_mom` |
| `SourceType` | `parquet`, `mariadb_table`, `hive_table` |
| `Status` | `passed`, `failed`, `errored`, `inconclusive` |
| `IssueStatus` | `new`, `recurring`, `resolved`, `none`, `unknown` |

### CheckDefinition
Fields: `checkId`, `tableName`, `columnName` (required), `dataType` (opt), `checkType` (required), `businessGlossary` (opt), `businessRuleFilter` (opt), `deviationTolerance` (default 0), `checkSql` (required), `evaluationMode` (required), `appCode` (opt). Associated with `configVersion` + `configVersionDate`.

### DqResult
Fields: `dqRefId` (UUID), `checkId`, `appCode`, `configVersion`, `configVersionDate`, `runId`, `runTimestamp`, `tableName`, `sourceType`, `columnName`, `partitionValue`, `checkType`, `evaluationMode`, `status`, `measures` (ViolationCount | ConsistencyMeasure | Empty), `description`, `issueStatus`.

### DqReport
Fields: `runId`, `runTimestamp`, `configVersion`, `configVersionDate`, `results`, `executed`, `passed`, `failed`, `errored`, `inconclusive`. Helper: `exceptionReport` → failed + errored + inconclusive.

## Database Schema

```sql
-- Version registry
CREATE TABLE config_versions (
    config_version      VARCHAR(64)  NOT NULL PRIMARY KEY,
    config_version_date DATETIME(3)  NOT NULL
);
CREATE INDEX idx_config_version_date ON config_versions (config_version_date);

-- Check definitions (no expected_value, no comparison_operator)
CREATE TABLE config_store (
    config_version       VARCHAR(64)   NOT NULL,
    check_id             VARCHAR(128)  NOT NULL,
    table_name           VARCHAR(256)  NOT NULL,
    column_name          VARCHAR(256)  NOT NULL,
    data_type            VARCHAR(64)   NULL,
    check_type           VARCHAR(32)   NOT NULL,        -- completeness|validity|consistency
    business_glossary    TEXT          NULL,
    business_rule_filter TEXT          NULL,
    deviation_tolerance  DECIMAL(18,6) NULL,            -- absolute (delta) or percentage (mom)
    check_sql            TEXT          NOT NULL,
    evaluation_mode      VARCHAR(32)   NOT NULL,        -- violation|consistency_delta|consistency_mom
    app_code             VARCHAR(64)   NULL,
    PRIMARY KEY (config_version, check_id),
    FOREIGN KEY (config_version) REFERENCES config_versions(config_version),
    CHECK (check_type IN ('completeness','validity','consistency')),
    CHECK (evaluation_mode IN ('violation','consistency_delta','consistency_mom'))
);
CREATE INDEX idx_config_store_version ON config_store (config_version);

-- Source catalog (parquet + mariadb_table + hive_table)
CREATE TABLE source_catalog (
    table_name          VARCHAR(256) NOT NULL PRIMARY KEY,
    source_type         VARCHAR(32)  NOT NULL,          -- parquet|mariadb_table|hive_table
    parquet_location    TEXT         NULL,
    partition_column    VARCHAR(256) NULL,
    connection_ref      VARCHAR(256) NULL,
    physical_table_name VARCHAR(256) NULL,
    filter_column       VARCHAR(256) NULL
);

-- Results with full mode-specific measures and app_code
CREATE TABLE result_store (
    dq_ref_id           VARCHAR(64)   NOT NULL PRIMARY KEY,
    check_id            VARCHAR(128)  NOT NULL,
    app_code            VARCHAR(64)   NULL,
    config_version      VARCHAR(64)   NOT NULL,
    config_version_date DATETIME(3)   NOT NULL,
    run_id              VARCHAR(64)   NOT NULL,
    run_timestamp       DATETIME(3)   NOT NULL,
    table_name          VARCHAR(256)  NOT NULL,
    source_type         VARCHAR(32)   NOT NULL,
    column_name         VARCHAR(256)  NOT NULL,
    partition_value     VARCHAR(256)  NULL,
    check_type          VARCHAR(32)   NOT NULL,
    evaluation_mode     VARCHAR(32)   NOT NULL,
    status              VARCHAR(16)   NOT NULL,
    issue_status        VARCHAR(16)   NOT NULL,
    violation_count     BIGINT        NULL,             -- violation mode
    current_value       DECIMAL(38,6) NULL,             -- consistency modes
    prior_value         DECIMAL(38,6) NULL,             -- consistency modes
    deviation           DECIMAL(38,6) NULL,             -- consistency modes (abs or pct)
    description         TEXT          NULL,
    FOREIGN KEY (config_version) REFERENCES config_versions(config_version),
    CHECK (status IN ('passed','failed','errored','inconclusive')),
    CHECK (issue_status IN ('new','recurring','resolved','none','unknown')),
    CHECK (check_type IN ('completeness','validity','consistency')),
    CHECK (evaluation_mode IN ('violation','consistency_delta','consistency_mom'))
);
CREATE INDEX idx_result_tbl_col_type_app_ts ON result_store (table_name, column_name, check_type, app_code, run_timestamp);
CREATE INDEX idx_result_run_ts ON result_store (run_timestamp);
```

## Docker Infrastructure

```yaml
# MariaDB_Store — engine metadata + results (port 3306)
mariadb-store:     mysql:8.0   →  dq_store / dq_engine / dq_engine_pw

# Sample MariaDB data source (port 3307)
mariadb-datasource: mysql:8.0  →  dq_datasource / dq_reader / dq_reader_pw

# Hive — HiveServer2 for hive_table source type (port 10000)
hive-server:       apache/hive:4.0.1  →  JDBC: jdbc:hive2://localhost:10000/default
```

## Error Handling

| Failure | Behaviour | Req |
|---------|-----------|-----|
| MariaDB_Store unreachable (config/catalog read) | Terminate run, no DQ_Report | 12.1 |
| Requested config_version not found | Terminate run, no DQ_Report | 2.11 |
| Invalid Selected_Check_Types | Terminate run, no DQ_Report | 11.2 |
| Source not found / unreachable | Affected checks errored; continue | 1.1.5, 1.2.5, 1.3.5, 12.2 |
| check_sql safety validation fails | That check errored; continue | 7.2–7.4 |
| check_sql execution fails | That check errored; continue | 7.4 |
| Result_Store write failure | Log; continue writing remaining | 9.4, 12.3 |
| Prior result lookup fails (consistency) | That check inconclusive; continue | 12.4 |

## Technology Stack

| Concern | Choice |
|---------|--------|
| Language | Scala 2.12.18 |
| Compute | Apache Spark 3.3.2 (Scala 2.12) |
| Engine DB | MariaDB / MySQL 8.0 in Docker |
| Data sources | Parquet (Spark), MariaDB (direct JDBC), Hive (HiveServer2 JDBC) |
| DB drivers | mariadb-java-client 3.1.4 · hive-jdbc 3.1.3 |
| Build | Maven (scala-maven-plugin, maven-shade-plugin) |
| Tests | ScalaTest — example-based, no ScalaCheck |

## Requirements Traceability Matrix

| Requirement | Design Components | Task |
|-------------|-------------------|------|
| Req 1 — Source Types | SourceReader · ParquetSourceReader · MariaDbTableReader · HiveTableReader · SourceResolver | Task 5 |
| Req 2 — Config Management | DqStore · ConfigLoader · ConfigParser | Task 3 · Task 4 |
| Req 3 — App Code | CheckDefinition · DqResult · DqEngine | Task 2 · Task 8 |
| Req 4 — Completeness | SqlCheckExecutor (violation) | Task 6 |
| Req 5 — Validity | SqlCheckExecutor (violation) | Task 6 |
| Req 6 — Consistency | SqlCheckExecutor (delta + mom) · DqStore (findPreviousResult · findMomResult) | Task 6 · Task 3 |
| Req 7 — Execution Engine | SqlSafetyValidator · SqlCheckExecutor | Task 6 |
| Req 8 — Classification | SqlCheckExecutor (classifyIssue) · DqStore | Task 6 · Task 3 |
| Req 9 — Persistence | DqStore · ResultWriter | Task 3 · Task 7 |
| Req 10 — Reporting | ReportGenerator · DqReport | Task 7 |
| Req 11 — Run Control | DqEngine · DqEngineMain | Task 8 |
| Req 12 — Error Handling | DqStore · DqEngine · SourceReaders | Task 3 · Task 5 · Task 8 |
