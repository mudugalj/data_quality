# Implementation Plan

> All `Req N.x` references point to the numbered acceptance criteria in `requirements.md`.

## Key invariants
- All checks are SQL-driven. `check_type` is a label only.
- `evaluation_mode` ∈ {violation, consistency_delta, consistency_mom}. No `metric` mode, no `expected_value`.
- Issue classification keys on `(table_name, column_name, check_type, app_code)`.
- All MariaDB_Store access uses parameterized PreparedStatements.
- No ScalaCheck or property-based testing.

---

### Task 1 — Project Scaffold and Schema `[ ]`

**Design refs:** Database Schema, Docker Infrastructure  
**Requirements:** Req 2.9 (config_versions), Req 1.4 (source_catalog), Req 9 (result_store), Req 1.3 (hive_table source type)

- `pom.xml`: Scala 2.12.18, Spark 3.3.2 (provided), mariadb-java-client 3.1.4, hive-jdbc 3.1.3 (provided at runtime via Hive JARs), ScalaTest 3.2.17, maven-shade-plugin uber jar
- `db/schema.sql`: all four tables with updated schema (no expected_value/comparison_operator; add app_code; add evaluation_mode to result_store; new check_type/evaluation_mode values; new index including app_code)
- `docker-compose.yml`: update to add `hive-server` service (image: apache/hive:4.0.1, port 10000, SERVICE_NAME=hiveserver2); keep existing mariadb-store + mariadb-datasource services
- `db/hive_datasource_schema.hql`: HiveQL to create sample Hive tables (customers_hive in default database)
- Package layout: domain, store, config, source, exec, engine under src/main/scala and src/test/scala

---

### Task 2 — Domain Model `[ ]`

**Design refs:** Data Model section  
**Requirements:** Req 2.2–2.3 (CheckDefinition fields), Req 3 (app_code), Req 9.3 (DqResult fields), Req 10 (DqReport)

- Sealed enums: `CheckType` (completeness|validity|consistency), `EvaluationMode` (violation|consistency_delta|consistency_mom), `SourceType` (parquet|mariadb_table|hive_table), `Status`, `IssueStatus` — each with `wire` + case-sensitive `fromWire`
- Case classes: `ConfigVersionInfo`, `CheckDefinition` (with `appCode: Option[String]`), `SourceCatalogEntry` (with `isPartitionedFileSource`), `PartitionParameter`, `RunContext`, `ReadScope`, `ScopeLabel`, `SqlTarget` (SparkTempView | JdbcConnection), `ResolvedSource`, `Measures` (ViolationCount | ConsistencyMeasure | Empty), `DqResult` (with `appCode`, `evaluationMode`), `DqReport`, engine result types, error types, `MariaDbConfig`, `HiveConfig`, `EngineConfig`
- **Tests:** fromWire round-trips; consistency_delta/consistency_mom are valid EvaluationMode values; exceptionReport correct subset; isPartitionedFileSource true only for parquet

---

### Task 3 — DqStore: All MariaDB_Store Operations `[ ]`

**Design refs:** DqStore component, Database Schema  
**Requirements:** Req 2 (config load/write), Req 6.1.3 (findPreviousResult), Req 6.2.3 (findMomResult), Req 9 (writeResult), Req 12.1–12.4

- JDBC connection provider; `MariaDbStoreException` + `ResultStoreUnavailable` error types
- All methods: `resolveVersion`, `registerVersion`, `loadCheckDefinitions`, `writeCheckDefinitions`, `loadSourceCatalog`, `writeSourceCatalog`, `writeResult`, `readResultById`, `findPreviousResult(tableName, columnName, checkType, appCode, beforeTs, windowStart)`, `findMomResult(tableName, columnName, checkType, appCode, momWindowLow, momWindowHigh)`, `purgeOlderThan`
- `findMomResult` query: `WHERE table_name=? AND column_name=? AND check_type=? AND app_code=? AND run_timestamp BETWEEN ? AND ? ORDER BY run_timestamp DESC LIMIT 1`
- **Tests (integration, Dockerized MariaDB):** round-trip config_store; round-trip result_store; `findPreviousResult` returns most recent in-window; `findMomResult` returns result in [−31d, −28d] window; `purgeOlderThan` deletes only out-of-window; adversarial values stored verbatim

---

### Task 4 — Config Loading, Versioning, and CSV Parser `[ ]`

**Design refs:** ConfigLoader, ConfigParser  
**Requirements:** Req 2.1–2.14, Req 3.1 (app_code optional), Req 1.4 (source catalog validation)

- `ConfigLoader.resolveVersion`: requested or latest; `Left(VersionNotFound)` terminates run
- `ConfigLoader.loadChecks`: validates all fields per design rules; check_type ∈ {completeness, validity, consistency}; evaluation_mode ∈ {violation, consistency_delta, consistency_mom}; app_code optional
- `ConfigLoader.loadCatalog`: validates source_type ∈ {parquet, mariadb_table, hive_table}; source-type-specific required fields
- Versioning: `initialUpload / reupload / amend` each assign fresh version+date
- `ConfigParser`: 14-field CSV; header skip; same validation; empty CSV → empty set
- **Tests:** each rejection path (missing required fields, invalid check_type, invalid evaluation_mode, duplicate check_id); catalog rejections (invalid source_type, parquet/mariadb/hive missing fields); versioning produces distinct dated versions; app_code optional (None when absent); CSV 14-field validation

---

### Task 5 — Source Reading: Parquet, MariaDB, Hive `[ ]`

**Design refs:** SourceReader, ParquetSourceReader, MariaDbTableReader, HiveTableReader, SourceResolver  
**Requirements:** Req 1.1–1.4, Req 12.2

- `SourceReader` trait: `sourceType`, `resolveScope`, `read` returning `Either[SourceError, ResolvedSource]`
- `ParquetSourceReader`: Hadoop FileSystem partition discovery; registers Spark temp view as `entry.tableName`; specific or latest partition; errors for missing partition or no partitions _(Req 1.1)_
- `MariaDbTableReader`: direct java.sql JDBC (not Spark JDBC) to avoid type-inference issues; full table or filter-column slice; connectivity failures → `Left(SourceError)` _(Req 1.2)_
- `HiveTableReader`: JDBC to HiveServer2 (`jdbc:hive2://host:port/database`); read via direct JDBC connection; `physical_table_name` is `database.table`; filter_column slice supported _(Req 1.3)_
- `SourceResolver`: dispatch by `match`; missing/duplicate table_name, unsupported source_type → errored, run continues _(Req 1.4.7–1.4.8)_
- **Tests (Spark-based, local Parquet):** specific + latest partition selection; missing partition → errored; only selected partition rows read; partition_value recorded. Integration tests (MariaDB): full-table + filter slice; connectivity failure → errored. Hive tests: HiveServer2 JDBC connection (if container available); graceful error when HiveServer2 unreachable

---

### Task 6 — Check Execution: All Modes + Classification `[ ]`

**Design refs:** SqlSafetyValidator, SqlCheckExecutor  
**Requirements:** Req 4, 5, 6, 7, 8

- `SqlSafetyValidator`: structural read-only check (Spark parser for Parquet; keyword+structural for JDBC targets); DML/DDL/multi-statement → rejected _(Req 7.2–7.3)_
- `SqlCheckExecutor` full pipeline:
  1. Safety gate → errored on rejection _(Req 7.2)_
  2. business_rule_filter scoping (overwrite temp view for Parquet; compose WHERE clause for JDBC) _(Req 4.4, 5.1.4, 6.3.1)_
  3. Execute on SqlTarget _(Req 7.1)_
  4. Judge by evaluation_mode:
     - **violation**: count rows; 0=passed, >0=failed _(Req 4.3, 5.1.2, 5.2.2)_
     - **consistency_delta**: `|current − prior| ≤ deviation_tolerance`; inconclusive if no prior _(Req 6.1)_
     - **consistency_mom**: `|current − prior_30d| / |prior_30d| × 100 ≤ deviation_tolerance`; inconclusive if no 28–31d prior or prior=0 _(Req 6.2)_
  5. Issue classification: lookup prior result keyed on `(table_name, column_name, check_type, app_code)` _(Req 8)_
- **Tests:** violation 0 rows → passed; violation N rows → failed; consistency_delta within/over tolerance; consistency_delta no prior → inconclusive; consistency_mom within/over tolerance %; consistency_mom no 28–31d prior → inconclusive; prior=0 → inconclusive (MOM); safety validator accepts SELECT, rejects INSERT/DDL/multi-statement; business_rule_filter scoping; app_code carried to DqResult; issue classification all 5 states (new/recurring/resolved/none/unknown)

---

### Task 7 — Result Persistence and Reporting `[ ]`

**Design refs:** ResultWriter, ReportGenerator  
**Requirements:** Req 9, 10

- `ResultWriter`: write each DqResult via DqStore; per-result isolation; all fields including app_code and evaluation_mode _(Req 9.1–9.4)_
- `ReportGenerator`: aggregate DqReport; executed = passed + failed + errored + inconclusive; skipped checks excluded; config_version + run metadata stamped through _(Req 10)_
- **Tests:** one write failure doesn't stop others; count invariants; exceptionReport is failed+errored+inconclusive; app_code carried through; evaluation_mode in each result

---

### Task 8 — Engine Orchestration, CLI, and Sample Data `[ ]`

**Design refs:** DqEngine, DqEngineMain, Docker Infrastructure  
**Requirements:** Req 11, 12, Req 2.10–2.12, Req 3.2–3.3

- `DqEngine.run(selectedCheckTypes, partitionParam, configVersionParam, appCodeFilter)`: full lifecycle per design _(Req 11, 12)_
- `DqEngineMain`: parse 4 CLI args — check_types, partition, config_version (opt), app_code (opt); wire all components; log run summary
- Sample data:
  - `data/create_parquet.py`: generate `date=*` partitions for customers Parquet source
  - `db/datasource_seed.sql`: seed orders + products in MariaDB datasource
  - `db/dq_checks_seed.sql`: demo checks for all 3 source types, all 3 check_types, all 3 evaluation_modes, multiple app_codes (CRM, ORDER_MGMT, INVENTORY)
  - `db/hive_datasource_schema.hql`: create Hive sample table and seed data
- **Tests (mocked stores):** TerminatedRun on missing version; TerminatedRun on store failure; invalid check_types rejected; app_code filter excludes non-matching checks; config_version carried to report; per-check error isolation
- **End-to-end:** run over Parquet + MariaDB sources; all 3 evaluation_modes; error isolation confirmed; results in result_store with app_code and evaluation_mode
