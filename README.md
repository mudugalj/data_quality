# DQ Engine

A source-agnostic, SQL-driven data quality engine on Apache Spark + Scala. Every check is a user-supplied SQL statement executed read-only against a resolved source. The engine supports **three source types** — Parquet files, MariaDB/MySQL tables, and Hive tables — and three evaluation modes: **violation** (returns bad rows), **consistency_delta** (vs. last run), and **consistency_mom** (vs. ~30 days ago). Every check carries an optional **app_code** so the same column can be checked differently by different applications, and runs can be scoped to one application.

> **Docker here is for local testing only — not for deployment.** The `docker-compose.yml` stack (MariaDB + Hive) exists so you can exercise all three source types on one laptop. In a real deployment you point the engine at your existing MariaDB metastore and enable Hive support against your cluster's HiveServer2; you do not ship these containers.

## What it checks

| check_type | What it means | evaluation_mode used |
|------------|---------------|----------------------|
| `completeness` | Missing / null values, optionally filtered by a business condition | `violation` |
| `validity` | Categorical values in an allowed set, or continuous values within a threshold | `violation` |
| `consistency` | An aggregate compared against history — either the last run or ~30 days ago | `consistency_delta`, `consistency_mom` |

**No `expected_value` / `comparison_operator` columns** — all thresholds live inside the `check_sql` itself. The only numeric config is `deviation_tolerance` (absolute for delta, percentage for MoM).

## Architecture

```
spark-submit dq-engine.jar  <checkTypes> [partition] [configVersion] [appCode]

  └─ DqEngine
       ├─ ConfigLoader    — load + validate checks and catalog from DqStore (MariaDB)
       ├─ SourceResolver  — dispatch by source_type:
       │     ├─ ParquetSourceReader  → Spark temp view over the selected partition
       │     ├─ MariaDbTableReader   → direct read-only JDBC
       │     └─ HiveTableReader      → HiveServer2 JDBC
       ├─ SqlCheckExecutor — run check_sql, judge by evaluation_mode, classify issue
       │     └─ SqlSafetyValidator   — rejects non-read-only SQL
       └─ DqStore         — all metadata/results I/O (parameterized statements)
```

## Prerequisites

| Tool | Tested version | Notes |
|------|----------------|-------|
| Java | 17 (OpenJDK) | Java 11+ works; `--add-opens` flags required (shown below) |
| Scala | 2.12.18 | Managed by Maven — **must stay 2.12** to match Spark 3.3.2 |
| Apache Maven | 3.8+ | |
| Apache Spark | 3.3.2 (**Scala 2.12**) | See the critical Spark version note below |
| Docker + Docker Compose | any recent | **testing only** |
| Python | 3.8+ with `pyarrow` | only to generate sample Parquet |

### ⚠️ Critical: Spark must be 3.3.2 / Scala 2.12

If your machine has a Spark 4.x or Scala 2.13 install (e.g. a global `pyspark`), it will crash the engine with `NoSuchMethodError: scala.runtime.ScalaRunTime$.wrapRefArray`. Two things to watch:

1. **`spark-submit` resolves `$SPARK_HOME` first.** If your shell exports `SPARK_HOME` pointing at a Spark 4.x install, even calling a 3.3.2 `spark-submit` runs the 4.x runtime. **Always pin `SPARK_HOME`** to a 3.3.2 install for each run (the run scripts below do this).
2. **A global `CLASSPATH`** pointing at Spark 4.x jars shadows the project. Keep `CLASSPATH` empty for builds and runs. (`pom.xml` already isolates `CLASSPATH` for the Maven test JVM.)

Easiest way to get a clean Spark 3.3.2 without touching system installs — a dedicated venv:

```bash
python3 -m venv .spark332-env
.spark332-env/bin/pip install pyspark==3.3.2
# spark-submit lives at:
#   .spark332-env/lib/python3.12/site-packages/pyspark/bin/spark-submit
```

## Build

```bash
export CLASSPATH=""          # guard against a polluted global CLASSPATH
mvn clean package -DskipTests
# → target/dq-engine-1.0.0-SNAPSHOT.jar   (uber jar, MariaDB + Hive JDBC shaded in)
```

Run the 43 unit tests (no DB needed — they use in-memory stub stores):

```bash
mvn test
```

## Quick Start (local testing with Docker)

### 1. Start the test infrastructure

```bash
docker compose up -d
```

This starts three containers (all **testing only**):

| Container | Port | Purpose |
|-----------|------|---------|
| `dq-mariadb-store` | 3306 | Engine metadata + results (`dq_store`) |
| `dq-mariadb-datasource` | 3307 | Sample MariaDB data source (`dq_datasource`) |
| `dq-hive-server` | 10000 | HiveServer2 sample source (`apache/hive:4.0.1`) |

Wait until all are healthy (Hive takes 1–2 minutes):

```bash
docker compose ps
```

### 2. Generate sample Parquet data

```bash
pip install pyarrow
python data/create_parquet.py
# → data/parquet/customers/date=2024-01-0{1,2,3}/part-00000.parquet
```

### 3. Seed the data sources

```bash
# MariaDB sample tables (orders, products)
docker exec -i dq-mariadb-datasource \
  mysql -u dq_reader -pdq_reader_pw dq_datasource < db/datasource_seed.sql

# Hive sample table (dq_demo.transactions)
docker cp db/hive_datasource_schema.hql dq-hive-server:/tmp/hive_seed.hql
docker exec dq-hive-server beeline -u 'jdbc:hive2://localhost:10000/' -f /tmp/hive_seed.hql
```

### 4. Seed the DQ configuration

```bash
docker exec -i dq-mariadb-store \
  mysql -u dq_engine -pdq_engine_pw dq_store < db/dq_checks_seed.sql
```

This installs config version `demo-v1` with 17 checks across four app codes (CRM, ORDER_MGMT, INVENTORY, FINANCE) and registers all four sources.

### 5. Point the Parquet source at your absolute path

The seed uses a placeholder Parquet path. Set it to where you generated the data:

```bash
docker exec -i dq-mariadb-store mysql -u dq_engine -pdq_engine_pw dq_store \
  -e "UPDATE source_catalog SET parquet_location='$(pwd)/data/parquet/customers' WHERE table_name='customers';"
```

### 6. Run the engine

```bash
export CLASSPATH=""
export SPARK_HOME=$(ls -d "$(pwd)"/.spark332-env/lib/python*/site-packages/pyspark)   # pin 3.3.2

# Engine metadata store
export DQ_STORE_HOST=127.0.0.1 DQ_STORE_PORT=3306 DQ_STORE_DB=dq_store \
       DQ_STORE_USER=dq_engine DQ_STORE_PASSWORD=dq_engine_pw

# MariaDB data source  (connection_ref "main-datasource" → DQ_DATASOURCE_MAIN_DATASOURCE_*)
export DQ_DATASOURCE_MAIN_DATASOURCE_HOST=127.0.0.1 DQ_DATASOURCE_MAIN_DATASOURCE_PORT=3307 \
       DQ_DATASOURCE_MAIN_DATASOURCE_DB=dq_datasource \
       DQ_DATASOURCE_MAIN_DATASOURCE_USER=dq_reader DQ_DATASOURCE_MAIN_DATASOURCE_PASSWORD=dq_reader_pw

# Hive data source     (connection_ref "main-hive" → DQ_HIVE_MAIN_HIVE_*)
export DQ_HIVE_MAIN_HIVE_HOST=127.0.0.1 DQ_HIVE_MAIN_HIVE_PORT=10000 DQ_HIVE_MAIN_HIVE_DB=dq_demo

"$SPARK_HOME/bin/spark-submit" \
  --master "local[2]" \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED" \
  --class com.dqengine.engine.DqEngineMain \
  target/dq-engine-1.0.0-SNAPSHOT.jar \
  "completeness,validity,consistency" "latest" "" ""
```

**Verified output — CRM app over the Parquet source (all check types):**

```
[DQ Engine] Run complete  config_version=demo-v1  appCode=CRM
  executed : 6   passed : 4   failed : 0   errored : 0   inconclusive : 1
```

Persisted result detail confirms every mode works against Parquet:

| check_id | check_type | evaluation_mode | status |
|----------|-----------|-----------------|--------|
| CRM-COMPL-EMAIL-ACTIVE | completeness | violation (conditional null, `business_rule_filter: status='active'`) | passed |
| CRM-COMPL-NAME | completeness | violation | passed |
| CRM-VAL-STATUS | validity (categorical) | violation | passed |
| CRM-VAL-AGE | validity (continuous / threshold) | violation | passed |
| CRM-CONS-COUNT | consistency | consistency_delta | **passed** (980 vs prior 980, deviation 0) |
| CRM-MOM-COUNT | consistency | consistency_mom | inconclusive (no result ~30 days old yet) |

The MariaDB sources (`orders`, `products`) behave identically — `INV-COMPL-PRICE` and `ORD-VAL-AMOUNT` correctly **fail** on the seeded bad rows (null price, negative amount), and `ORD-CONS-COMPLETED` (consistency_delta) **passes** on the second run (10 vs prior 10).

`consistency_delta` is `inconclusive` on the very first run (no prior history) and becomes `passed`/`failed` once a prior exists. `consistency_mom` stays `inconclusive` until a result ~30 days old is in the store.

> **Local Hive testing on Apple Silicon.** The `apache/hive:4.0.1` image does not boot reliably on ARM Macs (its embedded Derby metastore / HiveServer2 fails to bind port 10000). This is an upstream image limitation, **not** an engine issue: the Hive reader uses the *same* direct read-only JDBC code path as the fully-verified MariaDB reader, and when HiveServer2 is unreachable the engine correctly **isolates** those checks as `errored` and completes the run for every other source. Point `DQ_HIVE_MAIN_HIVE_*` at a real HiveServer2 (see "Deploying for real") to exercise the `transactions` checks. **You do not need Docker to deploy** — it exists only to demo all three readers on one laptop.

## CLI Arguments

```
spark-submit ... dq-engine.jar  <checkTypes> [partition] [configVersion] [appCode]
```

| Pos | Argument | Required | Description | Example |
|-----|----------|----------|-------------|---------|
| 1 | `checkTypes` | Yes | Comma-separated: `completeness`, `validity`, `consistency` | `"completeness,validity"` |
| 2 | `partition` | No | Partition value, or `latest` (default) | `"2024-01-03"` |
| 3 | `configVersion` | No | Config version to load (default: most recent) | `"demo-v1"` |
| 4 | `appCode` | No | Run only this application's checks (default: all) | `"FINANCE"` |

```bash
# Only FINANCE (Hive) completeness + validity checks
spark-submit ... dq-engine.jar "completeness,validity" "latest" "" "FINANCE"
```

## Environment Variables

### Engine metadata store (required)

| Variable | Default | Description |
|----------|---------|-------------|
| `DQ_STORE_HOST` / `_PORT` / `_DB` / `_USER` / `_PASSWORD` | `localhost` / `3306` / `dq_store` / `dq_engine` / `dq_engine_pw` | MariaDB metadata + results store |

### MariaDB data sources

For each `connection_ref` in `source_catalog`, set five vars. `<CONNREF>` = the ref uppercased with hyphens → underscores. `main-datasource` → `DQ_DATASOURCE_MAIN_DATASOURCE_*`:

```
DQ_DATASOURCE_<CONNREF>_HOST / _PORT / _DB / _USER / _PASSWORD
```

### Hive data sources

`main-hive` → `DQ_HIVE_MAIN_HIVE_*`:

```
DQ_HIVE_<CONNREF>_HOST / _PORT / _DB / _USER / _PASSWORD
```

## Configuring Your Own Checks

### 1. Register the source in `source_catalog`

```sql
-- Parquet (partitioned)
INSERT INTO source_catalog (table_name, source_type, parquet_location, partition_column)
VALUES ('customers', 'parquet', '/abs/path/customers', 'date');

-- MariaDB table
INSERT INTO source_catalog (table_name, source_type, connection_ref, physical_table_name)
VALUES ('orders', 'mariadb_table', 'main-datasource', 'orders');

-- Hive table  (physical name is database.table)
INSERT INTO source_catalog (table_name, source_type, connection_ref, physical_table_name)
VALUES ('transactions', 'hive_table', 'main-hive', 'dq_demo.transactions');
```

### 2. Register a config version and add checks

```sql
INSERT INTO config_versions (config_version, config_version_date) VALUES ('v2', NOW(3));

-- Completeness (conditional null check via business_rule_filter)
INSERT INTO config_store
  (config_version, check_id, table_name, column_name, check_type,
   business_rule_filter, check_sql, evaluation_mode, app_code)
VALUES
  ('v2','EMAIL-NULL','customers','email','completeness',
   'status = ''active''',
   'SELECT customer_id FROM customers WHERE email IS NULL',
   'violation','CRM');

-- Validity (categorical)
INSERT INTO config_store
  (config_version, check_id, table_name, column_name, check_type, check_sql, evaluation_mode, app_code)
VALUES
  ('v2','STATUS-DOMAIN','orders','status','validity',
   'SELECT order_id FROM orders WHERE status NOT IN (''completed'',''pending'',''shipped'')',
   'violation','ORDER_MGMT');

-- Consistency: delta (row count must not move more than 50 vs last run)
INSERT INTO config_store
  (config_version, check_id, table_name, column_name, check_type,
   deviation_tolerance, check_sql, evaluation_mode, app_code)
VALUES
  ('v2','ROWCOUNT-DELTA','customers','customer_id','consistency',
   50,'SELECT COUNT(*) AS cnt FROM customers','consistency_delta','CRM');

-- Consistency: month-on-month (must not vary more than 15% vs ~30 days ago)
INSERT INTO config_store
  (config_version, check_id, table_name, column_name, check_type,
   deviation_tolerance, check_sql, evaluation_mode, app_code)
VALUES
  ('v2','ROWCOUNT-MOM','customers','customer_id','consistency',
   15,'SELECT COUNT(*) AS cnt FROM customers','consistency_mom','CRM');
```

## App Codes

`app_code` lets multiple applications own checks on the same data independently:

- The same `table_name` + `column_name` can have several checks with different `app_code` and different `business_rule_filter` values.
- Pass the 4th CLI argument to run only one application's checks.
- Issue classification (new/recurring/resolved) is keyed on `(table_name, column_name, check_type, app_code)`, so each application has its own issue history.

## Evaluation Modes

| Mode | check_sql returns | Pass condition | `deviation_tolerance` |
|------|-------------------|----------------|-----------------------|
| `violation` | rows that violate the rule | 0 rows | unused |
| `consistency_delta` | one numeric value | `\|current − last\| ≤ tolerance` | absolute |
| `consistency_mom` | one numeric value | `\|current − ~30d ago\| / ~30d ago × 100 ≤ tolerance` | percentage |

For consistency modes, `inconclusive` is returned when no comparable prior exists (no prior run for delta; nothing in the 28–31-day window for MoM).

## Issue Classification

| Current | Prior (within 6-month window, same table+col+type+app) | issue_status |
|---------|--------------------------------------------------------|--------------|
| fail/error/inconclusive | fail/error/inconclusive | `recurring` |
| fail/error/inconclusive | passed or none | `new` |
| passed | fail/error/inconclusive | `resolved` |
| passed | passed or none | `none` |
| any | lookup failed | `unknown` |

## Querying Results

```sql
-- Latest run, grouped by source and application
SELECT source_type, app_code, status, COUNT(*)
FROM result_store
WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM result_store)
GROUP BY source_type, app_code, status;

-- Exception report (failed / errored / inconclusive)
SELECT app_code, check_id, table_name, column_name, status, issue_status, description
FROM result_store
WHERE status IN ('failed','errored','inconclusive')
ORDER BY run_timestamp DESC;

-- Recurring issues for one application
SELECT check_id, table_name, column_name, COUNT(*) AS seen
FROM result_store
WHERE app_code = 'FINANCE' AND issue_status = 'recurring'
GROUP BY check_id, table_name, column_name;
```

## Result Retention

Results are retained six months from `run_timestamp` (the window that bounds consistency lookups and recurrence). Purge out-of-window rows:

```sql
DELETE FROM result_store WHERE run_timestamp < DATE_SUB(NOW(), INTERVAL 6 MONTH);
```

## Deploying for real (not Docker)

The Docker stack is only to test all three readers on one machine. For a real environment:

1. **Metadata store** — point `DQ_STORE_*` at your existing MariaDB/MySQL. Apply `db/schema.sql` once to create the four tables.
2. **Spark** — submit the uber jar to your cluster (`--master yarn`, `--master k8s://...`, etc.). Keep it Spark 3.3.2 / Scala 2.12, or rebuild `pom.xml` against your cluster's exact Spark/Scala version.
3. **Hive** — set `DQ_HIVE_<ref>_*` to your cluster's HiveServer2 host/port. No container needed; the engine just needs JDBC reachability and a read-only account.
4. **MariaDB data sources** — set `DQ_DATASOURCE_<ref>_*` to each real source, using a least-privilege read-only account.
5. **Parquet** — set `parquet_location` to your real local/HDFS paths; HDFS URIs (`hdfs://...`) work unchanged.

## Project Structure

```
DQ/
├── pom.xml                       # Scala 2.12.18 + Spark 3.3.2 + Hive/MariaDB JDBC; uber jar
├── docker-compose.yml            # TEST-ONLY: mariadb-store, mariadb-datasource, hive-server
├── db/
│   ├── schema.sql                # Metadata store DDL (4 tables) — apply in any environment
│   ├── datasource_schema.sql     # Sample MariaDB source schema
│   ├── datasource_seed.sql       # Sample orders + products
│   ├── hive_datasource_schema.hql# Sample Hive transactions table + data
│   └── dq_checks_seed.sql        # demo-v1: 17 checks, 4 app codes, all 3 sources
├── data/create_parquet.py        # Generate sample Parquet partitions
├── src/main/scala/com/dqengine/
│   ├── domain/Models.scala       # enums, CheckDefinition, DqResult, configs
│   ├── store/DqStore.scala       # metadata/results I/O, findPreviousResult, findMomResult
│   ├── config/{ConfigLoader,ConfigParser}.scala
│   ├── source/{SourceReader,ParquetSourceReader,MariaDbTableReader,HiveTableReader,SourceResolver}.scala
│   ├── exec/{SqlSafetyValidator,SqlCheckExecutor}.scala
│   └── engine/{ResultWriter,ReportGenerator,DqEngine,DqEngineMain}.scala
└── src/test/scala/com/dqengine/  # 43 ScalaTest unit tests
```

## Troubleshooting

| Symptom | Cause / Fix |
|---------|-------------|
| `NoSuchMethodError: scala...wrapRefArray` | Running on Spark 4.x / Scala 2.13. Pin `SPARK_HOME` to a 3.3.2 install; keep `CLASSPATH` empty. |
| `RUN TERMINATED: ... config_version not found` | Run `db/dq_checks_seed.sql`, or pass a valid 3rd CLI arg. |
| `Cannot connect to MariaDB_Store` | `docker compose ps` — store must be healthy; check `DQ_STORE_*`. |
| Hive checks all `errored` | HiveServer2 not reachable. On Apple Silicon the `apache/hive:4.0.1` test image does **not** boot (Derby metastore init fails) — point `DQ_HIVE_*` at a real HiveServer2 instead. The run still completes; only Hive checks error (correct isolation behavior). |
| Parquet `partition not found` / wrong path | `parquet_location` still placeholder — see Quick Start step 5. |
| All consistency checks `inconclusive` | Expected on first run (no history). Run again; `consistency_mom` needs a ~30-day-old result. |
| `business_rule_filter` recursive view error | Fixed — the filter is applied via the DataFrame API, not a self-referencing `CREATE OR REPLACE VIEW`. Rebuild the jar if you see this on an old build. |
| `mvn test` Spark `IllegalAccessError` | `--add-opens` flags are already in `pom.xml`; ensure Java 11/17 and that Maven uses that JDK. |
