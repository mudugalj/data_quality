# DQ Engine

A source-agnostic, SQL-driven data quality engine built on Apache Spark and Scala. Every check is a user-supplied SQL statement evaluated against **Parquet datasets** or **MariaDB/MySQL tables**. Results are classified in three evaluation modes (metric, violation, consistency), labelled as new or recurring issues, and persisted to a MariaDB Result_Store.

## Architecture

```
spark-submit dq-engine-1.0.0-SNAPSHOT.jar \
  "accuracy,completeness,consistency"   # which check_type dimensions to run
  "latest"                              # partition selector (or a specific date)

  └─ DqEngine (orchestrator)
       ├─ ConfigLoader    — loads + validates Check_Definitions and Source_Catalog
       ├─ SourceResolver  — dispatches reads: Parquet (Spark temp view) | MariaDB (JDBC)
       ├─ SqlCheckExecutor— runs check_sql, judges by evaluation_mode, classifies issues
       └─ DqStore         — all MariaDB_Store I/O via parameterized PreparedStatements
```

## Prerequisites

| Tool | Tested version | Notes |
|------|---------------|-------|
| Java | 17 (OpenJDK) | Java 11+ required; `--add-opens` flags are pre-configured in `pom.xml` |
| Scala | 2.12.18 | Managed by Maven — do NOT use a Scala 2.13 Spark installation |
| Apache Maven | 3.8+ | |
| Apache Spark | 3.3.2 with **Scala 2.12** | Must match the Scala version the engine was compiled against |
| Docker + Docker Compose | any recent | |
| Python | 3.8+ with `pyarrow` | Only needed to generate sample Parquet data |

> **Important — Scala version must match.** If your `spark-submit --version` shows "Scala 2.13", you have a Spark 4.x or a 2.13-flavoured Spark 3.x. You can get a compatible Spark 3.3.2 without touching your existing installation by running:
> ```bash
> python3 -m venv .spark332-env
> .spark332-env/bin/pip install pyspark==3.3.2
> # Use this spark-submit:
> SPARK_SUBMIT=.spark332-env/lib/python*/site-packages/pyspark/bin/spark-submit
> ```

## Quick Start

### 1. Clone and enter the project

```bash
cd /path/to/DQ
```

### 2. Start the databases

```bash
docker compose up -d
```

This starts two MySQL 8.0 containers:

| Container | Port | Database | Purpose |
|-----------|------|----------|---------|
| `dq-mariadb-store` | 3306 | `dq_store` | Engine metadata + results (config, catalog, results) |
| `dq-mariadb-datasource` | 3307 | `dq_datasource` | Sample MariaDB data source for DQ checking |

Wait until both are healthy before proceeding:

```bash
docker compose ps   # STATUS column should show "healthy" for both
```

### 3. Generate sample Parquet data

```bash
pip install pyarrow
python data/create_parquet.py
```

This writes three Hive-style date partitions:

```
data/parquet/customers/
  date=2024-01-01/part-00000.parquet   (1 000 rows — ~5% null emails)
  date=2024-01-02/part-00000.parquet   (1 050 rows — ~2% null names)
  date=2024-01-03/part-00000.parquet   (  980 rows — clean)
```

### 4. Seed the sample data source

```bash
docker exec -i dq-mariadb-datasource \
  mysql -u dq_reader -pdq_reader_pw dq_datasource \
  < db/datasource_seed.sql
```

This inserts 15 orders (2 with negative amounts) and 8 products (1 with a null price) — intentional data quality issues for the demo checks to find.

### 5. Seed the DQ configuration

```bash
docker exec -i dq-mariadb-store \
  mysql -u dq_engine -pdq_engine_pw dq_store \
  < db/dq_checks_seed.sql
```

This creates config version `demo-v1` with 8 checks across all three evaluation modes and registers three data sources (`customers`/parquet, `orders`/MariaDB, `products`/MariaDB).

### 6. Update the Parquet path in the Source Catalog

The seed script registers the `customers` Parquet source with a placeholder path `/opt/dq/data/parquet/customers`. Replace it with the **absolute path** to where you generated the Parquet data:

```bash
PARQUET_PATH="$(pwd)/data/parquet/customers"

docker exec -i dq-mariadb-store \
  mysql -u dq_engine -pdq_engine_pw dq_store \
  -e "UPDATE source_catalog SET parquet_location = '${PARQUET_PATH}' WHERE table_name = 'customers';"
```

Verify:
```bash
docker exec dq-mariadb-store \
  mysql -u dq_engine -pdq_engine_pw dq_store \
  -e "SELECT table_name, source_type, parquet_location FROM source_catalog;"
```

### 7. Build the engine

```bash
mvn clean package -DskipTests
```

The shaded uber jar is at: `target/dq-engine-1.0.0-SNAPSHOT.jar`

Run the unit tests (50 tests, no database required):
```bash
mvn test
```

### 8. Run the engine

```bash
# If you installed pyspark 3.3.2 in a venv (see Prerequisites):
export SPARK_SUBMIT=.spark332-env/lib/python3.12/site-packages/pyspark/bin/spark-submit
# Otherwise set to your Spark 3.3.2 installation:
# export SPARK_SUBMIT=/path/to/spark-3.3.2/bin/spark-submit

# MariaDB_Store connection (defaults match docker-compose)
export DQ_STORE_HOST=localhost
export DQ_STORE_PORT=3306
export DQ_STORE_DB=dq_store
export DQ_STORE_USER=dq_engine
export DQ_STORE_PASSWORD=dq_engine_pw

# Data source connection — connection_ref "main-datasource" maps to this prefix
export DQ_DATASOURCE_MAIN_DATASOURCE_HOST=localhost
export DQ_DATASOURCE_MAIN_DATASOURCE_PORT=3307
export DQ_DATASOURCE_MAIN_DATASOURCE_DB=dq_datasource
export DQ_DATASOURCE_MAIN_DATASOURCE_USER=dq_reader
export DQ_DATASOURCE_MAIN_DATASOURCE_PASSWORD=dq_reader_pw

$SPARK_SUBMIT \
  --master "local[*]" \
  --class com.dqengine.engine.DqEngineMain \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.sql.shuffle.partitions=4" \
  target/dq-engine-1.0.0-SNAPSHOT.jar \
  "accuracy,completeness,consistency" \
  "latest"
```

> **Note:** The `--add-opens` flags are required for Spark 3.3.x on Java 11+/17. They are already pre-configured in `pom.xml` for `mvn test`.

**Expected output (first run):**

```
[DQ Engine] Starting run: checkTypes=accuracy,completeness,consistency, partition=Latest, configVersion=None
[DQ Engine] Run complete: run_id=<uuid>
  config_version : demo-v1 (2024-01-01T09:00:00Z)
  executed       : 8
  passed         : 6
  failed         : 2
  errored        : 0
  inconclusive   : 0
  exception count: 2

[DQ Engine] Exception report (failed/errored/inconclusive results):
  [FAILED] orders.amount    (ORD-ACC-AMOUNT):  2 violation(s) found
  [FAILED] products.price   (PROD-ACC-PRICE):  1 violation(s) found
```

> **Note on consistency checks:** `CUST-CONS-COUNT` will be `inconclusive` on the very first run (no prior measured value exists to compare against). Run the engine a **second time** and it will show `passed` — the row count is stable between runs.

**After the second run:**
- `CUST-CONS-COUNT` → `passed`, `issue_status = resolved`
- `ORD-ACC-AMOUNT` → `failed`, `issue_status = recurring` (still failing)
- `PROD-ACC-PRICE` → `failed`, `issue_status = recurring` (still failing)

## CLI Arguments

```
spark-submit ... dq-engine.jar <checkTypes> [partitionParam] [configVersion]
```

| Position | Argument | Required | Description | Example |
|----------|----------|----------|-------------|---------|
| 1 | `checkTypes` | Yes | Comma-separated dimensions to run | `"accuracy,completeness"` |
| 2 | `partitionParam` | No | Partition date or `"latest"` (default) | `"2024-01-03"` |
| 3 | `configVersion` | No | Config version to load (default: most recent by date) | `"demo-v1"` |

Run only accuracy checks against a specific partition:
```bash
spark-submit ... dq-engine.jar "accuracy" "2024-01-02"
```

Run completeness checks against a pinned config version:
```bash
spark-submit ... dq-engine.jar "completeness" "latest" "demo-v1"
```

## Environment Variables

### MariaDB_Store (engine metadata)

| Variable | Default | Description |
|----------|---------|-------------|
| `DQ_STORE_HOST` | `localhost` | Host of the MariaDB_Store |
| `DQ_STORE_PORT` | `3306` | Port |
| `DQ_STORE_DB` | `dq_store` | Database name |
| `DQ_STORE_USER` | `dq_engine` | Username |
| `DQ_STORE_PASSWORD` | `dq_engine_pw` | Password |

### MariaDB data sources (`mariadb_table` sources)

For each `connection_ref` used in `source_catalog`, set five env vars using the pattern:

```bash
DQ_DATASOURCE_<CONNREF>_HOST=...
DQ_DATASOURCE_<CONNREF>_PORT=...
DQ_DATASOURCE_<CONNREF>_DB=...
DQ_DATASOURCE_<CONNREF>_USER=...
DQ_DATASOURCE_<CONNREF>_PASSWORD=...
```

`<CONNREF>` = `connection_ref` uppercased, hyphens → underscores.

| `connection_ref` value | Env var prefix |
|------------------------|----------------|
| `main-datasource` | `DQ_DATASOURCE_MAIN_DATASOURCE_` |
| `analytics-db` | `DQ_DATASOURCE_ANALYTICS_DB_` |

## Configuring Your Own DQ Checks

### Step 1 — Register data sources in `source_catalog`

```sql
-- Parquet source (partitioned by date column)
INSERT INTO source_catalog
  (table_name, source_type, parquet_location, partition_column,
   connection_ref, physical_table_name, filter_column)
VALUES
  ('customers', 'parquet', '/absolute/path/to/parquet/customers', 'date',
   NULL, NULL, NULL);

-- MariaDB/MySQL table source
INSERT INTO source_catalog
  (table_name, source_type, parquet_location, partition_column,
   connection_ref, physical_table_name, filter_column)
VALUES
  ('orders', 'mariadb_table', NULL, NULL,
   'main-datasource', 'orders', NULL);
```

### Step 2 — Register a config version

```sql
INSERT INTO config_versions (config_version, config_version_date)
VALUES ('v2', NOW(3));
```

### Step 3 — Define checks

```sql
-- Violation mode: check_sql returns the offending rows; 0 rows = passed
INSERT INTO config_store
  (config_version, check_id, table_name, column_name, data_type, check_type,
   business_glossary, business_rule_filter, deviation_tolerance,
   check_sql, evaluation_mode, expected_value, comparison_operator)
VALUES
  ('v2', 'CHK-NULL-EMAIL', 'customers', 'email', 'STRING', 'completeness',
   'Email must not be null', NULL, NULL,
   'SELECT customer_id FROM customers WHERE email IS NULL',
   'violation', NULL, NULL);

-- Metric mode: check_sql returns one number compared against a threshold
INSERT INTO config_store VALUES
  ('v2', 'CHK-ROW-COUNT', 'customers', 'customer_id', 'INT', 'accuracy',
   'Partition must have >= 900 rows', NULL, NULL,
   'SELECT COUNT(*) AS cnt FROM customers',
   'metric', 900, '>=');

-- Consistency mode: compare current value to the previous run (deviation <= tolerance)
INSERT INTO config_store VALUES
  ('v2', 'CHK-COUNT-DRIFT', 'customers', 'customer_id', 'INT', 'consistency',
   'Row count must not drift more than 50 rows day-over-day', NULL, 50,
   'SELECT COUNT(*) AS cnt FROM customers',
   'consistency', NULL, NULL);
```

## Evaluation Modes

| Mode | `check_sql` must return | Pass condition | Required fields |
|------|------------------------|----------------|-----------------|
| `violation` | Rows that violate the rule | 0 rows returned | — |
| `metric` | Exactly one numeric value | `measured_value <op> expected_value ± deviation_tolerance` | `expected_value`, `comparison_operator` |
| `consistency` | Exactly one numeric value | `\|current − previous\| ≤ deviation_tolerance` | `deviation_tolerance` (default 0) |

**`comparison_operator` values:** `=`, `!=`, `<`, `<=`, `>`, `>=`

## Issue Classification

Every result gets an `issue_status` by comparing the current status to the most recent prior result for the same `(table_name, column_name, check_type)` within the 6-month retention window:

| Current status | Prior status | `issue_status` |
|----------------|-------------|----------------|
| failed/errored/inconclusive | failed/errored/inconclusive | `recurring` |
| failed/errored/inconclusive | passed or no prior | `new` |
| passed | failed/errored/inconclusive | `resolved` |
| passed | passed or no prior | `none` |
| any | prior lookup failed | `unknown` |

## DQ Result Fields

One row is written to `result_store` per executed check:

| Field | Description |
|-------|-------------|
| `dq_ref_id` | Unique UUID for this result record |
| `check_id` | Links back to the check definition |
| `config_version` | Config version that produced this result |
| `run_id` | UUID for the engine run |
| `run_timestamp` | When the run started |
| `status` | `passed` \| `failed` \| `errored` \| `inconclusive` |
| `issue_status` | `new` \| `recurring` \| `resolved` \| `none` \| `unknown` |
| `measured_value` | Metric mode — the numeric value returned by `check_sql` |
| `violation_count` | Violation mode — number of offending rows |
| `current_value` / `previous_value` / `deviation` | Consistency mode values |
| `description` | Human-readable error or failure reason |

## Querying Results

Connect to the MariaDB_Store and query `result_store`:

```bash
docker exec -it dq-mariadb-store \
  mysql -u dq_engine -pdq_engine_pw dq_store
```

```sql
-- All failed/errored/inconclusive results (exception report)
SELECT check_id, table_name, column_name, check_type,
       status, issue_status, description
FROM result_store
WHERE status IN ('failed', 'errored', 'inconclusive')
ORDER BY run_timestamp DESC;

-- New issues only (first time failing)
SELECT check_id, table_name, column_name, status, description
FROM result_store
WHERE issue_status = 'new'
ORDER BY run_timestamp DESC;

-- Recurring issues (failing across multiple runs)
SELECT check_id, table_name, column_name, COUNT(*) AS times_seen
FROM result_store
WHERE issue_status = 'recurring'
GROUP BY check_id, table_name, column_name
ORDER BY times_seen DESC;

-- Summary for the most recent run
SELECT run_id, config_version,
  SUM(status = 'passed')       AS passed,
  SUM(status = 'failed')       AS failed,
  SUM(status = 'errored')      AS errored,
  SUM(status = 'inconclusive') AS inconclusive,
  COUNT(*)                     AS total
FROM result_store
WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM result_store)
GROUP BY run_id, config_version;
```

## Purging Old Results

Results older than 6 months are excluded from consistency comparison and recurrence classification, and are eligible for deletion:

```sql
DELETE FROM result_store
WHERE run_timestamp < DATE_SUB(NOW(), INTERVAL 6 MONTH);
```

## Running Tests

```bash
# Run all 50 unit tests (no database required)
mvn test

# Build only, skip tests
mvn clean package -DskipTests
```

All tests use in-memory anonymous store subclasses — no running database needed. The Spark-based tests (`SqlCheckExecutorSpec`) start a local `SparkSession`.

## Project Structure

```
DQ/
├── pom.xml                                   # Maven build (Scala 2.12.18 + Spark 3.3.2)
├── docker-compose.yml                        # MariaDB_Store (3306) + sample data source (3307)
├── db/
│   ├── schema.sql                            # Engine metadata schema — applied automatically on first up
│   ├── datasource_schema.sql                 # Sample data source schema (orders, products)
│   ├── datasource_seed.sql                   # Sample rows (15 orders, 8 products)
│   └── dq_checks_seed.sql                    # Demo config: demo-v1, 8 checks, 3 sources
├── data/
│   ├── create_parquet.py                     # Generates 3 daily Parquet partitions
│   └── parquet/customers/                    # Output of create_parquet.py
│       ├── date=2024-01-01/
│       ├── date=2024-01-02/
│       └── date=2024-01-03/
├── src/main/scala/com/dqengine/
│   ├── domain/Models.scala                   # All sealed traits, case classes, ADTs
│   ├── store/DqStore.scala                   # MariaDB_Store JDBC operations
│   ├── config/
│   │   ├── ConfigLoader.scala                # Load, validate, version check definitions
│   │   └── ConfigParser.scala                # Optional DQ_CHECKS.csv ingestion
│   ├── source/
│   │   ├── SourceReader.scala                # Trait (extensible)
│   │   ├── ParquetSourceReader.scala         # Hive-partitioned Parquet via Hadoop FileSystem
│   │   ├── MariaDbTableReader.scala          # Full-table or filter-slice via JDBC
│   │   └── SourceResolver.scala              # Dispatches by source_type
│   ├── exec/
│   │   ├── SqlSafetyValidator.scala          # Structural read-only SQL validation
│   │   └── SqlCheckExecutor.scala            # All 3 modes + issue classification
│   └── engine/
│       ├── ResultWriter.scala                # Per-result isolated writes to Result_Store
│       ├── ReportGenerator.scala             # Aggregates DqReport with counts
│       ├── DqEngine.scala                    # Full run orchestration
│       └── DqEngineMain.scala                # spark-submit entry point
└── src/test/scala/com/dqengine/
    ├── domain/ModelsSpec.scala               # 13 enum + case class tests
    ├── config/ConfigLoaderSpec.scala         # 14 validation + CSV tests
    ├── exec/SqlCheckExecutorSpec.scala       # 10 executor + safety validator tests
    └── engine/
        ├── DqEngineSpec.scala                # 6 orchestrator tests (mock stores)
        └── ReportGeneratorSpec.scala         # 6 report aggregation + writer tests
```

## Sample DQ Checks (demo-v1)

The seed script installs these 8 checks out of the box:

| Check ID | Table | Column | Mode | What it detects |
|----------|-------|--------|------|-----------------|
| `CUST-COMPL-NAME` | customers | name | violation | Null names |
| `CUST-COMPL-EMAIL` | customers | email | violation | Null emails |
| `CUST-ACC-AGE` | customers | age | violation | Ages outside 0–120 |
| `CUST-METRIC-COUNT` | customers | customer_id | metric | Partition has < 900 rows |
| `ORD-ACC-AMOUNT` | orders | amount | violation | Negative order amounts |
| `ORD-METRIC-STATUS` | orders | status | metric | Completed orders < 60% |
| `CUST-CONS-COUNT` | customers | customer_id | consistency | Row count drift > 50 |
| `PROD-ACC-PRICE` | products | price | violation | Null prices |

## Troubleshooting

**`VersionNotFound` on startup**
→ The `config_versions` table is empty. Run `db/dq_checks_seed.sql` or insert a version manually.

**`Cannot connect to MariaDB_Store`**
→ Run `docker compose ps` — both services must show `healthy`. Check `DQ_STORE_*` env vars.

**Parquet partition not found / `parquet_location` errors**
→ The `source_catalog` still has the placeholder path `/opt/dq/data/parquet/customers`. Follow Quick Start step 6 to set the correct absolute path.

**`mariadb_table` source unreachable**
→ Verify the `DQ_DATASOURCE_<CONNREF>_*` env vars are set and the `connection_ref` in `source_catalog` matches (case-insensitive, hyphens = underscores in env key).

**Spark crashes with `IllegalAccessError` / `sun.nio.ch`**
→ You are on Java 11+. Add `--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"` to your `spark-submit` command (or use the full set shown in the run step above).

**All consistency checks are `inconclusive`**
→ Expected on the first run — there is no prior measured value to compare against. Run the engine a second time; results will show `passed` or `failed` based on whether the value drifted.

**`mvn test` fails with `IllegalAccessError` during Spark tests**
→ The `--add-opens` flags are already configured in `pom.xml` for the ScalaTest plugin. If you still see this error, ensure you are using Java 11 or 17 (not 8) and that Maven is using the same JDK (`mvn --version` should show the same Java home as `java -version`).

**`NoSuchMethodError: scala.Predef$.refArrayOps` on spark-submit**
→ Scala version mismatch. Your `spark-submit` is using a different Scala version than the engine was compiled for. Check with `spark-submit --version` — it must show `Scala version 2.12.x`. If it shows 2.13, install a compatible Spark 3.3.2 via `pip install pyspark==3.3.2` in a venv (see Prerequisites).

**`Table or view not found: <tableName>` in Parquet checks**
→ The Spark temp view is registered using the logical `table_name` from `source_catalog`. Ensure your `check_sql` references exactly that name (case-sensitive). Example: if `table_name = 'customers'`, write `SELECT ... FROM customers`.
