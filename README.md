# DQ Engine — MVP (Parquet · Dask · CSV)

A source-agnostic, **SQL-driven** data quality engine, reduced to a minimal,
runnable MVP:

- **Source:** Parquet only (Hive-style partitioned datasets).
- **Compute:** Python + **Dask** + **dask-sql** — every check is a user-supplied
  `check_sql` executed read-only against the data. The engine never computes
  metrics itself.
- **I/O:** CSV in, CSV out. No database.

```
config/dq_checks.csv  ─┐
config/source_catalog ─┤→  [ DQ Engine: read config → Dask → evaluate ]  →  output/results.csv
output/results.csv ────┘        (completeness · validity · consistency)      output/exceptions.csv
   (prior history)
```

## What it does

1. **Read config** — check definitions (with `check_sql`) and a source catalog from CSV.
2. **Process with Dask** — load the selected Parquet partition, register it as a
   dask-sql table, run each `check_sql`.
3. **Evaluate** — by `evaluation_mode`:
   - `violation` (completeness + validity): rows returned → `0 = passed`, `≥1 = failed`.
   - `consistency_mom`: single numeric aggregate compared month-on-month against
     the prior value in `output/results.csv` (28–31 day window).
4. **Classify** — each result as **new / recurring / resolved** from history.
5. **Write** — append all results to `output/results.csv`; write the failed/errored
   subset to `output/exceptions.csv`. The report carries the **month-end date**.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Run the demo

```bash
# 1. generate mock partitioned parquet
.venv/bin/python data/create_parquet.py

# 2. seed a prior run (~30 days ago) so MoM + recurring/resolved have history
PYTHONPATH=. .venv/bin/python data/seed_history.py

# 3. run all checks against the latest partition
PYTHONPATH=. .venv/bin/python -m dq_engine \
    --check-types completeness,validity,consistency \
    --partition latest
```

### Useful flags

| Flag | Meaning |
|------|---------|
| `--check-types` | comma-separated subset of `completeness,validity,consistency` |
| `--partition` | a specific partition value, or `latest` (default) |
| `--app-code` | restrict the run to one application's checks (e.g. `CRM`) |
| `--checks` / `--catalog` | config CSV paths |
| `--results` / `--exceptions` | output CSV paths |

## Tests

```bash
PYTHONPATH=. .venv/bin/python -m pytest -q
```

## Layout

```
config/        dq_checks.csv (checks + check_sql), source_catalog.csv
data/          create_parquet.py, seed_history.py, <table>/date=<value>/*.parquet
output/        results.csv (history), exceptions.csv (failed/errored)
dq_engine/     engine, config_loader, source_reader, check_executor,
               mom_evaluator, issue_classifier, sql_safety, result_writer
tests/         pytest suite
requirements/  requirements.md, design.md, tasks.md (MVP specs)
```

## Notes & scope

- The MoM history lives in `output/results.csv`; re-run `seed_history.py` before a
  fresh demo to reset the baseline.
- `config/dq_checks.csv` includes one intentionally-broken check (`X1_*`) to show
  per-check error isolation — it errors without aborting the run.
- **Out of scope for the MVP:** MariaDB/Hive sources, a database result store,
  case management, and scheduling. See `requirements/requirements.md`.
