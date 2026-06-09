"""DQ Engine — Parquet-only, Dask + dask-sql MVP.

Read DQ check definitions (with check_sql) from CSV config, process Parquet
sources with Dask, evaluate every check, classify issues (new / recurring /
resolved), and write results + exceptions to CSV.
"""

__version__ = "0.1.0"
