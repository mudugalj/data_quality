#!/usr/bin/env python3
"""
Creates sample Parquet partitions for the DQ Engine demo.

Layout:
  data/parquet/customers/
    date=2024-01-01/part-00000.parquet   (1000 rows, ~5% null emails)
    date=2024-01-02/part-00000.parquet   (1050 rows, ~2% null names)
    date=2024-01-03/part-00000.parquet   (980 rows, clean)

Requires: pip install pyarrow
Run from the DQ project root: python data/create_parquet.py
"""

import os
import random
import string

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip install pyarrow")
    raise

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "parquet", "customers")

COUNTRIES = ["US", "UK", "DE", "FR", "IN", "AU"]
STATUSES  = ["active", "inactive", "pending"]

def rand_email(name: str) -> str:
    domains = ["example.com", "test.org", "demo.net"]
    return f"{name.lower().replace(' ', '.')}@{random.choice(domains)}"

def rand_name() -> str:
    first = random.choice(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"])
    last  = random.choice(["Smith", "Jones", "Brown", "Taylor", "Wilson", "Davis"])
    return f"{first} {last}"

def make_partition(date_str: str, n_rows: int, null_email_pct: float = 0.0, null_name_pct: float = 0.0):
    customer_ids = list(range(1, n_rows + 1))
    names        = [rand_name() if random.random() > null_name_pct  else None for _ in range(n_rows)]
    emails       = [rand_email(n or "unknown") if random.random() > null_email_pct else None for n in names]
    ages         = [random.randint(18, 80) for _ in range(n_rows)]
    countries    = [random.choice(COUNTRIES) for _ in range(n_rows)]
    statuses     = [random.choice(STATUSES) for _ in range(n_rows)]

    table = pa.table({
        "customer_id": pa.array(customer_ids, type=pa.int32()),
        "name":        pa.array(names,        type=pa.string()),
        "email":       pa.array(emails,       type=pa.string()),
        "age":         pa.array(ages,         type=pa.int32()),
        "country":     pa.array(countries,    type=pa.string()),
        "status":      pa.array(statuses,     type=pa.string()),
        "date":        pa.array([date_str] * n_rows, type=pa.string()),
    })

    partition_dir = os.path.join(OUTPUT_DIR, f"date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    out_path = os.path.join(partition_dir, "part-00000.parquet")
    pq.write_table(table, out_path, compression="snappy")
    print(f"  Written {n_rows} rows -> {out_path}")

def main():
    print(f"Creating sample Parquet partitions in: {OUTPUT_DIR}")
    make_partition("2024-01-01", n_rows=1000, null_email_pct=0.05)
    make_partition("2024-01-02", n_rows=1050, null_name_pct=0.02)
    make_partition("2024-01-03", n_rows=980)
    print("Done. Three partitions created.")

if __name__ == "__main__":
    main()
