"""Generate mock partitioned Parquet datasets for the DQ Engine MVP.

Creates two Hive-style partitioned tables under data/:

    data/customers/date=2026-05-09/part.parquet   (prior month)
    data/customers/date=2026-06-09/part.parquet   (current — "latest")
    data/orders/date=2026-05-09/part.parquet
    data/orders/date=2026-06-09/part.parquet

The current ("latest") partition is hand-crafted so the sample checks produce a
mix of passed / failed / errored outcomes. Run with the project venv:

    .venv/bin/python data/create_parquet.py
"""

from __future__ import annotations

import os
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))

PRIOR = "2026-05-09"
CURRENT = "2026-06-09"


def _write(table: str, partition: str, df: pd.DataFrame) -> None:
    part_dir = os.path.join(HERE, table, f"date={partition}")
    os.makedirs(part_dir, exist_ok=True)
    path = os.path.join(part_dir, "part.parquet")
    df.to_parquet(path, index=False)
    print(f"wrote {len(df):>3} rows -> {os.path.relpath(path, HERE)}")


def customers_current() -> pd.DataFrame:
    # Designed outcomes:
    #   email null      -> id 2          (completeness violation -> FAILED)
    #   age out of range-> id 4 (age 200)(validity violation     -> FAILED)
    #   status all valid                 (validity               -> PASSED)
    #   active rows all have country     (conditional completeness-> PASSED)
    #   AVG(age) = 64.67                 (MoM vs prior 62.0       -> PASSED, 4.3%)
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        "email": ["alice@x.com", None, "carol@x.com", "dave@x.com",
                  "eve@x.com", "frank@x.com"],
        "status": ["active", "active", "inactive", "active", "pending", "active"],
        "age": [30, 45, 28, 200, 52, 33],
        "country": ["US", "UK", "US", "CA", "US", "DE"],
    })


def customers_prior() -> pd.DataFrame:
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dan", "Erin"],
        "email": ["alice@x.com", "bob@x.com", "carol@x.com", "dan@x.com", "erin@x.com"],
        "status": ["active", "active", "inactive", "active", "pending"],
        "age": [29, 44, 27, 60, 51],
        "country": ["US", "UK", "US", "CA", "US"],
    })


def orders_current() -> pd.DataFrame:
    # Designed outcomes:
    #   amount < 0     -> 102            (validity violation -> FAILED)
    #   customer_id all present          (completeness       -> PASSED)
    #   status all valid                 (validity           -> PASSED)
    #   SUM(amount) = 954.5              (MoM vs prior 1200.0 -> FAILED, 20.5%)
    return pd.DataFrame({
        "order_id": [101, 102, 103, 104, 105],
        "customer_id": [1, 2, 3, 1, 5],
        "amount": [250.00, -15.00, 99.50, 500.00, 120.00],
        "status": ["placed", "shipped", "delivered", "placed", "cancelled"],
        "order_date": ["2026-06-01", "2026-06-02", "2026-06-03",
                       "2026-06-04", "2026-06-05"],
    })


def orders_prior() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": [201, 202, 203, 204],
        "customer_id": [1, 2, 3, 4],
        "amount": [300.00, 400.00, 200.00, 300.00],  # sum 1200.0
        "status": ["placed", "shipped", "delivered", "cancelled"],
        "order_date": ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"],
    })


def main() -> None:
    _write("customers", PRIOR, customers_prior())
    _write("customers", CURRENT, customers_current())
    _write("orders", PRIOR, orders_prior())
    _write("orders", CURRENT, orders_current())
    print("done.")


if __name__ == "__main__":
    main()
