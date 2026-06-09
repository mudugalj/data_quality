"""Resolve and read Parquet sources with Dask.

A source is a Hive-style partitioned Parquet dataset:

    <parquet_location>/<partition_column>=<value>/*.parquet

The reader selects a partition (a specific value, or the lexicographically
greatest when "latest"), loads it into a Dask DataFrame, and returns the
DataFrame plus the resolved partition value.
"""

from __future__ import annotations

import os
import glob
from typing import List, Optional, Tuple

import dask.dataframe as dd

from .domain import SourceEntry


class SourceError(Exception):
    """Raised when a source cannot be resolved or read."""


def list_partitions(entry: SourceEntry) -> List[str]:
    """Return available partition values, sorted ascending."""
    prefix = f"{entry.partition_column}="
    if not os.path.isdir(entry.parquet_location):
        return []
    values = []
    for name in os.listdir(entry.parquet_location):
        full = os.path.join(entry.parquet_location, name)
        if os.path.isdir(full) and name.startswith(prefix):
            values.append(name[len(prefix):])
    return sorted(values)


def resolve_partition(entry: SourceEntry, requested: Optional[str]) -> str:
    """Resolve the partition value to read.

    requested None or 'latest' -> the lexicographically greatest partition.
    Raises SourceError when nothing is available or the request is absent.
    """
    available = list_partitions(entry)
    if not available:
        raise SourceError(
            f"no partitions found under '{entry.parquet_location}' "
            f"(expected '{entry.partition_column}=<value>' directories)"
        )
    if requested is None or requested.lower() == "latest":
        return available[-1]
    if requested not in available:
        raise SourceError(
            f"partition '{entry.partition_column}={requested}' not found "
            f"(available: {', '.join(available)})"
        )
    return requested


def read_partition(entry: SourceEntry, partition_value: str) -> dd.DataFrame:
    """Load one partition directory into a Dask DataFrame."""
    part_dir = os.path.join(
        entry.parquet_location, f"{entry.partition_column}={partition_value}"
    )
    files = glob.glob(os.path.join(part_dir, "*.parquet"))
    if not files:
        raise SourceError(f"partition directory '{part_dir}' has no parquet files")
    try:
        # Pass the directory (a single path string), not a file list: dask-sql's
        # parquet pushdown rejects a list argument.
        ddf = dd.read_parquet(part_dir)
    except Exception as exc:  # noqa: BLE001 — surface any read failure as SourceError
        raise SourceError(f"failed to read parquet at '{part_dir}': {exc}") from exc

    # The Hive partition column comes back redundantly (and as a 'category'
    # dtype that dask-sql cannot map) — drop it; partition_value is tracked
    # separately. Also normalise any remaining category columns to strings.
    if entry.partition_column in ddf.columns:
        ddf = ddf.drop(columns=[entry.partition_column])
    category_cols = [c for c, dt in ddf.dtypes.items() if str(dt) == "category"]
    for col in category_cols:
        ddf[col] = ddf[col].astype("object")
    return ddf


def resolve_and_read(
    entry: SourceEntry, requested_partition: Optional[str]
) -> Tuple[dd.DataFrame, str]:
    """Resolve the partition and read it. Returns (ddf, partition_value)."""
    partition_value = resolve_partition(entry, requested_partition)
    ddf = read_partition(entry, partition_value)
    return ddf, partition_value
