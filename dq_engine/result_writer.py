"""Write DqResults to CSV.

output/results.csv     — appended every run (full history; feeds MoM + classification)
output/exceptions.csv  — overwritten every run with this run's failed/errored rows
"""

from __future__ import annotations

import csv
import os
from typing import List

from .domain import DqResult, RESULT_COLUMNS


def _ensure_dir(path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)


def _row_for_csv(result: DqResult) -> dict:
    row = result.to_row()
    return {col: ("" if row.get(col) is None else row.get(col)) for col in RESULT_COLUMNS}


def append_results(path: str, results: List[DqResult]) -> None:
    """Append results to the history CSV, writing a header if new/empty."""
    _ensure_dir(path)
    new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        if new_file:
            writer.writeheader()
        for result in results:
            writer.writerow(_row_for_csv(result))


def write_exceptions(path: str, results: List[DqResult]) -> int:
    """Overwrite the exceptions CSV with this run's failed/errored results.
    Returns the number of exception rows written."""
    _ensure_dir(path)
    exceptions = [r for r in results if r.is_open()]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        for result in exceptions:
            writer.writerow(_row_for_csv(result))
    return len(exceptions)
