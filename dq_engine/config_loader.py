"""Load and validate DQ check definitions and the source catalog from CSV.

config/dq_checks.csv      -> list[CheckDefinition]
config/source_catalog.csv -> dict[table_name, SourceEntry]

Invalid rows are rejected with a descriptive reason and skipped; loading
continues for the remaining rows (per-row isolation). The first row of each
file is a header and is excluded.
"""

from __future__ import annotations

import csv
from typing import List, Dict, Tuple

from .domain import (
    CheckDefinition, SourceEntry,
    CHECK_TYPES, EVALUATION_MODES, SOURCE_TYPES,
)

CHECK_FIELDS = [
    "check_id", "table_name", "column_name", "data_type", "check_type",
    "business_glossary", "business_rule_filter", "deviation_tolerance",
    "check_sql", "evaluation_mode", "app_code",
]
SOURCE_FIELDS = ["table_name", "source_type", "parquet_location", "partition_column"]


class ConfigError(Exception):
    """Fatal config problem (e.g. file missing, header malformed)."""


def _blank_to_none(value):
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def load_checks(path: str) -> Tuple[List[CheckDefinition], List[str]]:
    """Return (valid checks, rejection messages)."""
    checks: List[CheckDefinition] = []
    rejects: List[str] = []
    seen_ids: Dict[str, int] = {}

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing_cols = [c for c in CHECK_FIELDS if c not in (reader.fieldnames or [])]
        if missing_cols:
            raise ConfigError(
                f"{path}: header missing column(s): {', '.join(missing_cols)}"
            )
        for lineno, row in enumerate(reader, start=2):
            errs = []
            check_id = _blank_to_none(row.get("check_id"))
            table_name = _blank_to_none(row.get("table_name"))
            column_name = _blank_to_none(row.get("column_name"))
            check_type = _blank_to_none(row.get("check_type"))
            evaluation_mode = _blank_to_none(row.get("evaluation_mode"))
            check_sql = _blank_to_none(row.get("check_sql"))

            for name, val in [
                ("check_id", check_id), ("table_name", table_name),
                ("column_name", column_name), ("check_type", check_type),
                ("evaluation_mode", evaluation_mode), ("check_sql", check_sql),
            ]:
                if val is None:
                    errs.append(f"missing required field '{name}'")

            if check_type is not None and check_type not in CHECK_TYPES:
                errs.append(
                    f"invalid check_type '{check_type}' "
                    f"(expected one of {', '.join(CHECK_TYPES)})"
                )
            if evaluation_mode is not None and evaluation_mode not in EVALUATION_MODES:
                errs.append(
                    f"invalid evaluation_mode '{evaluation_mode}' "
                    f"(expected one of {', '.join(EVALUATION_MODES)})"
                )

            tol_raw = _blank_to_none(row.get("deviation_tolerance"))
            tol = 0.0
            if tol_raw is not None:
                try:
                    tol = float(tol_raw)
                except ValueError:
                    errs.append(f"deviation_tolerance '{tol_raw}' is not numeric")

            if check_id is not None and check_id in seen_ids:
                errs.append(f"duplicate check_id '{check_id}'")

            if errs:
                rejects.append(f"line {lineno}: " + "; ".join(errs))
                continue

            seen_ids[check_id] = lineno
            checks.append(CheckDefinition(
                check_id=check_id,
                table_name=table_name,
                column_name=column_name,
                check_type=check_type,
                evaluation_mode=evaluation_mode,
                check_sql=check_sql,
                data_type=_blank_to_none(row.get("data_type")),
                business_glossary=_blank_to_none(row.get("business_glossary")),
                business_rule_filter=_blank_to_none(row.get("business_rule_filter")),
                deviation_tolerance=tol,
                app_code=_blank_to_none(row.get("app_code")),
            ))

    return checks, rejects


def load_catalog(path: str) -> Tuple[Dict[str, SourceEntry], List[str]]:
    """Return (table_name -> SourceEntry, rejection messages)."""
    catalog: Dict[str, SourceEntry] = {}
    rejects: List[str] = []
    duplicates = set()

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing_cols = [c for c in SOURCE_FIELDS if c not in (reader.fieldnames or [])]
        if missing_cols:
            raise ConfigError(
                f"{path}: header missing column(s): {', '.join(missing_cols)}"
            )
        for lineno, row in enumerate(reader, start=2):
            errs = []
            table_name = _blank_to_none(row.get("table_name"))
            source_type = _blank_to_none(row.get("source_type"))
            parquet_location = _blank_to_none(row.get("parquet_location"))
            partition_column = _blank_to_none(row.get("partition_column"))

            if table_name is None:
                errs.append("missing required field 'table_name'")
            if source_type is None:
                errs.append("missing required field 'source_type'")
            elif source_type not in SOURCE_TYPES:
                errs.append(
                    f"invalid source_type '{source_type}' "
                    f"(MVP supports only: {', '.join(SOURCE_TYPES)})"
                )
            if source_type == "parquet":
                if parquet_location is None:
                    errs.append("parquet source missing 'parquet_location'")
                if partition_column is None:
                    errs.append("parquet source missing 'partition_column'")

            if table_name is not None and table_name in catalog:
                duplicates.add(table_name)
                errs.append(f"duplicate catalog entry for table_name '{table_name}'")

            if errs:
                rejects.append(f"line {lineno}: " + "; ".join(errs))
                continue

            catalog[table_name] = SourceEntry(
                table_name=table_name,
                source_type=source_type,
                parquet_location=parquet_location,
                partition_column=partition_column,
            )

    # Drop any table that turned out to have duplicates (ambiguous resolution).
    for dup in duplicates:
        catalog.pop(dup, None)
        rejects.append(f"table_name '{dup}' dropped: duplicate catalog entries")

    return catalog, rejects
