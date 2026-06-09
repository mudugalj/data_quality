"""Domain model for the DQ Engine MVP.

All check definitions and results are plain dataclasses. The wire format for
every enum-like field is a lowercase string so it round-trips cleanly through
CSV.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional

# --- Controlled vocabularies (validated as plain strings, case-sensitive) ---

CHECK_TYPES = ("completeness", "validity", "consistency")
EVALUATION_MODES = ("violation", "consistency_mom")
SOURCE_TYPES = ("parquet",)            # MVP: parquet only
STATUSES = ("passed", "failed", "errored")
ISSUE_STATUSES = ("new", "recurring", "resolved")

# The open / "did not pass" set.
OPEN_STATUSES = ("failed", "errored")


@dataclass
class CheckDefinition:
    """One DQ check, sourced from config/dq_checks.csv."""

    check_id: str
    table_name: str
    column_name: str
    check_type: str          # completeness | validity | consistency
    evaluation_mode: str     # violation | consistency_mom
    check_sql: str
    data_type: Optional[str] = None
    business_glossary: Optional[str] = None
    business_rule_filter: Optional[str] = None
    deviation_tolerance: float = 0.0
    app_code: Optional[str] = None


@dataclass
class SourceEntry:
    """One source-catalog row, sourced from config/source_catalog.csv."""

    table_name: str
    source_type: str                    # always "parquet" in the MVP
    parquet_location: str
    partition_column: str


@dataclass
class DqResult:
    """One executed check's outcome. This is also the row written to
    output/results.csv (the full history that feeds MoM + classification)."""

    dq_ref_id: str
    check_id: str
    app_code: Optional[str]
    run_id: str
    run_timestamp: str          # ISO 8601
    month_end_date: str         # YYYY-MM-DD — reporting period month end
    table_name: str
    source_type: str
    column_name: str
    partition_value: Optional[str]
    check_type: str
    evaluation_mode: str
    status: str                 # passed | failed | errored
    issue_status: Optional[str] = None   # new | recurring | resolved | None
    violation_count: Optional[int] = None
    current_value: Optional[float] = None
    prior_value: Optional[float] = None
    deviation: Optional[float] = None    # percentage deviation (MoM)
    description: str = ""

    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES

    def to_row(self) -> dict:
        return asdict(self)


# Column order for the results / exceptions CSV files.
RESULT_COLUMNS = [
    "dq_ref_id", "check_id", "app_code", "run_id", "run_timestamp",
    "month_end_date", "table_name", "source_type", "column_name",
    "partition_value", "check_type", "evaluation_mode", "status",
    "issue_status", "violation_count", "current_value", "prior_value",
    "deviation", "description",
]
