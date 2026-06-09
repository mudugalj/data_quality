"""Read-only SQL gate.

The engine only ever executes user-supplied check_sql that reads data. This
module rejects anything that is not a single SELECT / WITH statement, plus any
data-modifying or DDL keyword and multi-statement batches.

It is intentionally lightweight (no parser dependency) for the MVP. A
structural parser (e.g. sqlglot) is the natural post-MVP hardening step.
"""

from __future__ import annotations

import re

# Whole-word keywords that must never appear in a read-only check.
_FORBIDDEN = {
    "insert", "update", "delete", "merge", "upsert",
    "create", "alter", "drop", "truncate", "replace",
    "grant", "revoke", "call", "exec", "execute", "copy",
    "attach", "vacuum", "analyze", "set",
}

_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE = re.compile(r"--[^\n]*")
_WORD = re.compile(r"[A-Za-z_]+")


class SqlSafetyError(ValueError):
    """Raised when check_sql is not a safe, single read-only statement."""


def _strip_comments(sql: str) -> str:
    sql = _COMMENT_BLOCK.sub(" ", sql)
    sql = _COMMENT_LINE.sub(" ", sql)
    return sql


def validate_read_only(sql: str) -> str:
    """Return the cleaned SQL if it is a single read-only SELECT/CTE.

    Raises SqlSafetyError otherwise.
    """
    if sql is None or not sql.strip():
        raise SqlSafetyError("check_sql is empty")

    cleaned = _strip_comments(sql).strip()

    # Reject multi-statement batches. A single trailing semicolon is allowed.
    body = cleaned.rstrip(";").strip()
    if ";" in body:
        raise SqlSafetyError("multiple statements are not allowed")

    lowered = body.lower()
    first_word = _WORD.search(lowered)
    if first_word is None or first_word.group(0) not in ("select", "with"):
        raise SqlSafetyError("check_sql must be a single SELECT or WITH query")

    tokens = set(_WORD.findall(lowered))
    forbidden = tokens & _FORBIDDEN
    if forbidden:
        raise SqlSafetyError(
            f"forbidden keyword(s) in check_sql: {', '.join(sorted(forbidden))}"
        )

    return body


def validate_filter(predicate: str) -> str:
    """Validate a business_rule_filter WHERE-predicate fragment."""
    if predicate is None or not predicate.strip():
        raise SqlSafetyError("empty business_rule_filter")
    cleaned = _strip_comments(predicate).strip().rstrip(";")
    if ";" in cleaned:
        raise SqlSafetyError("business_rule_filter must not contain ';'")
    tokens = set(_WORD.findall(cleaned.lower()))
    forbidden = tokens & _FORBIDDEN
    if forbidden:
        raise SqlSafetyError(
            f"forbidden keyword(s) in business_rule_filter: {', '.join(sorted(forbidden))}"
        )
    return cleaned
