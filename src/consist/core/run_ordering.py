from __future__ import annotations

from datetime import datetime
from typing import Optional

from consist.models.run import Run

RECENT_RUN_ORDER_BY_SQL: str = "created_at DESC, id DESC"


def recent_run_order_by_sql(table_alias: Optional[str] = None) -> str:
    """Return a DuckDB SQL ``ORDER BY`` clause for chronological ``Run`` ordering."""
    prefix = f"{table_alias.strip()}." if table_alias and table_alias.strip() else ""
    return f"{prefix}created_at DESC, {prefix}id DESC"


def recent_run_order_by(table_alias: Optional[str] = None) -> tuple[str, str]:
    """
    Return DuckDB ``ORDER BY`` expressions for stable chronological ``Run`` ordering.

    Ordering directly on ``TIMESTAMPTZ`` ``created_at`` preserves chronological order
    while allowing DuckDB row-group zonemap pruning on ``TOP_N`` queries.
    """
    prefix = f"{table_alias.strip()}." if table_alias and table_alias.strip() else ""
    return (
        f"{prefix}created_at DESC",
        f"{prefix}id DESC",
    )


def _timestamp_us_or_floor(value: Optional[datetime]) -> float:
    if value is None:
        return float("-inf")
    return value.timestamp()


def recent_run_sort_key(run: Run) -> tuple[float, str]:
    """Return a pure-Python sort key matching ``recent_run_order_by_sql`` (use ``reverse=True``)."""
    return (_timestamp_us_or_floor(run.created_at), run.id)
