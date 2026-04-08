from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlmodel import col

from consist.models.run import Run


def recent_run_order_by() -> tuple[Any, Any]:
    """
    Return a stable recency ordering for ``Run`` queries.

    DuckDB can truncate results for ``ORDER BY created_at DESC LIMIT ...`` on the
    ``run`` table. Ordering by ``epoch_us(created_at)`` preserves chronological
    order while avoiding that planner path, with ``id`` as a deterministic
    tiebreaker.
    """
    return (
        func.epoch_us(col(Run.created_at)).desc(),
        col(Run.id).desc(),
    )
