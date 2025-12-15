from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, Union

# A curated set of common `Run` fields that are stable and useful as dictionary keys.
# This intentionally excludes nested/JSON fields (e.g., `meta`) and list fields (e.g., `tags`).
RunIndexField: TypeAlias = Literal[
    "id",
    "model_name",
    "status",
    "year",
    "iteration",
    "parent_run_id",
    "config_hash",
    "input_hash",
    "git_hash",
    "signature",
    "description",
    "created_at",
    "started_at",
    "ended_at",
]


@dataclass(frozen=True, slots=True)
class RunFieldIndex:
    """Index `Tracker.find_runs(..., index_by=...)` results by a `Run` attribute."""

    field: RunIndexField


@dataclass(frozen=True, slots=True)
class FacetIndex:
    """
    Index `Tracker.find_runs(..., index_by=...)` results by a persisted facet value.

    The facet must be indexed to `RunConfigKV` (default when `facet_index=True`) and
    requires a DB-backed tracker.
    """

    key: str


IndexBySpec: TypeAlias = Union[RunFieldIndex, FacetIndex]


def index_by_field(field: RunIndexField) -> RunFieldIndex:
    """Typed helper for `index_by=...` keyed by a Run field."""

    return RunFieldIndex(field=field)


def index_by_facet(key: str) -> FacetIndex:
    """Typed helper for `index_by=...` keyed by a facet key."""

    return FacetIndex(key=key)

