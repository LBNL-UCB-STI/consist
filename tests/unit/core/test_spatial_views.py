"""Documentation-oriented unit tests for SpatialMetadataView behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import pytest

from consist.core.spatial_views import SpatialMetadataView


@dataclass
class _TrackerStub:
    ledger: Any


class _LedgerStub:
    def __init__(self, handler: Any = None) -> None:
        self._handler = handler
        self.calls: list[tuple[str, Any]] = []

    def query_df(self, query: str, params: Any = None) -> pd.DataFrame:
        self.calls.append((query, params))
        if self._handler is not None:
            return self._handler(query, params)
        return _metadata_frame()


def _metadata_frame(
    *,
    run_id: Any = "run-1",
    bounds: Any = [0.0, 0.0, 1.0, 1.0],  # noqa: B006
    feature_count: Any = 2,
    geometry_types: Any = ["Point"],
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "bounds": bounds,
                "crs": "EPSG:4326",
                "feature_count": feature_count,
                "geometry_types": geometry_types,
                "geometry_column": "geometry",
                "column_names": ["id", "geometry"],
                "run_id": run_id,
                "year": 2025,
                "iteration": 0,
            }
        ]
    )


def test_get_metadata_requires_database_connection() -> None:
    """`get_metadata` should fail fast when no DB ledger is configured."""
    view = SpatialMetadataView(_TrackerStub(ledger=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_metadata("tiny_spatial")


def test_get_metadata_builds_run_id_and_year_filters() -> None:
    """
    `get_metadata` should parameterize run-id and year filters.

    This test documents expected SQL/parameter behavior for contributors
    touching query construction logic.
    """
    ledger = _LedgerStub()
    view = SpatialMetadataView(_TrackerStub(ledger=ledger))

    out = view.get_metadata(
        "tiny_spatial",
        run_ids=["run-a", "run-b"],
        year=2025,
    )

    assert not out.empty
    assert len(ledger.calls) == 1
    query, params = ledger.calls[0]
    assert 'global_tables."tiny_spatial"' in query
    assert "IN (SELECT UNNEST(?))" in query
    assert "AND r.year = ?" in query
    assert params == [["run-a", "run-b"], 2025]


def test_get_metadata_returns_empty_dataframe_when_query_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`get_metadata` should degrade gracefully to an empty DataFrame on SQL errors."""

    def raise_boom(query: str, params: Any) -> pd.DataFrame:
        raise ValueError("boom")

    view = SpatialMetadataView(_TrackerStub(ledger=_LedgerStub(raise_boom)))

    out = view.get_metadata("tiny_spatial")

    assert out.empty
    assert "Failed to query spatial metadata for tiny_spatial" in caplog.text


def test_get_bounds_ignores_rows_without_valid_string_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_bounds` should only emit entries for non-empty string run IDs."""
    df = pd.DataFrame(
        [
            {"run_id": None, "bounds": [0, 0, 1, 1]},
            {"run_id": "", "bounds": [1, 1, 2, 2]},
            {"run_id": "run-ok", "bounds": [2, 2, 3, 3]},
        ]
    )

    monkeypatch.setattr(
        "consist.core.spatial_views.SpatialMetadataView.get_metadata",
        lambda self, concept_key, run_ids=None, year=None: df,
    )
    view = SpatialMetadataView(_TrackerStub(ledger=_LedgerStub()))

    assert view.get_bounds("tiny_spatial") == {"run-ok": [2, 2, 3, 3]}


def test_get_feature_counts_accepts_integral_and_float_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    `get_feature_counts` should coerce integer-like values and skip invalid ones.

    Float values are truncated via `int(...)` by current implementation.
    """
    df = pd.DataFrame(
        [
            {"run_id": "run-int", "feature_count": 5},
            {"run_id": "run-float", "feature_count": 7.9},
            {"run_id": "run-str", "feature_count": "not-a-number"},
            {"run_id": None, "feature_count": 11},
        ]
    )

    monkeypatch.setattr(
        "consist.core.spatial_views.SpatialMetadataView.get_metadata",
        lambda self, concept_key, run_ids=None, year=None: df,
    )
    view = SpatialMetadataView(_TrackerStub(ledger=_LedgerStub()))

    assert view.get_feature_counts("tiny_spatial") == {"run-int": 5, "run-float": 7}


def test_get_geometry_types_returns_empty_mapping_for_empty_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_geometry_types` should return `{}` when no metadata rows are available."""
    monkeypatch.setattr(
        "consist.core.spatial_views.SpatialMetadataView.get_metadata",
        lambda self, concept_key, run_ids=None, year=None: pd.DataFrame(),
    )
    view = SpatialMetadataView(_TrackerStub(ledger=_LedgerStub()))

    assert view.get_geometry_types("tiny_spatial") == {}


@pytest.mark.parametrize(
    "concept_key",
    [
        "tiny-spatial",
        "tiny spatial",
        "tiny.spatial",
        "1tiny_spatial",
        "tiny_spatial;DROP TABLE run;--",
        'tiny_spatial"or"1"="1',
    ],
)
def test_get_metadata_rejects_unsafe_concept_key(concept_key: str) -> None:
    """
    `get_metadata` should reject unsafe table identifiers before SQL construction.

    This test documents the injection boundary for dynamic global_tables names.
    """
    ledger = _LedgerStub()
    view = SpatialMetadataView(_TrackerStub(ledger=ledger))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_metadata(concept_key)

    assert ledger.calls == []
