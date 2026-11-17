from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.openmatrix_views import OpenMatrixMetadataView


def test_get_matrices_raises_runtime_error_without_tracker_db() -> None:
    view = OpenMatrixMetadataView(SimpleNamespace(db=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_matrices("demand")


def test_get_matrices_passes_filters_to_sql() -> None:
    captured: dict[str, object] = {}

    def fake_fetchdf(query: str, params: object = None) -> pd.DataFrame:
        captured["query"] = query
        captured["db"] = stub_db
        captured["params"] = params
        return pd.DataFrame(columns=["matrix_name"])

    stub_db = SimpleNamespace(
        fetchdf=fake_fetchdf,
        has_table=lambda *_a, **_kw: True,
        has_table_column=lambda *_a, **_kw: True,
    )
    tracker = SimpleNamespace(db=stub_db)
    view = OpenMatrixMetadataView(tracker)

    result = view.get_matrices(
        "demand",
        run_ids=["run_a", "run_b"],
        year=2024,
        min_rows=10,
        max_rows=1000,
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["db"] is tracker.db
    query_str = str(captured["query"])
    assert 'FROM "global_tables"."demand"' in query_str
    assert 'FROM "global_tables"."demand__shape"' in query_str
    assert "COALESCE(r.id, a.run_id) IN (?, ?)" in query_str
    assert "AND r.year = ?" in query_str
    assert "AND omx.n_rows >= ?" in query_str
    assert "AND omx.n_rows <= ?" in query_str
    assert captured["params"] == ["demand", "run_a", "run_b", 2024, 10, 1000]


@pytest.mark.parametrize(
    "concept_key",
    [
        "123bad",
        "demand-1",
        "demand; DROP TABLE run; --",
        'bad"or"1"="1',
    ],
)
def test_get_matrices_rejects_unsafe_concept_key(concept_key: str) -> None:
    called = {"fetchdf": False}

    def fake_fetchdf(*_args: object, **_kwargs: object) -> pd.DataFrame:
        called["fetchdf"] = True
        return pd.DataFrame()

    stub_db = SimpleNamespace(
        fetchdf=fake_fetchdf,
        has_table=lambda *_a, **_kw: True,
        has_table_column=lambda *_a, **_kw: True,
    )
    view = OpenMatrixMetadataView(SimpleNamespace(db=stub_db))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_matrices(concept_key)

    assert called["fetchdf"] is False
