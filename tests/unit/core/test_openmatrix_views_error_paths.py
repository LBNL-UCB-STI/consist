from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.openmatrix_views import OpenMatrixMetadataView


def test_get_matrices_raises_runtime_error_without_tracker_engine() -> None:
    view = OpenMatrixMetadataView(SimpleNamespace(engine=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_matrices("demand")


def test_get_matrices_passes_filters_to_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    tracker = SimpleNamespace(engine=object())
    view = OpenMatrixMetadataView(tracker)
    captured: dict[str, object] = {}

    def fake_read_sql(query, engine, params=None):
        captured["query"] = query
        captured["engine"] = engine
        captured["params"] = params
        return pd.DataFrame(columns=["matrix_name"])

    monkeypatch.setattr("consist.core.openmatrix_views.pd.read_sql", fake_read_sql)

    result = view.get_matrices(
        "demand",
        run_ids=["run_a", "run_b"],
        year=2024,
        min_rows=10,
        max_rows=1000,
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["engine"] is tracker.engine
    assert 'FROM "global_tables"."demand"' in str(captured["query"])
    assert 'FROM "global_tables"."demand__shape"' in str(captured["query"])
    assert "r.id IN (:0,:1)" in str(captured["query"])
    assert "AND r.year = :year" in str(captured["query"])
    assert "AND omx.n_rows >= :min_rows" in str(captured["query"])
    assert "AND omx.n_rows <= :max_rows" in str(captured["query"])
    assert captured["params"] == {
        "0": "run_a",
        "1": "run_b",
        "year": 2024,
        "min_rows": 10,
        "max_rows": 1000,
    }


@pytest.mark.parametrize(
    "concept_key",
    [
        "bad-key",
        "bad key",
        "bad.key",
        "1bad",
        "bad;DROP TABLE run;--",
        'bad"or"1"="1',
    ],
)
def test_get_matrices_rejects_unsafe_concept_key(
    monkeypatch: pytest.MonkeyPatch, concept_key: str
) -> None:
    called = {"read_sql": False}

    def fake_read_sql(*_args, **_kwargs):
        called["read_sql"] = True
        return pd.DataFrame()

    monkeypatch.setattr("consist.core.openmatrix_views.pd.read_sql", fake_read_sql)
    view = OpenMatrixMetadataView(SimpleNamespace(engine=object()))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_matrices(concept_key)

    assert called["read_sql"] is False
