from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.openmatrix_views import OpenMatrixMetadataView


def test_get_matrices_raises_runtime_error_without_tracker_ledger() -> None:
    view = OpenMatrixMetadataView(SimpleNamespace(ledger=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_matrices("demand")


def test_get_matrices_passes_filters_to_sql() -> None:
    captured: dict[str, object] = {}

    def fake_query_df(query: str, params: object = None) -> pd.DataFrame:
        captured["query"] = query
        captured["ledger"] = stub_ledger
        captured["params"] = params
        return pd.DataFrame(columns=["matrix_name"])

    stub_ledger = SimpleNamespace(query_df=fake_query_df)
    tracker = SimpleNamespace(ledger=stub_ledger)
    view = OpenMatrixMetadataView(tracker)

    result = view.get_matrices(
        "demand",
        run_ids=["run_a", "run_b"],
        year=2024,
        min_rows=10,
        max_rows=1000,
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["ledger"] is tracker.ledger
    query_str = str(captured["query"])
    assert 'FROM "global_tables"."demand"' in query_str
    assert 'FROM "global_tables"."demand__shape"' in query_str
    assert "COALESCE(r.id, a.run_id) IN (SELECT UNNEST(?))" in query_str
    assert "AND r.year = ?" in query_str
    assert "AND omx.n_rows >= ?" in query_str
    assert "AND omx.n_rows <= ?" in query_str
    assert captured["params"] == [["run_a", "run_b"], 2024, 10, 1000]


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
def test_get_matrices_rejects_unsafe_concept_key(concept_key: str) -> None:
    called = {"query_df": False}

    def fake_query_df(*_args: object, **_kwargs: object) -> pd.DataFrame:
        called["query_df"] = True
        return pd.DataFrame()

    stub_ledger = SimpleNamespace(query_df=fake_query_df)
    view = OpenMatrixMetadataView(SimpleNamespace(ledger=stub_ledger))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_matrices(concept_key)

    assert called["query_df"] is False
