from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.netcdf_views import NetCdfMetadataView


def test_get_variables_raises_runtime_error_without_tracker_ledger() -> None:
    view = NetCdfMetadataView(SimpleNamespace(ledger=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_variables("climate")


def test_get_variables_passes_run_year_and_variable_type_filters_to_sql() -> None:
    captured: dict[str, object] = {}

    def fake_query_df(query: str, params: object = None) -> pd.DataFrame:
        captured["query"] = query
        captured["ledger"] = stub_ledger
        captured["params"] = params
        return pd.DataFrame(columns=["variable_name"])

    stub_ledger = SimpleNamespace(query_df=fake_query_df)
    tracker = SimpleNamespace(ledger=stub_ledger)
    view = NetCdfMetadataView(tracker)

    result = view.get_variables(
        "climate",
        run_ids=["run_a", "run_b"],
        year=2024,
        variable_type="data",
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["ledger"] is tracker.ledger
    query_str = str(captured["query"])
    assert 'FROM "global_tables"."climate"' in query_str
    assert 'FROM "global_tables"."climate__dims"' in query_str
    assert 'FROM "global_tables"."climate__shape"' in query_str
    assert "COALESCE(r.id, a.run_id) IN (SELECT UNNEST(?))" in query_str
    assert "AND r.year = ?" in query_str
    assert "AND nc.variable_type = ?" in query_str
    assert captured["params"] == [["run_a", "run_b"], 2024, "data"]


def test_get_variables_returns_empty_dataframe_when_query_df_fails() -> None:
    def raise_query_df(*_args: object, **_kwargs: object) -> pd.DataFrame:
        raise RuntimeError("boom")

    stub_ledger = SimpleNamespace(query_df=raise_query_df)
    view = NetCdfMetadataView(SimpleNamespace(ledger=stub_ledger))

    df = view.get_variables("climate")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_dimensions_returns_empty_dict_when_no_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(ledger=object()))
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: pd.DataFrame())

    assert view.get_dimensions("climate") == {}


def test_get_dimensions_skips_malformed_rows_and_aggregates_valid_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(ledger=object()))
    rows = pd.DataFrame(
        [
            {"dims": ["lat", "lon"], "shape": [10, 20]},
            {"dims": "['lat', 'lon']", "shape": "[15, 18]"},
            {"dims": "not-json", "shape": "[1]"},
            {"dims": ["time"], "shape": "not-json"},
            {"dims": ["time"], "shape": [7]},
        ]
    )
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: rows)

    dims = view.get_dimensions("climate")

    assert dims == {"lat": 15, "lon": 20, "time": 7}


def test_get_attributes_handles_missing_variable_and_parses_json_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(ledger=object()))
    rows = pd.DataFrame(
        [
            {"variable_name": "temperature", "attributes": "{'units': 'K'}"},
            {"variable_name": "humidity", "attributes": {"units": "%"}},
        ]
    )
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: rows)

    assert view.get_attributes("climate", variable_name="missing") is None
    assert view.get_attributes("climate", variable_name="temperature") == {"units": "K"}


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
def test_get_variables_rejects_unsafe_concept_key(concept_key: str) -> None:
    called = {"query_df": False}

    def fake_query_df(*_args: object, **_kwargs: object) -> pd.DataFrame:
        called["query_df"] = True
        return pd.DataFrame()

    stub_ledger = SimpleNamespace(query_df=fake_query_df)
    view = NetCdfMetadataView(SimpleNamespace(ledger=stub_ledger))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_variables(concept_key)

    assert called["query_df"] is False
