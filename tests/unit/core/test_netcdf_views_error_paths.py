from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.netcdf_views import NetCdfMetadataView


def test_get_variables_raises_runtime_error_without_tracker_db() -> None:
    view = NetCdfMetadataView(SimpleNamespace(db=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_variables("climate")


def test_get_variables_passes_run_year_and_variable_type_filters_to_sql() -> None:
    captured: dict[str, object] = {}

    def fake_fetchdf(query: str, params: object = None) -> pd.DataFrame:
        captured["query"] = query
        captured["db"] = stub_db
        captured["params"] = params
        return pd.DataFrame(columns=["variable_name"])

    stub_db = SimpleNamespace(
        fetchdf=fake_fetchdf,
        has_table=lambda *_a, **_kw: True,
        has_table_column=lambda *_a, **_kw: True,
    )
    tracker = SimpleNamespace(db=stub_db)
    view = NetCdfMetadataView(tracker)

    result = view.get_variables(
        "climate",
        run_ids=["run_a", "run_b"],
        year=2024,
        variable_type="data",
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["db"] is tracker.db
    query_str = str(captured["query"])
    assert 'FROM "global_tables"."climate"' in query_str
    assert 'FROM "global_tables"."climate__dims"' in query_str
    assert 'FROM "global_tables"."climate__shape"' in query_str
    assert "COALESCE(r.id, a.run_id) IN (?, ?)" in query_str
    assert "AND r.year = ?" in query_str
    assert "AND nc.variable_type = ?" in query_str
    assert captured["params"] == ["climate", "run_a", "run_b", 2024, "data"]


def test_get_variables_returns_empty_dataframe_when_fetchdf_fails() -> None:
    def raise_fetchdf(*_args: object, **_kwargs: object) -> pd.DataFrame:
        raise RuntimeError("boom")

    stub_db = SimpleNamespace(
        fetchdf=raise_fetchdf,
        has_table=lambda *_a, **_kw: True,
        has_table_column=lambda *_a, **_kw: True,
    )
    view = NetCdfMetadataView(SimpleNamespace(db=stub_db))

    df = view.get_variables("climate")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_dimensions_returns_empty_dict_when_no_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(db=object()))
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: pd.DataFrame())

    assert view.get_dimensions("climate") == {}


def test_get_dimensions_skips_malformed_rows_and_aggregates_valid_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(db=object()))
    rows = pd.DataFrame(
        [
            {"dims": ["lat", "lon"], "shape": [10, 20]},
            {"dims": ["lat"], "shape": [10, 20]},
            {"dims": "not-a-list", "shape": [5]},
            {"dims": ["time", "lat"], "shape": [12, 10]},
            {"dims": ["bad"], "shape": ["oops"]},
        ]
    )
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: rows)

    assert view.get_dimensions("climate") == {"lat": 10, "lon": 20, "time": 12}


def test_get_attributes_handles_missing_variable_and_parses_json_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(db=object()))
    rows = pd.DataFrame(
        [
            {"variable_name": "temperature", "attributes": "{'units': 'K'}"},
            {"variable_name": "pressure", "attributes": '{"units": "Pa"}'},
            {"variable_name": "raw_dict", "attributes": {"units": "m"}},
        ]
    )
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: rows)

    assert view.get_attributes("climate", "missing") is None
    assert view.get_attributes("climate", "temperature") == {"units": "K"}
    assert view.get_attributes("climate", "pressure") == {"units": "Pa"}
    assert view.get_attributes("climate", "raw_dict") == {"units": "m"}


@pytest.mark.parametrize(
    "concept_key",
    [
        "123bad",
        "climate-1",
        "climate; DROP TABLE run; --",
        'bad"or"1"="1',
    ],
)
def test_get_variables_rejects_unsafe_concept_key(concept_key: str) -> None:
    called = {"fetchdf": False}

    def fake_fetchdf(*_args: object, **_kwargs: object) -> pd.DataFrame:
        called["fetchdf"] = True
        return pd.DataFrame()

    stub_db = SimpleNamespace(
        fetchdf=fake_fetchdf,
        has_table=lambda *_a, **_kw: True,
        has_table_column=lambda *_a, **_kw: True,
    )
    view = NetCdfMetadataView(SimpleNamespace(db=stub_db))

    with pytest.raises(ValueError, match="Invalid concept_key"):
        view.get_variables(concept_key)

    assert called["fetchdf"] is False
