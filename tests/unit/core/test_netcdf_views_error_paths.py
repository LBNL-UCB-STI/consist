from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.netcdf_views import NetCdfMetadataView


def test_get_variables_raises_runtime_error_without_tracker_engine() -> None:
    view = NetCdfMetadataView(SimpleNamespace(engine=None))

    with pytest.raises(RuntimeError, match="Database connection required"):
        view.get_variables("climate")


def test_get_variables_passes_run_year_and_variable_type_filters_to_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(engine=object())
    view = NetCdfMetadataView(tracker)
    captured: dict[str, object] = {}

    def fake_read_sql(query, engine, params=None):
        captured["query"] = query
        captured["engine"] = engine
        captured["params"] = params
        return pd.DataFrame(columns=["variable_name"])

    monkeypatch.setattr("consist.core.netcdf_views.pd.read_sql", fake_read_sql)

    result = view.get_variables(
        "climate",
        run_ids=["run_a", "run_b"],
        year=2024,
        variable_type="data",
    )

    assert isinstance(result, pd.DataFrame)
    assert captured["engine"] is tracker.engine
    assert "r.id IN (:0,:1)" in str(captured["query"])
    assert "AND r.year = :year" in str(captured["query"])
    assert "AND nc.variable_type = :var_type" in str(captured["query"])
    assert captured["params"] == {
        "0": "run_a",
        "1": "run_b",
        "year": 2024,
        "var_type": "data",
    }


def test_get_variables_returns_empty_dataframe_when_read_sql_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(engine=object()))

    def raise_read_sql(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("consist.core.netcdf_views.pd.read_sql", raise_read_sql)

    df = view.get_variables("climate")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_dimensions_returns_empty_dict_when_no_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(engine=object()))
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: pd.DataFrame())

    assert view.get_dimensions("climate") == {}


def test_get_dimensions_skips_malformed_rows_and_aggregates_valid_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view = NetCdfMetadataView(SimpleNamespace(engine=object()))
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
    view = NetCdfMetadataView(SimpleNamespace(engine=object()))
    rows = pd.DataFrame(
        [
            {"variable_name": "temperature", "attributes": "{'units': 'K'}"},
            {"variable_name": "humidity", "attributes": {"units": "%"}},
        ]
    )
    monkeypatch.setattr(view, "get_variables", lambda *args, **kwargs: rows)

    assert view.get_attributes("climate", variable_name="missing") is None
    assert view.get_attributes("climate", variable_name="temperature") == {"units": "K"}

