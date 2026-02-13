from types import SimpleNamespace

import pytest

from consist.integrations import dlt_loader
from consist.models.artifact import Artifact


def _artifact(*, key: str, driver: str, table_path: str | None = None) -> Artifact:
    return Artifact(
        key=key,
        container_uri=f"./{key}",
        driver=driver,
        table_path=table_path,
    )


def test_ingest_artifact_raises_when_no_data_iterable_or_path_is_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    artifact = _artifact(key="rows", driver="csv")
    run_context = SimpleNamespace(id="run-1", year=2025, iteration=1)

    with pytest.raises(ValueError, match="No data provided for ingestion"):
        dlt_loader.ingest_artifact(
            artifact=artifact,
            run_context=run_context,
            db_path="/tmp/provenance.duckdb",
            data_iterable=None,
        )


def test_ingest_artifact_h5_table_driver_requires_table_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    artifact = _artifact(key="h5_rows", driver="h5_table", table_path=None)
    run_context = SimpleNamespace(id="run-1", year=2025, iteration=1)

    with pytest.raises(ValueError, match="missing 'table_path'"):
        dlt_loader.ingest_artifact(
            artifact=artifact,
            run_context=run_context,
            db_path="/tmp/provenance.duckdb",
            data_iterable="/tmp/data.h5",
        )


def test_handle_netcdf_metadata_raises_when_xarray_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "xr", None)

    with pytest.raises(ImportError, match="xarray is required for NetCDF ingestion"):
        list(dlt_loader._handle_netcdf_metadata("/tmp/data.nc"))


def test_handle_netcdf_metadata_falls_back_to_default_open_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    open_calls: list[dict[str, str | None]] = []

    class _FakeArray:
        def __init__(self) -> None:
            self.dims = ("time",)
            self.shape = (3,)
            self.dtype = "float64"
            self.attrs = {"units": "m"}

    class _FakeDataset:
        data_vars = {"speed": _FakeArray()}
        coords = {"time": _FakeArray()}

    class _FakeXarray:
        @staticmethod
        def open_dataset(path: str, engine: str | None = None):
            open_calls.append({"path": path, "engine": engine})
            if engine is not None:
                raise RuntimeError("engine-specific reader failed")
            return _FakeDataset()

    monkeypatch.setattr(dlt_loader, "xr", _FakeXarray())
    monkeypatch.setattr(dlt_loader, "resolve_netcdf_engine", lambda: "h5netcdf")

    records = list(dlt_loader._handle_netcdf_metadata("/tmp/data.nc"))

    assert len(records) == 2
    assert records[0]["variable_name"] == "speed"
    assert records[0]["variable_type"] == "data"
    assert open_calls == [
        {"path": "/tmp/data.nc", "engine": "h5netcdf"},
        {"path": "/tmp/data.nc", "engine": None},
    ]


def test_handle_openmatrix_metadata_raises_when_openmatrix_and_h5py_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "openmatrix", None)
    monkeypatch.setattr(dlt_loader, "h5py", None)

    with pytest.raises(ImportError, match="h5py or openmatrix is required"):
        list(dlt_loader._handle_openmatrix_metadata("/tmp/data.omx"))


def test_handle_openmatrix_metadata_falls_back_to_h5py_on_openmatrix_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDataset:
        def __init__(self) -> None:
            self.shape = (2, 3)
            self.dtype = "float64"
            self.attrs = {"units": "trips"}

    class _FakeGroup:
        def __init__(self, values):
            self._values = values

        def items(self):
            return self._values.items()

    class _FakeFile:
        def __init__(self, *_args, **_kwargs) -> None:
            self._root = _FakeGroup({"matrix_a": _FakeDataset()})

        def __enter__(self):
            return self._root

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeOpenMatrix:
        @staticmethod
        def open_file(*_args, **_kwargs):
            raise RuntimeError("openmatrix failed")

    fake_h5py = SimpleNamespace(File=_FakeFile, Dataset=_FakeDataset, Group=_FakeGroup)
    monkeypatch.setattr(dlt_loader, "openmatrix", _FakeOpenMatrix())
    monkeypatch.setattr(dlt_loader, "h5py", fake_h5py)

    records = list(dlt_loader._handle_openmatrix_metadata("/tmp/data.omx"))

    assert records == [
        {
            "matrix_name": "matrix_a",
            "path": "/matrix_a",
            "shape": [2, 3],
            "dtype": "float64",
            "n_rows": 2,
            "n_cols": 3,
            "attributes": '{"units": "trips"}',
        }
    ]


def test_handle_spatial_metadata_raises_when_geopandas_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "geopandas", None)

    with pytest.raises(
        ImportError, match="geopandas is required for spatial ingestion"
    ):
        list(dlt_loader._handle_spatial_metadata("/tmp/data.geojson"))


def test_handle_spatial_metadata_wraps_reader_errors_in_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeGeoPandas:
        @staticmethod
        def read_file(_path: str):
            raise RuntimeError("boom")

    monkeypatch.setattr(dlt_loader, "geopandas", _FakeGeoPandas())

    with pytest.raises(ValueError, match="Failed to extract spatial metadata .* boom"):
        list(dlt_loader._handle_spatial_metadata("/tmp/data.geojson"))
