from types import SimpleNamespace

import pandas as pd
import pytest
from sqlmodel import SQLModel

from consist.integrations import dlt_loader
from consist.models.artifact import Artifact


class _StrictSchema(SQLModel):
    value: int


def _artifact(*, key: str, driver: str, table_path: str | None = None) -> Artifact:
    return Artifact(
        key=key,
        container_uri=f"./{key}",
        driver=driver,
        table_path=table_path,
    )


def _run_context() -> SimpleNamespace:
    return SimpleNamespace(id="run-1", year=2025, iteration=1)


def _lock_error() -> RuntimeError:
    return RuntimeError(
        'IO Error: Could not set lock on file "/tmp/provenance.duckdb": '
        "Conflicting lock is held in PID 0."
    )


def _fake_dlt_module(
    *,
    run_behavior=None,
    load_behavior=None,
    normalize_prefix: str = "normalized_",
    resource_observer=None,
):
    captured: dict[str, object] = {}

    class _FakePipeline:
        def __init__(self) -> None:
            self.default_schema = SimpleNamespace(
                naming=SimpleNamespace(
                    normalize_table_identifier=lambda name: f"{normalize_prefix}{name}"
                )
            )
            self.run_calls = 0
            self.load_calls = 0
            self.resource = None

        def run(self, resource):
            self.run_calls += 1
            self.resource = resource
            if run_behavior is not None:
                result = run_behavior(self, resource)
                if isinstance(result, Exception):
                    raise result
                return result
            list(resource["rows"])
            return {"phase": "run"}

        def load(self):
            self.load_calls += 1
            if load_behavior is not None:
                result = load_behavior(self)
                if isinstance(result, Exception):
                    raise result
                return result
            return {"phase": "load"}

    fake_pipeline = _FakePipeline()

    def _resource(rows, *, name, write_disposition, columns, schema_contract):
        payload = {
            "rows": rows,
            "name": name,
            "write_disposition": write_disposition,
            "columns": columns,
            "schema_contract": schema_contract,
        }
        captured["resource"] = payload
        if resource_observer is not None:
            resource_observer(payload)
        return payload

    fake_dlt = SimpleNamespace(
        destinations=SimpleNamespace(duckdb=lambda path: {"db_path": path}),
        pipeline=lambda **kwargs: fake_pipeline,
        resource=_resource,
    )
    return fake_dlt, fake_pipeline, captured


def test_ingest_artifact_raises_when_no_data_iterable_or_path_is_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    artifact = _artifact(key="rows", driver="csv")
    run_context = _run_context()

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
    run_context = _run_context()

    with pytest.raises(ValueError, match="missing 'table_path'"):
        dlt_loader.ingest_artifact(
            artifact=artifact,
            run_context=run_context,
            db_path="/tmp/provenance.duckdb",
            data_iterable="/tmp/data.h5",
        )


def test_ingest_artifact_unsupported_driver_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    with pytest.raises(ValueError, match="Ingestion not supported for driver"):
        dlt_loader.ingest_artifact(
            artifact=_artifact(key="bad", driver="unsupported_driver"),
            run_context=_run_context(),
            db_path="/tmp/provenance.duckdb",
            data_iterable="/tmp/input.unsupported",
        )


def test_ingest_artifact_strict_schema_violation_for_dataframe_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_dlt, _pipeline, _captured = _fake_dlt_module()
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)

    with pytest.raises(ValueError, match=r"undefined columns .*unexpected"):
        dlt_loader.ingest_artifact(
            artifact=_artifact(key="rows", driver="csv"),
            run_context=_run_context(),
            db_path="/tmp/provenance.duckdb",
            data_iterable=pd.DataFrame([{"value": 1, "unexpected": 99}]),
            schema_model=_StrictSchema,
        )


def test_ingest_artifact_strict_schema_violation_for_dict_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_dlt, _pipeline, _captured = _fake_dlt_module()
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)

    with pytest.raises(ValueError, match=r"undefined columns .*unexpected"):
        dlt_loader.ingest_artifact(
            artifact=_artifact(key="rows", driver="csv"),
            run_context=_run_context(),
            db_path="/tmp/provenance.duckdb",
            data_iterable=[{"value": 1, "unexpected": 99}],
            schema_model=_StrictSchema,
        )


def test_ingest_artifact_dataframe_enrichment_happens_before_resource_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def _resource_observer(payload) -> None:
        first = next(payload["rows"])
        observed["columns"] = list(first.columns)
        observed["run_id"] = first.iloc[0]["consist_run_id"]
        observed["artifact_id"] = first.iloc[0]["consist_artifact_id"]
        observed["year"] = first.iloc[0]["consist_year"]
        observed["iteration"] = first.iloc[0]["consist_iteration"]
        payload["rows"] = [first]

    fake_dlt, _pipeline, _captured = _fake_dlt_module(
        resource_observer=_resource_observer
    )
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)

    artifact = _artifact(key="rows", driver="csv")
    info, table = dlt_loader.ingest_artifact(
        artifact=artifact,
        run_context=_run_context(),
        db_path="/tmp/provenance.duckdb",
        data_iterable=pd.DataFrame([{"value": 1}]),
    )

    assert info == {"phase": "run"}
    assert table == "normalized_rows"
    assert {
        "consist_run_id",
        "consist_artifact_id",
        "consist_year",
        "consist_iteration",
    }.issubset(set(observed["columns"]))
    assert observed["run_id"] == "run-1"
    assert observed["artifact_id"] == str(artifact.id)
    assert observed["year"] == 2025
    assert observed["iteration"] == 1


def test_ingest_artifact_non_retryable_run_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = RuntimeError("non-retryable failure")

    def _run_behavior(_pipeline, _resource):
        return sentinel

    fake_dlt, _pipeline, _captured = _fake_dlt_module(run_behavior=_run_behavior)
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)

    with pytest.raises(RuntimeError) as exc_info:
        dlt_loader.ingest_artifact(
            artifact=_artifact(key="rows", driver="csv"),
            run_context=_run_context(),
            db_path="/tmp/provenance.duckdb",
            data_iterable=[{"value": 1}],
        )
    assert exc_info.value is sentinel


def test_ingest_artifact_retryable_lock_then_load_success_returns_info_and_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _run_behavior(_pipeline, _resource):
        return _lock_error()

    def _load_behavior(_pipeline):
        return {"phase": "load"}

    fake_dlt, pipeline, _captured = _fake_dlt_module(
        run_behavior=_run_behavior,
        load_behavior=_load_behavior,
    )
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)
    monkeypatch.setattr(dlt_loader, "_retry_sleep", lambda *_args, **_kwargs: None)

    info, table = dlt_loader.ingest_artifact(
        artifact=_artifact(key="rows", driver="csv"),
        run_context=_run_context(),
        db_path="/tmp/provenance.duckdb",
        data_iterable=[{"value": 1}],
        lock_retries=3,
    )

    assert info == {"phase": "load"}
    assert table == "normalized_rows"
    assert pipeline.run_calls == 1
    assert pipeline.load_calls == 1


def test_ingest_artifact_retryable_lock_exhausts_retries_and_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _run_behavior(_pipeline, _resource):
        return _lock_error()

    def _load_behavior(_pipeline):
        return _lock_error()

    fake_dlt, pipeline, _captured = _fake_dlt_module(
        run_behavior=_run_behavior,
        load_behavior=_load_behavior,
    )
    monkeypatch.setattr(dlt_loader, "dlt", fake_dlt)
    monkeypatch.setattr(dlt_loader, "_retry_sleep", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="Could not set lock on file"):
        dlt_loader.ingest_artifact(
            artifact=_artifact(key="rows", driver="csv"),
            run_context=_run_context(),
            db_path="/tmp/provenance.duckdb",
            data_iterable=[{"value": 1}],
            lock_retries=3,
        )

    assert pipeline.run_calls == 1
    assert pipeline.load_calls == 2


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
