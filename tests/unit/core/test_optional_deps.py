from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.matrix import MatrixViewFactory
from consist.core.tracker import Tracker
from consist.integrations import dlt_loader


def test_dlt_loader_missing_pandas_for_csv(monkeypatch) -> None:
    """
    CSV ingestion should raise ImportError when pandas is unavailable.
    """
    monkeypatch.setattr(dlt_loader, "pd", None)
    with pytest.raises(ImportError, match="Pandas required for CSV"):
        dlt_loader._handle_csv_path("fake.csv", {"consist_run_id": "run"})


def test_dlt_loader_missing_pandas_and_pyarrow_for_parquet(monkeypatch) -> None:
    """
    Parquet ingestion should raise ImportError when pandas/pyarrow are unavailable.
    """
    monkeypatch.setattr(dlt_loader, "pd", None)
    monkeypatch.setattr(dlt_loader, "pa", None)
    monkeypatch.setattr(dlt_loader, "pq", None)
    with pytest.raises(ImportError, match="Pandas or PyArrow required for Parquet"):
        dlt_loader._handle_parquet_path("fake.parquet", {"consist_run_id": "run"})


def test_dlt_loader_missing_xarray_for_zarr(monkeypatch) -> None:
    """
    Zarr metadata extraction should raise ImportError when xarray is missing.
    """
    monkeypatch.setattr(dlt_loader, "xr", None)
    with pytest.raises(ImportError, match="xarray and zarr are required"):
        list(dlt_loader._handle_zarr_metadata("fake.zarr"))


def test_matrix_view_factory_missing_xarray(tmp_path: Path, monkeypatch) -> None:
    """
    Matrix view should raise ImportError when xarray is unavailable.
    """
    tracker = Tracker(run_dir=tmp_path)
    factory = MatrixViewFactory(tracker)
    monkeypatch.setattr("consist.core.matrix.xr", None)
    with pytest.raises(ImportError, match="xarray is required"):
        factory.load_matrix_view("concept")
