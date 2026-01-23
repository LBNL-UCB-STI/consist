"""Integration tests for NetCDF artifact support."""

import pytest
import numpy as np

from consist.core.tracker import Tracker
from consist.types import DriverType

xr = pytest.importorskip("xarray")


def test_netcdf_loading(tracker: Tracker, write_netcdf):
    """
    Tests loading a NetCDF file as an xarray.Dataset.

    Verifies:
    1. load() returns xarray.Dataset for netcdf driver
    2. Dataset structure is preserved
    3. Dimensions and variables are accessible
    """
    # 1. Create a NetCDF file using xarray
    netcdf_path = tracker.run_dir / "climate_data.nc"
    netcdf_path.parent.mkdir(parents=True, exist_ok=True)

    # Create sample NetCDF data
    ds = xr.Dataset(
        {
            "temperature": (("x", "y", "time"), np.random.randn(5, 4, 10)),
            "humidity": (("x", "y", "time"), np.random.randn(5, 4, 10)),
        },
        coords={
            "x": np.arange(5),
            "y": np.arange(4),
            "time": np.arange(10),
        },
    )
    write_netcdf(ds, netcdf_path)

    # 2. Log the artifact
    with tracker.start_run("run_netcdf", model="test_model"):
        artifact = tracker.log_artifact(
            str(netcdf_path),
            key="climate",
            driver="netcdf",
        )

        # 3. Verify artifact properties
        assert artifact.key == "climate"
        assert artifact.driver == "netcdf"
        assert artifact.driver == DriverType.NETCDF.value

        # 4. Load the artifact
        loaded = tracker.load(artifact)

        # 5. Verify it's an xarray.Dataset
        assert isinstance(loaded, xr.Dataset)
        assert "temperature" in loaded.data_vars
        assert "humidity" in loaded.data_vars
        assert "time" in loaded.dims
        assert loaded.dims["time"] == 10


def test_netcdf_metadata_ingestion(tracker: Tracker, write_netcdf):
    """
    Tests metadata ingestion from NetCDF file into DuckDB.

    Verifies:
    1. Metadata is extracted from NetCDF structure
    2. netcdf_catalog table is created in DuckDB
    3. Variable metadata is queryable
    4. No raw data is ingested (metadata-only strategy)
    """
    if not tracker.engine:
        pytest.skip("Database not configured")

    # 1. Create a NetCDF file
    netcdf_path = tracker.run_dir / "data_with_metadata.nc"
    netcdf_path.parent.mkdir(parents=True, exist_ok=True)

    ds = xr.Dataset(
        {
            "temperature": (("lat", "lon"), np.random.randn(10, 10)),
            "pressure": (("lat", "lon"), np.random.randn(10, 10)),
        },
        coords={
            "lat": np.linspace(-90, 90, 10),
            "lon": np.linspace(-180, 180, 10),
        },
        attrs={"source": "test_data", "version": "1.0"},
    )
    write_netcdf(ds, netcdf_path)

    # 2. Log and ingest
    with tracker.start_run("run_netcdf_ingest", model="test_model"):
        artifact = tracker.log_artifact(
            str(netcdf_path),
            key="climate_metadata",
            driver="netcdf",
        )

        # 3. Ingest metadata only
        tracker.ingest(artifact)

        # 4. Query the netcdf_catalog table
        from sqlalchemy import text

        result = tracker.run_query(
            text(
                "SELECT variable_name FROM global_tables.climate_metadata "
                "WHERE consist_run_id = :run_id"
            ).bindparams(run_id=artifact.run_id)
        )

        # 5. Verify metadata was ingested
        assert len(result) > 0, "Metadata should be ingested"

        # Extract metadata rows
        var_names = [row[0] for row in result]
        assert "temperature" in var_names
        assert "pressure" in var_names
        assert "lat" in var_names
        assert "lon" in var_names  # Coordinates should be included


def test_netcdf_artifact_properties(tracker: Tracker, write_netcdf):
    """
    Tests that NetCDF artifacts have correct properties.

    Verifies:
    1. is_matrix property returns True for NetCDF
    2. is_tabular property returns False for NetCDF
    """
    netcdf_path = tracker.run_dir / "test.nc"
    netcdf_path.parent.mkdir(parents=True, exist_ok=True)

    ds = xr.Dataset(
        {"var": (("x",), np.arange(5))},
        coords={"x": np.arange(5)},
    )
    write_netcdf(ds, netcdf_path)

    with tracker.start_run("run_props", model="test"):
        artifact = tracker.log_artifact(
            str(netcdf_path),
            key="test_nc",
            driver="netcdf",
        )

        # NetCDF is matrix-like, not tabular
        assert artifact.is_matrix is True
        assert artifact.is_tabular is False


def test_netcdf_type_guards(tracker: Tracker, write_netcdf):
    """
    Tests type guards for NetCDF artifacts.

    Verifies:
    1. is_netcdf_artifact() correctly identifies NetCDF drivers
    2. Type narrowing works for load() overloads
    """
    from consist.api import is_netcdf_artifact

    netcdf_path = tracker.run_dir / "test.nc"
    netcdf_path.parent.mkdir(parents=True, exist_ok=True)

    ds = xr.Dataset({"var": (("x",), np.arange(5))})
    write_netcdf(ds, netcdf_path)

    with tracker.start_run("run_guard", model="test"):
        artifact = tracker.log_artifact(
            str(netcdf_path),
            key="test",
            driver="netcdf",
        )

        # Test type guard
        assert is_netcdf_artifact(artifact) is True

        # Type checker would narrow the return type here
        if is_netcdf_artifact(artifact):
            loaded = tracker.load(artifact)
            # Should be xarray.Dataset at type-check time
            assert isinstance(loaded, xr.Dataset)
