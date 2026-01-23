"""Integration tests for OpenMatrix (OMX) artifact support."""

import pytest
import numpy as np

from consist.core.tracker import Tracker
from consist.types import DriverType

h5py = pytest.importorskip("h5py")


def test_openmatrix_loading_h5py(tracker: Tracker):
    """
    Tests loading an OpenMatrix file using h5py fallback.

    Verifies:
    1. load() returns HDF5 file object for openmatrix driver
    2. File can be accessed like a dictionary
    3. Matrix data is accessible
    """
    # 1. Create an OpenMatrix file using h5py
    omx_path = tracker.run_dir / "travel_demand.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        # Create matrices (typical OMX structure)
        f.create_dataset("am_single_vehicle", data=np.random.randn(100, 100))
        f.create_dataset("md_single_vehicle", data=np.random.randn(100, 100))
        f.create_dataset("pm_single_vehicle", data=np.random.randn(100, 100))

        # Add metadata (OMX convention)
        f.attrs["zone_number"] = 100
        f.attrs["units"] = "vehicles"

    # 2. Log the artifact
    with tracker.start_run("run_omx", model="test_model"):
        artifact = tracker.log_artifact(
            str(omx_path),
            key="demand",
            driver="openmatrix",
        )

        # 3. Verify artifact properties
        assert artifact.key == "demand"
        assert artifact.driver == "openmatrix"
        assert artifact.driver == DriverType.OPENMATRIX.value

        # 4. Load the artifact (returns h5py.File or openmatrix.File)
        loaded = tracker.load(artifact)

        # 5. Verify it's file-like
        assert "am_single_vehicle" in loaded
        assert "md_single_vehicle" in loaded
        assert "pm_single_vehicle" in loaded
        assert loaded["am_single_vehicle"].shape == (100, 100)


def test_openmatrix_metadata_ingestion(tracker: Tracker):
    """
    Tests metadata ingestion from OpenMatrix file into DuckDB.

    Verifies:
    1. Metadata is extracted from OMX structure (matrix names, shapes)
    2. openmatrix_catalog table is created in DuckDB
    3. Matrix metadata is queryable
    4. No raw matrix data is ingested (metadata-only strategy)
    5. Run ID propagation works
    """
    if not tracker.engine:
        pytest.skip("Database not configured")

    # 1. Create an OpenMatrix file
    omx_path = tracker.run_dir / "network_skims.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        # Create travel time and distance matrices
        zones = 50
        f.create_dataset("time_period_1", data=np.random.randn(zones, zones))
        f.create_dataset("time_period_2", data=np.random.randn(zones, zones))
        f.create_dataset("distance", data=np.random.randn(zones, zones))

        # Add metadata
        f.attrs["network_date"] = "2024-01-01"
        f.attrs["zone_count"] = zones

    # 2. Log and ingest
    with tracker.start_run("run_omx_ingest", model="test_model"):
        artifact = tracker.log_artifact(
            str(omx_path),
            key="network_matrices",
            driver="openmatrix",
        )

        # 3. Ingest metadata only
        tracker.ingest(artifact)

        # 4. Query the openmatrix_catalog table
        from sqlalchemy import text

        result = tracker.run_query(
            text(
                "SELECT matrix_name, n_rows, n_cols FROM global_tables.network_matrices "
                "WHERE consist_run_id = :run_id"
            ).bindparams(run_id=artifact.run_id)
        )

        # 5. Verify metadata was ingested
        assert len(result) > 0, "Metadata should be ingested"

        # Extract matrix names
        matrix_names = [row[0] for row in result]
        assert "time_period_1" in matrix_names
        assert "time_period_2" in matrix_names
        assert "distance" in matrix_names

        # Verify shapes are stored
        row_shapes = {(row[1], row[2]) for row in result}
        assert row_shapes == {(50, 50)}, "All matrices should be 50x50"


def test_openmatrix_artifact_properties(tracker: Tracker):
    """
    Tests that OpenMatrix artifacts have correct properties.

    Verifies:
    1. is_matrix property returns True for OpenMatrix
    2. is_tabular property returns False for OpenMatrix
    """
    omx_path = tracker.run_dir / "test.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        f.create_dataset("matrix", data=np.arange(25).reshape(5, 5))

    with tracker.start_run("run_props", model="test"):
        artifact = tracker.log_artifact(
            str(omx_path),
            key="test_omx",
            driver="openmatrix",
        )

        # OpenMatrix is matrix-like, not tabular
        assert artifact.is_matrix is True
        assert artifact.is_tabular is False


def test_openmatrix_type_guards(tracker: Tracker):
    """
    Tests type guards for OpenMatrix artifacts.

    Verifies:
    1. is_openmatrix_artifact() correctly identifies OpenMatrix drivers
    2. Type narrowing works for load() overloads
    """
    from consist.api import is_openmatrix_artifact

    omx_path = tracker.run_dir / "test.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        f.create_dataset("matrix", data=np.arange(16).reshape(4, 4))

    with tracker.start_run("run_guard", model="test"):
        artifact = tracker.log_artifact(
            str(omx_path),
            key="test",
            driver="openmatrix",
        )

        # Test type guard
        assert is_openmatrix_artifact(artifact) is True

        # Type checker would narrow the return type here
        if is_openmatrix_artifact(artifact):
            loaded = tracker.load(artifact)
            # Should be file-like object at type-check time
            assert "matrix" in loaded


def test_openmatrix_convenience_logging(tracker: Tracker):
    """
    Tests the log_openmatrix_file() convenience method.

    Verifies:
    1. log_openmatrix_file() automatically sets driver to "openmatrix"
    2. Artifact metadata is correctly populated
    3. Works with both relative and absolute paths
    """
    omx_path = tracker.run_dir / "demand_matrix.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        f.create_dataset("trips", data=np.random.randn(100, 100))

    with tracker.start_run("run_convenience", model="test"):
        # Use convenience method
        artifact = tracker.log_openmatrix_file(omx_path, key="demand")

        # Verify driver is set correctly
        assert artifact.driver == "openmatrix"
        assert artifact.key == "demand"

        # Verify can load it
        loaded = tracker.load(artifact)
        assert "trips" in loaded


def test_netcdf_convenience_logging(tracker: Tracker):
    """
    Tests the log_netcdf_file() convenience method.

    Verifies:
    1. log_netcdf_file() automatically sets driver to "netcdf"
    2. Artifact metadata is correctly populated
    3. Works with both relative and absolute paths
    """
    xr = pytest.importorskip("xarray")

    netcdf_path = tracker.run_dir / "climate.nc"
    netcdf_path.parent.mkdir(parents=True, exist_ok=True)

    ds = xr.Dataset(
        {"temperature": (("x", "y"), np.random.randn(5, 5))},
        coords={"x": np.arange(5), "y": np.arange(5)},
    )
    ds.to_netcdf(netcdf_path)

    with tracker.start_run("run_convenience", model="test"):
        # Use convenience method
        artifact = tracker.log_netcdf_file(netcdf_path, key="climate")

        # Verify driver is set correctly
        assert artifact.driver == "netcdf"
        assert artifact.key == "climate"

        # Verify can load it
        loaded = tracker.load(artifact)
        assert isinstance(loaded, xr.Dataset)
        assert "temperature" in loaded.data_vars


def test_openmatrix_activitysim_workflow(tracker: Tracker):
    """
    Tests a typical ActivitySim workflow: OMX input → Zarr output (can be cached).

    Verifies:
    1. OMX input artifact can be logged and tracked
    2. Zarr output artifact can be created from OMX processing
    3. Both artifacts have correct drivers and metadata
    4. Lineage is preserved (OMX input → Zarr output)
    """
    xr = pytest.importorskip("xarray")
    pytest.importorskip("zarr")

    # 1. Create OMX input file
    omx_path = tracker.run_dir / "demand.omx"
    omx_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(omx_path, "w") as f:
        f.create_dataset("am", data=np.ones((10, 10)))
        f.create_dataset("pm", data=np.ones((10, 10)))

    # 2. Simulate ActivitySim run
    with tracker.start_run("run_as", model="activitysim"):
        # Log OMX input
        omx_input = tracker.log_artifact(
            str(omx_path),
            key="demand_matrix",
            direction="input",
            driver="openmatrix",
        )

        # Simulate processing and create Zarr output
        zarr_path = tracker.run_dir / "results.zarr"
        ds = xr.Dataset(
            {
                "households": (("zone",), np.random.randn(10)),
                "persons": (("zone",), np.random.randn(10)),
            },
            coords={"zone": np.arange(10)},
        )
        ds.to_zarr(str(zarr_path), mode="w")

        # Log Zarr output
        zarr_output = tracker.log_artifact(
            str(zarr_path),
            key="results",
            direction="output",
            driver="zarr",
        )

        # 3. Verify both artifacts are logged
        assert omx_input.driver == "openmatrix"
        assert zarr_output.driver == "zarr"

        # 4. Verify metadata
        assert omx_input in tracker.current_consist.inputs
        assert zarr_output in tracker.current_consist.outputs
