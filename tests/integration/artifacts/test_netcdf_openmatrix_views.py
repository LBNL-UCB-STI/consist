"""Integration tests for NetCDF and OpenMatrix metadata views."""

import pytest
import numpy as np

from consist.core.tracker import Tracker

xr = pytest.importorskip("xarray")
h5py = pytest.importorskip("h5py")


class TestNetCdfMetadataView:
    """Tests for NetCDF metadata views."""

    def test_get_variables(self, tracker: Tracker, write_netcdf):
        """Test querying all variables in a NetCDF artifact."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        # 1. Create and ingest NetCDF
        netcdf_path = tracker.run_dir / "climate_data.nc"
        netcdf_path.parent.mkdir(parents=True, exist_ok=True)

        ds = xr.Dataset(
            {
                "temperature": (("lat", "lon", "time"), np.random.randn(10, 20, 100)),
                "humidity": (("lat", "lon", "time"), np.random.randn(10, 20, 100)),
            },
            coords={
                "lat": np.linspace(-90, 90, 10),
                "lon": np.linspace(-180, 180, 20),
                "time": np.arange(100),
            },
        )
        write_netcdf(ds, netcdf_path)

        with tracker.start_run("run_nc_view", model="test"):
            artifact = tracker.log_artifact(
                str(netcdf_path), key="climate", driver="netcdf"
            )
            tracker.ingest(artifact)

            # 2. Query metadata view
            view = tracker.netcdf_metadata("climate")
            variables = view.get_variables("climate")

            # 3. Verify results
            assert len(variables) > 0
            assert "temperature" in variables["variable_name"].values
            assert "humidity" in variables["variable_name"].values
            assert "lat" in variables["variable_name"].values
            assert "lon" in variables["variable_name"].values
            assert "time" in variables["variable_name"].values

    def test_get_dimensions(self, tracker: Tracker, write_netcdf):
        """Test extracting dimensions from NetCDF metadata."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        netcdf_path = tracker.run_dir / "test_dims.nc"
        netcdf_path.parent.mkdir(parents=True, exist_ok=True)

        ds = xr.Dataset(
            {
                "var1": (("x", "y"), np.random.randn(5, 10)),
                "var2": (("x", "z"), np.random.randn(5, 15)),
            },
            coords={"x": np.arange(5), "y": np.arange(10), "z": np.arange(15)},
        )
        write_netcdf(ds, netcdf_path)

        with tracker.start_run("run_dims", model="test"):
            artifact = tracker.log_artifact(
                str(netcdf_path), key="dims_test", driver="netcdf"
            )
            tracker.ingest(artifact)

            view = tracker.netcdf_metadata("dims_test")
            dims = view.get_dimensions("dims_test")

            assert dims["x"] == 5
            assert dims["y"] == 10
            assert dims["z"] == 15

    def test_get_data_variables(self, tracker: Tracker, write_netcdf):
        """Test filtering to only data variables (not coordinates)."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        netcdf_path = tracker.run_dir / "test_vars.nc"
        netcdf_path.parent.mkdir(parents=True, exist_ok=True)

        ds = xr.Dataset(
            {
                "temperature": (("lat", "lon"), np.random.randn(5, 5)),
                "pressure": (("lat", "lon"), np.random.randn(5, 5)),
            },
            coords={"lat": np.arange(5), "lon": np.arange(5)},
        )
        write_netcdf(ds, netcdf_path)

        with tracker.start_run("run_datavars", model="test"):
            artifact = tracker.log_artifact(
                str(netcdf_path), key="vars_test", driver="netcdf"
            )
            tracker.ingest(artifact)

            view = tracker.netcdf_metadata("vars_test")
            data_vars = view.get_data_variables("vars_test")

        assert "temperature" in data_vars["variable_name"].values
        assert "pressure" in data_vars["variable_name"].values
        # Coordinates should not be included
        assert all(row != "coordinate" for row in data_vars["variable_type"].values)

    def test_summary(self, tracker: Tracker, write_netcdf):
        """Test human-readable summary of NetCDF structure."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        netcdf_path = tracker.run_dir / "test_summary.nc"
        netcdf_path.parent.mkdir(parents=True, exist_ok=True)

        ds = xr.Dataset(
            {"temperature": (("x", "y"), np.random.randn(3, 4))},
            coords={"x": np.arange(3), "y": np.arange(4)},
        )
        write_netcdf(ds, netcdf_path)

        with tracker.start_run("run_summary", model="test"):
            artifact = tracker.log_artifact(
                str(netcdf_path), key="summary_test", driver="netcdf"
            )
            tracker.ingest(artifact)

            view = tracker.netcdf_metadata("summary_test")
            summary = view.summary("summary_test")

            assert "NetCDF" in summary
            assert "temperature" in summary
            assert "Coordinates" in summary
            assert "Dimensions" in summary


class TestOpenMatrixMetadataView:
    """Tests for OpenMatrix metadata views."""

    def test_get_matrices(self, tracker: Tracker):
        """Test querying all matrices in an OpenMatrix artifact."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        # 1. Create and ingest OMX
        omx_path = tracker.run_dir / "demand.omx"
        omx_path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(omx_path, "w") as f:
            f.create_dataset("trips", data=np.ones((50, 50)))
            f.create_dataset("trucks", data=np.ones((50, 50)))
            f.create_dataset("walking", data=np.ones((50, 50)))

        with tracker.start_run("run_omx_view", model="test"):
            artifact = tracker.log_artifact(
                str(omx_path), key="demand", driver="openmatrix"
            )
            tracker.ingest(artifact)

            # 2. Query metadata view
            view = tracker.openmatrix_metadata("demand")
            matrices = view.get_matrices("demand")

            # 3. Verify results
            assert len(matrices) == 3
            assert "trips" in matrices["matrix_name"].values
            assert "trucks" in matrices["matrix_name"].values
            assert "walking" in matrices["matrix_name"].values

    def test_get_zone_counts(self, tracker: Tracker):
        """Test querying zone counts across runs."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        omx_path = tracker.run_dir / "zones.omx"
        omx_path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(omx_path, "w") as f:
            f.create_dataset("matrix", data=np.ones((100, 100)))

        with tracker.start_run("run_zones", model="test"):
            artifact = tracker.log_artifact(
                str(omx_path), key="zones_test", driver="openmatrix"
            )
            tracker.ingest(artifact)

            view = tracker.openmatrix_metadata("zones_test")
            zones = view.get_zone_counts("zones_test")

            assert "run_zones" in zones
            assert zones["run_zones"] == 100

    def test_get_matrix_names(self, tracker: Tracker):
        """Test listing unique matrix names."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        omx_path = tracker.run_dir / "names.omx"
        omx_path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(omx_path, "w") as f:
            f.create_dataset("am_peak", data=np.ones((50, 50)))
            f.create_dataset("md_peak", data=np.ones((50, 50)))
            f.create_dataset("pm_peak", data=np.ones((50, 50)))

        with tracker.start_run("run_names", model="test"):
            artifact = tracker.log_artifact(
                str(omx_path), key="names_test", driver="openmatrix"
            )
            tracker.ingest(artifact)

            view = tracker.openmatrix_metadata("names_test")
            names = view.get_matrix_names("names_test")

            assert len(names) == 3
            assert "am_peak" in names
            assert "md_peak" in names
            assert "pm_peak" in names

    def test_compare_runs(self, tracker: Tracker):
        """Test comparing matrices across multiple runs."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        # Create baseline OMX (5000 zones)
        baseline_path = tracker.run_dir / "baseline.omx"
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        with h5py.File(baseline_path, "w") as f:
            f.create_dataset("trips", data=np.ones((100, 100)))

        # Create scenario OMX (5100 zones)
        scenario_path = tracker.run_dir / "scenario.omx"
        scenario_path.parent.mkdir(parents=True, exist_ok=True)
        with h5py.File(scenario_path, "w") as f:
            f.create_dataset("trips", data=np.ones((110, 110)))

        with tracker.start_run("baseline", model="test"):
            art1 = tracker.log_artifact(
                str(baseline_path), key="compare_test", driver="openmatrix"
            )
            tracker.ingest(art1)

        with tracker.start_run("scenario_a", model="test"):
            art2 = tracker.log_artifact(
                str(scenario_path), key="compare_test", driver="openmatrix"
            )
            tracker.ingest(art2)

        # Compare
        view = tracker.openmatrix_metadata("compare_test")
        comparison = view.compare_runs("compare_test", ["baseline", "scenario_a"])

        # Should have rows for each matrix with columns for each run
        assert len(comparison) > 0
        assert "matrix_name" in comparison.columns
        assert "baseline" in comparison.columns
        assert "scenario_a" in comparison.columns

    def test_summary(self, tracker: Tracker):
        """Test human-readable summary of OpenMatrix structure."""
        if not tracker.engine:
            pytest.skip("Database not configured")

        omx_path = tracker.run_dir / "test_summary.omx"
        omx_path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(omx_path, "w") as f:
            f.create_dataset("am", data=np.ones((75, 75)))
            f.create_dataset("pm", data=np.ones((75, 75)))

        with tracker.start_run("run_omx_summary", model="test"):
            artifact = tracker.log_artifact(
                str(omx_path), key="summary_test", driver="openmatrix"
            )
            tracker.ingest(artifact)

            view = tracker.openmatrix_metadata("summary_test")
            summary = view.summary("summary_test")

            assert "OpenMatrix" in summary
            assert "am" in summary
            assert "pm" in summary
            assert "Zones" in summary
            assert "75" in summary
