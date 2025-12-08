import pytest
import h5py
import pandas as pd
from consist.core.tracker import Tracker
from consist.api import load

# Check for tables/h5py
try:
    import tables
    import h5py
    HAS_H5 = True
except ImportError:
    HAS_H5 = False


@pytest.mark.skipif(not HAS_H5, reason="PyTables/h5py not installed")
def test_h5_auto_discovery(tracker: Tracker):
    """
    Tests the automatic discovery of HDF5 tables using `log_h5_container`.

    Verifies:
    1. Child artifacts are created for internal datasets.
    2. Child artifacts are correctly linked to the parent container.
    3. Child artifacts inherit the RUN_ID (Critical Fix Verification).
    4. Table filtering works.
    """
    # 1. Create Complex H5 File using h5py directly for deterministic structure
    h5_path = tracker.run_dir / "complex.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(h5_path, "w") as f:
        # Create groups
        g2020 = f.create_group("year_2020")
        g2030 = f.create_group("year_2030")
        gmeta = f.create_group("metadata")

        # Create datasets (simple integers)
        g2020.create_dataset("households", data=[1, 2, 3])
        g2020.create_dataset("persons", data=[4, 5, 6])
        g2030.create_dataset("households", data=[7, 8, 9])
        gmeta.create_dataset("config", data=[0])

    # 2. Test Auto-Discovery with Filter
    with tracker.start_run("run_discovery", model="test_model"):
        # We only want 'households' tables
        container, children = tracker.log_h5_container(
            h5_path,
            key="simulation_data",
            table_filter=lambda name: "households" in name
        )

        # A. Check Container
        assert container.key == "simulation_data"
        assert container.meta["is_container"] is True
        assert container.run_id is not None

        # B. Check Children Count (Should be 2: 2020/households, 2030/households)
        assert len(children) == 2

        child_keys = [c.key for c in children]
        assert any("2020_households" in k for k in child_keys)
        assert any("2030_households" in k for k in child_keys)

        # C. CRITICAL: Check Run ID Propagation
        for child in children:
            assert child.run_id == container.run_id, "Child artifact must inherit Run ID"
            assert child.meta["parent_id"] == str(container.id)
            assert child.driver == "h5_table"

    # 3. Verify Persistence
    saved_children = tracker.get_artifacts_for_run(container.run_id).outputs
    # +1 for the container itself
    assert len(saved_children) == 3