import pytest
from consist.core.tracker import Tracker
from consist.types import H5ChildSpec

pytest.importorskip("tables")
h5py = pytest.importorskip("h5py")


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
            table_filter=lambda name: "households" in name,
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
            assert child.run_id == container.run_id, (
                "Child artifact must inherit Run ID"
            )
            assert child.parent_artifact_id == container.id
            assert child.meta["parent_id"] == str(container.id)
            assert child.driver == "h5_table"

    # 3. Verify Persistence
    saved_children = tracker.get_artifacts_for_run(container.run_id).outputs
    # +1 for the container itself
    assert len(saved_children) == 3


def test_h5_child_specs_include_only_and_semantic_overrides(tracker: Tracker):
    h5_path = tracker.run_dir / "customized.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(h5_path, "w") as f:
        year_2020 = f.create_group("year_2020")
        year_2020.create_dataset("households", data=[1, 2, 3])
        year_2020.create_dataset("persons", data=[4, 5, 6])

    with tracker.start_run("run_child_specs", model="test_model"):
        container, children = tracker.log_h5_container(
            h5_path,
            key="simulation_data",
            child_selection="include_only",
            child_specs={
                "/year_2020/households": H5ChildSpec(
                    key="households_2020",
                    description="Selected households table",
                    facet={"artifact_family": "urbansim", "dataset": "households"},
                    facet_schema_version="1",
                    facet_index=True,
                    metadata={"semantic_group": "population"},
                )
            },
        )

    assert container.meta["table_count"] == 1
    assert [child.key for child in children] == ["households_2020"]

    child = children[0]
    assert child.parent_artifact_id == container.id
    assert child.meta["parent_id"] == str(container.id)
    assert child.meta["description"] == "Selected households table"
    assert child.meta["semantic_group"] == "population"
    assert child.meta["artifact_facet_schema_version"] == "1"

    reloaded_child = tracker.get_artifact(child.id)
    assert reloaded_child is not None
    assert reloaded_child.parent_artifact_id == container.id

    resolved_parent = tracker.get_parent_artifact(reloaded_child)
    assert resolved_parent is not None
    assert resolved_parent.id == container.id

    children_from_query = tracker.get_child_artifacts(container)
    assert [artifact.id for artifact in children_from_query] == [child.id]


def test_h5_child_specs_customize_without_filtering_all_children(tracker: Tracker):
    h5_path = tracker.run_dir / "customized_all.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(h5_path, "w") as f:
        year_2020 = f.create_group("year_2020")
        year_2020.create_dataset("households", data=[1, 2, 3])
        year_2020.create_dataset("persons", data=[4, 5, 6])

    with tracker.start_run("run_child_specs_all", model="test_model"):
        container, children = tracker.log_h5_container(
            h5_path,
            key="simulation_data",
            child_specs={
                "year_2020/households": H5ChildSpec(
                    key="households_2020",
                    description="Customized households table",
                    metadata={"semantic_group": "population"},
                )
            },
        )

    assert container.meta["table_count"] == 2
    assert sorted(child.key for child in children) == [
        "households_2020",
        "simulation_data_year_2020_persons",
    ]

    customized = next(child for child in children if child.key == "households_2020")
    default_child = next(
        child for child in children if child.key == "simulation_data_year_2020_persons"
    )

    assert customized.meta["description"] == "Customized households table"
    assert customized.meta["semantic_group"] == "population"
    assert default_child.meta.get("description") is None


def test_log_h5_table_supports_semantic_metadata_and_parent_queries(tracker: Tracker):
    h5_path = tracker.run_dir / "single_table.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(h5_path, "w") as f:
        f.create_group("year_2030").create_dataset("households", data=[1, 2, 3])

    with tracker.start_run("run_single_table", model="test_model"):
        container, _ = tracker.log_h5_container(
            h5_path,
            key="single_table_container",
            discover_tables=False,
        )
        table = tracker.log_h5_table(
            h5_path,
            table_path="/year_2030/households",
            key="households_2030",
            parent=container,
            description="Standalone child artifact",
            facet={"artifact_family": "urbansim", "dataset": "households"},
            facet_schema_version=2,
            facet_index=True,
            semantic_group="population",
        )

    assert table.parent_artifact_id == container.id
    assert table.meta["parent_id"] == str(container.id)
    assert table.meta["description"] == "Standalone child artifact"
    assert table.meta["semantic_group"] == "population"
    assert table.meta["artifact_facet_schema_version"] == 2

    parent = tracker.get_parent_artifact(table)
    assert parent is not None
    assert parent.id == container.id
