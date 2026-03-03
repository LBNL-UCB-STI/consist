from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink


@pytest.fixture
def maintenance(tmp_path: Path) -> DatabaseMaintenance:
    db = DatabaseManager(str(tmp_path / "maintenance.duckdb"))
    manager = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs")
    try:
        yield manager
    finally:
        db.engine.dispose()


def test_expand_run_ids_recursive_and_cycle_safe(maintenance: DatabaseMaintenance) -> None:
    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="root", model_name="demo"),
                Run(id="child_a", model_name="demo", parent_run_id="root"),
                Run(id="child_b", model_name="demo", parent_run_id="child_a"),
                Run(id="cycle_1", model_name="demo", parent_run_id="cycle_2"),
                Run(id="cycle_2", model_name="demo", parent_run_id="cycle_1"),
            ]
        )
        session.commit()

    expanded = maintenance._expand_run_ids(["root", "cycle_1"])

    assert set(expanded) == {"root", "child_a", "child_b", "cycle_1", "cycle_2"}
    assert len(expanded) == 5
    assert expanded.index("root") < expanded.index("child_a") < expanded.index(
        "child_b"
    )


def test_find_orphaned_artifacts_linked_only_to_selected_runs(
    maintenance: DatabaseMaintenance,
) -> None:
    orphan_single = uuid.uuid4()
    orphan_multi = uuid.uuid4()
    shared = uuid.uuid4()
    retained = uuid.uuid4()

    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="purge_one", model_name="demo"),
                Run(id="purge_two", model_name="demo"),
                Run(id="keep", model_name="demo"),
            ]
        )
        session.add_all(
            [
                Artifact(
                    id=orphan_single,
                    key="orphan_single",
                    container_uri="outputs://orphan_single.parquet",
                    driver="parquet",
                ),
                Artifact(
                    id=orphan_multi,
                    key="orphan_multi",
                    container_uri="outputs://orphan_multi.parquet",
                    driver="parquet",
                ),
                Artifact(
                    id=shared,
                    key="shared",
                    container_uri="outputs://shared.parquet",
                    driver="parquet",
                ),
                Artifact(
                    id=retained,
                    key="retained",
                    container_uri="outputs://retained.parquet",
                    driver="parquet",
                ),
            ]
        )
        session.add_all(
            [
                RunArtifactLink(
                    run_id="purge_one", artifact_id=orphan_single, direction="output"
                ),
                RunArtifactLink(
                    run_id="purge_one", artifact_id=orphan_multi, direction="output"
                ),
                RunArtifactLink(
                    run_id="purge_two", artifact_id=orphan_multi, direction="input"
                ),
                RunArtifactLink(run_id="purge_one", artifact_id=shared, direction="output"),
                RunArtifactLink(run_id="keep", artifact_id=shared, direction="input"),
                RunArtifactLink(run_id="keep", artifact_id=retained, direction="output"),
            ]
        )
        session.commit()

    orphaned = maintenance._find_orphaned_artifacts(["purge_one", "purge_two"])
    assert set(orphaned) == {orphan_single, orphan_multi}


def test_discover_global_tables_from_schema(maintenance: DatabaseMaintenance) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql("CREATE TABLE global_tables.z_rows (id INTEGER)")
        conn.exec_driver_sql("CREATE TABLE global_tables.a_rows (id INTEGER)")
        conn.exec_driver_sql("CREATE TABLE local_rows (id INTEGER)")

    assert maintenance._discover_global_tables() == ["a_rows", "z_rows"]
