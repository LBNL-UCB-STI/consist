from __future__ import annotations

import json
from datetime import datetime, timezone
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlmodel import select

from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from consist.models.run import Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV


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


def test_classify_global_table_modes(maintenance: DatabaseMaintenance) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.run_scoped_table (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql("CREATE TABLE global_tables.run_link_table (run_id VARCHAR)")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.unscoped_cache_table (content_hash VARCHAR)"
        )

    assert maintenance._classify_global_table("run_scoped_table") == "run_scoped"
    assert maintenance._classify_global_table("run_link_table") == "run_link"
    assert maintenance._classify_global_table("unscoped_cache_table") == "unscoped_cache"


@pytest.mark.parametrize(
    ("table_alias", "expected_column"),
    [("", '"consist_run_id"'), ("gt", '"gt"."consist_run_id"')],
)
def test_resolve_global_table_filter_sql_run_scoped(
    maintenance: DatabaseMaintenance, table_alias: str, expected_column: str
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.scoped_filter_table (consist_run_id VARCHAR)"
        )

    sql = maintenance._resolve_global_table_filter_sql(
        "scoped_filter_table", ["run_1", "run_2"], table_alias=table_alias
    )

    assert sql == f"{expected_column} = ANY(['run_1', 'run_2'])"


@pytest.mark.parametrize(
    ("table_alias", "expected_column"), [("", '"run_id"'), ("gt", '"gt"."run_id"')]
)
def test_resolve_global_table_filter_sql_run_link(
    maintenance: DatabaseMaintenance, table_alias: str, expected_column: str
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql("CREATE TABLE global_tables.link_filter_table (run_id VARCHAR)")

    sql = maintenance._resolve_global_table_filter_sql(
        "link_filter_table", ["run_1", "run_2"], table_alias=table_alias
    )

    assert sql == f"{expected_column} = ANY(['run_1', 'run_2'])"


def test_resolve_global_table_filter_sql_unscoped_cache_returns_none(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.cache_filter_table (content_hash VARCHAR)"
        )

    sql = maintenance._resolve_global_table_filter_sql(
        "cache_filter_table", ["run_1"], table_alias="gt"
    )

    assert sql is None


@pytest.mark.parametrize("bad_table", ["bad-table", "global_tables.foo", "1table"])
def test_classify_global_table_rejects_invalid_identifiers(
    maintenance: DatabaseMaintenance, bad_table: str
) -> None:
    with pytest.raises(ValueError, match="Invalid table"):
        maintenance._classify_global_table(bad_table)


@pytest.mark.parametrize("bad_alias", ["bad-alias", "gt.foo", "1alias"])
def test_resolve_global_table_filter_sql_rejects_invalid_alias(
    maintenance: DatabaseMaintenance, bad_alias: str
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.alias_validation_table (consist_run_id VARCHAR)"
        )

    with pytest.raises(ValueError, match="Invalid table_alias"):
        maintenance._resolve_global_table_filter_sql(
            "alias_validation_table", ["run_1"], table_alias=bad_alias
        )


@pytest.mark.parametrize("table_name", ["scoped_empty_ids", "link_empty_ids"])
def test_resolve_global_table_filter_sql_empty_run_ids_is_non_destructive(
    maintenance: DatabaseMaintenance, table_name: str
) -> None:
    column = "consist_run_id" if "scoped" in table_name else "run_id"
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(f"CREATE TABLE global_tables.{table_name} ({column} VARCHAR)")

    sql = maintenance._resolve_global_table_filter_sql(table_name, [])

    assert sql == "1 = 0"


def test_build_intersection_column_list_returns_shared_columns_in_target_order(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS source_schema")
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS target_schema")
        conn.exec_driver_sql(
            """
            CREATE TABLE source_schema.shared_table (
                source_only INTEGER,
                col_b INTEGER,
                col_a INTEGER,
                common_col INTEGER
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE target_schema.shared_table (
                target_only INTEGER,
                common_col INTEGER,
                col_a INTEGER,
                col_b INTEGER
            )
            """
        )

    columns = maintenance._build_intersection_column_list(
        "shared_table",
        source_schema="source_schema",
        target_schema="target_schema",
    )

    assert columns == ['"common_col"', '"col_a"', '"col_b"']


def test_build_intersection_column_list_excludes_source_only_and_target_only_columns(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS source_drift")
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS target_drift")
        conn.exec_driver_sql(
            """
            CREATE TABLE source_drift.drift_table (
                id INTEGER,
                shared_left INTEGER,
                source_only INTEGER,
                shared_right INTEGER
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE target_drift.drift_table (
                target_only_a INTEGER,
                shared_right INTEGER,
                shared_left INTEGER,
                target_only_b INTEGER
            )
            """
        )

    columns = maintenance._build_intersection_column_list(
        "drift_table",
        source_schema="source_drift",
        target_schema="target_drift",
    )

    assert columns == ['"shared_right"', '"shared_left"']


def test_build_intersection_column_list_same_source_and_target_schema(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS same_schema")
        conn.exec_driver_sql(
            """
            CREATE TABLE same_schema.same_table (
                first_col INTEGER,
                second_col INTEGER,
                third_col INTEGER
            )
            """
        )

    columns = maintenance._build_intersection_column_list(
        "same_table",
        source_schema="same_schema",
        target_schema="same_schema",
    )

    assert columns == ['"first_col"', '"second_col"', '"third_col"']


@pytest.mark.parametrize(
    ("create_source", "create_target"),
    [(False, False), (True, False), (False, True)],
)
def test_build_intersection_column_list_missing_tables_returns_empty_list(
    maintenance: DatabaseMaintenance, create_source: bool, create_target: bool
) -> None:
    table_name = f"missing_table_{int(create_source)}_{int(create_target)}"
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS source_missing")
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS target_missing")
        if create_source:
            conn.exec_driver_sql(
                f"CREATE TABLE source_missing.{table_name} (shared_col INTEGER)"
            )
        if create_target:
            conn.exec_driver_sql(
                f"CREATE TABLE target_missing.{table_name} (shared_col INTEGER)"
            )

    columns = maintenance._build_intersection_column_list(
        table_name,
        source_schema="source_missing",
        target_schema="target_missing",
    )

    assert columns == []


@pytest.mark.parametrize(
    ("table", "source_schema", "target_schema", "error_label"),
    [
        ("bad-table", "source_schema", "target_schema", "table"),
        ("table_name", "bad-schema", "target_schema", "source_schema"),
        ("table_name", "source_schema", "1target", "target_schema"),
    ],
)
def test_build_intersection_column_list_rejects_invalid_identifiers(
    maintenance: DatabaseMaintenance,
    table: str,
    source_schema: str,
    target_schema: str,
    error_label: str,
) -> None:
    with pytest.raises(ValueError, match=f"Invalid {error_label}"):
        maintenance._build_intersection_column_list(
            table,
            source_schema=source_schema,
            target_schema=target_schema,
        )


def test_inspect_reports_expected_counts_and_global_sizes(
    maintenance: DatabaseMaintenance,
) -> None:
    now = datetime.now(timezone.utc)
    linked_artifact_id = uuid.uuid4()
    orphaned_artifact_id = uuid.uuid4()

    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(
                    id="run_completed",
                    model_name="demo",
                    status="completed",
                    ended_at=now,
                ),
                Run(
                    id="run_running_zombie",
                    model_name="demo",
                    status="running",
                    ended_at=now,
                ),
                Run(id="run_failed", model_name="demo", status="failed", ended_at=now),
                Artifact(
                    id=linked_artifact_id,
                    key="linked_artifact",
                    container_uri="outputs://linked.parquet",
                    driver="parquet",
                ),
                Artifact(
                    id=orphaned_artifact_id,
                    key="orphaned_artifact",
                    container_uri="outputs://orphaned.parquet",
                    driver="parquet",
                ),
                RunArtifactLink(
                    run_id="run_completed",
                    artifact_id=linked_artifact_id,
                    direction="output",
                ),
            ]
        )
        session.commit()

    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.sample_global (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.sample_global (consist_run_id)
            VALUES ('run_completed'), ('run_running_zombie'), ('run_failed')
            """
        )

    report = maintenance.inspect()

    assert report.total_runs == 3
    assert report.runs_by_status == {"completed": 1, "failed": 1, "running": 1}
    assert report.total_artifacts == 2
    assert report.orphaned_artifact_count == 1
    assert report.zombie_run_ids == ["run_running_zombie"]
    assert report.global_table_sizes == {"sample_global": 3}
    assert report.db_file_size_mb > 0.0
    assert report.json_snapshot_count == 0
    assert report.json_db_parity is False


def test_inspect_json_db_parity_true_with_snapshot_files(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="run_a", model_name="demo"),
                Run(id="run_b", model_name="demo"),
            ]
        )
        session.commit()

    snapshot_dir = maintenance.run_dir / "consist_runs"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "run_a.json").write_text(
        json.dumps({"run": {"id": "run_a"}}),
        encoding="utf-8",
    )
    (snapshot_dir / "run_b.json").write_text(
        json.dumps({"run": {"id": "run_b"}}),
        encoding="utf-8",
    )

    report = maintenance.inspect()

    assert report.json_snapshot_count == 2
    assert report.json_db_parity is True


def test_inspect_json_db_parity_false_with_mismatched_snapshot_ids(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.session_scope() as session:
        session.add(Run(id="run_only_in_db", model_name="demo"))
        session.commit()

    snapshot_dir = maintenance.run_dir / "consist_runs"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "run_only_in_db.json").write_text(
        json.dumps({"run": {"id": "run_only_in_db"}}),
        encoding="utf-8",
    )
    (snapshot_dir / "run_only_in_json.json").write_text(
        json.dumps({"run": {"id": "run_only_in_json"}}),
        encoding="utf-8",
    )

    report = maintenance.inspect()

    assert report.json_snapshot_count == 2
    assert report.json_db_parity is False


def test_doctor_reports_invariant_violations(maintenance: DatabaseMaintenance) -> None:
    now = datetime.now(timezone.utc)
    missing_producer_artifact = uuid.uuid4()
    produced_artifact = uuid.uuid4()

    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="zombie_run", model_name="demo", status="running", ended_at=now),
                Run(
                    id="completed_missing_end",
                    model_name="demo",
                    status="completed",
                    ended_at=None,
                ),
                Run(
                    id="failed_missing_end",
                    model_name="demo",
                    status="failed",
                    ended_at=None,
                ),
                Run(id="valid_parent", model_name="demo", status="completed", ended_at=now),
                Run(
                    id="dangling_child",
                    model_name="demo",
                    status="completed",
                    parent_run_id="missing_parent",
                    ended_at=now,
                ),
                Artifact(
                    id=missing_producer_artifact,
                    key="missing_producer",
                    container_uri="outputs://missing_producer.parquet",
                    driver="parquet",
                    run_id="missing_run",
                ),
                Artifact(
                    id=produced_artifact,
                    key="produced",
                    container_uri="outputs://produced.parquet",
                    driver="parquet",
                    run_id="valid_parent",
                ),
            ]
        )
        session.commit()

    report = maintenance.doctor()

    assert report.zombie_run_ids == ["zombie_run"]
    assert report.completed_without_end_time == [
        "completed_missing_end",
        "failed_missing_end",
    ]
    assert report.dangling_parent_run_ids == ["missing_parent"]
    assert report.artifacts_with_missing_producing_run == [missing_producer_artifact]
    assert report.global_table_schema_drift == {}


def test_snapshot_creates_destination_file(maintenance: DatabaseMaintenance) -> None:
    destination = maintenance.run_dir / "snapshots" / "maintenance_snapshot.duckdb"

    snapshot_path = maintenance.snapshot(destination)

    assert snapshot_path == destination
    assert snapshot_path.exists()
    assert snapshot_path.is_file()


def test_snapshot_writes_sidecar_metadata_file(maintenance: DatabaseMaintenance) -> None:
    destination = maintenance.run_dir / "snapshots" / "with_sidecar.duckdb"

    maintenance.snapshot(destination)

    sidecar_path = maintenance.db._snapshot_sidecar_path(destination)
    assert sidecar_path.exists()
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert payload["operation"] == "maintenance_snapshot"


def test_snapshot_merges_default_and_custom_metadata(
    maintenance: DatabaseMaintenance,
) -> None:
    destination = maintenance.run_dir / "snapshots" / "metadata_merge.duckdb"

    maintenance.snapshot(
        destination,
        metadata={
            "operation": "custom_snapshot",
            "requested_by": "unit_test",
        },
    )

    sidecar_path = maintenance.db._snapshot_sidecar_path(destination)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert payload["operation"] == "custom_snapshot"
    assert payload["requested_by"] == "unit_test"


def test_snapshot_checkpoint_false_path_works(maintenance: DatabaseMaintenance) -> None:
    destination = maintenance.run_dir / "snapshots" / "no_checkpoint.duckdb"

    snapshot_path = maintenance.snapshot(destination, checkpoint=False)

    assert snapshot_path == destination
    assert snapshot_path.exists()


def test_snapshot_writes_audit_log_entry(maintenance: DatabaseMaintenance) -> None:
    destination = maintenance.run_dir / "snapshots" / "audit_snapshot.duckdb"

    maintenance.snapshot(destination, checkpoint=False)

    audit_log = maintenance.run_dir / ".consist_audit.log"
    assert audit_log.exists()
    audit_text = audit_log.read_text(encoding="utf-8")
    assert "\tsnapshot\t" in audit_text
    assert f"dest_path={destination}" in audit_text
    assert "checkpoint=False" in audit_text


def test_global_table_row_counts_full_and_filtered_behavior(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.scoped_counts (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql("CREATE TABLE global_tables.link_counts (run_id VARCHAR)")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.cache_counts (content_hash VARCHAR)"
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.scoped_counts (consist_run_id)
            VALUES ('run_1'), ('run_1'), ('run_2')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.link_counts (run_id)
            VALUES ('run_1'), ('run_3')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.cache_counts (content_hash)
            VALUES ('hash_a'), ('hash_b')
            """
        )

    full_counts = maintenance._global_table_row_counts(
        ["scoped_counts", "link_counts", "cache_counts"], []
    )
    filtered_counts = maintenance._global_table_row_counts(
        ["scoped_counts", "link_counts", "cache_counts"], ["run_1", "run_2"]
    )

    assert full_counts == {"cache_counts": 2, "link_counts": 2, "scoped_counts": 3}
    assert list(full_counts.keys()) == ["cache_counts", "link_counts", "scoped_counts"]
    assert filtered_counts == {
        "cache_counts": 0,
        "link_counts": 1,
        "scoped_counts": 3,
    }
    assert list(filtered_counts.keys()) == [
        "cache_counts",
        "link_counts",
        "scoped_counts",
    ]


def test_plan_purge_includes_children_and_global_table_candidates(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    child_run_dir = tmp_path / "child_run_dir"
    child_run_dir.mkdir(parents=True, exist_ok=True)
    root_snapshot = maintenance.run_dir / "consist_runs" / "root.json"
    child_snapshot = child_run_dir / "consist_runs" / "child.json"
    root_snapshot.parent.mkdir(parents=True, exist_ok=True)
    child_snapshot.parent.mkdir(parents=True, exist_ok=True)
    root_snapshot.write_text("{}", encoding="utf-8")
    child_snapshot.write_text("{}", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="root", model_name="demo"),
                Run(
                    id="child",
                    model_name="demo",
                    parent_run_id="root",
                    meta={"_physical_run_dir": str(child_run_dir)},
                ),
                Run(id="keep", model_name="demo"),
            ]
        )
        session.commit()

    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.scoped_table (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql("CREATE TABLE global_tables.link_table (run_id VARCHAR)")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.cache_table (content_hash VARCHAR)"
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.scoped_table (consist_run_id)
            VALUES ('root'), ('child'), ('keep')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.link_table (run_id)
            VALUES ('child'), ('keep')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.cache_table (content_hash)
            VALUES ('hash_a'), ('hash_b')
            """
        )

    plan = maintenance.plan_purge("root", include_children=True)

    assert plan.run_ids == ["root", "child"]
    assert plan.child_run_ids == ["child"]
    assert plan.orphaned_artifact_ids == []
    assert plan.json_files == [root_snapshot, child_snapshot]
    assert plan.disk_files == []
    assert plan.ingested_data == {"cache_table": 0, "link_table": 1, "scoped_table": 2}
    assert plan.ingested_table_modes == {
        "cache_table": "unscoped_cache",
        "link_table": "run_link",
        "scoped_table": "run_scoped",
    }


def test_resolve_artifact_disk_paths_workspace_relative(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    run_workspace = tmp_path / "workspace_run"
    artifact_path = run_workspace / "outputs" / "artifact.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("artifact", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add(
            Run(
                id="workspace_run",
                model_name="demo",
                meta={"_physical_run_dir": str(run_workspace)},
            )
        )
        session.add(
            Artifact(
                id=artifact_id,
                key="workspace_artifact",
                container_uri="./outputs/artifact.txt",
                driver="txt",
                run_id="workspace_run",
            )
        )
        session.commit()

    resolved_paths = maintenance._resolve_artifact_disk_paths([artifact_id])

    assert resolved_paths == [artifact_path.resolve()]


def test_resolve_artifact_disk_paths_mounted_uri_from_run_and_artifact_metadata(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    run_mount_artifact_id = uuid.uuid4()
    artifact_mount_artifact_id = uuid.uuid4()
    run_mount_root = tmp_path / "mount_root_run"
    artifact_mount_root = tmp_path / "mount_root_artifact"
    run_mount_file = run_mount_root / "dataset" / "run_mount.csv"
    artifact_mount_file = artifact_mount_root / "dataset" / "artifact_mount.csv"
    run_mount_file.parent.mkdir(parents=True, exist_ok=True)
    artifact_mount_file.parent.mkdir(parents=True, exist_ok=True)
    run_mount_file.write_text("run", encoding="utf-8")
    artifact_mount_file.write_text("artifact", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add(
            Run(
                id="mount_run",
                model_name="demo",
                meta={"mounts": {"inputs": str(run_mount_root)}},
            )
        )
        session.add_all(
            [
                Artifact(
                    id=run_mount_artifact_id,
                    key="run_mount_artifact",
                    container_uri="inputs://dataset/run_mount.csv",
                    driver="csv",
                    run_id="mount_run",
                ),
                Artifact(
                    id=artifact_mount_artifact_id,
                    key="artifact_mount_artifact",
                    container_uri="inputs://dataset/artifact_mount.csv",
                    driver="csv",
                    run_id="mount_run",
                    meta={"mount_root": str(artifact_mount_root)},
                ),
            ]
        )
        session.commit()

    resolved_paths = maintenance._resolve_artifact_disk_paths(
        [run_mount_artifact_id, artifact_mount_artifact_id]
    )

    assert resolved_paths == [
        run_mount_file.resolve(),
        artifact_mount_file.resolve(),
    ]


def test_purge_dry_run_makes_no_db_changes(maintenance: DatabaseMaintenance) -> None:
    artifact_id = uuid.uuid4()
    snapshot_path = maintenance.run_dir / "consist_runs" / "dry_run_target.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("{}", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add(Run(id="dry_run_target", model_name="demo"))
        session.add(
            Artifact(
                id=artifact_id,
                key="dry_artifact",
                container_uri="outputs://dry_artifact.parquet",
                driver="parquet",
                run_id="dry_run_target",
            )
        )
        session.add(
            RunArtifactLink(
                run_id="dry_run_target",
                artifact_id=artifact_id,
                direction="output",
            )
        )
        session.commit()

    result = maintenance.purge(
        "dry_run_target",
        include_children=False,
        delete_files=False,
        delete_ingested_data=True,
        dry_run=True,
    )

    assert result.executed is False
    assert result.plan.run_ids == ["dry_run_target"]
    assert snapshot_path.exists()
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "dry_run_target") is not None
        assert session.get(Artifact, artifact_id) is not None
        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 1


def test_purge_delete_files_removes_files_and_directories(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    file_artifact_id = uuid.uuid4()
    dir_artifact_id = uuid.uuid4()
    run_workspace = tmp_path / "purge_delete_workspace"
    file_path = run_workspace / "outputs" / "delete_me.txt"
    dir_path = run_workspace / "outputs" / "delete_dir"
    nested_path = dir_path / "nested.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path.write_text("file", encoding="utf-8")
    nested_path.write_text("nested", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add(
            Run(
                id="purge_delete_files_run",
                model_name="demo",
                meta={"_physical_run_dir": str(run_workspace)},
            )
        )
        session.add_all(
            [
                Artifact(
                    id=file_artifact_id,
                    key="delete_file_artifact",
                    container_uri="./outputs/delete_me.txt",
                    driver="txt",
                    run_id="purge_delete_files_run",
                ),
                Artifact(
                    id=dir_artifact_id,
                    key="delete_dir_artifact",
                    container_uri="./outputs/delete_dir",
                    driver="other",
                    run_id="purge_delete_files_run",
                ),
                RunArtifactLink(
                    run_id="purge_delete_files_run",
                    artifact_id=file_artifact_id,
                    direction="output",
                ),
                RunArtifactLink(
                    run_id="purge_delete_files_run",
                    artifact_id=dir_artifact_id,
                    direction="output",
                ),
            ]
        )
        session.commit()

    result = maintenance.purge(
        "purge_delete_files_run",
        include_children=False,
        delete_files=True,
        delete_ingested_data=False,
        dry_run=False,
    )

    assert result.executed is True
    assert {path.resolve() for path in result.plan.disk_files} == {
        file_path.resolve(),
        dir_path.resolve(),
    }
    assert not file_path.exists()
    assert not dir_path.exists()


def test_purge_executes_core_deletes_and_ingested_skip(
    maintenance: DatabaseMaintenance,
) -> None:
    orphan_root_id = uuid.uuid4()
    orphan_child_id = uuid.uuid4()
    shared_id = uuid.uuid4()
    keep_id = uuid.uuid4()
    root_snapshot = maintenance.run_dir / "consist_runs" / "purge_root.json"
    child_snapshot = maintenance.run_dir / "consist_runs" / "purge_child.json"
    root_snapshot.parent.mkdir(parents=True, exist_ok=True)
    root_snapshot.write_text("{}", encoding="utf-8")
    child_snapshot.write_text("{}", encoding="utf-8")

    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="purge_root", model_name="demo"),
                Run(id="purge_child", model_name="demo", parent_run_id="purge_root"),
                Run(id="keep_run", model_name="demo"),
                Artifact(
                    id=orphan_root_id,
                    key="orphan_root",
                    container_uri="outputs://orphan_root.parquet",
                    driver="parquet",
                    run_id="purge_root",
                ),
                Artifact(
                    id=orphan_child_id,
                    key="orphan_child",
                    container_uri="outputs://orphan_child.parquet",
                    driver="parquet",
                    run_id="purge_child",
                ),
                Artifact(
                    id=shared_id,
                    key="shared",
                    container_uri="outputs://shared.parquet",
                    driver="parquet",
                    run_id="purge_root",
                ),
                Artifact(
                    id=keep_id,
                    key="keep",
                    container_uri="outputs://keep.parquet",
                    driver="parquet",
                    run_id="keep_run",
                ),
                RunArtifactLink(
                    run_id="purge_root",
                    artifact_id=orphan_root_id,
                    direction="output",
                ),
                RunArtifactLink(
                    run_id="purge_child",
                    artifact_id=orphan_child_id,
                    direction="output",
                ),
                RunArtifactLink(
                    run_id="purge_root",
                    artifact_id=shared_id,
                    direction="output",
                ),
                RunArtifactLink(run_id="keep_run", artifact_id=shared_id, direction="input"),
                RunArtifactLink(run_id="keep_run", artifact_id=keep_id, direction="output"),
                RunConfigKV(
                    run_id="purge_root",
                    facet_id="facet_1",
                    namespace="demo",
                    key="a",
                    value_type="str",
                    value_str="purge",
                ),
                RunConfigKV(
                    run_id="keep_run",
                    facet_id="facet_1",
                    namespace="demo",
                    key="a",
                    value_type="str",
                    value_str="keep",
                ),
                ArtifactKV(
                    artifact_id=orphan_root_id,
                    facet_id="facet_a",
                    key_path="k",
                    namespace="demo",
                    value_type="str",
                    value_str="orphan_root",
                ),
                ArtifactKV(
                    artifact_id=orphan_child_id,
                    facet_id="facet_a",
                    key_path="k",
                    namespace="demo",
                    value_type="str",
                    value_str="orphan_child",
                ),
                ArtifactKV(
                    artifact_id=shared_id,
                    facet_id="facet_a",
                    key_path="k",
                    namespace="demo",
                    value_type="str",
                    value_str="shared",
                ),
                ArtifactSchema(id="schema_1", summary_json={}),
                ArtifactSchemaObservation(
                    artifact_id=orphan_root_id,
                    schema_id="schema_1",
                    run_id="purge_root",
                    source="file",
                ),
                ArtifactSchemaObservation(
                    artifact_id=orphan_child_id,
                    schema_id="schema_1",
                    run_id="purge_child",
                    source="file",
                ),
                ArtifactSchemaObservation(
                    artifact_id=shared_id,
                    schema_id="schema_1",
                    run_id="keep_run",
                    source="file",
                ),
            ]
        )
        session.commit()

    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.scoped_delete (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql("CREATE TABLE global_tables.link_delete (run_id VARCHAR)")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.cache_delete (content_hash VARCHAR)"
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.scoped_delete (consist_run_id)
            VALUES ('purge_root'), ('purge_child'), ('keep_run')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.link_delete (run_id)
            VALUES ('purge_root'), ('keep_run')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.cache_delete (content_hash)
            VALUES ('hash_a'), ('hash_b')
            """
        )

    result = maintenance.purge(
        "purge_root",
        include_children=True,
        delete_files=False,
        delete_ingested_data=False,
        dry_run=False,
    )

    assert result.executed is True
    assert result.ingested_data_skipped is True
    assert not root_snapshot.exists()
    assert not child_snapshot.exists()
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "purge_root") is None
        assert session.get(Run, "purge_child") is None
        assert session.get(Run, "keep_run") is not None
        assert session.get(Artifact, orphan_root_id) is None
        assert session.get(Artifact, orphan_child_id) is None
        shared = session.get(Artifact, shared_id)
        assert shared is not None
        assert shared.run_id is None
        assert session.get(Artifact, keep_id) is not None

        remaining_links = session.exec(select(RunArtifactLink)).all()
        assert {(row.run_id, row.artifact_id) for row in remaining_links} == {
            ("keep_run", shared_id),
            ("keep_run", keep_id),
        }
        remaining_run_kv = session.exec(select(RunConfigKV)).all()
        assert [row.run_id for row in remaining_run_kv] == ["keep_run"]
        remaining_artifact_kv = session.exec(select(ArtifactKV)).all()
        assert {row.artifact_id for row in remaining_artifact_kv} == {shared_id}
        remaining_observations = session.exec(select(ArtifactSchemaObservation)).all()
        assert len(remaining_observations) == 1
        assert remaining_observations[0].artifact_id == shared_id
        assert remaining_observations[0].run_id == "keep_run"

    with maintenance.db.engine.begin() as conn:
        scoped_rows = conn.exec_driver_sql(
            "SELECT consist_run_id FROM global_tables.scoped_delete ORDER BY consist_run_id"
        ).fetchall()
        link_rows = conn.exec_driver_sql(
            "SELECT run_id FROM global_tables.link_delete ORDER BY run_id"
        ).fetchall()
        cache_rows = conn.exec_driver_sql(
            "SELECT content_hash FROM global_tables.cache_delete ORDER BY content_hash"
        ).fetchall()

    assert [row[0] for row in scoped_rows] == ["keep_run", "purge_child", "purge_root"]
    assert [row[0] for row in link_rows] == ["keep_run", "purge_root"]
    assert [row[0] for row in cache_rows] == ["hash_a", "hash_b"]


def test_purge_deletes_global_rows_when_requested(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.session_scope() as session:
        session.add_all(
            [
                Run(id="purge_target", model_name="demo"),
                Run(id="keep_target", model_name="demo"),
            ]
        )
        session.commit()

    with maintenance.db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.scoped_prune (consist_run_id VARCHAR)"
        )
        conn.exec_driver_sql("CREATE TABLE global_tables.link_prune (run_id VARCHAR)")
        conn.exec_driver_sql(
            "CREATE TABLE global_tables.cache_prune (content_hash VARCHAR)"
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.scoped_prune (consist_run_id)
            VALUES ('purge_target'), ('keep_target')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.link_prune (run_id)
            VALUES ('purge_target'), ('keep_target')
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO global_tables.cache_prune (content_hash)
            VALUES ('cache_a'), ('cache_b')
            """
        )

    result = maintenance.purge(
        "purge_target",
        include_children=False,
        delete_files=False,
        delete_ingested_data=True,
        dry_run=False,
    )

    assert result.executed is True
    assert result.ingested_data_skipped is False
    with maintenance.db.engine.begin() as conn:
        scoped_rows = conn.exec_driver_sql(
            "SELECT consist_run_id FROM global_tables.scoped_prune ORDER BY consist_run_id"
        ).fetchall()
        link_rows = conn.exec_driver_sql(
            "SELECT run_id FROM global_tables.link_prune ORDER BY run_id"
        ).fetchall()
        cache_rows = conn.exec_driver_sql(
            "SELECT content_hash FROM global_tables.cache_prune ORDER BY content_hash"
        ).fetchall()

    assert [row[0] for row in scoped_rows] == ["keep_target"]
    assert [row[0] for row in link_rows] == ["keep_target"]
    assert [row[0] for row in cache_rows] == ["cache_a", "cache_b"]


@pytest.mark.parametrize("new_status", ["completed", "failed"])
def test_fix_status_running_to_terminal_sets_end_time_and_reason(
    maintenance: DatabaseMaintenance, new_status: str
) -> None:
    with maintenance.db.session_scope() as session:
        session.add(Run(id=f"run_{new_status}", model_name="demo", status="running"))
        session.commit()

    updated = maintenance.fix_status(
        f"run_{new_status}", new_status, reason=f"mark_{new_status}"
    )

    assert updated.status == new_status
    assert updated.ended_at is not None
    assert isinstance(updated.meta, dict)
    assert updated.meta["status_fix_reason"] == f"mark_{new_status}"
    audit_log_path = maintenance.run_dir / ".consist_audit.log"
    assert audit_log_path.exists()
    assert "\tfix_status\t" in audit_log_path.read_text(encoding="utf-8")


def test_fix_status_terminal_to_running_clears_ended_at(
    maintenance: DatabaseMaintenance,
) -> None:
    now = datetime.now(timezone.utc)
    with maintenance.db.session_scope() as session:
        session.add(
            Run(
                id="run_terminal",
                model_name="demo",
                status="completed",
                ended_at=now,
            )
        )
        session.commit()

    updated = maintenance.fix_status("run_terminal", "running", reason=None, force=True)

    assert updated.status == "running"
    assert updated.ended_at is None


def test_fix_status_terminal_to_running_requires_force(
    maintenance: DatabaseMaintenance,
) -> None:
    now = datetime.now(timezone.utc)
    with maintenance.db.session_scope() as session:
        session.add(
            Run(
                id="run_terminal_requires_force",
                model_name="demo",
                status="completed",
                ended_at=now,
            )
        )
        session.commit()

    with pytest.raises(ValueError, match="Use --force"):
        maintenance.fix_status(
            "run_terminal_requires_force",
            "running",
            reason=None,
            force=False,
        )


def test_fix_status_invalid_status_raises_value_error(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.db.session_scope() as session:
        session.add(Run(id="run_bad_status", model_name="demo", status="running"))
        session.commit()

    with pytest.raises(ValueError, match="Invalid status"):
        maintenance.fix_status("run_bad_status", "queued", reason=None)


def test_fix_status_missing_run_raises_value_error(
    maintenance: DatabaseMaintenance,
) -> None:
    with pytest.raises(ValueError, match="Run not found"):
        maintenance.fix_status("missing_run", "completed", reason=None)


def test_export_copies_core_global_rows_and_snapshots(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "export_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    other_artifact_id = uuid.uuid4()
    try:
        with maintenance.db.session_scope() as session:
            session.add_all(
                [
                    Run(id="root_run", model_name="demo"),
                    Run(id="child_run", model_name="demo", parent_run_id="root_run"),
                    Run(id="other_run", model_name="demo"),
                    Artifact(
                        id=root_artifact_id,
                        key="root_output",
                        container_uri="outputs://root.parquet",
                        driver="parquet",
                        run_id="root_run",
                    ),
                    Artifact(
                        id=child_artifact_id,
                        key="child_output",
                        container_uri="outputs://child.parquet",
                        driver="parquet",
                        run_id="child_run",
                    ),
                    Artifact(
                        id=other_artifact_id,
                        key="other_output",
                        container_uri="outputs://other.parquet",
                        driver="parquet",
                        run_id="other_run",
                    ),
                    RunArtifactLink(
                        run_id="root_run",
                        artifact_id=root_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="child_run",
                        artifact_id=child_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="other_run",
                        artifact_id=other_artifact_id,
                        direction="output",
                    ),
                    RunConfigKV(
                        run_id="root_run",
                        facet_id="facet_a",
                        namespace="demo",
                        key="param",
                        value_type="str",
                        value_str="root",
                    ),
                    RunConfigKV(
                        run_id="child_run",
                        facet_id="facet_a",
                        namespace="demo",
                        key="param",
                        value_type="str",
                        value_str="child",
                    ),
                    RunConfigKV(
                        run_id="other_run",
                        facet_id="facet_a",
                        namespace="demo",
                        key="param",
                        value_type="str",
                        value_str="other",
                    ),
                    ArtifactKV(
                        artifact_id=root_artifact_id,
                        facet_id="facet_a",
                        key_path="k",
                        namespace="demo",
                        value_type="str",
                        value_str="root",
                    ),
                    ArtifactKV(
                        artifact_id=child_artifact_id,
                        facet_id="facet_a",
                        key_path="k",
                        namespace="demo",
                        value_type="str",
                        value_str="child",
                    ),
                    ArtifactKV(
                        artifact_id=other_artifact_id,
                        facet_id="facet_a",
                        key_path="k",
                        namespace="demo",
                        value_type="str",
                        value_str="other",
                    ),
                    ArtifactSchema(id="schema_export", summary_json={}),
                    ArtifactSchemaObservation(
                        artifact_id=root_artifact_id,
                        schema_id="schema_export",
                        run_id="root_run",
                        source="file",
                    ),
                    ArtifactSchemaObservation(
                        artifact_id=child_artifact_id,
                        schema_id="schema_export",
                        run_id="child_run",
                        source="file",
                    ),
                    ArtifactSchemaObservation(
                        artifact_id=other_artifact_id,
                        schema_id="schema_export",
                        run_id="other_run",
                        source="file",
                    ),
                ]
            )
            session.commit()

        with maintenance.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.scoped_export (consist_run_id VARCHAR)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.link_export (run_id VARCHAR)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_export (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.scoped_export (consist_run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.link_export (run_id)
                VALUES ('child_run'), ('other_run')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.cache_export (content_hash)
                VALUES ('hash_a'), ('hash_b')
                """
            )

        source_snapshot_dir = maintenance.run_dir / "consist_runs"
        source_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (source_snapshot_dir / "root_run.json").write_text("{}", encoding="utf-8")
        (source_snapshot_dir / "child_run.json").write_text("{}", encoding="utf-8")

        shard_path = tmp_path / "exported_shard.duckdb"
        result = maintenance.export(
            "root_run",
            shard_path,
            include_data=True,
            include_snapshots=True,
        )

        assert result.run_ids == ["root_run", "child_run"]
        assert result.artifact_count == 2
        assert result.ingested_rows == {"link_export": 1, "scoped_export": 2}
        assert result.ingested_table_modes == {
            "cache_export": "unscoped_cache",
            "link_export": "run_link",
            "scoped_export": "run_scoped",
        }
        assert result.unscoped_cache_tables_skipped == ["cache_export"]
        assert result.snapshots_copied == 2

        shard_db = DatabaseManager(str(shard_path))
        try:
            with shard_db.engine.begin() as conn:
                run_count = conn.exec_driver_sql(
                    'SELECT COUNT(*) FROM "run"'
                ).fetchone()
                artifact_count = conn.exec_driver_sql(
                    'SELECT COUNT(*) FROM "artifact"'
                ).fetchone()
                link_count = conn.exec_driver_sql(
                    'SELECT COUNT(*) FROM "run_artifact_link"'
                ).fetchone()
                run_config_count = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM run_config_kv"
                ).fetchone()
                artifact_kv_count = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM artifact_kv"
                ).fetchone()
                observation_count = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM artifact_schema_observation"
                ).fetchone()
                scoped_count = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM global_tables.scoped_export"
                ).fetchone()
                link_table_count = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM global_tables.link_export"
                ).fetchone()
            assert int(run_count[0] if run_count else 0) == 2
            assert int(artifact_count[0] if artifact_count else 0) == 2
            assert int(link_count[0] if link_count else 0) == 2
            assert int(run_config_count[0] if run_config_count else 0) == 2
            assert int(artifact_kv_count[0] if artifact_kv_count else 0) == 2
            assert int(observation_count[0] if observation_count else 0) == 2
            assert int(scoped_count[0] if scoped_count else 0) == 2
            assert int(link_table_count[0] if link_table_count else 0) == 1
        finally:
            shard_db.engine.dispose()

        shard_snapshot_dir = shard_path.parent / "shard_snapshots"
        assert (shard_snapshot_dir / "root_run.json").exists()
        assert (shard_snapshot_dir / "child_run.json").exists()
    finally:
        db.engine.dispose()


def test_export_no_children_only_copies_requested_run(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "export_no_children_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    try:
        with maintenance.db.session_scope() as session:
            session.add_all(
                [
                    Run(id="root_run", model_name="demo"),
                    Run(id="child_run", model_name="demo", parent_run_id="root_run"),
                    Artifact(
                        id=root_artifact_id,
                        key="root_output",
                        container_uri="outputs://root.parquet",
                        driver="parquet",
                        run_id="root_run",
                    ),
                    Artifact(
                        id=child_artifact_id,
                        key="child_output",
                        container_uri="outputs://child.parquet",
                        driver="parquet",
                        run_id="child_run",
                    ),
                    RunArtifactLink(
                        run_id="root_run",
                        artifact_id=root_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="child_run",
                        artifact_id=child_artifact_id,
                        direction="output",
                    ),
                ]
            )
            session.commit()

        shard_path = tmp_path / "exported_no_children.duckdb"
        result = maintenance.export(
            "root_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
            include_children=False,
        )

        assert result.run_ids == ["root_run"]
        assert result.artifact_count == 1

        shard_db = DatabaseManager(str(shard_path))
        try:
            with shard_db.engine.begin() as conn:
                run_count = conn.exec_driver_sql('SELECT COUNT(*) FROM "run"').fetchone()
                artifact_count = conn.exec_driver_sql(
                    'SELECT COUNT(*) FROM "artifact"'
                ).fetchone()
            assert int(run_count[0] if run_count else 0) == 1
            assert int(artifact_count[0] if artifact_count else 0) == 1
        finally:
            shard_db.engine.dispose()
    finally:
        db.engine.dispose()


def test_export_dry_run_previews_without_writes(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "export_dry_run_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    artifact_id = uuid.uuid4()
    try:
        with maintenance.db.session_scope() as session:
            session.add(Run(id="root_run", model_name="demo"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="root_output",
                    container_uri="outputs://root.parquet",
                    driver="parquet",
                    run_id="root_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="root_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()

        with maintenance.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.scoped_export_preview (consist_run_id VARCHAR)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_export_preview (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.scoped_export_preview (consist_run_id)
                VALUES ('root_run')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.cache_export_preview (content_hash)
                VALUES ('cache_a')
                """
            )

        source_snapshot_dir = maintenance.run_dir / "consist_runs"
        source_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (source_snapshot_dir / "root_run.json").write_text("{}", encoding="utf-8")

        shard_path = tmp_path / "exported_dry_run_shard.duckdb"
        result = maintenance.export(
            "root_run",
            shard_path,
            include_data=True,
            include_snapshots=True,
            dry_run=True,
        )

        assert result.run_ids == ["root_run"]
        assert result.artifact_count == 1
        assert result.ingested_rows == {"scoped_export_preview": 1}
        assert result.unscoped_cache_tables_skipped == ["cache_export_preview"]
        assert result.snapshots_copied == 0
        assert not shard_path.exists()
        assert not (shard_path.parent / "shard_snapshots").exists()
        assert not (maintenance.run_dir / ".consist_audit.log").exists()
        with maintenance.db.session_scope() as session:
            assert session.get(Run, "root_run") is not None
    finally:
        db.engine.dispose()


def test_merge_happy_path_and_conflict_skip_policy(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_target.duckdb"))
    source_maintenance = DatabaseMaintenance(source_db, run_dir=tmp_path / "runs_source")
    target_maintenance = DatabaseMaintenance(target_db, run_dir=tmp_path / "runs_target")
    artifact_id = uuid.uuid4()
    try:
        with source_db.session_scope() as session:
            session.add(
                Run(
                    id="merge_run",
                    model_name="source_model",
                    meta={"_physical_run_dir": str(source_maintenance.run_dir)},
                )
            )
            session.add(
                Artifact(
                    id=artifact_id,
                    key="merge_artifact",
                    container_uri="outputs://merge.parquet",
                    driver="parquet",
                    run_id="merge_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="merge_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.add(
                RunConfigKV(
                    run_id="merge_run",
                    facet_id="facet_a",
                    namespace="demo",
                    key="param",
                    value_type="str",
                    value_str="merge",
                )
            )
            session.add(
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id="facet_a",
                    key_path="k",
                    namespace="demo",
                    value_type="str",
                    value_str="merge",
                )
            )
            session.add(ArtifactSchema(id="schema_merge", summary_json={}))
            session.add(
                ArtifactSchemaObservation(
                    artifact_id=artifact_id,
                    schema_id="schema_merge",
                    run_id="merge_run",
                    source="file",
                )
            )
            session.commit()

        with source_db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.scoped_merge (consist_run_id VARCHAR)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_merge (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.scoped_merge (consist_run_id)
                VALUES ('merge_run')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.cache_merge (content_hash)
                VALUES ('cache_only')
                """
            )

        source_snapshot_dir = source_maintenance.run_dir / "consist_runs"
        source_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (source_snapshot_dir / "merge_run.json").write_text("{}", encoding="utf-8")

        shard_path = tmp_path / "merge_shard.duckdb"
        source_maintenance.export(
            "merge_run",
            shard_path,
            include_data=True,
            include_snapshots=True,
        )

        merge_result = target_maintenance.merge(
            shard_path,
            conflict="error",
            include_snapshots=True,
        )
        assert merge_result.runs_merged == ["merge_run"]
        assert merge_result.runs_skipped == []
        assert merge_result.artifacts_merged == 1
        assert merge_result.ingested_tables_merged == ["scoped_merge"]
        assert merge_result.unscoped_cache_tables_skipped == []
        assert merge_result.conflicts_detected == []
        assert merge_result.snapshots_merged == 1

        with target_db.session_scope() as session:
            merged_run = session.get(Run, "merge_run")
            assert merged_run is not None
            assert merged_run.model_name == "source_model"

        with target_db.engine.begin() as conn:
            scoped_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM global_tables.scoped_merge"
            ).fetchone()
            run_config_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM run_config_kv"
            ).fetchone()
            artifact_kv_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM artifact_kv"
            ).fetchone()
            observation_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM artifact_schema_observation"
            ).fetchone()
            assert int(scoped_count[0] if scoped_count else 0) == 1
            assert int(run_config_count[0] if run_config_count else 0) == 1
            assert int(artifact_kv_count[0] if artifact_kv_count else 0) == 1
            assert int(observation_count[0] if observation_count else 0) == 1

        conflict_result = target_maintenance.merge(
            shard_path,
            conflict="skip",
            include_snapshots=False,
        )
        assert conflict_result.runs_merged == []
        assert conflict_result.runs_skipped == ["merge_run"]
        assert conflict_result.conflicts_detected == ["merge_run"]

        with target_db.engine.begin() as conn:
            run_config_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM run_config_kv"
            ).fetchone()
            artifact_kv_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM artifact_kv"
            ).fetchone()
            observation_count = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM artifact_schema_observation"
            ).fetchone()
            assert int(run_config_count[0] if run_config_count else 0) == 1
            assert int(artifact_kv_count[0] if artifact_kv_count else 0) == 1
            assert int(observation_count[0] if observation_count else 0) == 1
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_merge_dry_run_previews_without_writes(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_dry_run_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_dry_run_target.duckdb"))
    source_maintenance = DatabaseMaintenance(source_db, run_dir=tmp_path / "runs_source")
    target_maintenance = DatabaseMaintenance(target_db, run_dir=tmp_path / "runs_target")
    conflict_artifact_id = uuid.uuid4()
    new_artifact_id = uuid.uuid4()
    try:
        with source_db.session_scope() as session:
            session.add_all(
                [
                    Run(id="conflict_run", model_name="source_model"),
                    Run(id="new_run", model_name="source_model"),
                    Artifact(
                        id=conflict_artifact_id,
                        key="conflict_artifact",
                        container_uri="outputs://conflict.parquet",
                        driver="parquet",
                        run_id="conflict_run",
                    ),
                    Artifact(
                        id=new_artifact_id,
                        key="new_artifact",
                        container_uri="outputs://new.parquet",
                        driver="parquet",
                        run_id="new_run",
                    ),
                    RunArtifactLink(
                        run_id="conflict_run",
                        artifact_id=conflict_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="new_run",
                        artifact_id=new_artifact_id,
                        direction="output",
                    ),
                ]
            )
            session.commit()

        shard_path = tmp_path / "merge_dry_run_shard.duckdb"
        source_maintenance.export(
            ["conflict_run", "new_run"],
            shard_path,
            include_data=False,
            include_snapshots=False,
            include_children=False,
        )

        shard_snapshot_dir = shard_path.parent / "shard_snapshots"
        shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (shard_snapshot_dir / "new_run.json").write_text("{}", encoding="utf-8")

        with target_db.session_scope() as session:
            session.add(Run(id="conflict_run", model_name="target_model"))
            session.commit()

        merge_result = target_maintenance.merge(
            shard_path,
            conflict="skip",
            include_snapshots=True,
            dry_run=True,
        )
        assert merge_result.runs_merged == ["new_run"]
        assert merge_result.runs_skipped == ["conflict_run"]
        assert merge_result.conflicts_detected == ["conflict_run"]
        assert merge_result.artifacts_merged == 1
        assert merge_result.snapshots_merged == 1

        with target_db.session_scope() as session:
            assert session.get(Run, "new_run") is None
            persisted_conflict = session.get(Run, "conflict_run")
            assert persisted_conflict is not None
            assert persisted_conflict.model_name == "target_model"
            assert session.get(Artifact, new_artifact_id) is None

        assert not (target_maintenance.run_dir / "consist_runs" / "new_run.json").exists()
        assert not (target_maintenance.run_dir / ".consist_audit.log").exists()
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_merge_tolerates_target_side_schema_drift_on_aux_tables(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_drift_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_drift_target.duckdb"))
    source_maintenance = DatabaseMaintenance(source_db, run_dir=tmp_path / "runs_source")
    target_maintenance = DatabaseMaintenance(target_db, run_dir=tmp_path / "runs_target")
    artifact_id = uuid.uuid4()
    try:
        with source_db.session_scope() as session:
            session.add(Run(id="drift_run", model_name="source_model"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="drift_artifact",
                    container_uri="outputs://drift.parquet",
                    driver="parquet",
                    run_id="drift_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="drift_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.add(
                RunConfigKV(
                    run_id="drift_run",
                    facet_id="facet_a",
                    namespace="demo",
                    key="param",
                    value_type="str",
                    value_str="drift",
                )
            )
            session.add(
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id="facet_a",
                    key_path="k",
                    namespace="demo",
                    value_type="str",
                    value_str="drift",
                )
            )
            session.commit()

        shard_path = tmp_path / "merge_drift_shard.duckdb"
        source_maintenance.export(
            "drift_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        with target_db.engine.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE run_config_kv ADD COLUMN target_extra VARCHAR")
            conn.exec_driver_sql("ALTER TABLE artifact_kv ADD COLUMN target_extra VARCHAR")

        merge_result = target_maintenance.merge(
            shard_path,
            conflict="error",
            include_snapshots=False,
        )
        assert merge_result.runs_merged == ["drift_run"]

        with target_db.engine.begin() as conn:
            run_config_row = conn.exec_driver_sql(
                """
                SELECT run_id, value_str, target_extra
                FROM run_config_kv
                WHERE run_id = 'drift_run'
                """
            ).fetchone()
            artifact_kv_row = conn.exec_driver_sql(
                """
                SELECT CAST(artifact_id AS VARCHAR), value_str, target_extra
                FROM artifact_kv
                WHERE CAST(artifact_id AS VARCHAR) = ?
                """,
                (str(artifact_id),),
            ).fetchone()

        assert run_config_row is not None
        assert run_config_row[0] == "drift_run"
        assert run_config_row[1] == "drift"
        assert run_config_row[2] is None
        assert artifact_kv_row is not None
        assert artifact_kv_row[0] == str(artifact_id)
        assert artifact_kv_row[1] == "drift"
        assert artifact_kv_row[2] is None
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def _snapshot_payload(
    *,
    run_id: str,
    model_name: str = "demo",
    output_artifact_ids: list[uuid.UUID] | None = None,
    facet: dict | None = None,
    run_meta: dict | None = None,
    output_artifact_meta: dict[uuid.UUID, dict] | None = None,
) -> dict:
    outputs = []
    for index, artifact_id in enumerate(output_artifact_ids or []):
        artifact_meta = (
            output_artifact_meta.get(artifact_id, {}) if output_artifact_meta else {}
        )
        outputs.append(
            {
                "id": str(artifact_id),
                "key": f"output_{index}",
                "container_uri": f"outputs://output_{index}.parquet",
                "driver": "parquet",
                "meta": artifact_meta,
            }
        )
    return {
        "run": {
            "id": run_id,
            "model_name": model_name,
            "meta": run_meta or {},
        },
        "inputs": [],
        "outputs": outputs,
        "config": {},
        "facet": facet or {},
    }


def test_rebuild_from_json_happy_path_and_idempotent_run_counting(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    existing_run = Run(id="existing_run", model_name="demo")
    with maintenance.db.session_scope() as session:
        session.add(existing_run)
        session.commit()

    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "a_existing.json").write_text(
        json.dumps(_snapshot_payload(run_id="existing_run")),
        encoding="utf-8",
    )
    (json_dir / "b_new.json").write_text(
        json.dumps(_snapshot_payload(run_id="new_run", output_artifact_ids=[artifact_id])),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False)

    assert result.json_files_scanned == 2
    assert result.runs_inserted == 1
    assert result.runs_already_present == 1
    assert result.artifacts_inserted == 1
    assert result.errors == []
    assert result.dry_run is False
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "new_run") is not None
        assert session.get(Artifact, artifact_id) is not None
        links = session.exec(select(RunArtifactLink)).all()
        assert {(row.run_id, row.artifact_id) for row in links} == {
            ("new_run", artifact_id)
        }


def test_rebuild_from_json_dry_run_does_not_write(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_dry_run_snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "dry_run.json").write_text(
        json.dumps(_snapshot_payload(run_id="dry_run_rebuild", output_artifact_ids=[artifact_id])),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=True)

    assert result.dry_run is True
    assert result.runs_inserted == 1
    assert result.runs_already_present == 0
    assert result.artifacts_inserted == 1
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "dry_run_rebuild") is None
        assert session.get(Artifact, artifact_id) is None
    assert not (maintenance.run_dir / ".consist_audit.log").exists()


def test_rebuild_from_json_collects_malformed_errors(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    json_dir = tmp_path / "rebuild_malformed_snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "good.json").write_text(
        json.dumps(_snapshot_payload(run_id="good_run")),
        encoding="utf-8",
    )
    (json_dir / "bad.json").write_text("{bad-json", encoding="utf-8")

    result = maintenance.rebuild_from_json(json_dir, dry_run=False)

    assert result.json_files_scanned == 2
    assert result.runs_inserted == 1
    assert result.runs_already_present == 0
    assert len(result.errors) == 1
    assert "bad.json" in result.errors[0]
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "good_run") is not None


def test_rebuild_from_json_full_mode_populates_run_config_kv(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    json_dir = tmp_path / "rebuild_full_mode_run_facet"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "full_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="full_run",
                facet={"alpha": 1, "nested": {"flag": True, "items": [1, 2]}},
                run_meta={
                    "config_facet_id": "facet_run_id",
                    "config_facet_namespace": "custom_namespace",
                },
            )
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False, mode="full")

    assert result.errors == []
    with maintenance.db.session_scope() as session:
        rows = session.exec(
            select(RunConfigKV)
            .where(RunConfigKV.run_id == "full_run")
            .order_by(RunConfigKV.key)
        ).all()

    assert [row.key for row in rows] == ["alpha", "nested.flag", "nested.items"]
    assert all(row.facet_id == "facet_run_id" for row in rows)
    assert all(row.namespace == "custom_namespace" for row in rows)
    assert rows[0].value_type == "int"
    assert rows[0].value_num == 1.0
    assert rows[1].value_type == "bool"
    assert rows[1].value_bool is True
    assert rows[2].value_type == "json"
    assert rows[2].value_json == [1, 2]


def test_rebuild_from_json_full_mode_populates_artifact_schema_observation(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    schema_id = "schema_full_mode_001"
    json_dir = tmp_path / "rebuild_full_mode_schema"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "schema_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="schema_run",
                output_artifact_ids=[artifact_id],
                output_artifact_meta={
                    artifact_id: {
                        "schema_id": schema_id,
                        "schema_summary": {
                            "source": "file",
                            "sample_rows": 42,
                            "profile_version": 2,
                        },
                        "schema_profile": {"fields": [{"name": "col_a"}]},
                    }
                },
            )
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False, mode="full")

    assert result.errors == []
    with maintenance.db.session_scope() as session:
        schema_row = session.get(ArtifactSchema, schema_id)
        observations = session.exec(
            select(ArtifactSchemaObservation).where(
                ArtifactSchemaObservation.run_id == "schema_run",
                ArtifactSchemaObservation.artifact_id == artifact_id,
                ArtifactSchemaObservation.schema_id == schema_id,
            )
        ).all()

    assert schema_row is not None
    assert schema_row.summary_json["source"] == "file"
    assert schema_row.profile_json == {"fields": [{"name": "col_a"}]}
    assert len(observations) == 1
    assert observations[0].source == "file"
    assert observations[0].sample_rows == 42


def test_rebuild_from_json_full_mode_is_idempotent_for_optional_rows(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_full_mode_idempotent"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "idempotent_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="idempotent_run",
                facet={"a": 1, "b": "two"},
                output_artifact_ids=[artifact_id],
                output_artifact_meta={
                    artifact_id: {
                        "artifact_facet": {"color": "blue", "score": 3},
                        "schema_id": "schema_idempotent",
                        "schema_summary": {"source": "duckdb", "sample_rows": 7},
                    }
                },
            )
        ),
        encoding="utf-8",
    )

    first = maintenance.rebuild_from_json(json_dir, dry_run=False, mode="full")
    second = maintenance.rebuild_from_json(json_dir, dry_run=False, mode="full")

    assert first.errors == []
    assert second.errors == []
    with maintenance.db.session_scope() as session:
        run_kv_rows = session.exec(
            select(RunConfigKV).where(RunConfigKV.run_id == "idempotent_run")
        ).all()
        artifact_kv_rows = session.exec(
            select(ArtifactKV).where(ArtifactKV.artifact_id == artifact_id)
        ).all()
        observation_rows = session.exec(
            select(ArtifactSchemaObservation).where(
                ArtifactSchemaObservation.run_id == "idempotent_run",
                ArtifactSchemaObservation.artifact_id == artifact_id,
            )
        ).all()

    assert len(run_kv_rows) == 2
    assert len(artifact_kv_rows) == 2
    assert len(observation_rows) == 1


def test_rebuild_from_json_minimal_mode_does_not_populate_optional_tables(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_minimal_mode_optional"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "minimal_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="minimal_run",
                facet={"x": 10},
                output_artifact_ids=[artifact_id],
                output_artifact_meta={
                    artifact_id: {
                        "artifact_facet": {"k": "v"},
                        "schema_id": "schema_minimal",
                        "schema_summary": {"source": "file", "sample_rows": 3},
                    }
                },
            )
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False, mode="minimal")

    assert result.errors == []
    with maintenance.db.session_scope() as session:
        assert session.get(Run, "minimal_run") is not None
        assert session.get(Artifact, artifact_id) is not None
        assert session.exec(select(RunConfigKV)).all() == []
        assert session.exec(select(ArtifactKV)).all() == []
        assert session.exec(select(ArtifactSchema)).all() == []
        assert session.exec(select(ArtifactSchemaObservation)).all() == []


def test_compact_runs_vacuum_and_writes_audit(maintenance: DatabaseMaintenance) -> None:
    with patch.object(
        maintenance.db,
        "execute_with_retry",
        wraps=maintenance.db.execute_with_retry,
    ) as wrapped_execute:
        maintenance.compact()

    called_operation_names = [
        call.kwargs.get("operation_name") for call in wrapped_execute.call_args_list
    ]
    assert "maintenance_compact" in called_operation_names
    audit_log = maintenance.run_dir / ".consist_audit.log"
    assert audit_log.exists()
    assert "\tcompact\t" in audit_log.read_text(encoding="utf-8")
