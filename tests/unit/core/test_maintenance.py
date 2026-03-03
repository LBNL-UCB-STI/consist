from __future__ import annotations

import json
from datetime import datetime, timezone
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
