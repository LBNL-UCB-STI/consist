from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch
import uuid

import duckdb
import pytest

from consist.artifact import Artifact
from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.engine import observe
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from consist.models.run import Run
from consist.store.detect import ConsistError, DatabaseMigrationRequired
from tests.helpers.artifact_factory import make_artifact
from tests.helpers.v1_db_builder import (
    create_v1_schema,
    insert_v1_artifact,
    insert_v1_link,
    insert_v1_run,
)


@pytest.fixture
def maintenance(tmp_path: Path) -> Iterator[DatabaseMaintenance]:
    db = DatabaseManager(str(tmp_path / "maintenance.duckdb"))
    manager = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs")
    try:
        yield manager
    finally:
        db.close()


def _run(**kwargs: Any) -> Run:
    kwargs.setdefault("config_hash", None)
    kwargs.setdefault("git_hash", None)
    return Run(**kwargs)


def _distinct_hex(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _seed_artifact(
    db: DatabaseManager,
    *,
    artifact_id: uuid.UUID,
    key: str,
    container_uri: str,
    driver: str,
    run_id: str | None = None,
    schema_id: str | None = None,
    meta: dict[str, Any] | None = None,
    hash_hex: str | None = None,
) -> Artifact:
    effective_hex = hash_hex or _distinct_hex(f"{artifact_id}:{key}:{container_uri}")
    art = make_artifact(
        id=artifact_id,
        key=key,
        container_uri=container_uri,
        driver=driver,
        hash=effective_hex,
        run_id=run_id,
        schema_id=schema_id,
        meta=meta,
    )
    db.upsert_artifact(art)
    return art


def _link_artifact(
    db: DatabaseManager,
    *,
    run_id: str,
    artifact: Artifact,
    direction: str,
) -> None:
    db.sync_artifact(artifact, run_id=run_id, direction=direction)


def _seed_observed_disk_artifact(
    maintenance: DatabaseMaintenance,
    *,
    artifact_id: uuid.UUID,
    path: Path,
    key: str,
    run_id: str,
    workspace_root: Path,
    direction: str = "output",
) -> Artifact:
    observed = observe(
        path,
        key=key,
        ledger=maintenance.ledger,
        mounts={"workspace": workspace_root},
    )
    art = replace(observed, id=artifact_id, run_id=run_id)
    with maintenance.ledger.transaction() as cur:
        cur.execute("DELETE FROM main.artifact WHERE id = ?", [observed.id])
    maintenance.db.sync_artifact(art, run_id=run_id, direction=direction)
    return art


def test_expand_run_ids_recursive_and_cycle_safe(
    maintenance: DatabaseMaintenance,
) -> None:
    for run in (
        _run(id="root", model_name="demo"),
        _run(id="child_a", model_name="demo", parent_run_id="root"),
        _run(id="child_b", model_name="demo", parent_run_id="child_a"),
        _run(id="cycle_1", model_name="demo", parent_run_id="cycle_2"),
        _run(id="cycle_2", model_name="demo", parent_run_id="cycle_1"),
    ):
        maintenance.db.sync_run(run)

    expanded = maintenance._expand_run_ids(["root", "cycle_1"])

    assert set(expanded) == {"root", "child_a", "child_b", "cycle_1", "cycle_2"}
    assert len(expanded) == 5
    assert (
        expanded.index("root") < expanded.index("child_a") < expanded.index("child_b")
    )


def test_find_orphaned_artifacts_linked_only_to_selected_runs(
    maintenance: DatabaseMaintenance,
) -> None:
    orphan_single = uuid.uuid4()
    orphan_multi = uuid.uuid4()
    shared = uuid.uuid4()
    retained = uuid.uuid4()

    for run_id in ("purge_one", "purge_two", "keep"):
        maintenance.db.sync_run(_run(id=run_id, model_name="demo"))

    art_single = _seed_artifact(
        maintenance.db,
        artifact_id=orphan_single,
        key="orphan_single",
        container_uri="outputs://orphan_single.parquet",
        driver="parquet",
    )
    art_multi = _seed_artifact(
        maintenance.db,
        artifact_id=orphan_multi,
        key="orphan_multi",
        container_uri="outputs://orphan_multi.parquet",
        driver="parquet",
    )
    art_shared = _seed_artifact(
        maintenance.db,
        artifact_id=shared,
        key="shared",
        container_uri="outputs://shared.parquet",
        driver="parquet",
    )
    art_retained = _seed_artifact(
        maintenance.db,
        artifact_id=retained,
        key="retained",
        container_uri="outputs://retained.parquet",
        driver="parquet",
    )

    _link_artifact(
        maintenance.db, run_id="purge_one", artifact=art_single, direction="output"
    )
    _link_artifact(
        maintenance.db, run_id="purge_one", artifact=art_multi, direction="output"
    )
    _link_artifact(
        maintenance.db, run_id="purge_two", artifact=art_multi, direction="input"
    )
    _link_artifact(
        maintenance.db, run_id="purge_one", artifact=art_shared, direction="output"
    )
    _link_artifact(
        maintenance.db, run_id="keep", artifact=art_shared, direction="input"
    )
    _link_artifact(
        maintenance.db, run_id="keep", artifact=art_retained, direction="output"
    )

    orphaned = maintenance._find_orphaned_artifacts(["purge_one", "purge_two"])
    assert set(orphaned) == {orphan_single, orphan_multi}


def test_discover_global_tables_from_schema(maintenance: DatabaseMaintenance) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.z_rows (id INTEGER)")
        cur.execute("CREATE TABLE global_tables.a_rows (id INTEGER)")
        cur.execute("CREATE TABLE local_rows (id INTEGER)")

    assert maintenance._discover_global_tables() == ["a_rows", "z_rows"]


def test_classify_global_table_modes(maintenance: DatabaseMaintenance) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
            "CREATE TABLE global_tables.run_scoped_table (consist_run_id VARCHAR)"
        )
        cur.execute("CREATE TABLE global_tables.run_link_table (run_id VARCHAR)")
        cur.execute(
            "CREATE TABLE global_tables.unscoped_cache_table (content_hash VARCHAR)"
        )

    assert maintenance._classify_global_table("run_scoped_table") == "run_scoped"
    assert maintenance._classify_global_table("run_link_table") == "run_link"
    assert (
        maintenance._classify_global_table("unscoped_cache_table") == "unscoped_cache"
    )


@pytest.mark.parametrize(
    ("table_alias", "expected_column"),
    [("", '"consist_run_id"'), ("gt", '"gt"."consist_run_id"')],
)
def test_resolve_global_table_filter_sql_run_scoped(
    maintenance: DatabaseMaintenance, table_alias: str, expected_column: str
) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
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
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.link_filter_table (run_id VARCHAR)")

    sql = maintenance._resolve_global_table_filter_sql(
        "link_filter_table", ["run_1", "run_2"], table_alias=table_alias
    )

    assert sql == f"{expected_column} = ANY(['run_1', 'run_2'])"


def test_resolve_global_table_filter_sql_unscoped_cache_returns_none(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
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
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
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
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(f"CREATE TABLE global_tables.{table_name} ({column} VARCHAR)")

    sql = maintenance._resolve_global_table_filter_sql(table_name, [])

    assert sql == "1 = 0"


def test_list_table_columns_cur_preserves_target_column_order_for_shared_projection(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS source_schema")
        cur.execute("CREATE SCHEMA IF NOT EXISTS target_schema")
        cur.execute(
            """
            CREATE TABLE source_schema.shared_table (
                source_only INTEGER,
                col_b INTEGER,
                col_a INTEGER,
                common_col INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE target_schema.shared_table (
                target_only INTEGER,
                common_col INTEGER,
                col_a INTEGER,
                col_b INTEGER
            )
            """
        )

    with maintenance.ledger.exec_cursor() as cur:
        src_cols = set(
            maintenance._list_table_columns_cur(
                cur, "shared_table", schema="source_schema"
            )
        )
        dst_cols = maintenance._list_table_columns_cur(
            cur, "shared_table", schema="target_schema"
        )

    shared_quoted = [maintenance._quote_ident(c) for c in dst_cols if c in src_cols]
    assert shared_quoted == ['"common_col"', '"col_a"', '"col_b"']


def test_list_table_columns_with_types_cur_missing_table_returns_empty_dict(
    maintenance: DatabaseMaintenance,
) -> None:
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS empty_lookup_schema")

    with maintenance.ledger.exec_cursor() as cur:
        cols = maintenance._list_table_columns_cur(
            cur, "nonexistent_table", schema="empty_lookup_schema"
        )
        col_types = maintenance._list_table_columns_with_types_cur(
            cur, "nonexistent_table", schema="empty_lookup_schema"
        )

    assert cols == []
    assert col_types == {}


@pytest.mark.parametrize(
    ("table", "schema", "catalog", "error_label"),
    [
        ("bad-table", "main", None, "table"),
        ("table_name", "bad-schema", None, "schema"),
        ("table_name", "main", "1catalog", "catalog"),
    ],
)
def test_list_table_columns_cur_rejects_invalid_identifiers(
    maintenance: DatabaseMaintenance,
    table: str,
    schema: str,
    catalog: str | None,
    error_label: str,
) -> None:
    with maintenance.ledger.exec_cursor() as cur:
        with pytest.raises(ValueError, match=f"Invalid {error_label}"):
            maintenance._list_table_columns_cur(
                cur,
                table,
                schema=schema,
                catalog=catalog,
            )


def test_inspect_reports_expected_counts_and_global_sizes(
    maintenance: DatabaseMaintenance,
) -> None:
    now = datetime.now(timezone.utc)
    linked_artifact_id = uuid.uuid4()
    orphaned_artifact_id = uuid.uuid4()

    for run in (
        _run(
            id="run_completed",
            model_name="demo",
            status="completed",
            ended_at=now,
        ),
        _run(
            id="run_running_zombie",
            model_name="demo",
            status="running",
            ended_at=now,
        ),
        _run(id="run_failed", model_name="demo", status="failed", ended_at=now),
    ):
        maintenance.db.sync_run(run)

    linked_art = _seed_artifact(
        maintenance.db,
        artifact_id=linked_artifact_id,
        key="linked_artifact",
        container_uri="outputs://linked.parquet",
        driver="parquet",
    )
    _seed_artifact(
        maintenance.db,
        artifact_id=orphaned_artifact_id,
        key="orphaned_artifact",
        container_uri="outputs://orphaned.parquet",
        driver="parquet",
    )
    _link_artifact(
        maintenance.db,
        run_id="run_completed",
        artifact=linked_art,
        direction="output",
    )

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.sample_global (consist_run_id VARCHAR)")
        cur.execute(
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
    maintenance.db.sync_run(_run(id="run_a", model_name="demo"))
    maintenance.db.sync_run(_run(id="run_b", model_name="demo"))

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
    maintenance.db.sync_run(_run(id="run_only_in_db", model_name="demo"))

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
    missing_content_artifact = uuid.uuid4()
    produced_artifact = uuid.uuid4()

    for run in (
        _run(id="zombie_run", model_name="demo", status="running", ended_at=now),
        _run(
            id="completed_missing_end",
            model_name="demo",
            status="completed",
            ended_at=None,
        ),
        _run(
            id="failed_missing_end",
            model_name="demo",
            status="failed",
            ended_at=None,
        ),
        _run(
            id="valid_parent",
            model_name="demo",
            status="completed",
            ended_at=now,
        ),
        _run(
            id="dangling_child",
            model_name="demo",
            status="completed",
            parent_run_id="missing_parent",
            ended_at=now,
        ),
    ):
        maintenance.db.sync_run(run)

    _seed_artifact(
        maintenance.db,
        artifact_id=missing_producer_artifact,
        key="missing_producer",
        container_uri="outputs://missing_producer.parquet",
        driver="parquet",
        run_id="missing_run",
    )
    produced_art = _seed_artifact(
        maintenance.db,
        artifact_id=produced_artifact,
        key="produced",
        container_uri="outputs://produced.parquet",
        driver="parquet",
        run_id="valid_parent",
    )

    with maintenance.ledger.transaction() as cur:
        cur.execute(
            """
            INSERT INTO main.artifact (id, key, driver, anchor, run_id, meta, created_at)
            VALUES (?, 'missing_content', 'parquet', 'sha256:file:' || repeat('f', 64), 'valid_parent', '{}'::JSON, ?)
            """,
            [missing_content_artifact, now],
        )
        cur.execute(
            """
            INSERT INTO main.run_binding (
                run_id, direction, occurrence_index, slot, artifact_id, anchor, ordinal, role
            ) VALUES ('nonexistent_run', 'output', 0, 'out', ?, ?, 0, 'out')
            """,
            [produced_art.id, str(produced_art.identity.anchor)],
        )
        cur.execute(
            """
            INSERT INTO main.content_digest_conflict (
                anchor, scheme, kept_value, rejected_value, source, seen_at
            ) VALUES (?, 'sha256:file', ?, ?, 'test', ?)
            """,
            [
                str(produced_art.identity.anchor),
                produced_art.identity.anchor.hex,
                "0" * 64,
                now,
            ],
        )
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.drift_gt (consist_run_id VARCHAR)")
        cur.execute(
            "INSERT INTO global_tables.drift_gt (consist_run_id) VALUES ('ghost_run')"
        )

    report = maintenance.doctor()

    assert report.zombie_run_ids == ["zombie_run"]
    assert report.completed_without_end_time == [
        "completed_missing_end",
        "failed_missing_end",
    ]
    assert report.dangling_parent_run_ids == ["missing_parent"]
    assert report.artifacts_with_missing_producing_run == [missing_producer_artifact]
    assert report.orphaned_run_binding_count == 1
    assert report.artifacts_with_missing_content == [missing_content_artifact]
    assert report.digest_conflict_count == 1
    assert "drift_gt" in report.global_table_schema_drift


def test_snapshot_creates_destination_file(maintenance: DatabaseMaintenance) -> None:
    destination = maintenance.run_dir / "snapshots" / "maintenance_snapshot.duckdb"

    snapshot_path = maintenance.snapshot(destination)

    assert snapshot_path == destination
    assert snapshot_path.exists()
    assert snapshot_path.is_file()


def test_snapshot_writes_sidecar_metadata_file(
    maintenance: DatabaseMaintenance,
) -> None:
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
    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.scoped_counts (consist_run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.link_counts (run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.cache_counts (content_hash VARCHAR)")
        cur.execute(
            """
            INSERT INTO global_tables.scoped_counts (consist_run_id)
            VALUES ('run_1'), ('run_1'), ('run_2')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.link_counts (run_id)
            VALUES ('run_1'), ('run_3')
            """
        )
        cur.execute(
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

    for run in (
        _run(id="root", model_name="demo"),
        _run(
            id="child",
            model_name="demo",
            parent_run_id="root",
            meta={"_physical_run_dir": str(child_run_dir)},
        ),
        _run(id="keep", model_name="demo"),
    ):
        maintenance.db.sync_run(run)

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.scoped_table (consist_run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.link_table (run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.cache_table (content_hash VARCHAR)")
        cur.execute(
            """
            INSERT INTO global_tables.scoped_table (consist_run_id)
            VALUES ('root'), ('child'), ('keep')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.link_table (run_id)
            VALUES ('child'), ('keep')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.cache_table (content_hash)
            VALUES ('hash_a'), ('hash_b')
            """
        )

    plan = maintenance.plan_purge("root", include_children=True)

    assert plan.run_ids == ["root", "child"]
    assert plan.child_run_ids == ["child"]
    assert plan.orphaned_artifact_ids == []
    assert plan.json_files == [root_snapshot.resolve()]
    assert plan.disk_files == []
    assert plan.unsafe_json_files == [child_snapshot.resolve()]
    assert plan.unsafe_disk_files == []
    assert plan.ingested_data == {"cache_table": 0, "link_table": 1, "scoped_table": 2}
    assert plan.ingested_table_modes == {
        "cache_table": "unscoped_cache",
        "link_table": "run_link",
        "scoped_table": "run_scoped",
    }


def test_collect_prune_candidates_workspace_relative(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    run_workspace = tmp_path / "workspace_run"
    artifact_path = run_workspace / "outputs" / "artifact.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("artifact", encoding="utf-8")

    maintenance.db.sync_run(
        _run(
            id="workspace_run",
            model_name="demo",
            meta={"_physical_run_dir": str(run_workspace)},
        )
    )
    _seed_artifact(
        maintenance.db,
        artifact_id=artifact_id,
        key="workspace_artifact",
        container_uri="workspace://outputs/artifact.txt",
        driver="txt",
        run_id="workspace_run",
    )

    resolved_paths = [
        cand.path for cand in maintenance._collect_prune_candidates([artifact_id])
    ]

    assert resolved_paths == [artifact_path.resolve()]


def test_collect_prune_candidates_mounted_uri_from_run_and_artifact_metadata(
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

    maintenance.db.sync_run(
        _run(
            id="mount_run",
            model_name="demo",
            meta={"mounts": {"inputs": str(run_mount_root)}},
        )
    )
    _seed_artifact(
        maintenance.db,
        artifact_id=run_mount_artifact_id,
        key="run_mount_artifact",
        container_uri="inputs://dataset/run_mount.csv",
        driver="csv",
        run_id="mount_run",
    )
    _seed_artifact(
        maintenance.db,
        artifact_id=artifact_mount_artifact_id,
        key="artifact_mount_artifact",
        container_uri="inputs://dataset/artifact_mount.csv",
        driver="csv",
        run_id="mount_run",
        meta={"mount_root": str(artifact_mount_root)},
    )

    resolved_paths = [
        cand.path
        for cand in maintenance._collect_prune_candidates(
            [run_mount_artifact_id, artifact_mount_artifact_id]
        )
    ]

    assert resolved_paths == [
        run_mount_file.resolve(),
        artifact_mount_file.resolve(),
    ]


def test_purge_dry_run_makes_no_db_changes(maintenance: DatabaseMaintenance) -> None:
    artifact_id = uuid.uuid4()
    snapshot_path = maintenance.run_dir / "consist_runs" / "dry_run_target.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("{}", encoding="utf-8")

    maintenance.db.sync_run(_run(id="dry_run_target", model_name="demo"))
    art = _seed_artifact(
        maintenance.db,
        artifact_id=artifact_id,
        key="dry_artifact",
        container_uri="outputs://dry_artifact.parquet",
        driver="parquet",
        run_id="dry_run_target",
    )
    _link_artifact(
        maintenance.db, run_id="dry_run_target", artifact=art, direction="output"
    )

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
    assert maintenance.ledger.get_run("dry_run_target") is not None
    assert maintenance.ledger.get_artifact(artifact_id) is not None
    with maintenance.ledger.exec_cursor() as cur:
        binding_count = cur.execute("SELECT COUNT(*) FROM main.run_binding").fetchone()
    assert int(binding_count[0] if binding_count else 0) == 1


def test_purge_delete_files_removes_files_and_directories_in_default_run_dir(
    maintenance: DatabaseMaintenance,
) -> None:
    file_artifact_id = uuid.uuid4()
    dir_artifact_id = uuid.uuid4()
    file_path = maintenance.run_dir / "outputs" / "default_delete_me.txt"
    dir_path = maintenance.run_dir / "outputs" / "default_delete_dir"
    nested_path = dir_path / "nested.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path.write_text("file", encoding="utf-8")
    nested_path.write_text("nested", encoding="utf-8")

    maintenance.db.sync_run(_run(id="default_ws_purge_run", model_name="demo"))
    _seed_observed_disk_artifact(
        maintenance,
        artifact_id=file_artifact_id,
        path=file_path,
        key="default_delete_file",
        run_id="default_ws_purge_run",
        workspace_root=maintenance.run_dir,
    )
    _seed_observed_disk_artifact(
        maintenance,
        artifact_id=dir_artifact_id,
        path=dir_path,
        key="default_delete_dir",
        run_id="default_ws_purge_run",
        workspace_root=maintenance.run_dir,
    )

    result = maintenance.purge(
        "default_ws_purge_run",
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
    assert result.plan.unsafe_disk_files == []
    assert not file_path.exists()
    assert not dir_path.exists()


def test_purge_delete_files_removes_files_and_directories(
    maintenance: DatabaseMaintenance,
) -> None:
    file_artifact_id = uuid.uuid4()
    dir_artifact_id = uuid.uuid4()
    run_workspace = maintenance.run_dir / "purge_delete_workspace"
    file_path = run_workspace / "outputs" / "delete_me.txt"
    dir_path = run_workspace / "outputs" / "delete_dir"
    nested_path = dir_path / "nested.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path.write_text("file", encoding="utf-8")
    nested_path.write_text("nested", encoding="utf-8")

    maintenance.db.sync_run(
        _run(
            id="purge_delete_files_run",
            model_name="demo",
            meta={"_physical_run_dir": str(run_workspace)},
        )
    )
    _seed_observed_disk_artifact(
        maintenance,
        artifact_id=file_artifact_id,
        path=file_path,
        key="delete_file_artifact",
        run_id="purge_delete_files_run",
        workspace_root=run_workspace,
    )
    _seed_observed_disk_artifact(
        maintenance,
        artifact_id=dir_artifact_id,
        path=dir_path,
        key="delete_dir_artifact",
        run_id="purge_delete_files_run",
        workspace_root=run_workspace,
    )

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
    assert result.plan.unsafe_disk_files == []
    assert not file_path.exists()
    assert not dir_path.exists()


def test_purge_delete_files_rejects_file_uri_outside_run_dir(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("outside", encoding="utf-8")

    maintenance.db.sync_run(_run(id="unsafe_file_uri_run", model_name="demo"))
    art = _seed_artifact(
        maintenance.db,
        artifact_id=artifact_id,
        key="unsafe_file_uri_artifact",
        container_uri=outside_file.resolve().as_uri(),
        driver="txt",
        run_id="unsafe_file_uri_run",
    )
    _link_artifact(
        maintenance.db, run_id="unsafe_file_uri_run", artifact=art, direction="output"
    )

    plan = maintenance.plan_purge("unsafe_file_uri_run", include_children=False)
    assert plan.disk_files == []
    assert plan.unsafe_disk_files == [outside_file.resolve()]

    with pytest.raises(ValueError, match="outside allowed roots"):
        maintenance.purge(
            "unsafe_file_uri_run",
            include_children=False,
            delete_files=True,
            delete_ingested_data=False,
            dry_run=False,
        )

    assert outside_file.exists()


def test_purge_delete_files_rejects_absolute_path_outside_run_dir(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    outside_file = tmp_path / "absolute-outside.txt"
    outside_file.write_text("outside", encoding="utf-8")

    maintenance.db.sync_run(_run(id="unsafe_absolute_run", model_name="demo"))
    art = _seed_artifact(
        maintenance.db,
        artifact_id=artifact_id,
        key="unsafe_absolute_artifact",
        container_uri=str(outside_file.resolve()),
        driver="txt",
        run_id="unsafe_absolute_run",
    )
    _link_artifact(
        maintenance.db, run_id="unsafe_absolute_run", artifact=art, direction="output"
    )

    plan = maintenance.plan_purge("unsafe_absolute_run", include_children=False)
    assert plan.disk_files == []
    assert plan.unsafe_disk_files == [outside_file.resolve()]

    with pytest.raises(ValueError, match="outside allowed roots"):
        maintenance.purge(
            "unsafe_absolute_run",
            include_children=False,
            delete_files=True,
            delete_ingested_data=False,
            dry_run=False,
        )

    assert outside_file.exists()


def test_purge_delete_files_rejects_snapshot_outside_run_dir(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    outside_workspace = tmp_path / "outside_workspace"
    snapshot_path = outside_workspace / "consist_runs" / "unsafe_snapshot_run.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("{}", encoding="utf-8")

    maintenance.db.sync_run(
        _run(
            id="unsafe_snapshot_run",
            model_name="demo",
            meta={"_physical_run_dir": str(outside_workspace)},
        )
    )

    plan = maintenance.plan_purge("unsafe_snapshot_run", include_children=False)
    assert plan.json_files == []
    assert plan.unsafe_json_files == [snapshot_path.resolve()]

    with pytest.raises(ValueError, match="outside allowed roots"):
        maintenance.purge(
            "unsafe_snapshot_run",
            include_children=False,
            delete_files=False,
            delete_ingested_data=False,
            dry_run=False,
        )

    assert snapshot_path.exists()


def test_purge_delete_files_skip_mode_deletes_safe_targets_and_reports_unsafe(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    safe_artifact_id = uuid.uuid4()
    unsafe_artifact_id = uuid.uuid4()
    run_workspace = maintenance.run_dir / "skip_workspace"
    safe_file = run_workspace / "outputs" / "safe.txt"
    unsafe_file = tmp_path / "unsafe.txt"
    safe_file.parent.mkdir(parents=True, exist_ok=True)
    safe_file.write_text("safe", encoding="utf-8")
    unsafe_file.write_text("unsafe", encoding="utf-8")

    maintenance.db.sync_run(
        _run(
            id="skip_mode_run",
            model_name="demo",
            meta={"_physical_run_dir": str(run_workspace)},
        )
    )
    _seed_observed_disk_artifact(
        maintenance,
        artifact_id=safe_artifact_id,
        path=safe_file,
        key="safe_artifact",
        run_id="skip_mode_run",
        workspace_root=run_workspace,
    )
    unsafe_art = _seed_artifact(
        maintenance.db,
        artifact_id=unsafe_artifact_id,
        key="unsafe_artifact",
        container_uri=unsafe_file.resolve().as_uri(),
        driver="txt",
        run_id="skip_mode_run",
    )
    _link_artifact(
        maintenance.db,
        run_id="skip_mode_run",
        artifact=unsafe_art,
        direction="output",
    )

    result = maintenance.purge(
        "skip_mode_run",
        include_children=False,
        delete_files=True,
        delete_ingested_data=False,
        dry_run=False,
        unsafe_delete_targets="skip",
    )

    assert result.executed is True
    assert result.plan.disk_files == [safe_file.resolve()]
    assert result.plan.unsafe_disk_files == [unsafe_file.resolve()]
    assert result.skipped_unsafe_disk_files == [unsafe_file.resolve()]
    assert not safe_file.exists()
    assert unsafe_file.exists()


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

    maintenance.db.sync_run(
        _run(
            id="purge_root",
            model_name="demo",
            meta={
                "facet": {"a": "purge"},
                "config_facet_id": "facet_1",
                "config_facet_namespace": "demo",
            },
        )
    )
    maintenance.db.sync_run(
        _run(id="purge_child", model_name="demo", parent_run_id="purge_root")
    )
    maintenance.db.sync_run(
        _run(
            id="keep_run",
            model_name="demo",
            meta={
                "facet": {"a": "keep"},
                "config_facet_id": "facet_1",
                "config_facet_namespace": "demo",
            },
        )
    )

    art_orphan_root = _seed_artifact(
        maintenance.db,
        artifact_id=orphan_root_id,
        key="orphan_root",
        container_uri="outputs://orphan_root.parquet",
        driver="parquet",
        run_id="purge_root",
        meta={
            "facet": {"k": "orphan_root"},
            "artifact_facet_id": "facet_a",
            "artifact_facet_namespace": "demo",
        },
    )
    art_orphan_child = _seed_artifact(
        maintenance.db,
        artifact_id=orphan_child_id,
        key="orphan_child",
        container_uri="outputs://orphan_child.parquet",
        driver="parquet",
        run_id="purge_child",
        meta={
            "facet": {"k": "orphan_child"},
            "artifact_facet_id": "facet_a",
            "artifact_facet_namespace": "demo",
        },
    )
    art_shared = _seed_artifact(
        maintenance.db,
        artifact_id=shared_id,
        key="shared",
        container_uri="outputs://shared.parquet",
        driver="parquet",
        run_id="purge_root",
        meta={
            "facet": {"k": "shared"},
            "artifact_facet_id": "facet_a",
            "artifact_facet_namespace": "demo",
        },
    )
    art_keep = _seed_artifact(
        maintenance.db,
        artifact_id=keep_id,
        key="keep",
        container_uri="outputs://keep.parquet",
        driver="parquet",
        run_id="keep_run",
    )

    _link_artifact(
        maintenance.db,
        run_id="purge_root",
        artifact=art_orphan_root,
        direction="output",
    )
    _link_artifact(
        maintenance.db,
        run_id="purge_child",
        artifact=art_orphan_child,
        direction="output",
    )
    _link_artifact(
        maintenance.db, run_id="purge_root", artifact=art_shared, direction="output"
    )
    _link_artifact(
        maintenance.db, run_id="keep_run", artifact=art_shared, direction="input"
    )
    _link_artifact(
        maintenance.db, run_id="keep_run", artifact=art_keep, direction="output"
    )

    schema = ArtifactSchema(id="schema_1", summary_json={})
    for aid, rid in (
        (orphan_root_id, "purge_root"),
        (orphan_child_id, "purge_child"),
        (shared_id, "keep_run"),
    ):
        maintenance.ledger.upsert_artifact_schema(
            schema,
            [],
            observation=ArtifactSchemaObservation(
                artifact_id=aid,
                schema_id="schema_1",
                run_id=rid,
                source="file",
            ),
        )

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.scoped_delete (consist_run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.link_delete (run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.cache_delete (content_hash VARCHAR)")
        cur.execute(
            """
            INSERT INTO global_tables.scoped_delete (consist_run_id)
            VALUES ('purge_root'), ('purge_child'), ('keep_run')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.link_delete (run_id)
            VALUES ('purge_root'), ('keep_run')
            """
        )
        cur.execute(
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

    assert maintenance.ledger.get_run("purge_root") is None
    assert maintenance.ledger.get_run("purge_child") is None
    assert maintenance.ledger.get_run("keep_run") is not None
    assert maintenance.ledger.get_artifact(orphan_root_id) is None
    assert maintenance.ledger.get_artifact(orphan_child_id) is None
    shared_after = maintenance.ledger.get_artifact(shared_id)
    assert shared_after is not None
    assert shared_after.run_id is None
    assert maintenance.ledger.get_artifact(keep_id) is not None

    with maintenance.ledger.exec_cursor() as cur:
        remaining_links = cur.execute(
            "SELECT run_id, artifact_id FROM main.run_binding"
        ).fetchall()
        remaining_run_kv = cur.execute(
            "SELECT run_id FROM main.run_config_kv"
        ).fetchall()
        remaining_artifact_kv = cur.execute(
            "SELECT artifact_id FROM main.artifact_kv"
        ).fetchall()
        remaining_observations = cur.execute(
            "SELECT artifact_id, run_id FROM main.artifact_schema_observation"
        ).fetchall()
        scoped_rows = cur.execute(
            "SELECT consist_run_id FROM global_tables.scoped_delete ORDER BY consist_run_id"
        ).fetchall()
        link_rows = cur.execute(
            "SELECT run_id FROM global_tables.link_delete ORDER BY run_id"
        ).fetchall()
        cache_rows = cur.execute(
            "SELECT content_hash FROM global_tables.cache_delete ORDER BY content_hash"
        ).fetchall()

    assert {(str(r[0]), uuid.UUID(str(r[1]))) for r in remaining_links} == {
        ("keep_run", shared_id),
        ("keep_run", keep_id),
    }
    assert [str(r[0]) for r in remaining_run_kv] == ["keep_run"]
    assert {uuid.UUID(str(r[0])) for r in remaining_artifact_kv} == {shared_id}
    assert len(remaining_observations) == 1
    assert uuid.UUID(str(remaining_observations[0][0])) == shared_id
    assert str(remaining_observations[0][1]) == "keep_run"
    assert [row[0] for row in scoped_rows] == ["keep_run", "purge_child", "purge_root"]
    assert [row[0] for row in link_rows] == ["keep_run", "purge_root"]
    assert [row[0] for row in cache_rows] == ["hash_a", "hash_b"]


def test_purge_deletes_global_rows_when_requested(
    maintenance: DatabaseMaintenance,
) -> None:
    maintenance.db.sync_run(_run(id="purge_target", model_name="demo"))
    maintenance.db.sync_run(_run(id="keep_target", model_name="demo"))

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.scoped_prune (consist_run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.link_prune (run_id VARCHAR)")
        cur.execute("CREATE TABLE global_tables.cache_prune (content_hash VARCHAR)")
        cur.execute(
            """
            INSERT INTO global_tables.scoped_prune (consist_run_id)
            VALUES ('purge_target'), ('keep_target')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.link_prune (run_id)
            VALUES ('purge_target'), ('keep_target')
            """
        )
        cur.execute(
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
    with maintenance.ledger.exec_cursor() as cur:
        scoped_rows = cur.execute(
            "SELECT consist_run_id FROM global_tables.scoped_prune ORDER BY consist_run_id"
        ).fetchall()
        link_rows = cur.execute(
            "SELECT run_id FROM global_tables.link_prune ORDER BY run_id"
        ).fetchall()
        cache_rows = cur.execute(
            "SELECT content_hash FROM global_tables.cache_prune ORDER BY content_hash"
        ).fetchall()

    assert [row[0] for row in scoped_rows] == ["keep_target"]
    assert [row[0] for row in link_rows] == ["keep_target"]
    assert [row[0] for row in cache_rows] == ["cache_a", "cache_b"]


def test_purge_prune_cache_noops_without_derivable_reference_path(
    maintenance: DatabaseMaintenance,
) -> None:
    maintenance.db.sync_run(_run(id="purge_target", model_name="demo"))
    maintenance.db.sync_run(_run(id="keep_target", model_name="demo"))

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute("CREATE TABLE global_tables.link_without_hash (run_id VARCHAR)")
        cur.execute(
            "CREATE TABLE global_tables.cache_no_derivation (content_hash VARCHAR)"
        )
        cur.execute(
            """
            INSERT INTO global_tables.link_without_hash (run_id)
            VALUES ('purge_target'), ('keep_target')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.cache_no_derivation (content_hash)
            VALUES ('cache_only_purge'), ('cache_keep')
            """
        )

    maintenance.purge(
        "purge_target",
        include_children=False,
        delete_files=False,
        delete_ingested_data=True,
        prune_cache=True,
        dry_run=False,
    )

    with maintenance.ledger.exec_cursor() as cur:
        link_rows = cur.execute(
            "SELECT run_id FROM global_tables.link_without_hash ORDER BY run_id"
        ).fetchall()
        cache_rows = cur.execute(
            """
            SELECT content_hash
            FROM global_tables.cache_no_derivation
            ORDER BY content_hash
            """
        ).fetchall()

    assert [row[0] for row in link_rows] == ["keep_target"]
    assert [row[0] for row in cache_rows] == ["cache_keep", "cache_only_purge"]


def test_purge_prune_cache_without_delete_ingested_data_is_noop(
    maintenance: DatabaseMaintenance,
) -> None:
    maintenance.db.sync_run(_run(id="purge_target", model_name="demo"))
    maintenance.db.sync_run(_run(id="keep_target", model_name="demo"))

    with maintenance.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
            """
            CREATE TABLE global_tables.link_with_hash (
                run_id VARCHAR,
                content_hash VARCHAR
            )
            """
        )
        cur.execute("CREATE TABLE global_tables.cache_safe_noop (content_hash VARCHAR)")
        cur.execute(
            """
            INSERT INTO global_tables.link_with_hash (run_id, content_hash)
            VALUES ('purge_target', 'cache_only_purge'), ('keep_target', 'cache_keep')
            """
        )
        cur.execute(
            """
            INSERT INTO global_tables.cache_safe_noop (content_hash)
            VALUES ('cache_only_purge'), ('cache_keep')
            """
        )

    result = maintenance.purge(
        "purge_target",
        include_children=False,
        delete_files=False,
        delete_ingested_data=False,
        prune_cache=True,
        dry_run=False,
    )

    assert result.ingested_data_skipped is True
    with maintenance.ledger.exec_cursor() as cur:
        link_rows = cur.execute(
            """
            SELECT run_id, content_hash
            FROM global_tables.link_with_hash
            ORDER BY run_id
            """
        ).fetchall()
        cache_rows = cur.execute(
            """
            SELECT content_hash
            FROM global_tables.cache_safe_noop
            ORDER BY content_hash
            """
        ).fetchall()

    assert [(row[0], row[1]) for row in link_rows] == [
        ("keep_target", "cache_keep"),
        ("purge_target", "cache_only_purge"),
    ]
    assert [row[0] for row in cache_rows] == ["cache_keep", "cache_only_purge"]


@pytest.mark.parametrize("new_status", ["completed", "failed"])
def test_fix_status_running_to_terminal_sets_end_time_and_reason(
    maintenance: DatabaseMaintenance, new_status: str
) -> None:
    maintenance.db.sync_run(
        _run(id=f"run_{new_status}", model_name="demo", status="running")
    )

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
    maintenance.db.sync_run(
        _run(
            id="run_terminal",
            model_name="demo",
            status="completed",
            ended_at=now,
        )
    )

    updated = maintenance.fix_status("run_terminal", "running", reason=None, force=True)

    assert updated.status == "running"
    assert updated.ended_at is None


def test_fix_status_terminal_to_running_requires_force(
    maintenance: DatabaseMaintenance,
) -> None:
    now = datetime.now(timezone.utc)
    maintenance.db.sync_run(
        _run(
            id="run_terminal_requires_force",
            model_name="demo",
            status="completed",
            ended_at=now,
        )
    )

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
    maintenance.db.sync_run(
        _run(id="run_bad_status", model_name="demo", status="running")
    )

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
        db.sync_run(
            _run(
                id="root_run",
                model_name="demo",
                meta={
                    "facet": {"param": "root"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )
        db.sync_run(
            _run(
                id="child_run",
                model_name="demo",
                parent_run_id="root_run",
                meta={
                    "facet": {"param": "child"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )
        db.sync_run(
            _run(
                id="other_run",
                model_name="demo",
                meta={
                    "facet": {"param": "other"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )

        root_art = _seed_artifact(
            db,
            artifact_id=root_artifact_id,
            key="root_output",
            container_uri="outputs://root.parquet",
            driver="parquet",
            run_id="root_run",
            meta={
                "facet": {"k": "root"},
                "artifact_facet_id": "facet_a",
                "artifact_facet_namespace": "demo",
            },
        )
        child_art = _seed_artifact(
            db,
            artifact_id=child_artifact_id,
            key="child_output",
            container_uri="outputs://child.parquet",
            driver="parquet",
            run_id="child_run",
            meta={
                "facet": {"k": "child"},
                "artifact_facet_id": "facet_a",
                "artifact_facet_namespace": "demo",
            },
        )
        other_art = _seed_artifact(
            db,
            artifact_id=other_artifact_id,
            key="other_output",
            container_uri="outputs://other.parquet",
            driver="parquet",
            run_id="other_run",
            meta={
                "facet": {"k": "other"},
                "artifact_facet_id": "facet_a",
                "artifact_facet_namespace": "demo",
            },
        )

        _link_artifact(db, run_id="root_run", artifact=root_art, direction="output")
        _link_artifact(db, run_id="child_run", artifact=child_art, direction="output")
        _link_artifact(db, run_id="other_run", artifact=other_art, direction="output")

        schema = ArtifactSchema(id="schema_export", summary_json={})
        for aid, rid in (
            (root_artifact_id, "root_run"),
            (child_artifact_id, "child_run"),
            (other_artifact_id, "other_run"),
        ):
            db.ledger.upsert_artifact_schema(
                schema,
                [],
                observation=ArtifactSchemaObservation(
                    artifact_id=aid,
                    schema_id="schema_export",
                    run_id=rid,
                    source="file",
                ),
            )

        with db.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.scoped_export (consist_run_id VARCHAR)"
            )
            cur.execute("CREATE TABLE global_tables.link_export (run_id VARCHAR)")
            cur.execute(
                "CREATE TABLE global_tables.cache_export (content_hash VARCHAR)"
            )
            cur.execute(
                """
                INSERT INTO global_tables.scoped_export (consist_run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_export (run_id)
                VALUES ('child_run'), ('other_run')
                """
            )
            cur.execute(
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
            with shard_db.ledger.exec_cursor() as cur:
                run_count = cur.execute("SELECT COUNT(*) FROM main.run").fetchone()
                artifact_count = cur.execute(
                    "SELECT COUNT(*) FROM main.artifact"
                ).fetchone()
                binding_count = cur.execute(
                    "SELECT COUNT(*) FROM main.run_binding"
                ).fetchone()
                run_config_count = cur.execute(
                    "SELECT COUNT(*) FROM main.run_config_kv"
                ).fetchone()
                artifact_kv_count = cur.execute(
                    "SELECT COUNT(*) FROM main.artifact_kv"
                ).fetchone()
                observation_count = cur.execute(
                    "SELECT COUNT(*) FROM main.artifact_schema_observation"
                ).fetchone()
                scoped_count = cur.execute(
                    "SELECT COUNT(*) FROM global_tables.scoped_export"
                ).fetchone()
                link_table_count = cur.execute(
                    "SELECT COUNT(*) FROM global_tables.link_export"
                ).fetchone()
            assert int(run_count[0] if run_count else 0) == 2
            assert int(artifact_count[0] if artifact_count else 0) == 2
            assert int(binding_count[0] if binding_count else 0) == 2
            assert int(run_config_count[0] if run_config_count else 0) == 2
            assert int(artifact_kv_count[0] if artifact_kv_count else 0) == 2
            assert int(observation_count[0] if observation_count else 0) == 2
            assert int(scoped_count[0] if scoped_count else 0) == 2
            assert int(link_table_count[0] if link_table_count else 0) == 1
        finally:
            shard_db.close()

        shard_snapshot_dir = shard_path.parent / "shard_snapshots"
        assert (shard_snapshot_dir / "root_run.json").exists()
        assert (shard_snapshot_dir / "child_run.json").exists()
    finally:
        db.close()


def test_attached_database_commits_detaches_and_rejects_nested_transaction(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "attach_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    shard_path = tmp_path / "attach_shard.duckdb"
    maintenance._init_shard_schema(shard_path)
    try:
        with maintenance._attached_database(
            shard_path,
            "shard_export",
            read_only=False,
            mutate_main=False,
        ) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS shard_export.main.temp_table (id INTEGER)"
            )
            cur.execute("INSERT INTO shard_export.main.temp_table VALUES (7)")

        assert db.ledger._tx_depth == 0
        with db.ledger.exec_cursor() as cur:
            attached_names = {
                str(r[0])
                for r in cur.execute(
                    "SELECT database_name FROM duckdb_databases()"
                ).fetchall()
            }
        assert "shard_export" not in attached_names

        with db.ledger.transaction():
            with pytest.raises(ConsistError, match="Cannot ATTACH shard database"):
                with maintenance._attached_database(shard_path, "shard_nested"):
                    pass
    finally:
        db.close()


def test_export_no_children_only_copies_requested_run(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "export_no_children_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    try:
        db.sync_run(_run(id="root_run", model_name="demo"))
        db.sync_run(_run(id="child_run", model_name="demo", parent_run_id="root_run"))
        root_art = _seed_artifact(
            db,
            artifact_id=root_artifact_id,
            key="root_output",
            container_uri="outputs://root.parquet",
            driver="parquet",
            run_id="root_run",
        )
        child_art = _seed_artifact(
            db,
            artifact_id=child_artifact_id,
            key="child_output",
            container_uri="outputs://child.parquet",
            driver="parquet",
            run_id="child_run",
        )
        _link_artifact(db, run_id="root_run", artifact=root_art, direction="output")
        _link_artifact(db, run_id="child_run", artifact=child_art, direction="output")

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
            with shard_db.ledger.exec_cursor() as cur:
                run_count = cur.execute("SELECT COUNT(*) FROM main.run").fetchone()
                artifact_count = cur.execute(
                    "SELECT COUNT(*) FROM main.artifact"
                ).fetchone()
            assert int(run_count[0] if run_count else 0) == 1
            assert int(artifact_count[0] if artifact_count else 0) == 1
        finally:
            shard_db.close()
    finally:
        db.close()


def test_export_dry_run_previews_without_writes(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "export_dry_run_source.duckdb"))
    maintenance = DatabaseMaintenance(db=db, run_dir=tmp_path / "runs_source")
    artifact_id = uuid.uuid4()
    try:
        db.sync_run(_run(id="root_run", model_name="demo"))
        art = _seed_artifact(
            db,
            artifact_id=artifact_id,
            key="root_output",
            container_uri="outputs://root.parquet",
            driver="parquet",
            run_id="root_run",
        )
        _link_artifact(db, run_id="root_run", artifact=art, direction="output")

        with db.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.scoped_export_preview (consist_run_id VARCHAR)"
            )
            cur.execute(
                "CREATE TABLE global_tables.cache_export_preview (content_hash VARCHAR)"
            )
            cur.execute(
                """
                INSERT INTO global_tables.scoped_export_preview (consist_run_id)
                VALUES ('root_run')
                """
            )
            cur.execute(
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
        assert db.ledger.get_run("root_run") is not None
    finally:
        db.close()


def test_merge_happy_path_and_conflict_skip_policy(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_target.duckdb"))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "runs_source"
    )
    target_maintenance = DatabaseMaintenance(
        target_db, run_dir=tmp_path / "runs_target"
    )
    artifact_id = uuid.uuid4()
    try:
        source_db.sync_run(
            _run(
                id="merge_run",
                model_name="source_model",
                meta={
                    "_physical_run_dir": str(source_maintenance.run_dir),
                    "facet": {"param": "merge"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )
        art = _seed_artifact(
            source_db,
            artifact_id=artifact_id,
            key="merge_artifact",
            container_uri="outputs://merge.parquet",
            driver="parquet",
            run_id="merge_run",
            meta={
                "facet": {"k": "merge"},
                "artifact_facet_id": "facet_a",
                "artifact_facet_namespace": "demo",
            },
        )
        _link_artifact(source_db, run_id="merge_run", artifact=art, direction="output")
        source_db.ledger.upsert_artifact_schema(
            ArtifactSchema(id="schema_merge", summary_json={}),
            [],
            observation=ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id="schema_merge",
                run_id="merge_run",
                source="file",
            ),
        )

        with source_db.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.scoped_merge (consist_run_id VARCHAR)"
            )
            cur.execute("CREATE TABLE global_tables.cache_merge (content_hash VARCHAR)")
            cur.execute(
                """
                INSERT INTO global_tables.scoped_merge (consist_run_id)
                VALUES ('merge_run')
                """
            )
            cur.execute(
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

        merged_run = target_db.ledger.get_run("merge_run")
        assert merged_run is not None
        assert merged_run.model_name == "source_model"

        with target_db.ledger.exec_cursor() as cur:
            scoped_count = cur.execute(
                "SELECT COUNT(*) FROM global_tables.scoped_merge"
            ).fetchone()
            run_config_count = cur.execute(
                "SELECT COUNT(*) FROM main.run_config_kv"
            ).fetchone()
            artifact_kv_count = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_kv"
            ).fetchone()
            observation_count = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_schema_observation"
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

        with target_db.ledger.exec_cursor() as cur:
            run_config_count = cur.execute(
                "SELECT COUNT(*) FROM main.run_config_kv"
            ).fetchone()
            artifact_kv_count = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_kv"
            ).fetchone()
            observation_count = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_schema_observation"
            ).fetchone()
            assert int(run_config_count[0] if run_config_count else 0) == 1
            assert int(artifact_kv_count[0] if artifact_kv_count else 0) == 1
            assert int(observation_count[0] if observation_count else 0) == 1
    finally:
        source_db.close()
        target_db.close()


def test_merge_dry_run_previews_without_writes(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_dry_run_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_dry_run_target.duckdb"))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "runs_source"
    )
    target_maintenance = DatabaseMaintenance(
        target_db, run_dir=tmp_path / "runs_target"
    )
    conflict_artifact_id = uuid.uuid4()
    new_artifact_id = uuid.uuid4()
    try:
        source_db.sync_run(_run(id="conflict_run", model_name="source_model"))
        source_db.sync_run(_run(id="new_run", model_name="source_model"))
        conflict_art = _seed_artifact(
            source_db,
            artifact_id=conflict_artifact_id,
            key="conflict_artifact",
            container_uri="outputs://conflict.parquet",
            driver="parquet",
            run_id="conflict_run",
        )
        new_art = _seed_artifact(
            source_db,
            artifact_id=new_artifact_id,
            key="new_artifact",
            container_uri="outputs://new.parquet",
            driver="parquet",
            run_id="new_run",
        )
        _link_artifact(
            source_db, run_id="conflict_run", artifact=conflict_art, direction="output"
        )
        _link_artifact(
            source_db, run_id="new_run", artifact=new_art, direction="output"
        )

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

        target_db.sync_run(_run(id="conflict_run", model_name="target_model"))

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

        assert target_db.ledger.get_run("new_run") is None
        persisted_conflict = target_db.ledger.get_run("conflict_run")
        assert persisted_conflict is not None
        assert persisted_conflict.model_name == "target_model"
        assert target_db.ledger.get_artifact(new_artifact_id) is None

        assert not (
            target_maintenance.run_dir / "consist_runs" / "new_run.json"
        ).exists()
        assert not (target_maintenance.run_dir / ".consist_audit.log").exists()
    finally:
        source_db.close()
        target_db.close()


def test_merge_tolerates_target_side_schema_drift_on_global_tables_and_v1_shards(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_drift_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_drift_target.duckdb"))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "runs_source"
    )
    target_maintenance = DatabaseMaintenance(
        target_db, run_dir=tmp_path / "runs_target"
    )
    artifact_id = uuid.uuid4()
    try:
        source_db.sync_run(
            _run(
                id="drift_run",
                model_name="source_model",
                meta={
                    "facet": {"param": "drift"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )
        art = _seed_artifact(
            source_db,
            artifact_id=artifact_id,
            key="drift_artifact",
            container_uri="outputs://drift.parquet",
            driver="parquet",
            run_id="drift_run",
            meta={
                "facet": {"k": "drift"},
                "artifact_facet_id": "facet_a",
                "artifact_facet_namespace": "demo",
            },
        )
        _link_artifact(source_db, run_id="drift_run", artifact=art, direction="output")

        with source_db.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.scoped_drift (
                    consist_run_id VARCHAR,
                    metric INTEGER
                )
                """
            )
            cur.execute(
                "INSERT INTO global_tables.scoped_drift VALUES ('drift_run', 42)"
            )

        shard_path = tmp_path / "merge_drift_shard.duckdb"
        source_maintenance.export(
            "drift_run",
            shard_path,
            include_data=True,
            include_snapshots=False,
        )

        with target_db.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.scoped_drift (
                    consist_run_id VARCHAR,
                    metric INTEGER,
                    target_extra VARCHAR
                )
                """
            )

        merge_result = target_maintenance.merge(
            shard_path,
            conflict="error",
            include_snapshots=False,
        )
        assert merge_result.runs_merged == ["drift_run"]
        assert merge_result.ingested_tables_merged == ["scoped_drift"]

        with target_db.ledger.exec_cursor() as cur:
            gt_row = cur.execute(
                """
                SELECT consist_run_id, metric, target_extra
                FROM global_tables.scoped_drift
                WHERE consist_run_id = 'drift_run'
                """
            ).fetchone()
            run_config_row = cur.execute(
                """
                SELECT run_id, value_str
                FROM main.run_config_kv
                WHERE run_id = 'drift_run'
                """
            ).fetchone()
            artifact_kv_row = cur.execute(
                """
                SELECT CAST(artifact_id AS VARCHAR), value_str
                FROM main.artifact_kv
                WHERE CAST(artifact_id AS VARCHAR) = ?
                """,
                [str(artifact_id)],
            ).fetchone()

        assert gt_row == ("drift_run", 42, None)
        assert run_config_row == ("drift_run", "drift")
        assert artifact_kv_row == (str(artifact_id), "drift")

        # Also verify merging a Schema v1 shard requires auto_migrate and succeeds when enabled
        v1_shard_path = tmp_path / "legacy_v1_shard.duckdb"
        v1_art_id = str(uuid.uuid4())
        con = duckdb.connect(str(v1_shard_path))
        try:
            create_v1_schema(con)
            insert_v1_run(con, run_id="v1_shard_run", model_name="v1_model")
            insert_v1_artifact(
                con,
                artifact_id=v1_art_id,
                key="v1_out",
                container_uri="outputs://v1.parquet",
                driver="parquet",
                hash_val=_distinct_hex("v1_shard_art"),
                run_id="v1_shard_run",
            )
            insert_v1_link(
                con,
                run_id="v1_shard_run",
                artifact_id=v1_art_id,
                direction="output",
            )
        finally:
            con.close()

        with pytest.raises(DatabaseMigrationRequired):
            target_maintenance.merge(v1_shard_path, auto_migrate=False)

        v1_merge_result = target_maintenance.merge(
            v1_shard_path,
            conflict="error",
            auto_migrate="quick",
        )
        assert v1_merge_result.runs_merged == ["v1_shard_run"]
        assert target_db.ledger.get_run("v1_shard_run") is not None
    finally:
        source_db.close()
        target_db.close()


def _snapshot_payload(
    *,
    run_id: str,
    model_name: str = "demo",
    output_artifact_ids: list[uuid.UUID] | None = None,
    facet: dict[str, Any] | None = None,
    run_meta: dict[str, Any] | None = None,
    output_artifact_meta: dict[uuid.UUID, dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
                "hash": _distinct_hex(f"{run_id}:{artifact_id}:{index}"),
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
    maintenance.db.sync_run(_run(id="existing_run", model_name="demo"))

    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "a_existing.json").write_text(
        json.dumps(_snapshot_payload(run_id="existing_run")),
        encoding="utf-8",
    )
    (json_dir / "b_new.json").write_text(
        json.dumps(
            _snapshot_payload(run_id="new_run", output_artifact_ids=[artifact_id])
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False)

    assert result.json_files_scanned == 2
    assert result.runs_inserted == 1
    assert result.runs_already_present == 1
    assert result.artifacts_inserted == 1
    assert result.errors == []
    assert result.dry_run is False
    assert maintenance.ledger.get_run("new_run") is not None
    assert maintenance.ledger.get_artifact(artifact_id) is not None
    with maintenance.ledger.exec_cursor() as cur:
        links = cur.execute(
            "SELECT run_id, artifact_id FROM main.run_binding"
        ).fetchall()
    assert {(str(r[0]), uuid.UUID(str(r[1]))) for r in links} == {
        ("new_run", artifact_id)
    }


def test_rebuild_from_json_dry_run_does_not_write(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "rebuild_dry_run_snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "dry_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="dry_run_rebuild", output_artifact_ids=[artifact_id]
            )
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=True)

    assert result.dry_run is True
    assert result.runs_inserted == 1
    assert result.runs_already_present == 0
    assert result.artifacts_inserted == 1
    assert maintenance.ledger.get_run("dry_run_rebuild") is None
    assert maintenance.ledger.get_artifact(artifact_id) is None
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
    assert maintenance.ledger.get_run("good_run") is not None


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
    with maintenance.ledger.exec_cursor() as cur:
        rows = cur.execute(
            """
            SELECT key, facet_id, namespace, value_type, value_num, value_bool, value_json
            FROM main.run_config_kv
            WHERE run_id = 'full_run'
            ORDER BY key
            """
        ).fetchall()

    assert [row[0] for row in rows] == ["alpha", "nested.flag", "nested.items"]
    assert all(row[1] == "facet_run_id" for row in rows)
    assert all(row[2] == "custom_namespace" for row in rows)
    assert rows[0][3] == "int"
    assert rows[0][4] == 1.0
    assert rows[1][3] == "bool"
    assert rows[1][5] is True
    assert rows[2][3] == "json"
    parsed_json = json.loads(rows[2][6]) if isinstance(rows[2][6], str) else rows[2][6]
    assert parsed_json == [1, 2]


def test_rebuild_from_json_restores_canonical_stage_phase(
    maintenance: DatabaseMaintenance, tmp_path: Path
) -> None:
    json_dir = tmp_path / "rebuild_stage_phase_restore"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "stage_phase_run.json").write_text(
        json.dumps(
            _snapshot_payload(
                run_id="stage_phase_run",
                run_meta={
                    "stage": "supply_demand_loop",
                    "phase": "traffic_assignment",
                },
            )
        ),
        encoding="utf-8",
    )

    result = maintenance.rebuild_from_json(json_dir, dry_run=False)

    assert result.errors == []
    restored = maintenance.ledger.get_run("stage_phase_run")

    assert restored is not None
    assert restored.stage == "supply_demand_loop"
    assert restored.phase == "traffic_assignment"
    assert restored.meta["stage"] == "supply_demand_loop"
    assert restored.meta["phase"] == "traffic_assignment"


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
    schema_and_fields = maintenance.ledger.get_artifact_schema(schema_id)
    assert schema_and_fields is not None
    schema_row, _fields = schema_and_fields
    with maintenance.ledger.exec_cursor() as cur:
        observations = cur.execute(
            """
            SELECT source, sample_rows
            FROM main.artifact_schema_observation
            WHERE run_id = 'schema_run' AND artifact_id = ? AND schema_id = ?
            """,
            [artifact_id, schema_id],
        ).fetchall()

    assert schema_row.summary_json["source"] == "file"
    assert schema_row.profile_json == {"fields": [{"name": "col_a"}]}
    assert observations == [("file", 42)]


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
    with maintenance.ledger.exec_cursor() as cur:
        run_kv_rows = cur.execute(
            "SELECT key FROM main.run_config_kv WHERE run_id = 'idempotent_run'"
        ).fetchall()
        artifact_kv_rows = cur.execute(
            "SELECT key_path FROM main.artifact_kv WHERE artifact_id = ?",
            [artifact_id],
        ).fetchall()
        observation_rows = cur.execute(
            """
            SELECT id FROM main.artifact_schema_observation
            WHERE run_id = 'idempotent_run' AND artifact_id = ?
            """,
            [artifact_id],
        ).fetchall()

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
    assert maintenance.ledger.get_run("minimal_run") is not None
    assert maintenance.ledger.get_artifact(artifact_id) is not None
    with maintenance.ledger.exec_cursor() as cur:
        assert cur.execute("SELECT COUNT(*) FROM main.run_config_kv").fetchone()[0] == 0
        assert cur.execute("SELECT COUNT(*) FROM main.artifact_kv").fetchone()[0] == 0
        assert (
            cur.execute("SELECT COUNT(*) FROM main.artifact_schema").fetchone()[0] == 0
        )
        assert (
            cur.execute(
                "SELECT COUNT(*) FROM main.artifact_schema_observation"
            ).fetchone()[0]
            == 0
        )


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
