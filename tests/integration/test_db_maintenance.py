from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
import uuid

import pytest

from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.digest import Digest
from consist.models.artifact import Artifact
from consist.models.run import Run


def _run(**kwargs: Any) -> Run:
    kwargs.setdefault("config_hash", None)
    kwargs.setdefault("git_hash", None)
    return Run(**kwargs)


def _write_snapshot(run_dir: Path, run_id: str) -> Path:
    snapshot_path = run_dir / "consist_runs" / f"{run_id}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps({"run": {"id": run_id}}), encoding="utf-8")
    return snapshot_path


def test_db_maintenance_export_merge_e2e_with_subtree_and_filtered_data(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    other_artifact_id = uuid.uuid4()

    try:
        source.db.sync_run(_run(id="root_run", model_name="demo"))
        source.db.sync_run(
            _run(id="child_run", model_name="demo", parent_run_id="root_run")
        )
        source.db.sync_run(_run(id="other_run", model_name="demo"))
        source.db.sync_artifact(
            Artifact(
                id=root_artifact_id,
                key="root_out",
                uri="outputs://root.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "a1" * 32),
                run_id="root_run",
            ),
            run_id="root_run",
            direction="output",
        )
        source.db.sync_artifact(
            Artifact(
                id=child_artifact_id,
                key="child_out",
                uri="outputs://child.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "b2" * 32),
                run_id="child_run",
            ),
            run_id="child_run",
            direction="output",
        )
        source.db.sync_artifact(
            Artifact(
                id=other_artifact_id,
                key="other_out",
                uri="outputs://other.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "c3" * 32),
                run_id="other_run",
            ),
            run_id="other_run",
            direction="output",
        )

        with source.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.scoped_e2e (consist_run_id VARCHAR)"
            )
            cur.execute("CREATE TABLE global_tables.link_e2e (run_id VARCHAR)")
            cur.execute("CREATE TABLE global_tables.cache_e2e (content_hash VARCHAR)")
            cur.execute(
                """
                INSERT INTO global_tables.scoped_e2e (consist_run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_e2e (run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.cache_e2e (content_hash)
                VALUES ('cache_a'), ('cache_b')
                """
            )

        _write_snapshot(source.run_dir, "root_run")
        _write_snapshot(source.run_dir, "child_run")

        shard_path = tmp_path / "exported.duckdb"
        export_result = source.export(
            "root_run",
            shard_path,
            include_data=True,
            include_snapshots=True,
        )
        assert export_result.run_ids == ["root_run", "child_run"]
        assert export_result.artifact_count == 2
        assert export_result.ingested_rows == {"link_e2e": 2, "scoped_e2e": 2}
        assert export_result.unscoped_cache_tables_skipped == ["cache_e2e"]
        assert export_result.snapshots_copied == 2

        shard_db = DatabaseManager(str(shard_path))
        try:
            run_rows = shard_db.query_rows("SELECT id FROM main.run ORDER BY id")
            scoped_rows = shard_db.query_rows(
                "SELECT consist_run_id FROM global_tables.scoped_e2e ORDER BY consist_run_id"
            )
            link_rows = shard_db.query_rows(
                "SELECT run_id FROM global_tables.link_e2e ORDER BY run_id"
            )
            assert list(run_rows) == ["child_run", "root_run"]
            assert list(scoped_rows) == ["child_run", "root_run"]
            assert list(link_rows) == ["child_run", "root_run"]
        finally:
            shard_db.close()

        shard_snapshot_dir = shard_path.parent / "shard_snapshots"
        assert (shard_snapshot_dir / "root_run.json").exists()
        assert (shard_snapshot_dir / "child_run.json").exists()

        merge_result = target.merge(
            shard_path,
            conflict="error",
            include_snapshots=True,
        )
        assert merge_result.runs_merged == ["child_run", "root_run"]
        assert merge_result.runs_skipped == []
        assert merge_result.artifacts_merged == 2
        assert merge_result.ingested_tables_merged == ["link_e2e", "scoped_e2e"]
        assert merge_result.snapshots_merged == 2

        merged_runs = target.db.query_rows("SELECT id FROM main.run ORDER BY id")
        scoped_rows = target.db.query_rows(
            "SELECT consist_run_id FROM global_tables.scoped_e2e ORDER BY consist_run_id"
        )
        link_rows = target.db.query_rows(
            "SELECT run_id FROM global_tables.link_e2e ORDER BY run_id"
        )
        assert [str(r) for r in merged_runs] == ["child_run", "root_run"]
        assert list(scoped_rows) == ["child_run", "root_run"]
        assert list(link_rows) == ["child_run", "root_run"]

        assert (target.run_dir / "consist_runs" / "root_run.json").exists()
        assert (target.run_dir / "consist_runs" / "child_run.json").exists()
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_purge_e2e_dry_run_and_execute(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "purge.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_runs")

    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    keep_artifact_id = uuid.uuid4()

    try:
        maintenance.db.sync_run(_run(id="purge_root", model_name="demo"))
        maintenance.db.sync_run(
            _run(id="purge_child", model_name="demo", parent_run_id="purge_root")
        )
        maintenance.db.sync_run(_run(id="keep_run", model_name="demo"))
        maintenance.db.sync_artifact(
            Artifact(
                id=root_artifact_id,
                key="root_art",
                uri="outputs://root.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "d1" * 32),
                run_id="purge_root",
            ),
            run_id="purge_root",
            direction="output",
        )
        maintenance.db.sync_artifact(
            Artifact(
                id=child_artifact_id,
                key="child_art",
                uri="outputs://child.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "d2" * 32),
                run_id="purge_child",
            ),
            run_id="purge_child",
            direction="output",
        )
        maintenance.db.sync_artifact(
            Artifact(
                id=keep_artifact_id,
                key="keep_art",
                uri="outputs://keep.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "d3" * 32),
                run_id="keep_run",
            ),
            run_id="keep_run",
            direction="output",
        )

        root_snapshot = _write_snapshot(maintenance.run_dir, "purge_root")
        child_snapshot = _write_snapshot(maintenance.run_dir, "purge_child")

        with maintenance.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.scoped_purge (consist_run_id VARCHAR)"
            )
            cur.execute(
                """
                INSERT INTO global_tables.scoped_purge (consist_run_id)
                VALUES ('purge_root'), ('purge_child'), ('keep_run')
                """
            )

        dry_result = maintenance.purge(
            "purge_root",
            include_children=True,
            delete_files=False,
            delete_ingested_data=True,
            dry_run=True,
        )
        assert dry_result.executed is False
        assert dry_result.plan.run_ids == ["purge_root", "purge_child"]
        assert root_snapshot.exists()
        assert child_snapshot.exists()
        assert maintenance.db.get_run("purge_root") is not None
        assert maintenance.db.get_run("purge_child") is not None

        execute_result = maintenance.purge(
            "purge_root",
            include_children=True,
            delete_files=False,
            delete_ingested_data=True,
            dry_run=False,
        )
        assert execute_result.executed is True
        assert execute_result.ingested_data_skipped is False
        assert not root_snapshot.exists()
        assert not child_snapshot.exists()

        assert maintenance.db.get_run("purge_root") is None
        assert maintenance.db.get_run("purge_child") is None
        assert maintenance.db.get_run("keep_run") is not None
        assert maintenance.db.get_artifact(root_artifact_id) is None
        assert maintenance.db.get_artifact(child_artifact_id) is None
        assert maintenance.db.get_artifact(keep_artifact_id) is not None

        scoped_rows = maintenance.db.query_rows(
            "SELECT consist_run_id FROM global_tables.scoped_purge ORDER BY consist_run_id"
        )
        assert list(scoped_rows) == ["keep_run"]
    finally:
        db.close()


def test_db_maintenance_purge_prune_cache_keeps_shared_hashes(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "purge_prune_cache.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_prune_cache_runs")

    try:
        maintenance.db.sync_run(_run(id="purge_run", model_name="demo"))
        maintenance.db.sync_run(_run(id="keep_run", model_name="demo"))

        with maintenance.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.link_primary (
                    run_id VARCHAR,
                    content_hash VARCHAR
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE global_tables.link_secondary (
                    run_id VARCHAR,
                    content_hash VARCHAR
                )
                """
            )
            cur.execute(
                "CREATE TABLE global_tables.cache_unscoped (content_hash VARCHAR)"
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_primary (run_id, content_hash)
                VALUES
                    ('purge_run', 'hash_drop'),
                    ('purge_run', 'hash_shared'),
                    ('purge_run', 'hash_cross_table'),
                    ('keep_run', 'hash_shared')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_secondary (run_id, content_hash)
                VALUES
                    ('keep_run', 'hash_cross_table'),
                    ('keep_run', 'hash_keep_only')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.cache_unscoped (content_hash)
                VALUES
                    ('hash_drop'),
                    ('hash_shared'),
                    ('hash_cross_table'),
                    ('hash_keep_only'),
                    ('hash_unused')
                """
            )

        result = maintenance.purge(
            "purge_run",
            include_children=False,
            delete_files=False,
            delete_ingested_data=True,
            prune_cache=True,
            dry_run=False,
        )
        assert result.executed is True

        primary_rows = maintenance.db.query_rows(
            "SELECT run_id, content_hash FROM global_tables.link_primary ORDER BY run_id, content_hash"
        )
        secondary_rows = maintenance.db.query_rows(
            "SELECT run_id, content_hash FROM global_tables.link_secondary ORDER BY run_id, content_hash"
        )
        cache_rows = maintenance.db.query_rows(
            "SELECT content_hash FROM global_tables.cache_unscoped ORDER BY content_hash"
        )

        assert [(row[0], row[1]) for row in primary_rows] == [
            ("keep_run", "hash_shared")
        ]
        assert [(row[0], row[1]) for row in secondary_rows] == [
            ("keep_run", "hash_cross_table"),
            ("keep_run", "hash_keep_only"),
        ]
        assert list(cache_rows) == [
            "hash_cross_table",
            "hash_keep_only",
            "hash_shared",
            "hash_unused",
        ]
    finally:
        db.close()


def test_db_maintenance_purge_rejects_unsafe_filesystem_targets(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "purge_unsafe.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_runs")

    artifact_id = uuid.uuid4()
    outside_workspace = tmp_path / "outside_workspace"
    outside_snapshot = outside_workspace / "consist_runs" / "unsafe_run.json"
    outside_file = tmp_path / "outside-output.txt"
    outside_snapshot.parent.mkdir(parents=True, exist_ok=True)
    outside_snapshot.write_text(
        json.dumps({"run": {"id": "unsafe_run"}}), encoding="utf-8"
    )
    outside_file.write_text("outside", encoding="utf-8")

    try:
        maintenance.db.sync_run(
            _run(
                id="unsafe_run",
                model_name="demo",
                meta={"_physical_run_dir": str(outside_workspace)},
            )
        )
        maintenance.db.sync_artifact(
            Artifact(
                id=artifact_id,
                key="unsafe_out",
                uri=outside_file.resolve().as_uri(),
                driver="txt",
                digest=Digest("sha256:file", "e1" * 32),
                run_id="unsafe_run",
            ),
            run_id="unsafe_run",
            direction="output",
        )

        plan = maintenance.plan_purge("unsafe_run", include_children=False)
        assert plan.json_files == []
        assert plan.disk_files == []
        assert plan.unsafe_json_files == [outside_snapshot.resolve()]
        assert plan.unsafe_disk_files == [outside_file.resolve()]

        with pytest.raises(ValueError, match="outside allowed roots"):
            maintenance.purge(
                "unsafe_run",
                include_children=False,
                delete_files=True,
                delete_ingested_data=False,
                dry_run=False,
            )

        assert outside_snapshot.exists()
        assert outside_file.exists()

        skip_result = maintenance.purge(
            "unsafe_run",
            include_children=False,
            delete_files=True,
            delete_ingested_data=False,
            dry_run=False,
            unsafe_delete_targets="skip",
        )
        assert skip_result.executed is True
        assert skip_result.skipped_unsafe_json_files == [outside_snapshot.resolve()]
        assert skip_result.skipped_unsafe_disk_files == [outside_file.resolve()]
        assert outside_snapshot.exists()
        assert outside_file.exists()
    finally:
        db.close()


def test_db_maintenance_merge_conflict_error_mode_raises(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_conflict_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_conflict_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    source_artifact_id = uuid.uuid4()

    try:
        source.db.sync_run(_run(id="conflict_run", model_name="source_model"))
        source.db.sync_artifact(
            Artifact(
                id=source_artifact_id,
                key="source_art",
                uri="outputs://source.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "f1" * 32),
                run_id="conflict_run",
            ),
            run_id="conflict_run",
            direction="output",
        )

        shard_path = tmp_path / "merge_conflict_shard.duckdb"
        source.export(
            "conflict_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        target.db.sync_run(_run(id="conflict_run", model_name="target_model"))

        with pytest.raises(ValueError, match="Run ID conflicts detected"):
            target.merge(shard_path, conflict="error", include_snapshots=False)

        persisted = target.db.get_run("conflict_run")
        assert persisted is not None
        assert persisted.model_name == "target_model"
        all_runs = target.db.query_rows("SELECT id FROM main.run ORDER BY id")
        assert [str(r) for r in all_runs] == ["conflict_run"]
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_merge_conflict_skip_mode_merges_non_conflicting_runs(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_skip_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_skip_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    conflict_artifact_id = uuid.uuid4()
    new_artifact_id = uuid.uuid4()

    try:
        source.db.sync_run(_run(id="conflict_run", model_name="source_model"))
        source.db.sync_run(_run(id="new_run", model_name="source_model"))
        source.db.sync_artifact(
            Artifact(
                id=conflict_artifact_id,
                key="conflict_art",
                uri="outputs://conflict.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "1a" * 32),
                run_id="conflict_run",
            ),
            run_id="conflict_run",
            direction="output",
        )
        source.db.sync_artifact(
            Artifact(
                id=new_artifact_id,
                key="new_art",
                uri="outputs://new.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "2b" * 32),
                run_id="new_run",
            ),
            run_id="new_run",
            direction="output",
        )

        shard_path = tmp_path / "merge_skip_shard.duckdb"
        source.export(
            ["conflict_run", "new_run"],
            shard_path,
            include_data=False,
            include_snapshots=False,
            include_children=False,
        )

        target.db.sync_run(_run(id="conflict_run", model_name="target_model"))

        merge_result = target.merge(
            shard_path, conflict="skip", include_snapshots=False
        )
        assert merge_result.runs_skipped == ["conflict_run"]
        assert merge_result.runs_merged == ["new_run"]
        assert merge_result.conflicts_detected == ["conflict_run"]
        assert merge_result.artifacts_merged == 1

        conflict_run = target.db.get_run("conflict_run")
        new_run = target.db.get_run("new_run")
        assert conflict_run is not None
        assert conflict_run.model_name == "target_model"
        assert new_run is not None
        assert new_run.model_name == "source_model"
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_merge_global_table_incompatible_schema_error_mode_raises(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_error_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_error_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        source.db.sync_run(_run(id="table_error_run", model_name="source_model"))

        with source.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.link_incompat (
                    run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_incompat (run_id, payload)
                VALUES ('table_error_run', 'source_payload')
                """
            )

        shard_path = tmp_path / "merge_table_error_shard.duckdb"
        source.export(
            "table_error_run",
            shard_path,
            include_data=True,
            include_snapshots=False,
        )

        with target.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.link_incompat (
                    run_id INTEGER,
                    payload VARCHAR
                )
                """
            )

        with pytest.raises(
            ValueError, match="link_incompat: incompatible shared column type\\(s\\)"
        ):
            target.merge(shard_path, conflict="error", include_snapshots=False)

        assert target.db.get_run("table_error_run") is None
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_merge_global_table_incompatible_schema_skip_mode_skips_table(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_skip_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_skip_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        source.db.sync_run(_run(id="table_skip_run", model_name="source_model"))

        with source.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.scoped_ok (
                    consist_run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE global_tables.link_bad (
                    run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.scoped_ok (consist_run_id, payload)
                VALUES ('table_skip_run', 'scoped_payload')
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.link_bad (run_id, payload)
                VALUES ('table_skip_run', 'link_payload')
                """
            )

        shard_path = tmp_path / "merge_table_skip_shard.duckdb"
        source.export(
            "table_skip_run",
            shard_path,
            include_data=True,
            include_snapshots=False,
        )

        with target.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.link_bad (
                    run_id INTEGER,
                    payload VARCHAR
                )
                """
            )

        merge_result = target.merge(
            shard_path, conflict="skip", include_snapshots=False
        )
        assert merge_result.runs_merged == ["table_skip_run"]
        assert merge_result.ingested_tables_merged == ["scoped_ok"]
        assert set(merge_result.incompatible_global_tables_skipped.keys()) == {
            "link_bad"
        }
        assert "run_id" in merge_result.incompatible_global_tables_skipped["link_bad"]

        scoped_rows = target.db.query_rows(
            "SELECT consist_run_id, payload FROM global_tables.scoped_ok ORDER BY consist_run_id"
        )
        bad_rows = target.db.query_rows(
            "SELECT run_id, payload FROM global_tables.link_bad"
        )

        assert scoped_rows == [("table_skip_run", "scoped_payload")]
        assert bad_rows == []
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_merge_global_table_additive_drift_still_merges(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_additive_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_additive_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        source.db.sync_run(_run(id="table_additive_run", model_name="source_model"))

        with source.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.scoped_additive (
                    consist_run_id VARCHAR,
                    source_only VARCHAR
                )
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.scoped_additive (consist_run_id, source_only)
                VALUES ('table_additive_run', 'source_value')
                """
            )

        shard_path = tmp_path / "merge_table_additive_shard.duckdb"
        source.export(
            "table_additive_run",
            shard_path,
            include_data=True,
            include_snapshots=False,
        )

        with target.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.scoped_additive (
                    consist_run_id VARCHAR,
                    target_only VARCHAR
                )
                """
            )

        merge_result = target.merge(
            shard_path, conflict="error", include_snapshots=False
        )
        assert merge_result.runs_merged == ["table_additive_run"]
        assert merge_result.ingested_tables_merged == ["scoped_additive"]
        assert merge_result.incompatible_global_tables_skipped == {}

        rows = target.db.query_rows(
            """
            SELECT consist_run_id, target_only
            FROM global_tables.scoped_additive
            WHERE consist_run_id = 'table_additive_run'
            """
        )
        assert rows == [("table_additive_run", None)]
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_merge_schema_drift_target_extra_column_still_merges(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_drift_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_drift_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    artifact_id = uuid.uuid4()

    try:
        source.db.sync_run(
            _run(
                id="drift_run",
                model_name="source_model",
                meta={
                    "facet": {"param": "drift_value"},
                    "config_facet_id": "facet_a",
                    "config_facet_namespace": "demo",
                },
            )
        )
        source.db.sync_artifact(
            Artifact(
                id=artifact_id,
                key="drift_art",
                uri="outputs://drift.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "3c" * 32),
                run_id="drift_run",
            ),
            run_id="drift_run",
            direction="output",
        )
        with source.db.transaction() as cur:
            cur.execute(
                """
                INSERT INTO main.config_facet (
                    id, namespace, schema_name, "json", created_at
                ) VALUES ('facet_a', 'demo', 'demo', '{"param": "drift_value"}', CURRENT_TIMESTAMP)
                """
            )
            cur.execute(
                """
                INSERT INTO main.run_config_kv (
                    run_id, facet_id, namespace, key, value_type, value_str, created_at
                ) VALUES ('drift_run', 'facet_a', 'demo', 'param', 'str', 'drift_value', CURRENT_TIMESTAMP)
                """
            )
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.drift_tbl (
                    consist_run_id VARCHAR,
                    value_str VARCHAR
                )
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.drift_tbl (consist_run_id, value_str)
                VALUES ('drift_run', 'drift_value')
                """
            )

        shard_path = tmp_path / "merge_drift_shard.duckdb"
        source.export(
            "drift_run",
            shard_path,
            include_data=True,
            include_snapshots=False,
        )

        with target.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.drift_tbl (
                    consist_run_id VARCHAR,
                    value_str VARCHAR
                )
                """
            )
            cur.execute(
                "ALTER TABLE global_tables.drift_tbl ADD COLUMN target_extra VARCHAR"
            )

        merge_result = target.merge(
            shard_path, conflict="error", include_snapshots=False
        )
        assert merge_result.runs_merged == ["drift_run"]

        kv_rows = target.db.query_rows(
            """
            SELECT run_id, value_str
            FROM main.run_config_kv
            WHERE run_id = 'drift_run' AND key = 'param'
            """
        )
        tbl_rows = target.db.query_rows(
            """
            SELECT consist_run_id, value_str, target_extra
            FROM global_tables.drift_tbl
            WHERE consist_run_id = 'drift_run'
            """
        )
        assert kv_rows == [("drift_run", "drift_value")]
        assert tbl_rows == [("drift_run", "drift_value", None)]
    finally:
        source_db.close()
        target_db.close()


def test_db_maintenance_export_rejects_self_path_and_aliases(tmp_path: Path) -> None:
    db_path = tmp_path / "source_self.duckdb"
    db = DatabaseManager(str(db_path))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "source_runs")

    try:
        maintenance.db.sync_run(_run(id="self_export_run", model_name="demo"))

        symlink_path = tmp_path / "source_symlink.duckdb"
        symlink_path.symlink_to(db_path)

        hardlink_path = tmp_path / "source_hardlink.duckdb"
        os.link(db_path, hardlink_path)

        for bad_target in (db_path, symlink_path, hardlink_path):
            with pytest.raises(
                ValueError, match="Refusing to overwrite active source database"
            ):
                maintenance.export(
                    "self_export_run",
                    bad_target,
                    include_data=False,
                    include_snapshots=False,
                )

        assert db_path.exists()
        assert maintenance.db.get_run("self_export_run") is not None
    finally:
        db.close()


def test_db_maintenance_rebuild_from_json_e2e_with_malformed_file(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "rebuild.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "rebuild_runs")
    artifact_id = uuid.uuid4()
    json_dir = tmp_path / "snapshots"

    try:
        json_dir.mkdir(parents=True, exist_ok=True)
        (json_dir / "01_good.json").write_text(
            json.dumps(
                {
                    "run": {"id": "rebuilt_run_1", "model_name": "demo"},
                    "inputs": [],
                    "outputs": [
                        {
                            "id": str(artifact_id),
                            "key": "out",
                            "container_uri": "outputs://out.parquet",
                            "driver": "parquet",
                        }
                    ],
                    "config": {},
                    "facet": {},
                }
            ),
            encoding="utf-8",
        )
        (json_dir / "02_bad.json").write_text("{broken-json", encoding="utf-8")
        (json_dir / "03_good.json").write_text(
            json.dumps(
                {
                    "run": {"id": "rebuilt_run_2", "model_name": "demo"},
                    "inputs": [],
                    "outputs": [],
                    "config": {},
                    "facet": {},
                }
            ),
            encoding="utf-8",
        )

        result = maintenance.rebuild_from_json(json_dir, dry_run=False)
        assert result.json_files_scanned == 3
        assert result.runs_inserted == 2
        assert result.runs_already_present == 0
        assert result.artifacts_inserted == 1
        assert len(result.errors) == 1
        assert "02_bad.json" in result.errors[0]

        assert maintenance.db.get_run("rebuilt_run_1") is not None
        assert maintenance.db.get_run("rebuilt_run_2") is not None
        assert maintenance.db.get_artifact(artifact_id) is not None
        links = maintenance.db.query_rows(
            "SELECT run_id, artifact_id FROM main.run_artifact_link"
        )
        assert {(str(row[0]), uuid.UUID(str(row[1]))) for row in links} == {
            ("rebuilt_run_1", artifact_id)
        }
    finally:
        db.close()


def test_db_maintenance_compact_runs_after_writes_and_purge(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "compact.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "compact_runs")
    artifact_id = uuid.uuid4()

    try:
        maintenance.db.sync_run(_run(id="compact_run", model_name="demo"))
        maintenance.db.sync_artifact(
            Artifact(
                id=artifact_id,
                key="compact_art",
                uri="outputs://compact.parquet",
                driver="parquet",
                digest=Digest("sha256:file", "7a" * 32),
                run_id="compact_run",
            ),
            run_id="compact_run",
            direction="output",
        )

        maintenance.purge(
            "compact_run",
            include_children=False,
            delete_files=False,
            delete_ingested_data=False,
            dry_run=False,
        )

        maintenance.compact()

        run_count = maintenance.db.query_rows("SELECT COUNT(*) FROM main.run")[0]
        assert int(run_count) == 0

        audit_log = maintenance.run_dir / ".consist_audit.log"
        assert audit_log.exists()
        audit_text = audit_log.read_text(encoding="utf-8")
        assert "\tpurge\t" in audit_text
        assert "\tcompact\t" in audit_text
    finally:
        db.close()
