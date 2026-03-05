from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import uuid

import pytest
from sqlalchemy.exc import OperationalError
from sqlmodel import select

from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV


def _run(**kwargs: Any) -> Run:
    kwargs.setdefault("config_hash", None)
    kwargs.setdefault("git_hash", None)
    return Run(**kwargs)


def _write_snapshot(run_dir: Path, run_id: str) -> Path:
    snapshot_path = run_dir / "consist_runs" / f"{run_id}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps({"run": {"id": run_id}}), encoding="utf-8")
    return snapshot_path


def _inject_single_lock_failure(
    monkeypatch: pytest.MonkeyPatch,
    db: DatabaseManager,
    operation_name: str,
) -> dict[str, int]:
    original_execute_with_retry = db.execute_with_retry
    state = {"calls": 0}

    def wrapped_execute_with_retry(func, *args, **kwargs):
        current_operation = kwargs.get("operation_name")
        if current_operation != operation_name:
            return original_execute_with_retry(func, *args, **kwargs)

        def flaky():
            state["calls"] += 1
            if state["calls"] == 1:
                raise OperationalError(
                    statement="BEGIN",
                    params=None,
                    orig=Exception("database is locked"),
                )
            return func()

        return original_execute_with_retry(flaky, *args, **kwargs)

    monkeypatch.setattr(db, "execute_with_retry", wrapped_execute_with_retry)
    return state


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
        with source.db.session_scope() as session:
            session.add_all(
                [
                    _run(id="root_run", model_name="demo"),
                    _run(id="child_run", model_name="demo", parent_run_id="root_run"),
                    _run(id="other_run", model_name="demo"),
                    Artifact(
                        id=root_artifact_id,
                        key="root_out",
                        container_uri="outputs://root.parquet",
                        driver="parquet",
                        run_id="root_run",
                    ),
                    Artifact(
                        id=child_artifact_id,
                        key="child_out",
                        container_uri="outputs://child.parquet",
                        driver="parquet",
                        run_id="child_run",
                    ),
                    Artifact(
                        id=other_artifact_id,
                        key="other_out",
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
                ]
            )
            session.commit()

        with source.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.scoped_e2e (consist_run_id VARCHAR)"
            )
            conn.exec_driver_sql("CREATE TABLE global_tables.link_e2e (run_id VARCHAR)")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_e2e (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.scoped_e2e (consist_run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.link_e2e (run_id)
                VALUES ('root_run'), ('child_run'), ('other_run')
                """
            )
            conn.exec_driver_sql(
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
            with shard_db.engine.begin() as conn:
                run_rows = conn.exec_driver_sql(
                    'SELECT id FROM "run" ORDER BY id'
                ).fetchall()
                scoped_rows = conn.exec_driver_sql(
                    """
                    SELECT consist_run_id
                    FROM global_tables.scoped_e2e
                    ORDER BY consist_run_id
                    """
                ).fetchall()
                link_rows = conn.exec_driver_sql(
                    "SELECT run_id FROM global_tables.link_e2e ORDER BY run_id"
                ).fetchall()
            assert [row[0] for row in run_rows] == ["child_run", "root_run"]
            assert [row[0] for row in scoped_rows] == ["child_run", "root_run"]
            assert [row[0] for row in link_rows] == ["child_run", "root_run"]
        finally:
            shard_db.engine.dispose()

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

        with target.db.session_scope() as session:
            merged_runs = session.exec(select(Run.id).order_by(Run.id)).all()
        assert [str(run_id) for run_id in merged_runs] == ["child_run", "root_run"]

        with target.db.engine.begin() as conn:
            scoped_rows = conn.exec_driver_sql(
                """
                SELECT consist_run_id
                FROM global_tables.scoped_e2e
                ORDER BY consist_run_id
                """
            ).fetchall()
            link_rows = conn.exec_driver_sql(
                "SELECT run_id FROM global_tables.link_e2e ORDER BY run_id"
            ).fetchall()
        assert [row[0] for row in scoped_rows] == ["child_run", "root_run"]
        assert [row[0] for row in link_rows] == ["child_run", "root_run"]

        assert (target.run_dir / "consist_runs" / "root_run.json").exists()
        assert (target.run_dir / "consist_runs" / "child_run.json").exists()
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_purge_e2e_dry_run_and_execute(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "purge.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_runs")

    root_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    keep_artifact_id = uuid.uuid4()

    try:
        with maintenance.db.session_scope() as session:
            session.add_all(
                [
                    _run(id="purge_root", model_name="demo"),
                    _run(
                        id="purge_child", model_name="demo", parent_run_id="purge_root"
                    ),
                    _run(id="keep_run", model_name="demo"),
                    Artifact(
                        id=root_artifact_id,
                        key="root_art",
                        container_uri="outputs://root.parquet",
                        driver="parquet",
                        run_id="purge_root",
                    ),
                    Artifact(
                        id=child_artifact_id,
                        key="child_art",
                        container_uri="outputs://child.parquet",
                        driver="parquet",
                        run_id="purge_child",
                    ),
                    Artifact(
                        id=keep_artifact_id,
                        key="keep_art",
                        container_uri="outputs://keep.parquet",
                        driver="parquet",
                        run_id="keep_run",
                    ),
                    RunArtifactLink(
                        run_id="purge_root",
                        artifact_id=root_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="purge_child",
                        artifact_id=child_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="keep_run",
                        artifact_id=keep_artifact_id,
                        direction="output",
                    ),
                ]
            )
            session.commit()

        root_snapshot = _write_snapshot(maintenance.run_dir, "purge_root")
        child_snapshot = _write_snapshot(maintenance.run_dir, "purge_child")

        with maintenance.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.scoped_purge (consist_run_id VARCHAR)"
            )
            conn.exec_driver_sql(
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
        with maintenance.db.session_scope() as session:
            assert session.get(Run, "purge_root") is not None
            assert session.get(Run, "purge_child") is not None

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

        with maintenance.db.session_scope() as session:
            assert session.get(Run, "purge_root") is None
            assert session.get(Run, "purge_child") is None
            assert session.get(Run, "keep_run") is not None
            assert session.get(Artifact, root_artifact_id) is None
            assert session.get(Artifact, child_artifact_id) is None
            assert session.get(Artifact, keep_artifact_id) is not None

        with maintenance.db.engine.begin() as conn:
            scoped_rows = conn.exec_driver_sql(
                """
                SELECT consist_run_id
                FROM global_tables.scoped_purge
                ORDER BY consist_run_id
                """
            ).fetchall()
        assert [row[0] for row in scoped_rows] == ["keep_run"]
    finally:
        db.engine.dispose()


def test_db_maintenance_purge_prune_cache_keeps_shared_hashes(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "purge_prune_cache.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_prune_cache_runs")

    try:
        with maintenance.db.session_scope() as session:
            session.add_all(
                [
                    _run(id="purge_run", model_name="demo"),
                    _run(id="keep_run", model_name="demo"),
                ]
            )
            session.commit()

        with maintenance.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.link_primary (
                    run_id VARCHAR,
                    content_hash VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.link_secondary (
                    run_id VARCHAR,
                    content_hash VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_unscoped (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.link_primary (run_id, content_hash)
                VALUES
                    ('purge_run', 'hash_drop'),
                    ('purge_run', 'hash_shared'),
                    ('purge_run', 'hash_cross_table'),
                    ('keep_run', 'hash_shared')
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.link_secondary (run_id, content_hash)
                VALUES
                    ('keep_run', 'hash_cross_table'),
                    ('keep_run', 'hash_keep_only')
                """
            )
            conn.exec_driver_sql(
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

        with maintenance.db.engine.begin() as conn:
            primary_rows = conn.exec_driver_sql(
                """
                SELECT run_id, content_hash
                FROM global_tables.link_primary
                ORDER BY run_id, content_hash
                """
            ).fetchall()
            secondary_rows = conn.exec_driver_sql(
                """
                SELECT run_id, content_hash
                FROM global_tables.link_secondary
                ORDER BY run_id, content_hash
                """
            ).fetchall()
            cache_rows = conn.exec_driver_sql(
                """
                SELECT content_hash
                FROM global_tables.cache_unscoped
                ORDER BY content_hash
                """
            ).fetchall()

        assert [(row[0], row[1]) for row in primary_rows] == [
            ("keep_run", "hash_shared")
        ]
        assert [(row[0], row[1]) for row in secondary_rows] == [
            ("keep_run", "hash_cross_table"),
            ("keep_run", "hash_keep_only"),
        ]
        assert [row[0] for row in cache_rows] == [
            "hash_cross_table",
            "hash_keep_only",
            "hash_shared",
            "hash_unused",
        ]
    finally:
        db.engine.dispose()


def test_db_maintenance_merge_conflict_error_mode_raises(tmp_path: Path) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_conflict_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_conflict_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    source_artifact_id = uuid.uuid4()

    try:
        with source.db.session_scope() as session:
            session.add(
                _run(
                    id="conflict_run",
                    model_name="source_model",
                )
            )
            session.add(
                Artifact(
                    id=source_artifact_id,
                    key="source_art",
                    container_uri="outputs://source.parquet",
                    driver="parquet",
                    run_id="conflict_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="conflict_run",
                    artifact_id=source_artifact_id,
                    direction="output",
                )
            )
            session.commit()

        shard_path = tmp_path / "merge_conflict_shard.duckdb"
        source.export(
            "conflict_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        with target.db.session_scope() as session:
            session.add(
                _run(
                    id="conflict_run",
                    model_name="target_model",
                )
            )
            session.commit()

        with pytest.raises(ValueError, match="Run ID conflicts detected"):
            target.merge(shard_path, conflict="error", include_snapshots=False)

        with target.db.session_scope() as session:
            persisted = session.get(Run, "conflict_run")
            assert persisted is not None
            assert persisted.model_name == "target_model"
            all_runs = session.exec(select(Run.id).order_by(Run.id)).all()
        assert [str(run_id) for run_id in all_runs] == ["conflict_run"]
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


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
        with source.db.session_scope() as session:
            session.add_all(
                [
                    _run(id="conflict_run", model_name="source_model"),
                    _run(id="new_run", model_name="source_model"),
                    Artifact(
                        id=conflict_artifact_id,
                        key="conflict_art",
                        container_uri="outputs://conflict.parquet",
                        driver="parquet",
                        run_id="conflict_run",
                    ),
                    Artifact(
                        id=new_artifact_id,
                        key="new_art",
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

        shard_path = tmp_path / "merge_skip_shard.duckdb"
        source.export(
            ["conflict_run", "new_run"],
            shard_path,
            include_data=False,
            include_snapshots=False,
            include_children=False,
        )

        with target.db.session_scope() as session:
            session.add(
                _run(
                    id="conflict_run",
                    model_name="target_model",
                )
            )
            session.commit()

        merge_result = target.merge(
            shard_path, conflict="skip", include_snapshots=False
        )
        assert merge_result.runs_skipped == ["conflict_run"]
        assert merge_result.runs_merged == ["new_run"]
        assert merge_result.conflicts_detected == ["conflict_run"]
        assert merge_result.artifacts_merged == 1

        with target.db.session_scope() as session:
            conflict_run = session.get(Run, "conflict_run")
            new_run = session.get(Run, "new_run")
        assert conflict_run is not None
        assert conflict_run.model_name == "target_model"
        assert new_run is not None
        assert new_run.model_name == "source_model"
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_merge_global_table_incompatible_schema_error_mode_raises(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_error_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_error_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        with source.db.session_scope() as session:
            session.add(_run(id="table_error_run", model_name="source_model"))
            session.commit()

        with source.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.link_incompat (
                    run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
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

        with target.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
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

        with target.db.session_scope() as session:
            assert session.get(Run, "table_error_run") is None
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_merge_global_table_incompatible_schema_skip_mode_skips_table(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_skip_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_skip_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        with source.db.session_scope() as session:
            session.add(_run(id="table_skip_run", model_name="source_model"))
            session.commit()

        with source.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.scoped_ok (
                    consist_run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.link_bad (
                    run_id VARCHAR,
                    payload VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.scoped_ok (consist_run_id, payload)
                VALUES ('table_skip_run', 'scoped_payload')
                """
            )
            conn.exec_driver_sql(
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

        with target.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
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

        with target.db.engine.begin() as conn:
            scoped_rows = conn.exec_driver_sql(
                """
                SELECT consist_run_id, payload
                FROM global_tables.scoped_ok
                ORDER BY consist_run_id
                """
            ).fetchall()
            bad_rows = conn.exec_driver_sql(
                "SELECT run_id, payload FROM global_tables.link_bad"
            ).fetchall()

        assert scoped_rows == [("table_skip_run", "scoped_payload")]
        assert bad_rows == []
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_merge_global_table_additive_drift_still_merges(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_table_additive_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_table_additive_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")

    try:
        with source.db.session_scope() as session:
            session.add(_run(id="table_additive_run", model_name="source_model"))
            session.commit()

        with source.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                """
                CREATE TABLE global_tables.scoped_additive (
                    consist_run_id VARCHAR,
                    source_only VARCHAR
                )
                """
            )
            conn.exec_driver_sql(
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

        with target.db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
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

        with target.db.engine.begin() as conn:
            row = conn.exec_driver_sql(
                """
                SELECT consist_run_id, target_only
                FROM global_tables.scoped_additive
                WHERE consist_run_id = 'table_additive_run'
                """
            ).fetchone()
        assert row == ("table_additive_run", None)
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_merge_schema_drift_target_extra_column_still_merges(
    tmp_path: Path,
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_drift_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_drift_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    artifact_id = uuid.uuid4()

    try:
        with source.db.session_scope() as session:
            session.add(_run(id="drift_run", model_name="source_model"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="drift_art",
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
                    value_str="drift_value",
                )
            )
            session.commit()

        shard_path = tmp_path / "merge_drift_shard.duckdb"
        source.export(
            "drift_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        with target.db.engine.begin() as conn:
            conn.exec_driver_sql(
                "ALTER TABLE run_config_kv ADD COLUMN target_extra VARCHAR"
            )

        merge_result = target.merge(
            shard_path, conflict="error", include_snapshots=False
        )
        assert merge_result.runs_merged == ["drift_run"]

        with target.db.engine.begin() as conn:
            row = conn.exec_driver_sql(
                """
                SELECT run_id, value_str, target_extra
                FROM run_config_kv
                WHERE run_id = 'drift_run'
                """
            ).fetchone()
        assert row is not None
        assert row[0] == "drift_run"
        assert row[1] == "drift_value"
        assert row[2] is None
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_merge_retries_on_transient_lock_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db = DatabaseManager(str(tmp_path / "merge_retry_source.duckdb"))
    target_db = DatabaseManager(str(tmp_path / "merge_retry_target.duckdb"))
    source = DatabaseMaintenance(source_db, run_dir=tmp_path / "source_runs")
    target = DatabaseMaintenance(target_db, run_dir=tmp_path / "target_runs")
    artifact_id = uuid.uuid4()

    try:
        with source.db.session_scope() as session:
            session.add(_run(id="retry_run", model_name="source_model"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="retry_art",
                    container_uri="outputs://retry.parquet",
                    driver="parquet",
                    run_id="retry_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="retry_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()

        shard_path = tmp_path / "merge_retry_shard.duckdb"
        source.export(
            "retry_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        original_begin = target.db.engine.begin
        begin_call_count = {"count": 0}

        def flaky_begin():
            begin_call_count["count"] += 1
            if begin_call_count["count"] == 1:
                raise OperationalError(
                    statement="BEGIN",
                    params=None,
                    orig=Exception("database is locked"),
                )
            return original_begin()

        monkeypatch.setattr(target.db.engine, "begin", flaky_begin)

        result = target.merge(shard_path, conflict="error", include_snapshots=False)
        assert result.runs_merged == ["retry_run"]
        assert begin_call_count["count"] >= 2

        with target.db.session_scope() as session:
            merged_run = session.get(Run, "retry_run")
            merged_artifact = session.get(Artifact, artifact_id)
            links = session.exec(select(RunArtifactLink)).all()
        assert merged_run is not None
        assert merged_artifact is not None
        assert {(link.run_id, link.artifact_id) for link in links} == {
            ("retry_run", artifact_id)
        }
    finally:
        source_db.engine.dispose()
        target_db.engine.dispose()


def test_db_maintenance_export_retries_on_transient_lock_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = DatabaseManager(str(tmp_path / "export_retry_source.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "source_runs")
    artifact_id = uuid.uuid4()

    try:
        with maintenance.db.session_scope() as session:
            session.add(_run(id="export_retry_run", model_name="source_model"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="retry_art",
                    container_uri="outputs://retry.parquet",
                    driver="parquet",
                    run_id="export_retry_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="export_retry_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()

        retry_state = _inject_single_lock_failure(
            monkeypatch,
            maintenance.db,
            "maintenance_export",
        )

        shard_path = tmp_path / "export_retry_shard.duckdb"
        result = maintenance.export(
            "export_retry_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )

        assert result.run_ids == ["export_retry_run"]
        assert retry_state["calls"] >= 2

        shard_db = DatabaseManager(str(shard_path))
        try:
            with shard_db.session_scope() as session:
                assert session.get(Run, "export_retry_run") is not None
        finally:
            shard_db.engine.dispose()
    finally:
        db.engine.dispose()


def test_db_maintenance_purge_retries_on_transient_lock_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = DatabaseManager(str(tmp_path / "purge_retry.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "purge_runs")
    artifact_id = uuid.uuid4()

    try:
        with maintenance.db.session_scope() as session:
            session.add(_run(id="purge_retry_run", model_name="demo"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="retry_art",
                    container_uri="outputs://retry.parquet",
                    driver="parquet",
                    run_id="purge_retry_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="purge_retry_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()

        retry_state = _inject_single_lock_failure(
            monkeypatch,
            maintenance.db,
            "maintenance_purge",
        )

        result = maintenance.purge(
            "purge_retry_run",
            include_children=False,
            delete_files=False,
            delete_ingested_data=False,
            dry_run=False,
        )
        assert result.executed is True
        assert retry_state["calls"] >= 2

        with maintenance.db.session_scope() as session:
            assert session.get(Run, "purge_retry_run") is None
    finally:
        db.engine.dispose()


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

        with maintenance.db.session_scope() as session:
            assert session.get(Run, "rebuilt_run_1") is not None
            assert session.get(Run, "rebuilt_run_2") is not None
            assert session.get(Artifact, artifact_id) is not None
            links = session.exec(select(RunArtifactLink)).all()
        assert {(row.run_id, row.artifact_id) for row in links} == {
            ("rebuilt_run_1", artifact_id)
        }
    finally:
        db.engine.dispose()


def test_db_maintenance_compact_runs_after_writes_and_purge(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "compact.duckdb"))
    maintenance = DatabaseMaintenance(db, run_dir=tmp_path / "compact_runs")
    artifact_id = uuid.uuid4()

    try:
        with maintenance.db.session_scope() as session:
            session.add(_run(id="compact_run", model_name="demo"))
            session.add(
                Artifact(
                    id=artifact_id,
                    key="compact_art",
                    container_uri="outputs://compact.parquet",
                    driver="parquet",
                    run_id="compact_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="compact_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()

        maintenance.purge(
            "compact_run",
            include_children=False,
            delete_files=False,
            delete_ingested_data=False,
            dry_run=False,
        )

        maintenance.compact()

        with maintenance.db.engine.begin() as conn:
            run_count_row = conn.exec_driver_sql(
                'SELECT COUNT(*) FROM "run"'
            ).fetchone()
        assert int(run_count_row[0] if run_count_row else 0) == 0

        audit_log = maintenance.run_dir / ".consist_audit.log"
        assert audit_log.exists()
        audit_text = audit_log.read_text(encoding="utf-8")
        assert "\tpurge\t" in audit_text
        assert "\tcompact\t" in audit_text
    finally:
        db.engine.dispose()
