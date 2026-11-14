from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from consist.core.maintenance import DatabaseMaintenance
from consist.core.migrate import migrate_database
from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker
from consist.integrations.containers import api as containers_api
from consist.integrations.containers.backends import DockerBackend
from consist.location import ArtifactLocationRow, FileStat
from consist.models.artifact import Artifact
from consist.models.run import Run, RunBindingInvocation


def _make_run(run_id: str, *, parent_run_id: str | None = None) -> Run:
    return Run(
        id=run_id,
        model_name="test_model",
        config_hash="cfg_hash",
        git_hash="git_hash",
        status="completed",
        parent_run_id=parent_run_id,
    )


def test_export_and_merge_preserve_binding_and_coupler_tables(tmp_path: Path) -> None:
    src_db_path = tmp_path / "src.duckdb"
    shard_path = tmp_path / "shard.duckdb"
    dst_db_path = tmp_path / "dst.duckdb"
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir(parents=True)

    src_db = DatabaseManager(str(src_db_path))
    now = datetime.now(timezone.utc)
    art_id = uuid.uuid4()
    binding_id = uuid.uuid4()

    art = Artifact(
        id=art_id,
        key="out_k",
        uri="workspace://out.parquet",
        driver="parquet",
        run_id="step_1",
    )

    try:
        src_db.sync_run(_make_run("scen_1"))
        src_db.sync_run(
            _make_run("step_1", parent_run_id="scen_1"),
            binding_invocation=RunBindingInvocation(
                id=binding_id,
                requested_run_id="step_1",
                execution_run_id="step_1",
                cache_source_run_id=None,
                cache_outcome="miss",
                binding_json='{"mode": "fresh"}',
                created_at=now,
            ),
        )
        src_db.sync_artifact(art)
        src_db.link_artifact_to_run(
            artifact_id=art_id,
            run_id="step_1",
            direction="output",
            role="out_k",
        )
        src_db.sync_run_coupler_snapshot(
            scenario_run_id="scen_1",
            run_id="step_1",
            phase="post_step",
            step_ordinal=1,
            artifacts={"out_k": art},
        )

        maint_src = DatabaseMaintenance(src_db, run_dir=run_dir)
        export_res = maint_src.export(
            "step_1",
            shard_path,
            include_snapshots=False,
            include_data=False,
        )
        assert "step_1" in export_res.run_ids
    finally:
        src_db.close()

    shard_db = DatabaseManager(str(shard_path), read_only=True)
    try:
        assert shard_db.fetchall(
            "SELECT id, requested_run_id FROM run_binding_invocation"
        ) == [(binding_id, "step_1")]
        assert shard_db.fetchall(
            "SELECT run_id, phase, role_key, scenario_run_id FROM run_coupler_snapshot"
        ) == [("step_1", "post_step", "out_k", "scen_1")]
    finally:
        shard_db.close()

    dst_db = DatabaseManager(str(dst_db_path))
    try:
        maint_dst = DatabaseMaintenance(dst_db, run_dir=run_dir)
        maint_dst.merge(shard_path, conflict="skip", include_snapshots=False)
        # Re-merging the same shard should be idempotent via ON CONFLICT DO NOTHING.
        maint_dst.merge(shard_path, conflict="skip", include_snapshots=False)

        assert dst_db.fetchall(
            "SELECT id, requested_run_id, cache_outcome FROM run_binding_invocation"
        ) == [(binding_id, "step_1", "miss")]
        assert dst_db.fetchall(
            "SELECT run_id, phase, role_key, scenario_run_id FROM run_coupler_snapshot"
        ) == [("step_1", "post_step", "out_k", "scen_1")]
    finally:
        dst_db.close()


def test_migrate_aborted_transaction_preserves_original_error(tmp_path: Path) -> None:
    src_path = tmp_path / "v04.duckdb"
    con = duckdb.connect(str(src_path))
    try:
        con.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY)")
    finally:
        con.close()

    with patch(
        "consist.core.migrate._copy_v04_runs_sql",
        side_effect=duckdb.InvalidInputException("boom inside migration"),
    ):
        with pytest.raises(duckdb.InvalidInputException, match="boom inside migration"):
            migrate_database(src_path)


def test_migrate_null_facet_json_and_missing_file_stale(tmp_path: Path) -> None:
    run_dir = tmp_path / "workspace"
    run_dir.mkdir(parents=True)
    src_path = run_dir / "v04.duckdb"

    con = duckdb.connect(str(src_path))
    try:
        con.execute(
            """
            CREATE TABLE run (
                id VARCHAR PRIMARY KEY,
                model_name VARCHAR,
                config_hash VARCHAR,
                git_hash VARCHAR,
                status VARCHAR,
                meta JSON
            )
            """
        )
        con.execute("INSERT INTO run VALUES ('r1', 'm', 'ch', 'gh', 'completed', '{}')")
        con.execute(
            """
            CREATE TABLE artifact (
                id UUID PRIMARY KEY,
                key VARCHAR,
                container_uri VARCHAR,
                driver VARCHAR,
                hash VARCHAR,
                run_id VARCHAR,
                meta JSON
            )
            """
        )
        art_id = uuid.uuid4()
        con.execute(
            "INSERT INTO artifact VALUES (?, 'missing_k', 'workspace://missing.parquet', 'parquet', NULL, 'r1', '{}')",
            [art_id],
        )
        con.execute(
            "CREATE TABLE run_artifact_link (run_id VARCHAR, artifact_id UUID, direction VARCHAR)"
        )
        con.execute(
            "INSERT INTO run_artifact_link VALUES ('r1', ?, 'output')", [art_id]
        )
        con.execute(
            """
            CREATE TABLE config_facet (
                id VARCHAR PRIMARY KEY,
                namespace VARCHAR NOT NULL,
                schema_name VARCHAR NOT NULL,
                schema_version VARCHAR,
                facet_json JSON,
                created_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        con.execute(
            "INSERT INTO config_facet VALUES ('cf1', 'ns', 'Schema', '1', NULL, CURRENT_TIMESTAMP)"
        )
        con.execute(
            """
            CREATE TABLE artifact_facet (
                id VARCHAR PRIMARY KEY,
                namespace VARCHAR,
                schema_name VARCHAR,
                schema_version VARCHAR,
                facet_json JSON,
                created_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        con.execute(
            "INSERT INTO artifact_facet VALUES ('af1', 'ns', 'ArtSchema', '1', NULL, CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    migrate_database(src_path, run_dir=run_dir)

    dst_db = DatabaseManager(str(src_path), read_only=True)
    try:
        assert dst_db.fetchall('SELECT id, "json" FROM config_facet') == [("cf1", "{}")]
        assert dst_db.fetchall('SELECT id, "json" FROM artifact_facet') == [
            ("af1", "{}")
        ]
        loc_rows = dst_db.fetchall(
            "SELECT site, status, witness_json FROM artifact_location"
        )
        assert loc_rows == [("workspace", "stale", None)]
    finally:
        dst_db.close()


def test_rebuild_from_json_marks_missing_file_stale(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir(parents=True)
    db_path = tmp_path / "rebuilt.duckdb"

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    missing_file = run_dir / "outputs" / "gone.csv"
    missing_file.parent.mkdir(parents=True, exist_ok=True)
    missing_file.write_text("a,b\n1,2\n")

    with tracker.start_run("r_rebuild", model="m"):
        tracker.log_artifact(missing_file, key="gone_csv", direction="output")

    # Remove the file on disk before rebuilding from JSON.
    missing_file.unlink()
    if tracker.db is not None:
        tracker.db.close()
    db_path.unlink()

    rebuilt_db = DatabaseManager(str(db_path))
    try:
        maint = DatabaseMaintenance(rebuilt_db, run_dir=run_dir)
        maint.rebuild_from_json(json_dir=run_dir, dry_run=False)
        loc_rows = rebuilt_db.fetchall(
            "SELECT site, status FROM artifact_location WHERE uri LIKE '%gone.csv'"
        )
        assert loc_rows == [("workspace", "stale")]
    finally:
        rebuilt_db.close()


def test_recovery_marks_missing_witnessless_location_stale(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir(parents=True)
    db_path = tmp_path / "recovery.duckdb"
    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))

    try:
        assert tracker.db is not None
        art = Artifact(
            id=uuid.uuid4(),
            key="missing_item",
            uri="workspace://outputs/never_there.txt",
            driver="txt",
            digest="sha256:file:" + ("a" * 64),
            run_id="r1",
        )
        tracker.db.sync_run(_make_run("r1"))
        tracker.db.sync_artifact(art)

        m_name, m_inst, canonical_uri = tracker.fs.location_coordinate_for_uri(art.uri)
        loc_row = ArtifactLocationRow.create(
            digest=str(art.digest),
            artifact_id=art.id,
            site="workspace",
            mount_name=m_name,
            mount_instance_id=m_inst,
            uri=canonical_uri,
            member="",
            status="active",
            witness=None,
            observed_at_ns=1_000,
        )
        tracker.db.upsert_artifact_locations([loc_row])

        resolved = tracker.resolve_artifact_path(art)
        assert not resolved.exists()
        status_rows = tracker.db.fetchall(
            "SELECT status FROM artifact_location WHERE location_id = ?",
            [loc_row.location_id],
        )
        assert status_rows == [("stale",)]
    finally:
        if tracker.db is not None:
            tracker.db.close()


def test_container_overwrite_stales_child_member_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "container_overwrite.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        old_witness = FileStat(size=100, mtime_ns=1_000, ctime_ns=1_000, ino=1)
        new_witness = FileStat(size=200, mtime_ns=2_000, ctime_ns=2_000, ino=2)
        container_uri = "workspace://outputs/archive.h5"

        old_container = ArtifactLocationRow.create(
            digest="sha256:file:" + ("1" * 64),
            artifact_id=uuid.uuid4(),
            site="workspace",
            mount_name="workspace",
            mount_instance_id="inst1",
            uri=container_uri,
            member="",
            status="active",
            witness=old_witness,
            observed_at_ns=10,
        )
        stale_child = ArtifactLocationRow.create(
            digest="sha256:file:" + ("2" * 64),
            artifact_id=uuid.uuid4(),
            site="workspace",
            mount_name="workspace",
            mount_instance_id="inst1",
            uri=container_uri,
            member="group/old_ds",
            status="active",
            witness=old_witness,
            observed_at_ns=10,
        )
        matching_child = ArtifactLocationRow.create(
            digest="sha256:file:" + ("3" * 64),
            artifact_id=uuid.uuid4(),
            site="workspace",
            mount_name="workspace",
            mount_instance_id="inst1",
            uri=container_uri,
            member="group/matching_ds",
            status="active",
            witness=new_witness,
            observed_at_ns=15,
        )
        db.upsert_artifact_locations([old_container, stale_child, matching_child])

        new_container = ArtifactLocationRow.create(
            digest="sha256:file:" + ("4" * 64),
            artifact_id=uuid.uuid4(),
            site="workspace",
            mount_name="workspace",
            mount_instance_id="inst1",
            uri=container_uri,
            member="",
            status="active",
            witness=new_witness,
            observed_at_ns=20,
        )
        db.upsert_artifact_locations([new_container])

        rows = {
            row[0]: row[1]
            for row in db.fetchall("SELECT location_id, status FROM artifact_location")
        }
        assert rows[old_container.location_id] == "stale"
        assert rows[stale_child.location_id] == "stale"
        assert rows[matching_child.location_id] == "active"
        assert rows[new_container.location_id] == "active"
    finally:
        db.close()


def test_container_cache_hit_uses_bulk_linking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir(parents=True)
    db_path = tmp_path / "containers.duckdb"
    in_file = run_dir / "input.txt"
    out_file = run_dir / "output.txt"
    in_file.write_text("hello\n")

    exec_counter = {"count": 0}

    def fake_init(self, pull_latest: bool = False) -> None:
        self.pull_latest = pull_latest
        self.client = None

    def fake_resolve(self, image: str) -> str:
        return "sha256:dummy"

    def fake_run(
        self, image, command, volumes, env, working_dir=None, volume_modes=None
    ) -> bool:
        exec_counter["count"] += 1
        out_file.write_text("container-output\n")
        return True

    monkeypatch.setattr(DockerBackend, "__init__", fake_init, raising=False)
    monkeypatch.setattr(
        DockerBackend, "resolve_image_digest", fake_resolve, raising=False
    )
    monkeypatch.setattr(DockerBackend, "run", fake_run, raising=False)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))

    try:
        res1 = containers_api.run_container(
            tracker=tracker,
            run_id="c_run_1",
            image="alpine:3.19",
            command=["echo", "hi"],
            volumes={},
            inputs=[in_file],
            outputs=[out_file],
            strict_mounts=False,
        )
        assert res1.cache_hit is False
        assert exec_counter["count"] == 1

        assert tracker.db is not None
        with (
            patch.object(
                tracker.db,
                "link_artifacts_to_run_bulk",
                wraps=tracker.db.link_artifacts_to_run_bulk,
            ) as bulk_spy,
            patch.object(
                tracker.db,
                "link_artifact_to_run",
                wraps=tracker.db.link_artifact_to_run,
            ) as single_spy,
        ):
            res2 = containers_api.run_container(
                tracker=tracker,
                run_id="c_run_2",
                image="alpine:3.19",
                command=["echo", "hi"],
                volumes={},
                inputs=[in_file],
                outputs=[out_file],
                strict_mounts=False,
            )

        assert res2.cache_hit is True
        assert exec_counter["count"] == 1
        assert bulk_spy.call_count >= 1
        assert single_spy.call_count == 0

        links_1 = set(
            tracker.db.fetchall(
                "SELECT artifact_id, direction, role, is_implicit "
                "FROM run_artifact_link WHERE run_id = 'c_run_1'"
            )
        )
        links_2 = set(
            tracker.db.fetchall(
                "SELECT artifact_id, direction, role, is_implicit "
                "FROM run_artifact_link WHERE run_id = 'c_run_2'"
            )
        )
        assert links_1.issubset(links_2)
        assert len(links_1) >= 2
    finally:
        if tracker.db is not None:
            tracker.db.close()
