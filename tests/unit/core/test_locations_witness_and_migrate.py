from __future__ import annotations

import ast
from datetime import datetime, timezone
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
import uuid

import duckdb
import h5py
import pandas as pd
import pytest
from sqlmodel import Field, SQLModel
from typer.testing import CliRunner

import consist
from consist.cli import app
from consist.core import tracker_recovery
from consist.core.identity import IdentityManager
from consist.core.maintenance import DatabaseMaintenance
from consist.core.materialize import materialize_artifacts
from consist.core.migrate import migrate_database, undo_migrate_database
from consist.core.persistence import DatabaseMigrationRequired
from consist.core.tracker import Tracker
from consist.digest import Digest, file_sha256
from consist.location import (
    ArtifactLocationRow,
    FileStat,
    compute_location_id,
)
from consist.types import CacheOptions, ExecutionOptions, OutputSet


class _IngestedMetricRow(SQLModel, table=True):
    __tablename__ = "evict_ingested_metrics"

    metric_id: int = Field(primary_key=True)
    score: float


def _hot_migration_step(raw: pd.DataFrame) -> pd.DataFrame:
    out = raw.copy()
    out["doubled"] = out["val"] * 2
    return out


def _write_shapefile_bundle(shp_path: Path, *, value_offset: int = 0) -> list[Path]:
    shp_path.parent.mkdir(parents=True, exist_ok=True)
    for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        sidecar = shp_path.with_suffix(ext)
        sidecar.write_bytes(f"sidecar-{ext}-offset-{value_offset}\n".encode("utf-8"))
    return sorted(shp_path.parent.glob(f"{shp_path.stem}.*"))


def test_safe_eviction_and_archive_move_gates(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        # 1. Refuses eviction when neither verified archive nor ingested table exists
        unbacked_path = run_dir / "unbacked.parquet"
        run_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"x": [1, 2]}).to_parquet(unbacked_path)
        with tracker.start_run("run_unbacked", model="evict_demo"):
            art_unbacked = tracker.log_output(unbacked_path, key="unbacked")

        with pytest.raises(ValueError, match="surviving verified archive"):
            tracker.evict_artifact(art_unbacked)
        assert unbacked_path.exists()

        # 2. Refuses eviction when user modified scratch bytes after logging (even with archive)
        edited_path = run_dir / "edited.parquet"
        pd.DataFrame({"x": [10, 20]}).to_parquet(edited_path)
        with tracker.start_run("run_edited", model="evict_demo"):
            art_edited = tracker.log_output(edited_path, key="edited")
        tracker.archive_artifact(art_edited, archive_dir, mode="copy")

        # User edits scratch file after logging
        pd.DataFrame({"x": [999, 888]}).to_parquet(edited_path)
        with pytest.raises(ValueError, match="modified after logging"):
            consist.evict_artifact(art_edited, tracker=tracker)
        assert edited_path.exists()
        # User's edited scratch bytes remain untouched on disk
        assert pd.read_parquet(edited_path)["x"].tolist() == [999, 888]

        # 3. Succeeds when verified archive exists and scratch matches digest
        backed_path = run_dir / "backed.parquet"
        pd.DataFrame({"x": [7, 8, 9]}).to_parquet(backed_path)
        with tracker.start_run("run_backed", model="evict_demo"):
            art_backed = tracker.log_output(backed_path, key="backed")
        tracker.archive_artifact(art_backed, archive_dir, mode="copy")

        evicted = consist.evict_artifact(art_backed, tracker=tracker)
        assert evicted == backed_path.resolve()
        assert not backed_path.exists()
        assert tracker.db is not None
        ws_statuses = tracker.db.fetchall(
            "SELECT status FROM artifact_location WHERE artifact_id = ? AND site = 'workspace'",
            [art_backed.id],
        )
        assert [r[0] for r in ws_statuses] == ["evicted"]
        # art.path and consist.load resolve seamlessly from archive
        assert art_backed.path == (archive_dir / "backed.parquet").resolve()
        loaded_backed = consist.load_df(art_backed, tracker=tracker)
        assert isinstance(loaded_backed, pd.DataFrame)
        assert loaded_backed["x"].tolist() == [7, 8, 9]

        # 4. Succeeds when hot global_tables ingestion exists without file archive
        ingested_path = run_dir / "ingested.parquet"
        pd.DataFrame({"metric_id": [1, 2], "score": [3.5, 4.5]}).to_parquet(
            ingested_path
        )
        with tracker.start_run("run_ingested", model="evict_demo"):
            art_ingested = tracker.log_output(ingested_path, key="ingested")
            tracker.ingest(art_ingested, schema=_IngestedMetricRow)

        evicted_ing = tracker.evict_artifact(art_ingested)
        assert evicted_ing == ingested_path.resolve()
        assert not ingested_path.exists()
        loaded_ing = consist.load_df(
            art_ingested, tracker=tracker, db_fallback="always"
        )
        assert isinstance(loaded_ing, pd.DataFrame)
        assert sorted(loaded_ing["score"].tolist()) == [3.5, 4.5]

        # 5. archive_run_outputs(..., mode="move") handles regular + OutputSet artifacts
        def _produce_bundle(ctx: consist.RunContext) -> None:
            single_p = ctx.run_dir / "single_move.parquet"
            pd.DataFrame({"k": [42]}).to_parquet(single_p)
            bundle_root = ctx.run_dir / "bundle_out"
            (bundle_root / "sub").mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"m": [11]}).to_parquet(bundle_root / "m1.parquet")
            pd.DataFrame({"m": [22]}).to_parquet(bundle_root / "sub" / "m2.parquet")

        move_archive = tmp_path / "move_archive"
        move_res = tracker.run(
            fn=_produce_bundle,
            name="produce_move_bundle",
            output_paths={"single": "single_move.parquet"},
            output_sets={
                "bundle": OutputSet(root="bundle_out", include="**/*.parquet")
            },
            execution_options=ExecutionOptions(inject_context="ctx"),
            cache_options=CacheOptions(code_identity="callable_module"),
        )
        single_src = move_res.outputs["single"].path
        bundle_src = move_res.outputs["bundle"].path
        assert single_src.exists()
        assert (bundle_src / "m1.parquet").exists()

        archived_move = tracker.archive_run_outputs(
            move_res.run.id, move_archive, mode="move"
        )

        assert not single_src.exists()
        assert not (bundle_src / "m1.parquet").exists()
        assert move_res.outputs["single"].path == archived_move["single"].resolve()
        bundle_art = move_res.outputs["bundle"]
        assert bundle_art.path == archived_move["bundle"].resolve()
        member_rows = tracker.db.fetchall(
            "SELECT site, status FROM artifact_location "
            "WHERE artifact_id IN (SELECT id FROM artifact WHERE parent_artifact_id = ?)",
            [bundle_art.id],
        )
        assert ("workspace", "evicted") in member_rows
        assert ("archive", "active") in member_rows
    finally:
        tracker.close()


def test_overwritten_scratch_falls_back_to_verified_archive(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    stage_dir = tmp_path / "restaged"
    db_path = tmp_path / "prov.duckdb"

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        scratch_file = run_dir / "clean.parquet"
        run_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"v": [100, 200, 300]}).to_parquet(scratch_file)

        with tracker.start_run("run_clean", model="corrupt_guard"):
            art = tracker.log_output(scratch_file, key="clean")
        assert "recovery_roots" not in art.meta

        tracker.archive_artifact(art, archive_dir, mode="copy")
        assert "recovery_roots" not in art.meta
        assert art.recovery_roots == [str(archive_dir.resolve())]

        # Overwrite scratch file with corrupt/impostor Parquet content!
        pd.DataFrame({"v": [-999, -888]}).to_parquet(scratch_file)

        expected_archive_file = (archive_dir / "clean.parquet").resolve()
        # Every resolution surface must reject corrupt scratch and return verified archive
        assert art.path == expected_archive_file
        assert art.as_path() == expected_archive_file

        acc = art.accessibility()
        assert acc.status == "recovery"
        assert acc.locations[0].exists is False
        assert acc.locations[1].exists is True
        assert acc.locations[1].path == expected_archive_file

        loaded = consist.load_df(art, tracker=tracker)
        assert isinstance(loaded, pd.DataFrame)
        assert loaded["v"].tolist() == [100, 200, 300]

        staged = art.ensure_local_path(target_root=stage_dir, mutable=True)
        assert staged is not None
        assert pd.read_parquet(staged)["v"].tolist() == [100, 200, 300]

        assert tracker.db is not None
        ws_status = tracker.db.fetchone(
            "SELECT status FROM artifact_location WHERE artifact_id = ? AND site = 'workspace'",
            [art.id],
        )
        assert ws_status is not None and ws_status[0] == "stale"
    finally:
        tracker.close()


def test_multi_run_dir_workspace_instance_isolation(tmp_path: Path) -> None:
    shared_db = tmp_path / "shared.duckdb"
    dir_a = tmp_path / "ws_a"
    dir_b = tmp_path / "ws_b"
    dir_a.mkdir()
    dir_b.mkdir()

    tracker_a = Tracker(run_dir=dir_a, db_path=str(shared_db))
    tracker_b = Tracker(run_dir=dir_b, db_path=str(shared_db))
    try:
        file_a = dir_a / "result.parquet"
        file_b = dir_b / "result.parquet"
        pd.DataFrame({"src": ["A"], "n": [1]}).to_parquet(file_a)
        pd.DataFrame({"src": ["B"], "n": [2]}).to_parquet(file_b)

        with tracker_a.start_run("run_a", model="iso"):
            art_a = tracker_a.log_output(file_a, key="result")

        with tracker_b.start_run("run_b", model="iso", cache_mode="overwrite"):
            art_b = tracker_b.log_output(file_b, key="result")

        assert art_a.uri == art_b.uri
        assert art_a.digest != art_b.digest

        assert tracker_a.db is not None
        rows = tracker_a.db.fetchall(
            "SELECT artifact_id, mount_name, mount_instance_id, status FROM artifact_location"
        )
        assert len(rows) == 2
        inst_ids = {str(r[2]) for r in rows}
        assert len(inst_ids) == 2
        assert all(len(i) == 16 for i in inst_ids)
        assert all(r[3] == "active" for r in rows)

        assert art_a.path == file_a.resolve()
        assert art_b.path == file_b.resolve()
        df_a = consist.load_df(art_a, tracker=tracker_a)
        df_b = consist.load_df(art_b, tracker=tracker_b)
        assert isinstance(df_a, pd.DataFrame) and df_a["src"].tolist() == ["A"]
        assert isinstance(df_b, pd.DataFrame) and df_b["src"].tolist() == ["B"]
    finally:
        tracker_a.close()
        tracker_b.close()


def test_filestat_now_ns_racy_aging_writable_and_readonly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "runs"
    db_path = tmp_path / "racy.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_path = run_dir / "fresh_racy.parquet"
    pd.DataFrame({"z": [1, 2, 3]}).to_parquet(data_path)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        with tracker.start_run("run_racy", model="racy_demo"):
            # Force mtime right at current wall time inside start_run so slow DB init never ages past 2s
            now_wall_ns = time.time_ns()
            os.utime(data_path, ns=(now_wall_ns, now_wall_ns))
            art = tracker.log_output(data_path, key="fresh_racy")

        assert tracker.db is not None
        loc_rows = tracker.db.get_active_artifact_locations(
            artifact_id=art.id, digest=art.digest
        )
        assert len(loc_rows) == 1
        assert loc_rows[0].witness is not None
        assert loc_rows[0].witness.racy is True

        # Snapshot DB copy BEFORE writable settling so we can test read_only=True settling too!
        ro_db_path = tmp_path / "racy_ro_copy.duckdb"
        tracker.db.execute("CHECKPOINT")
        ro_db_path.write_bytes(db_path.read_bytes())

        verify_calls = 0
        orig_verify = tracker_recovery.verify_digest

        def _counted_verify(*args: object, **kwargs: object) -> bool:
            nonlocal verify_calls
            verify_calls += 1
            return orig_verify(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(tracker_recovery, "verify_digest", _counted_verify)

        future_ns = data_path.stat().st_mtime_ns + 3_000_000_000
        # First call at +3s verifies digest ONCE and settles witness_json in DB
        resolved = tracker.resolve_artifact_path(art, now_ns=future_ns)
        assert resolved == data_path.resolve()
        assert verify_calls == 1

        persisted_after = tracker.db.get_active_artifact_locations(
            artifact_id=art.id, digest=art.digest
        )
        assert persisted_after[0].witness is not None
        assert persisted_after[0].witness.racy is False

        # 100 subsequent calls execute 0 digest verifications
        for _ in range(100):
            assert (
                tracker.resolve_artifact_path(art, now_ns=future_ns)
                == data_path.resolve()
            )
        assert verify_calls == 1
    finally:
        tracker.close()

    # Now test read_only=True Tracker against the still-racy snapshot DB!
    ro_tracker = Tracker(run_dir=run_dir, db_path=str(ro_db_path), read_only=True)
    try:
        ro_art = ro_tracker.get_artifact(str(art.id))
        assert ro_art is not None
        ro_calls = 0
        orig_verify_ro = tracker_recovery.verify_digest

        def _counted_ro_verify(*args: object, **kwargs: object) -> bool:
            nonlocal ro_calls
            ro_calls += 1
            return orig_verify_ro(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(tracker_recovery, "verify_digest", _counted_ro_verify)
        future_ns = data_path.stat().st_mtime_ns + 3_000_000_000
        assert (
            ro_tracker.resolve_artifact_path(ro_art, now_ns=future_ns)
            == data_path.resolve()
        )
        assert ro_calls == 1
        for _ in range(100):
            assert (
                ro_tracker.resolve_artifact_path(ro_art, now_ns=future_ns)
                == data_path.resolve()
            )
        assert ro_calls == 1
    finally:
        ro_tracker.close()


def test_multimember_hdf5_and_symlinked_shapefile_sidecar_witnesses(
    tmp_path: Path,
) -> None:
    real_base = tmp_path / "real_storage"
    real_base.mkdir()
    sym_base = tmp_path / "sym_tmp"
    sym_base.symlink_to(real_base, target_is_directory=True)

    run_dir = sym_base / "runs"
    archive_dir = sym_base / "archive"
    stage_dir = sym_base / "staged_shp"
    db_path = sym_base / "prov.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        # Part A: Multi-member HDF5 container witness refresh
        h5_path = run_dir / "shared.h5"
        with h5py.File(h5_path, "w") as hf:
            hf.create_dataset("/a", data=[10, 20, 30])

        with tracker.start_run("h5_step_a", model="h5_demo"):
            art_a = tracker.log_h5_table(h5_path, table_path="/a", key="table_a")

        assert tracker.db is not None
        before_loc = tracker.db.get_active_artifact_locations(
            artifact_id=art_a.id, digest=art_a.digest, member="/a"
        )[0]
        assert isinstance(before_loc.witness, FileStat)
        before_size = before_loc.witness.size

        # Append sibling dataset /b -> alters FileStat of shared.h5 without changing /a!
        with h5py.File(h5_path, "a") as hf:
            hf.create_dataset("/b", data=list(range(5000)))
        future_ns = h5_path.stat().st_mtime_ns + 5_000_000_000

        resolved_a = tracker.resolve_artifact_path(art_a, now_ns=future_ns)
        assert resolved_a == h5_path.resolve()
        after_loc = tracker.db.get_active_artifact_locations(
            artifact_id=art_a.id, digest=art_a.digest, member="/a"
        )[0]
        assert after_loc.status == "active"
        assert isinstance(after_loc.witness, FileStat)
        assert after_loc.witness.size > before_size
        assert after_loc.witness.racy is False

        # Actually mutating /a marks /a's location stale!
        with h5py.File(h5_path, "a") as hf:
            del hf["/a"]
            hf.create_dataset("/a", data=[999, 888, 777])
        with pytest.raises(FileNotFoundError):
            _ = art_a.path
        stale_rows = tracker.db.fetchall(
            "SELECT status FROM artifact_location WHERE artifact_id = ?",
            [art_a.id],
        )
        assert [r[0] for r in stale_rows] == ["stale"]

        # Part B: Shapefile sidecar bundle DirTreeStat witness under symlinked path
        shp_path = run_dir / "zones" / "tracts.shp"
        sidecars = _write_shapefile_bundle(shp_path, value_offset=0)
        assert len(sidecars) >= 5

        with tracker.start_run("shp_step", model="shp_demo"):
            art_shp = tracker.log_output(shp_path, key="tracts", driver="shapefile")

        archived_shp_bundle = tracker.archive_artifact(
            art_shp, archive_dir, mode="copy"
        )

        # Corrupt ONLY the .dbf sidecar in scratch (leaving .shp untouched!)
        dbf_scratch = shp_path.with_suffix(".dbf")
        dbf_scratch.write_bytes(b"CORRUPT_DBF_BYTES")

        # Witness check immediately catches .dbf sidecar corruption, marks scratch stale,
        # and resolves to the verified archive .shp!
        expected_arch_shp = (archived_shp_bundle / "tracts.shp").resolve()
        assert art_shp.path == expected_arch_shp
        assert expected_arch_shp.with_suffix(".dbf").read_bytes() == (
            b"sidecar-.dbf-offset-0\n"
        )

        # Atomic restaging restores ALL shapefile sidecars (.shp, .shx, .dbf, .prj, .cpg)
        staged_shp = art_shp.ensure_local_path(target_root=stage_dir, mutable=True)
        assert staged_shp is not None
        for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
            sidecar_file = staged_shp.with_suffix(suffix)
            assert sidecar_file.is_file()
            assert sidecar_file.read_bytes() == f"sidecar-{suffix}-offset-0\n".encode(
                "utf-8"
            )

        # Also test sidecar deletion detection on a second shapefile
        shp2_path = run_dir / "zones2" / "links.shp"
        _write_shapefile_bundle(shp2_path, value_offset=50)
        with tracker.start_run("shp_step_2", model="shp_demo"):
            art_shp2 = tracker.log_output(shp2_path, key="links", driver="shapefile")
        archived_shp2 = tracker.archive_artifact(art_shp2, archive_dir, mode="copy")
        shp2_path.with_suffix(".dbf").unlink()
        assert art_shp2.path == (archived_shp2 / "links.shp").resolve()
    finally:
        tracker.close()


def _build_synthetic_v04_database(
    db_path: Path,
    *,
    hot_run_dir: Path,
    hot_archive_dir: Path,
    cold_dir: Path,
) -> dict[str, Any]:
    hot_run_dir.mkdir(parents=True, exist_ok=True)
    hot_archive_dir.mkdir(parents=True, exist_ok=True)
    cold_dir.mkdir(parents=True, exist_ok=True)

    hot_in_path = hot_run_dir / "raw_in.parquet"
    hot_out_path = hot_run_dir / "hot_out.parquet"
    hot_arch_copy = hot_archive_dir / "hot_out.parquet"
    fast_path = hot_run_dir / "fast_obs.parquet"
    cold_path = cold_dir / "cold_trips.parquet"

    raw_df = pd.DataFrame({"val": [5, 10, 15]})
    out_df = _hot_migration_step(raw_df)
    raw_df.to_parquet(hot_in_path)
    out_df.to_parquet(hot_out_path)
    out_df.to_parquet(hot_arch_copy)
    pd.DataFrame({"f": [1, 2]}).to_parquet(fast_path)
    pd.DataFrame({"trip_id": [101, 102], "dist": [4.2, 8.4]}).to_parquet(cold_path)

    # Age mtime > 3s so migrated witnesses settle racy=False immediately
    aged_ns = time.time_ns() - 10_000_000_000
    for p in (hot_in_path, hot_out_path, hot_arch_copy, fast_path, cold_path):
        os.utime(p, ns=(aged_ns, aged_ns))

    hot_in_sha = file_sha256(hot_in_path)
    hot_out_sha = file_sha256(hot_out_path)
    cold_sha = file_sha256(cold_path)

    ident = IdentityManager(project_root=str(hot_run_dir))
    git_hash = ident.resolve_code_version(
        mode="callable_module", func=_hot_migration_step
    )
    config_hash = ident.compute_run_config_hash(
        config={"scale": 2},
        model="_hot_migration_step",
        year=None,
        iteration=None,
        cache_epoch=1,
    )

    hot_in_id = uuid.uuid4()
    hot_out_id = uuid.uuid4()
    fast_art_id = uuid.uuid4()
    cold_art_id = uuid.uuid4()
    now_ts = datetime.now(timezone.utc)

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "CREATE TABLE consist_schema ("
            "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR)"
        )
        con.execute("INSERT INTO consist_schema VALUES (1, ?, '0.4.2')", [now_ts])

        con.execute(
            "CREATE TABLE run ("
            "id VARCHAR PRIMARY KEY, parent_run_id VARCHAR, status VARCHAR, "
            "model_name VARCHAR, description VARCHAR, year INTEGER, iteration INTEGER, "
            "tags JSON, config_hash VARCHAR, git_hash VARCHAR, input_hash VARCHAR, "
            "signature VARCHAR, facet_id VARCHAR, meta JSON, "
            "started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, "
            "created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)"
        )
        con.execute(
            "CREATE TABLE artifact ("
            "id UUID PRIMARY KEY, key VARCHAR, container_uri VARCHAR, driver VARCHAR, "
            "table_path VARCHAR, array_path VARCHAR, hash VARCHAR, content_id UUID, "
            "run_id VARCHAR, parent_artifact_id UUID, facet_id VARCHAR, "
            "meta JSON, created_at TIMESTAMPTZ)"
        )
        con.execute(
            "CREATE TABLE run_artifact_link ("
            "run_id VARCHAR, artifact_id UUID, direction VARCHAR, is_implicit BOOLEAN)"
        )
        con.execute(
            "CREATE TABLE config_facet ("
            "id VARCHAR PRIMARY KEY, namespace VARCHAR, schema_name VARCHAR, "
            "schema_version VARCHAR, facet_json JSON, created_at TIMESTAMPTZ)"
        )
        con.execute(
            "INSERT INTO config_facet VALUES ('cf_1', '_hot_migration_step', 'dict', '1', ?, ?)",
            [json.dumps({"scale": 2}), now_ts],
        )

        # Hot completed run produced by v0.4 tracker.run(...)
        hot_run_meta = {
            "_physical_run_dir": str(hot_run_dir.resolve()),
            "mounts": {},
            "declared_outputs": ["hot_out"],
            "input_binding": {
                "version": "roles-v1",
                "bindings": [
                    {
                        "kind": "named",
                        "role": "raw",
                        "artifact_id": str(hot_in_id),
                    }
                ],
            },
            "input_identity": {
                "mode": "action-v2",
                "version": 2,
                "code": {"mode": "callable_module", "digest": git_hash},
                "bindings": [
                    {
                        "kind": "named",
                        "role": "raw",
                        "mode": "content-v1",
                        "value": f"sha256:bytes:{hot_in_sha}",
                        "artifact_id": str(hot_in_id),
                        "selector": {
                            "driver": "parquet",
                            "table_path": None,
                            "array_path": None,
                        },
                    }
                ],
            },
        }
        con.execute(
            "INSERT INTO run VALUES (?, NULL, 'completed', '_hot_migration_step', "
            "'hot step', NULL, NULL, '[]', ?, ?, 'sha256:action-v2:' || repeat('a', 64), "
            "repeat('b', 64), 'cf_1', ?, ?, ?, ?, ?)",
            [
                "hot_run_v04",
                config_hash,
                git_hash,
                json.dumps(hot_run_meta),
                now_ts,
                now_ts,
                now_ts,
                now_ts,
            ],
        )

        # Cold consumer run referencing unmounted cold://cold_trips.parquet
        cold_run_meta = {
            "_physical_run_dir": str(hot_run_dir.resolve()),
            "mounts": {},
            "input_binding": {
                "version": "roles-v1",
                "bindings": [
                    {
                        "kind": "named",
                        "role": "trips",
                        "artifact_id": str(cold_art_id),
                    }
                ],
            },
            "input_identity": {
                "mode": "action-v2",
                "version": 2,
                "code": {"mode": "callable_module", "digest": git_hash},
                "bindings": [
                    {
                        "kind": "named",
                        "role": "trips",
                        "mode": "content-v1",
                        "value": f"sha256:bytes:{cold_sha}",
                        "artifact_id": str(cold_art_id),
                    }
                ],
            },
        }
        con.execute(
            "INSERT INTO run VALUES ('cold_consumer_v04', NULL, 'completed', 'cold_step', "
            "NULL, NULL, NULL, '[]', ?, ?, 'sha256:action-v2:' || repeat('c', 64), "
            "repeat('d', 64), NULL, ?, ?, ?, ?, ?)",
            [
                config_hash,
                git_hash,
                json.dumps(cold_run_meta),
                now_ts,
                now_ts,
                now_ts,
                now_ts,
            ],
        )

        # Artifacts: 64-hex input, 64-hex output with recovery_roots, fast:... file, cold:// unmounted file
        con.execute(
            "INSERT INTO artifact VALUES (?, 'raw', './raw_in.parquet', 'parquet', "
            "NULL, NULL, ?, NULL, NULL, NULL, NULL, '{}', ?)",
            [hot_in_id, hot_in_sha, now_ts],
        )
        con.execute(
            "INSERT INTO artifact VALUES (?, 'hot_out', './hot_out.parquet', 'parquet', "
            "NULL, NULL, ?, NULL, 'hot_run_v04', NULL, NULL, ?, ?)",
            [
                hot_out_id,
                hot_out_sha,
                json.dumps({"recovery_roots": [str(hot_archive_dir.resolve())]}),
                now_ts,
            ],
        )
        con.execute(
            "INSERT INTO artifact VALUES (?, 'fast_obs', './fast_obs.parquet', 'parquet', "
            "NULL, NULL, 'fast:999999', NULL, 'hot_run_v04', NULL, NULL, ?, ?)",
            [
                fast_art_id,
                json.dumps(
                    {
                        "hash_semantics": {
                            "version": 1,
                            "source": "computed_fast",
                            "kind": "file",
                        }
                    }
                ),
                now_ts,
            ],
        )
        con.execute(
            "INSERT INTO artifact VALUES (?, 'cold_trips', 'cold://cold_trips.parquet', 'parquet', "
            "NULL, NULL, ?, NULL, NULL, NULL, NULL, ?, ?)",
            [
                cold_art_id,
                cold_sha,
                json.dumps({"recovery_roots": ["cold://"]}),
                now_ts,
            ],
        )

        con.execute(
            "INSERT INTO run_artifact_link VALUES "
            "('hot_run_v04', ?, 'input', FALSE), "
            "('hot_run_v04', ?, 'output', FALSE), "
            "('hot_run_v04', ?, 'output', FALSE), "
            "('cold_consumer_v04', ?, 'input', FALSE)",
            [hot_in_id, hot_out_id, fast_art_id, cold_art_id],
        )

        # Hot ingested table in global_tables + custom SQL view in main
        con.execute("CREATE SCHEMA global_tables")
        con.execute(
            "CREATE TABLE global_tables.people AS "
            "SELECT 1 AS person_id, 34 AS age UNION ALL SELECT 2 AS person_id, 12 AS age"
        )
        con.execute(
            "CREATE VIEW v_adult_people AS "
            "SELECT person_id, age FROM global_tables.people WHERE age >= 18"
        )

    return {
        "hot_in_path": hot_in_path,
        "hot_out_path": hot_out_path,
        "hot_in_id": hot_in_id,
        "hot_out_id": hot_out_id,
        "fast_art_id": fast_art_id,
        "cold_art_id": cold_art_id,
        "cold_sha": cold_sha,
    }


def test_consist_migrate_hot_cache_hit_cold_mount_trust_hashes_and_undo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "legacy_v04.duckdb"
    hot_run_dir = tmp_path / "hot_runs"
    hot_archive_dir = tmp_path / "hot_archive"
    cold_dir = tmp_path / "unmounted_lustre_cold"

    info = _build_synthetic_v04_database(
        db_path,
        hot_run_dir=hot_run_dir,
        hot_archive_dir=hot_archive_dir,
        cold_dir=cold_dir,
    )

    # 1. Pre-migration opening raises DatabaseMigrationRequired
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=hot_run_dir, db_path=str(db_path))

    runner = CliRunner()

    # 2. Dry-run leaves source database untouched
    dry_res = runner.invoke(app, ["migrate", str(db_path), "--dry-run", "--json"])
    assert dry_res.exit_code == 0, dry_res.output
    dry_payload = json.loads(dry_res.stdout)
    assert dry_payload["dry_run"] is True
    assert dry_payload["runs_migrated"] == 2
    assert not Path(f"{db_path}.v04.bak").exists()
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=hot_run_dir, db_path=str(db_path))

    # 3. Full migration (cold_dir NOT mounted during migration!)
    mig_res = runner.invoke(app, ["migrate", str(db_path), "--json"])
    assert mig_res.exit_code == 0, mig_res.output
    mig_payload = json.loads(mig_res.stdout)
    assert mig_payload["already_current"] is False
    assert mig_payload["cacheable_runs"] == 1
    assert mig_payload["uncacheable_runs"] == 1
    assert mig_payload["artifacts_cold_unverified"] == 1
    assert mig_payload["global_tables_copied"] == 1
    assert mig_payload["views_copied"] == 1
    assert Path(f"{db_path}.v04.bak").is_file()

    # Idempotent re-run on version=2 DB
    idem = migrate_database(db_path)
    assert idem.already_current is True

    # 4. Open migrated DB under v0.5 Tracker -> 100% live cache hit on hot_run_v04!
    tracker = Tracker(run_dir=hot_run_dir, db_path=str(db_path))
    try:
        assert tracker.db is not None
        adults = tracker.db.fetchall("SELECT person_id, age FROM v_adult_people")
        assert adults == [(1, 34)]

        hot_out_art = tracker.get_artifact(str(info["hot_out_id"]))
        assert hot_out_art is not None
        assert "recovery_roots" not in hot_out_art.meta
        assert hot_out_art.recovery_roots == [str(hot_archive_dir.resolve())]

        cold_art_unmounted = tracker.get_artifact(str(info["cold_art_id"]))
        assert cold_art_unmounted is not None
        assert cold_art_unmounted.digest is None
        assert cold_art_unmounted.meta.get("legacy_v04_hash") == info["cold_sha"]

        cold_run = tracker.get_run("cold_consumer_v04")
        assert cold_run is not None
        assert cold_run.cacheable is False

        # Re-execute _hot_migration_step with identical code, config, and input -> LIVE CACHE HIT!
        hit_res = tracker.run(
            fn=_hot_migration_step,
            inputs={"raw": info["hot_in_path"]},
            config={"scale": 2},
            outputs=["hot_out"],
            cache_options=CacheOptions(code_identity="callable_module"),
        )
        assert hit_res.cache_hit is True
        assert hit_res.run.meta.get("cache_source") == "hot_run_v04"
        loaded_hot = consist.load_df(hit_res.outputs["hot_out"], tracker=tracker)
        assert isinstance(loaded_hot, pd.DataFrame)
        assert loaded_hot["doubled"].tolist() == [10, 20, 30]
    finally:
        tracker.close()

    # 5. Mount the cold archive later -> art_cold.path and consist.load resolve immediately!
    mounted_tracker = Tracker(
        run_dir=hot_run_dir,
        db_path=str(db_path),
        mounts={"cold": str(cold_dir)},
    )
    try:
        cold_art = mounted_tracker.get_artifact(str(info["cold_art_id"]))
        assert cold_art is not None
        assert cold_art.path == (cold_dir / "cold_trips.parquet").resolve()
        cold_df = consist.load_df(cold_art, tracker=mounted_tracker)
        assert isinstance(cold_df, pd.DataFrame)
        assert cold_df["trip_id"].tolist() == [101, 102]
    finally:
        mounted_tracker.close()

    # 6. Undo migration via CLI -> restores v0.4 DB atomically!
    undo_res = runner.invoke(app, ["migrate", "undo", str(db_path)])
    assert undo_res.exit_code == 0, undo_res.output
    assert not Path(f"{db_path}.v04.bak").exists()
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=hot_run_dir, db_path=str(db_path))

    # 7. Migrate with --trust-v04-hashes: skips byte reads on 64-hex files, hashes fast:... files!
    import consist.core.migrate as migrate_mod

    byte_hash_paths: list[Path] = []
    orig_compute_digest = migrate_mod.compute_digest

    def _spy_compute_digest(
        path: Path | str, *args: object, **kwargs: object
    ) -> Digest:
        byte_hash_paths.append(Path(path).resolve())
        return orig_compute_digest(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(migrate_mod, "compute_digest", _spy_compute_digest)

    trust_report = migrate_database(db_path, trust_v04_hashes=True)
    assert trust_report.artifacts_trusted == 2  # raw_in.parquet + hot_out.parquet
    assert trust_report.artifacts_hashed == 1  # fast_obs.parquet MUST be hashed!
    assert trust_report.artifacts_cold_unverified == 1
    assert byte_hash_paths == [(hot_run_dir / "fast_obs.parquet").resolve()]

    # Undo via Python API also works cleanly
    undo_migrate_database(db_path)
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=hot_run_dir, db_path=str(db_path))


def test_location_layer2_import_dag_and_runtime_isolation() -> None:
    import consist.location as loc_mod

    src_root = Path(loc_mod.__file__).resolve().parent
    loc_tree = ast.parse((src_root / "location.py").read_text(encoding="utf-8"))
    imported_modules: list[str] = []
    for node in ast.walk(loc_tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    forbidden_prefixes = (
        "consist.core",
        "consist.api",
        "consist.cli",
        "consist.models",
        "sqlalchemy",
        "sqlmodel",
        "duckdb",
    )
    for mod_name in imported_modules:
        assert not mod_name.startswith(forbidden_prefixes), (
            f"Layer-2 consist.location imported forbidden module {mod_name!r}"
        )

    for layer1_rel in ("digest.py", "types.py", "protocols.py"):
        l1_tree = ast.parse((src_root / layer1_rel).read_text(encoding="utf-8"))
        for node in ast.walk(l1_tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("consist.location")
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("consist.location")

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys, consist.location; "
                "assert 'consist.core' not in sys.modules; "
                "assert 'sqlalchemy' not in sys.modules; "
                "assert 'sqlmodel' not in sys.modules; "
                "assert 'duckdb' not in sys.modules"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_database_maintenance_roundtrips_artifact_locations(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "source.duckdb"
    export_path = tmp_path / "exported.duckdb"
    merge_path = tmp_path / "merged.duckdb"
    rebuilt_path = tmp_path / "rebuilt.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        p1 = run_dir / "one.parquet"
        p2 = run_dir / "two.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(p1)
        pd.DataFrame({"a": [2]}).to_parquet(p2)
        with tracker.start_run("maint_r1", model="maint", config={"step": 1}):
            a1 = tracker.log_output(p1, key="one")
        tracker.archive_artifact(a1, archive_dir, mode="copy")
        with tracker.start_run("maint_r2", model="maint", config={"step": 2}):
            a2 = tracker.log_output(p2, key="two")

        assert tracker.db is not None
        maint = DatabaseMaintenance(db=tracker.db, run_dir=tracker.run_dir)
        maint.export(
            ["maint_r1"],
            export_path,
            include_data=False,
            include_snapshots=True,
        )

        exp_tracker = Tracker(run_dir=run_dir, db_path=str(export_path), read_only=True)
        try:
            exp_a1 = exp_tracker.get_artifact(str(a1.id))
            assert exp_a1 is not None
            assert exp_a1.recovery_roots == [str(archive_dir.resolve())]
        finally:
            exp_tracker.close()

        merge_tracker = Tracker(run_dir=run_dir, db_path=str(merge_path))
        try:
            assert merge_tracker.db is not None
            DatabaseMaintenance(
                db=merge_tracker.db, run_dir=merge_tracker.run_dir
            ).merge(
                export_path,
                conflict="skip",
                include_snapshots=True,
            )
            merged_a1 = merge_tracker.get_artifact(str(a1.id))
            assert merged_a1 is not None
            assert merged_a1.recovery_roots == [str(archive_dir.resolve())]
        finally:
            merge_tracker.close()

        maint.purge(["maint_r2"], dry_run=False, prune_cache=True)
        surviving_a2_locs = tracker.db.fetchall(
            "SELECT 1 FROM artifact_location WHERE artifact_id = ?",
            [a2.id],
        )
        assert surviving_a2_locs == []
        assert len(tracker.db.get_artifact_recovery_locations(a1.id)) == 1
    finally:
        tracker.close()

    rebuild_tracker = Tracker(run_dir=run_dir, db_path=str(rebuilt_path))
    try:
        assert rebuild_tracker.db is not None
        DatabaseMaintenance(
            db=rebuild_tracker.db, run_dir=rebuild_tracker.run_dir
        ).rebuild_from_json(run_dir / "consist_runs", dry_run=False, mode="full")
        reb_a1 = rebuild_tracker.get_artifact(str(a1.id))
        assert reb_a1 is not None
        assert reb_a1.path == p1.resolve()
        assert rebuild_tracker.db.get_active_artifact_locations(
            artifact_id=a1.id, digest=a1.digest
        )
    finally:
        rebuild_tracker.close()


def test_ensure_local_path_mutable_protects_archive_and_external_mounts_and_preserves_writable_permissions(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    inputs_dir = tmp_path / "ext_inputs"
    stage_ro_dir = tmp_path / "stage_ro"
    stage_ext_dir = tmp_path / "stage_ext"
    db_path = tmp_path / "prov.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)
    inputs_dir.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(
        run_dir=run_dir,
        db_path=str(db_path),
        mounts={"inputs": str(inputs_dir)},
    )
    try:
        scratch_file = run_dir / "clean.parquet"
        pd.DataFrame({"v": [11, 22, 33]}).to_parquet(scratch_file)
        ext_file = inputs_dir / "shared.parquet"
        pd.DataFrame({"e": [7, 8]}).to_parquet(ext_file)

        with tracker.start_run("run_mutable_guard", model="guard"):
            ext_art = tracker.log_input(ext_file, key="shared_in")
            art = tracker.log_output(scratch_file, key="clean")

        arch_copy = tracker.archive_artifact(art, archive_dir, mode="copy").resolve()
        aged_ns = time.time_ns() + 5_000_000_000

        # 1. mutable=False staging never chmods archive to 0o444 or invalidates archive witness,
        # and staged file remains writable for subsequent re-runs without PermissionError!
        staged_ro = art.ensure_local_path(target_root=stage_ro_dir, mutable=False)
        assert staged_ro is not None and staged_ro.is_file()
        assert os.access(arch_copy, os.W_OK)
        assert os.access(staged_ro, os.W_OK)
        assert tracker.db is not None
        arch_rows = [
            r
            for r in tracker.db.get_active_artifact_locations(
                artifact_id=art.id, digest=art.digest
            )
            if r.site == "archive"
        ]
        assert len(arch_rows) == 1
        assert arch_rows[0].witness is not None
        assert (
            arch_rows[0].witness.check(arch_copy, now_ns=aged_ns).freshness.value
            != "stat_mismatch"
        )

        # Overwriting staged_ro in place succeeds without PermissionError and leaves archive untouched
        pd.DataFrame({"v": [999]}).to_parquet(staged_ro)
        assert pd.read_parquet(arch_copy)["v"].tolist() == [11, 22, 33]
        assert (
            arch_rows[0].witness.check(arch_copy, now_ns=aged_ns).freshness.value
            != "stat_mismatch"
        )

        # 2. When scratch was overwritten, ensure_local_path(mutable=True) without target_root
        # raises FileExistsError instead of returning arch_copy or clobbering scratch edits!
        pd.DataFrame({"v": [-1, -2]}).to_parquet(scratch_file)
        with pytest.raises(FileExistsError, match="target_root"):
            art.ensure_local_path(mutable=True)
        assert pd.read_parquet(arch_copy)["v"].tolist() == [11, 22, 33]
        assert pd.read_parquet(scratch_file)["v"].tolist() == [-1, -2]

        # 3. When scratch was deleted, ensure_local_path(mutable=True) stages an independent
        # writable copy back to scratch_file without mutating archive!
        scratch_file.unlink()
        restaged_ws = art.ensure_local_path(mutable=True)
        assert restaged_ws == scratch_file.resolve()
        pd.DataFrame({"v": [555]}).to_parquet(restaged_ws)
        assert pd.read_parquet(arch_copy)["v"].tolist() == [11, 22, 33]

        # 4. External-mount artifact refuses in-place mutable=True unless target_root is passed
        with pytest.raises(ValueError, match="external-mount"):
            ext_art.ensure_local_path(mutable=True)
        staged_ext = ext_art.ensure_local_path(target_root=stage_ext_dir, mutable=True)
        assert staged_ext is not None
        pd.DataFrame({"e": [100]}).to_parquet(staged_ext)
        assert pd.read_parquet(ext_file)["e"].tolist() == [7, 8]
    finally:
        tracker.close()


def test_shapefile_staging_missing_shp_edited_dbf_and_unrelated_siblings(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    dest_dir = tmp_path / "dest"
    db_path = tmp_path / "prov.duckdb"
    zones_dir = run_dir / "zones"
    zones_dir.mkdir(parents=True, exist_ok=True)

    # Case-insensitive stem sidecars + unrelated .gdb directory and .parquet sibling
    shp_upper = zones_dir / "PARCELS.SHP"
    shp_upper.write_bytes(b"SHP-ORIG")
    for ext in (".shx", ".dbf", ".prj", ".cpg"):
        (zones_dir / f"parcels{ext}").write_bytes(f"ORIG-{ext}".encode("utf-8"))
    gdb_dir = zones_dir / "parcels.gdb"
    gdb_dir.mkdir()
    (gdb_dir / "a00000001.gdbtable").write_bytes(b"GDB-DATA")
    (zones_dir / "parcels.parquet").write_bytes(b"PARQUET-SIBLING")

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        with tracker.start_run("run_shp_guard", model="shp_guard"):
            art_shp = tracker.log_output(shp_upper, key="parcels", driver="shapefile")

        manifest = art_shp.meta.get("file_bundle_manifest", {})
        member_paths = {m["path"] for m in manifest.get("entries", [])}
        assert "PARCELS.SHP" in member_paths
        assert "parcels.dbf" in member_paths
        assert "parcels.parquet" not in member_paths
        assert not any("gdb" in p for p in member_paths)

        arch_bundle_dir = tracker.archive_artifact(art_shp, archive_dir, mode="copy")
        arch_shp = (arch_bundle_dir / "PARCELS.SHP").resolve()

        # Stage initial bundle to dest_dir
        staged = art_shp.ensure_local_path(target_root=dest_dir, mutable=True)
        assert staged is not None and staged.name == "PARCELS.SHP"
        dest_bundle = staged.parent

        # Delete .SHP entry, edit .dbf sidecar, and add extra .qpj sidecar
        staged.unlink()
        (dest_bundle / "parcels.dbf").write_bytes(b"USER-EDITED-DBF")
        (dest_bundle / "parcels.qpj").write_bytes(b"EXTRA-QPJ")

        # overwrite=False MUST refuse to clobber user-edited parcels.dbf even though PARCELS.SHP is missing!
        with pytest.raises(FileExistsError):
            tracker_recovery.stage_verified_source(
                art_shp,
                arch_shp,
                dest_bundle / "PARCELS.SHP",
                mutable=True,
                overwrite=False,
            )
        assert (dest_bundle / "parcels.dbf").read_bytes() == b"USER-EDITED-DBF"

        # overwrite=True restores PARCELS.SHP + original parcels.dbf and cleans up extra parcels.qpj
        tracker_recovery.stage_verified_source(
            art_shp, arch_shp, dest_bundle / "PARCELS.SHP", mutable=True, overwrite=True
        )
        restored = dest_bundle / "PARCELS.SHP"
        assert restored.is_file()
        assert (dest_bundle / "parcels.dbf").read_bytes() == b"ORIG-.dbf"
        assert not (dest_bundle / "parcels.qpj").exists()
        assert tracker_recovery.verify_candidate_against_artifact(restored, art_shp)

        # Pre-staging source verification rejects corrupt source before touching destination
        corrupt_src = tmp_path / "corrupt_src" / "PARCELS.SHP"
        corrupt_src.parent.mkdir(parents=True, exist_ok=True)
        corrupt_src.write_bytes(b"CORRUPT")
        clean_target = tmp_path / "untouched_dest" / "PARCELS.SHP"
        with pytest.raises(ValueError, match="failed.*verification"):
            tracker_recovery.stage_verified_source(
                art_shp, corrupt_src, clean_target, mutable=True, overwrite=True
            )
        assert not clean_target.exists()
    finally:
        tracker.close()


def test_migrate_atomic_swap_rollback_mounts_cli_dry_run_no_mkdir_and_strict_bindings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert consist.migrate_database is migrate_database
    assert consist.undo_migrate_database is undo_migrate_database

    db_path = tmp_path / "v04_strict.duckdb"
    vanished_run_dir = tmp_path / "vanished_historical_run_dir"
    cold_dir = tmp_path / "mounted_cold"
    extra_dir = tmp_path / "mounted_extra"
    cold_dir.mkdir(parents=True, exist_ok=True)
    extra_dir.mkdir(parents=True, exist_ok=True)

    strict_file = cold_dir / "strict_cfg.parquet"
    shared_file = extra_dir / "shared_multi.parquet"
    pd.DataFrame({"s": [1, 2]}).to_parquet(strict_file)
    pd.DataFrame({"m": [3, 4]}).to_parquet(shared_file)
    strict_sha = file_sha256(strict_file)
    shared_sha = file_sha256(shared_file)

    strict_aid = uuid.uuid4()
    shared_aid = uuid.uuid4()
    inv_id = uuid.uuid4()
    now_ts = datetime.now(timezone.utc)

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "CREATE TABLE consist_schema ("
            "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR)"
        )
        con.execute("INSERT INTO consist_schema VALUES (1, ?, '0.4.2')", [now_ts])
        con.execute(
            "CREATE TABLE run ("
            "id VARCHAR PRIMARY KEY, parent_run_id VARCHAR, status VARCHAR, "
            "model_name VARCHAR, description VARCHAR, year INTEGER, iteration INTEGER, "
            "tags JSON, config_hash VARCHAR, git_hash VARCHAR, input_hash VARCHAR, "
            "signature VARCHAR, facet_id VARCHAR, meta JSON, "
            "started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, "
            "created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)"
        )
        con.execute(
            "CREATE TABLE artifact ("
            "id UUID PRIMARY KEY, key VARCHAR, container_uri VARCHAR, driver VARCHAR, "
            "table_path VARCHAR, array_path VARCHAR, hash VARCHAR, content_id UUID, "
            "run_id VARCHAR, parent_artifact_id UUID, facet_id VARCHAR, "
            "meta JSON, created_at TIMESTAMPTZ)"
        )
        con.execute(
            "CREATE TABLE run_artifact_link ("
            "run_id VARCHAR, artifact_id UUID, direction VARCHAR, is_implicit BOOLEAN)"
        )
        con.execute(
            "CREATE TABLE run_binding_invocation ("
            "id UUID PRIMARY KEY, requested_run_id VARCHAR, execution_run_id VARCHAR, "
            "cache_source_run_id VARCHAR, cache_outcome VARCHAR, "
            "binding_json VARCHAR, created_at TIMESTAMPTZ)"
        )

        run_meta = {
            "_physical_run_dir": str(vanished_run_dir),
            "mounts": {},
            "input_binding": {
                "version": "roles-v1",
                "bindings": [
                    {
                        "kind": "named",
                        "role": "strict_in",
                        "artifact_id": str(strict_aid),
                    },
                    {"kind": "named", "role": "left", "artifact_id": str(shared_aid)},
                    {"kind": "named", "role": "right", "artifact_id": str(shared_aid)},
                ],
            },
            "input_identity": {
                "mode": "action-v2",
                "version": 2,
                "strict_input_count": 1,
                "strict_binding_identity": "a" * 64,
                "code": {"mode": "git_commit", "digest": "deadbeef"},
                "bindings": [
                    {
                        "kind": "named",
                        "role": "left",
                        "mode": "content-v1",
                        "value": f"sha256:bytes:{shared_sha}",
                        "artifact_id": str(shared_aid),
                    },
                    {
                        "kind": "named",
                        "role": "right",
                        "mode": "content-v1",
                        "value": f"sha256:bytes:{shared_sha}",
                        "artifact_id": str(shared_aid),
                    },
                ],
            },
        }
        con.execute(
            "INSERT INTO run VALUES ('strict_scen_run', NULL, 'completed', 'scen_step', "
            "NULL, NULL, NULL, '[]', 'cfg_h', 'deadbeef', 'old_in_h', 'old_sig', NULL, "
            "?, ?, ?, ?, ?)",
            [json.dumps(run_meta), now_ts, now_ts, now_ts, now_ts],
        )
        con.execute(
            "INSERT INTO artifact VALUES "
            "(?, 'strict_in', 'cold://strict_cfg.parquet', 'parquet', NULL, NULL, ?, NULL, NULL, NULL, NULL, '{}', ?), "
            "(?, 'shared', 'extra://shared_multi.parquet', 'parquet', NULL, NULL, ?, NULL, NULL, NULL, NULL, '{}', ?)",
            [strict_aid, strict_sha, now_ts, shared_aid, shared_sha, now_ts],
        )
        con.execute(
            "INSERT INTO run_artifact_link VALUES "
            "('strict_scen_run', ?, 'input', FALSE), "
            "('strict_scen_run', ?, 'input', FALSE)",
            [strict_aid, shared_aid],
        )
        binding_evidence = {
            "schema_version": 1,
            "step_name": "scen_step",
            "step_contract_identity": f"sha256:step-v1:{'b' * 64}",
            "inputs": {
                "strict_in": {
                    "parameter": "strict_in",
                    "destination": None,
                    "artifact": {
                        "artifact_id": str(strict_aid),
                        "identity": strict_sha,
                    },
                }
            },
            "admission_evidence": {"strict_in": {"observed_identity": strict_sha}},
        }
        con.execute(
            "INSERT INTO run_binding_invocation VALUES "
            "(?, 'strict_scen_run', 'strict_scen_run', NULL, 'executed', ?, ?)",
            [inv_id, json.dumps(binding_evidence), now_ts],
        )

    runner = CliRunner()

    # 1. --dry-run must NOT create vanished_historical_run_dir on disk!
    dry = runner.invoke(app, ["migrate", str(db_path), "--dry-run", "--json"])
    assert dry.exit_code == 0, dry.output
    assert not vanished_run_dir.exists()

    # 2. Atomic swap rollback: simulated EXDEV during tmp -> src swap restores original DB!
    real_replace = os.replace

    def _failing_second_replace(
        src: str | os.PathLike[str], dst: str | os.PathLike[str]
    ) -> None:
        if Path(dst) == db_path.resolve() and ".migrate-" in str(src):
            raise OSError(errno.EXDEV, "Simulated cross-device link")
        real_replace(src, dst)

    with monkeypatch.context() as mp:
        mp.setattr(os, "replace", _failing_second_replace)
        with pytest.raises(OSError, match="Simulated cross-device link"):
            migrate_database(db_path)
    assert db_path.is_file()
    assert not Path(f"{db_path}.v04.bak").exists()
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=tmp_path / "probe", db_path=str(db_path))

    # 3. Cold migration (cold:// unmounted): strict-binding run MUST be cacheable=False!
    cold_rep = migrate_database(db_path, mounts={"extra": str(extra_dir)})
    assert not vanished_run_dir.exists()
    assert cold_rep.cacheable_runs == 0
    assert cold_rep.uncacheable_runs == 1
    undo_migrate_database(db_path)

    # 4. CLI migration passing BOTH --mounts and --mount upgrades strict binding & preserves multi-role links!
    warm_cli = runner.invoke(
        app,
        [
            "migrate",
            str(db_path),
            "--mounts",
            f"cold={cold_dir}",
            "--mount",
            f"extra={extra_dir}",
            "--json",
        ],
    )
    assert warm_cli.exit_code == 0, warm_cli.output
    warm_payload = json.loads(warm_cli.stdout)
    assert warm_payload["cacheable_runs"] == 1
    assert not vanished_run_dir.exists()

    check_tracker = Tracker(
        run_dir=tmp_path / "check_runs",
        db_path=str(db_path),
        mounts={"cold": str(cold_dir), "extra": str(extra_dir)},
        read_only=True,
    )
    try:
        assert check_tracker.db is not None
        roles = sorted(
            r[0]
            for r in check_tracker.db.fetchall(
                "SELECT role FROM run_artifact_link WHERE run_id = 'strict_scen_run' AND artifact_id = ?",
                [shared_aid],
            )
        )
        assert roles == ["named:left", "named:right"]
        inv_row = check_tracker.db.fetchone(
            "SELECT binding_json FROM run_binding_invocation WHERE id = ?",
            [inv_id],
        )
        assert inv_row is not None
        upgraded_ev = json.loads(inv_row[0])
        assert upgraded_ev["inputs"]["strict_in"]["artifact"]["identity"] == (
            f"sha256:file:{strict_sha}"
        )
    finally:
        check_tracker.close()


def test_evict_artifact_path_kwarg_shared_hdf5_and_ingested_load_fallback(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    extra_scratch = tmp_path / "extra_scratch"
    db_path = tmp_path / "prov.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)
    extra_scratch.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        # 1. Shared HDF5 container: refuse eviction when sibling member lacks backing!
        h5_path = run_dir / "shared_evict.h5"
        with h5py.File(h5_path, "w") as hf:
            hf.create_dataset("/m1", data=[1, 2, 3])
            hf.create_dataset("/m2", data=[4, 5, 6])

        with tracker.start_run("run_h5_evict", model="h5_evict"):
            art_m1 = tracker.log_h5_table(h5_path, table_path="/m1", key="m1")
            art_m2 = tracker.log_h5_table(h5_path, table_path="/m2", key="m2")

        tracker.archive_artifact(art_m1, archive_dir, mode="copy")
        # art_m2 has NOT been archived yet -> unlinking shared_evict.h5 must be refused!
        with pytest.raises(ValueError, match="shared container"):
            tracker.evict_artifact(art_m1)
        assert h5_path.is_file()

        # Once art_m2 also has surviving archive backing, evicting art_m1 unlinks container
        # and marks BOTH art_m1 and art_m2 workspace locations 'evicted'!
        tracker.archive_artifact(art_m2, archive_dir, mode="copy")
        evicted_h5 = tracker.evict_artifact(art_m1)
        assert evicted_h5 == h5_path.resolve()
        assert not h5_path.exists()
        assert tracker.db is not None
        sib_statuses = tracker.db.fetchall(
            "SELECT artifact_id, status FROM artifact_location "
            "WHERE site = 'workspace' AND artifact_id IN (?, ?)",
            [art_m1.id, art_m2.id],
        )
        assert len(sib_statuses) == 2
        assert all(st == "evicted" for _, st in sib_statuses)

        # 2. Explicit path= rejects archive paths and preserves untouched workspace status on alt scratch
        parq_path = run_dir / "alt_evict.parquet"
        alt_copy = extra_scratch / "alt_evict.parquet"
        pd.DataFrame({"k": [9, 10]}).to_parquet(parq_path)
        alt_copy.write_bytes(parq_path.read_bytes())
        with tracker.start_run("run_alt_path", model="evict_path"):
            art_alt = tracker.log_output(parq_path, key="alt")
        arch_alt = tracker.archive_artifact(art_alt, archive_dir, mode="copy")

        with pytest.raises(ValueError, match="archive"):
            consist.evict_artifact(art_alt, tracker=tracker, path=arch_alt)
        assert arch_alt.is_file()

        evicted_alt = consist.evict_artifact(art_alt, tracker=tracker, path=alt_copy)
        assert evicted_alt == alt_copy.resolve()
        assert not alt_copy.exists()
        assert parq_path.is_file()
        ws_alt_status = tracker.db.fetchone(
            "SELECT status FROM artifact_location WHERE artifact_id = ? AND site = 'workspace'",
            [art_alt.id],
        )
        assert ws_alt_status is not None and ws_alt_status[0] == "active"

        # 3. global_tables empty-row verification + consist.load default fallback on evicted output
        hot_empty_path = run_dir / "hot_empty.parquet"
        pd.DataFrame({"metric_id": [10], "score": [1.5]}).to_parquet(hot_empty_path)
        with tracker.start_run("run_hot_empty", model="hot_evict"):
            art_empty = tracker.log_output(hot_empty_path, key="hot_empty")
            tracker.ingest(art_empty, schema=_IngestedMetricRow)
        tracker.db.execute(
            "DELETE FROM global_tables.evict_ingested_metrics WHERE consist_artifact_id = ?",
            [str(art_empty.id)],
        )
        with pytest.raises(ValueError, match="surviving verified archive"):
            tracker.evict_artifact(art_empty)
        assert hot_empty_path.is_file()

        hot_ok_path = run_dir / "hot_ok.parquet"
        pd.DataFrame({"metric_id": [20, 21], "score": [7.5, 8.5]}).to_parquet(
            hot_ok_path
        )
        with tracker.start_run("run_hot_ok", model="hot_evict"):
            art_ok = tracker.log_output(hot_ok_path, key="hot_ok")
            tracker.ingest(art_ok, schema=_IngestedMetricRow)
        tracker.evict_artifact(art_ok)
        assert not hot_ok_path.exists()

        # Default db_fallback="inputs-only" falls back to global_tables because workspace status == 'evicted'!
        loaded_default = consist.load_df(art_ok, tracker=tracker)
        assert isinstance(loaded_default, pd.DataFrame)
        assert sorted(loaded_default["score"].tolist()) == [7.5, 8.5]
    finally:
        tracker.close()


def test_cross_process_epoch_invalidation_named_workspace_mount_isolation_and_cold_append(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared_epoch.duckdb"
    shared_runs = tmp_path / "shared_runs"
    ws_one = tmp_path / "ws_one"
    ws_two = tmp_path / "ws_two"
    local_arch = tmp_path / "local_arch"
    for d in (shared_runs, ws_one, ws_two, local_arch):
        d.mkdir(parents=True, exist_ok=True)

    # 1. Named mounts={"workspace": ...} instance isolation across trackers sharing run_dir & DB
    t1 = Tracker(
        run_dir=shared_runs, db_path=str(db_path), mounts={"workspace": str(ws_one)}
    )
    t2 = Tracker(
        run_dir=shared_runs, db_path=str(db_path), mounts={"workspace": str(ws_two)}
    )
    try:
        p1 = ws_one / "isolated.parquet"
        p2 = ws_two / "isolated.parquet"
        pd.DataFrame({"src": [1]}).to_parquet(p1)
        # Identical content/digest on ws_two so digest lookup sees both workspace rows!
        p2.write_bytes(p1.read_bytes())

        with t1.start_run("ws_iso_1", model="ws_iso"):
            a1 = t1.log_output(p1, key="iso")
        with t2.start_run("ws_iso_2", model="ws_iso", cache_mode="overwrite"):
            a2 = t2.log_output(p2, key="iso")

        assert a1.uri == "workspace://isolated.parquet"
        assert a2.uri == "workspace://isolated.parquet"
        assert a1.digest == a2.digest
        assert a1.path == p1.resolve()
        assert a2.path == p2.resolve()
        # Removing p1 must NOT resolve a1 to ws_two's p2 unlesshistorical_run_dir matches;
        # specifically primary mount resolution on t1 ignores ws_two's mount_instance_id!
        assert (
            t1.fs.resolve_location_coordinate(
                "workspace",
                t2.fs.location_coordinate_for_uri(a2.uri)[1],
                a2.uri,
            )
            is None
        )

        # 2. Unmounted cold:// scheme root preserved when appending local archive root
        t2.set_artifact_recovery_roots(a2, ["cold://archive_bucket"], append=False)
        assert a2.recovery_roots == ["cold://archive_bucket"]
        (local_arch / "isolated.parquet").write_bytes(p2.read_bytes())
        t2.set_artifact_recovery_roots(a2, [local_arch], append=True)
        assert "cold://archive_bucket" in a2.recovery_roots
        assert str(local_arch.resolve()) in a2.recovery_roots
    finally:
        t1.close()
        t2.close()

    # 3. suspend_for_subprocesses() + read_only=True DB/WAL epoch invalidation
    parent_tracker = Tracker(run_dir=shared_runs, db_path=str(db_path))
    try:
        live_p = shared_runs / "subproc_live.parquet"
        pd.DataFrame({"q": [42]}).to_parquet(live_p)
        with parent_tracker.start_run("subproc_parent", model="epoch"):
            parent_art = parent_tracker.log_output(live_p, key="live")
        assert parent_art.path == live_p.resolve()
        assert parent_tracker.db is not None
        parent_tracker.db.execute("CHECKPOINT")

        ro_reader = Tracker(run_dir=shared_runs, db_path=str(db_path), read_only=True)
        try:
            ro_art = ro_reader.get_artifact(str(parent_art.id))
            assert ro_art is not None
            # Prime read_only cache pointing at live_p
            assert ro_art.path == live_p.resolve()

            sub_arch = tmp_path / "sub_arch"
            assert parent_tracker.db is not None
            with parent_tracker.db.suspend_for_subprocesses():
                child_writer = Tracker(run_dir=shared_runs, db_path=str(db_path))
                try:
                    child_art = child_writer.get_artifact(str(parent_art.id))
                    assert child_art is not None
                    child_writer.archive_artifact(child_art, sub_arch, mode="move")
                    assert child_writer.db is not None
                    child_writer.db.execute("CHECKPOINT")
                finally:
                    child_writer.close()

            expected_moved = (sub_arch / "subproc_live.parquet").resolve()
            assert not live_p.exists()
            # Both resumed parent_tracker and read_only reader detect epoch advance and resolve archive!
            assert parent_art.path == expected_moved
            assert ro_art.path == expected_moved
        finally:
            ro_reader.close()
    finally:
        parent_tracker.close()


def test_maintenance_rebuild_producer_priority_archive_sidecar_export_merge_and_purge_protection(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    arch_dir = tmp_path / "arch"
    db_path = tmp_path / "maint_full.duckdb"
    rebuilt_db = tmp_path / "rebuilt_full.duckdb"
    export_db = tmp_path / "exported_full.duckdb"
    merge_src_db = tmp_path / "merge_src.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        shared_out = run_dir / "prod_out.parquet"
        pd.DataFrame({"val": [10, 20]}).to_parquet(shared_out)

        # Producer run sorts alphabetically AFTER consumer run ("z_producer" > "a_consumer")
        with tracker.start_run("z_producer", model="maint"):
            prod_art = tracker.log_output(shared_out, key="prod_out")

        # Post-run archive writes .locations.json sidecar alongside z_producer.json
        arch_file = tracker.archive_artifact(prod_art, arch_dir, mode="copy").resolve()

        def _consume_two_roles(left: pd.DataFrame, right: pd.DataFrame) -> None:
            assert len(left) == len(right)

        tracker.run(
            fn=_consume_two_roles,
            name="a_consumer",
            run_id="a_consumer",
            inputs={"left": prod_art, "right": prod_art},
            cache_options=CacheOptions(code_identity="callable_module"),
        )

        # Second run producing identical bytes (same digest, distinct artifact_id)
        dup_out = run_dir / "dup_out.parquet"
        dup_out.write_bytes(shared_out.read_bytes())
        with tracker.start_run("r_dup_digest", model="maint", config={"dup": 1}):
            dup_art = tracker.log_output(dup_out, key="dup_out")
        dup_arch_dir = tmp_path / "dup_arch"
        tracker.archive_artifact(dup_art, dup_arch_dir, mode="copy")

        assert tracker.db is not None
        maint = DatabaseMaintenance(db=tracker.db, run_dir=run_dir)

        # 1. Export ONLY z_producer -> digest-matched location from r_dup_digest must NOT dangle artifact_id!
        maint.export(
            ["z_producer"], export_db, include_data=False, include_snapshots=False
        )
        with duckdb.connect(str(export_db), read_only=True) as con:
            dangling = con.execute(
                "SELECT COUNT(*) FROM artifact_location "
                "WHERE artifact_id IS NOT NULL AND artifact_id NOT IN (SELECT id FROM artifact)"
            ).fetchone()
            assert dangling == (0,)

        # 2. Rebuild from JSON snapshots into fresh DB (verifying non-existent run_dir isn't mkdir'd on dry-run)
        vanished_rd = tmp_path / "vanished_rebuild_rd"
        reb_tracker = Tracker(run_dir=run_dir, db_path=str(rebuilt_db))
        try:
            assert reb_tracker.db is not None
            DatabaseMaintenance(
                db=reb_tracker.db, run_dir=vanished_rd
            ).rebuild_from_json(run_dir / "consist_runs", dry_run=True, mode="full")
            assert not vanished_rd.exists()

            DatabaseMaintenance(db=reb_tracker.db, run_dir=run_dir).rebuild_from_json(
                run_dir / "consist_runs", dry_run=False, mode="full"
            )
            reb_prod = reb_tracker.get_artifact(str(prod_art.id))
            assert reb_prod is not None
            assert reb_prod.run_id == "z_producer"
            assert str(arch_dir.resolve()) in reb_prod.recovery_roots
            reb_roles = sorted(
                r[0]
                for r in reb_tracker.db.fetchall(
                    "SELECT role FROM run_artifact_link WHERE run_id = 'a_consumer' AND artifact_id = ?",
                    [prod_art.id],
                )
            )
            assert reb_roles == ["left", "right"]
        finally:
            reb_tracker.close()

        # 3. Merge conflict supersession at identical (site, mount_name, mount_instance_id, uri, member)
        existing_arch = [
            r
            for r in tracker.db.get_active_artifact_locations(
                artifact_id=prod_art.id, digest=prod_art.digest
            )
            if r.site == "archive"
        ][0]
        ext_tracker = Tracker(run_dir=run_dir, db_path=str(merge_src_db))
        try:
            assert ext_tracker.db is not None
            newer_file = run_dir / "newer_merge.parquet"
            pd.DataFrame({"val": [999]}).to_parquet(newer_file)
            with ext_tracker.start_run("r_merge_newer", model="maint", config={"m": 1}):
                newer_art = ext_tracker.log_output(newer_file, key="newer_out")
            assert newer_art.digest is not None
            newer_digest = newer_art.digest
            newer_row = ArtifactLocationRow(
                location_id=compute_location_id(
                    digest=newer_digest,
                    artifact_id=None,
                    site=existing_arch.site,
                    mount_name=existing_arch.mount_name,
                    mount_instance_id=existing_arch.mount_instance_id,
                    uri=existing_arch.uri,
                    member=existing_arch.member,
                ),
                digest=str(newer_digest),
                artifact_id=None,
                site=existing_arch.site,
                mount_name=existing_arch.mount_name,
                mount_instance_id=existing_arch.mount_instance_id,
                uri=existing_arch.uri,
                member=existing_arch.member,
                status="active",
                witness=existing_arch.witness,
                observed_at_ns=existing_arch.observed_at_ns + 10_000_000_000,
            )
            ext_tracker.db.upsert_artifact_locations([newer_row])
        finally:
            ext_tracker.close()

        maint.merge(merge_src_db, conflict="skip", include_snapshots=False)
        coord_rows = tracker.db.fetchall(
            "SELECT digest, status FROM artifact_location "
            "WHERE site = ? AND mount_name = ? AND mount_instance_id = ? AND uri = ? AND member = ?",
            [
                existing_arch.site,
                existing_arch.mount_name,
                existing_arch.mount_instance_id,
                existing_arch.uri,
                existing_arch.member,
            ],
        )
        assert (str(newer_digest), "active") in coord_rows
        assert (str(prod_art.digest), "stale") in coord_rows
        # Restore prod_art archive location for delete_files protection check below
        tracker.archive_artifact(prod_art, arch_dir, mode="copy")

        # 4. Purge cleans orphaned digest-only locations AND protects surviving active location files on delete_files=True!
        maint.purge(
            ["r_dup_digest", "r_merge_newer"],
            dry_run=False,
            delete_files=True,
            prune_cache=True,
        )
        orphan_newer = tracker.db.fetchall(
            "SELECT 1 FROM artifact_location WHERE digest = ?",
            [str(newer_digest)],
        )
        assert orphan_newer == []
        # Surviving prod_art's files remain protected on disk!
        assert shared_out.is_file()
        assert arch_file.is_file()
    finally:
        tracker.close()


def test_view_factory_and_materialize_artifacts_reject_corrupt_or_evicted_scratch(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    arch_dir = tmp_path / "arch"
    mat_dir = tmp_path / "materialized"
    db_path = tmp_path / "views.duckdb"
    run_dir.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        f1 = run_dir / "trips_r1.parquet"
        f2 = run_dir / "trips_r2.parquet"
        pd.DataFrame({"trip_id": [1, 2]}).to_parquet(f1)
        pd.DataFrame({"trip_id": [3, 4]}).to_parquet(f2)

        with tracker.start_run("view_r1", model="view_test", config={"r": 1}):
            a1 = tracker.log_output(f1, key="trips")
        with tracker.start_run("view_r2", model="view_test", config={"r": 2}):
            a2 = tracker.log_output(f2, key="trips")

        tracker.archive_artifact(a2, arch_dir, mode="copy")

        # Corrupt f1 (no archive) and corrupt f2 in scratch (valid archive survives!)
        pd.DataFrame({"trip_id": [-999]}).to_parquet(f1)
        pd.DataFrame({"trip_id": [-888]}).to_parquet(f2)

        # 1. Hybrid view excludes unarchived corrupt f1 and includes verified archive for a2!
        tracker.create_view("vw_trips", "trips")
        assert tracker.db is not None
        rows = sorted(r[0] for r in tracker.db.fetchall("SELECT trip_id FROM vw_trips"))
        assert rows == [3, 4]

        # 2. materialize_artifacts rejects corrupt f1 and stages clean archive bytes for a2!
        dest_f1 = mat_dir / "out_f1.parquet"
        dest_f2 = mat_dir / "out_f2.parquet"
        with pytest.raises(FileNotFoundError):
            materialize_artifacts(tracker, [(a1, dest_f1)], on_missing="raise")
        assert not dest_f1.exists()

        staged_map = materialize_artifacts(tracker, [(a2, dest_f2)], on_missing="raise")
        assert staged_map == {"trips": str(dest_f2.resolve())}
        assert pd.read_parquet(dest_f2)["trip_id"].tolist() == [3, 4]
    finally:
        tracker.close()
