from __future__ import annotations

from datetime import datetime, timezone
import ast
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
from consist.core.migrate import migrate_database, undo_migrate_database
from consist.core.persistence import DatabaseMigrationRequired
from consist.core.tracker import Tracker
from consist.digest import Digest, file_sha256
from consist.location import FileStat
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
    # Force mtime right at current wall time so initial capture is racy=True
    now_wall_ns = time.time_ns()
    os.utime(data_path, ns=(now_wall_ns, now_wall_ns))

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        with tracker.start_run("run_racy", model="racy_demo"):
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
