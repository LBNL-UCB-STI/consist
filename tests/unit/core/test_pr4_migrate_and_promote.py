from __future__ import annotations

import ast
from collections import namedtuple
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import uuid

import duckdb
import h5py
import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from consist.cli import app
from consist.core.artifacts import _hash_h5_dataset
from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker
from consist.core.views import ViewFactory
from consist.digest import (
    Digest,
    Kind,
    compute_ctd1_digest,
    compute_logical_matrix_digest,
    digest_bytes,
    file_sha256,
)
from consist.engine import locate
from consist.migrations.v1.fasthash import (
    RACY_STAT_WINDOW_NS,
    compute_v1_fast_file_hash,
)
from consist.migrations.v1.migrator import (
    _migration_lock_context,
    default_migration_workers,
    migrate_v1_to_v2,
    open_v2_store,
    plan_migration,
    preflight_check_migration,
    promote_historical_artifact,
    undo_migration,
    verify_migration,
)
from consist.store.detect import (
    ConsistError,
    DatabaseMigrationRequired,
    SchemaState,
    detect_schema,
)
from consist.store.ledger import StoreLedger


def _init_minimal_v1_db(db_path: Path) -> None:
    db = DatabaseManager(str(db_path))
    db.dispose()


def test_overwritten_path_proof_classifier_all_10_bases_and_no_fasthash_dedup(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v1_all_bases.duckdb"
    _init_minimal_v1_db(db_path)

    # Fixed past timestamps so non-racy files are well outside RACY_STAT_WINDOW_NS (2s)
    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_created_at = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )

    # 1. directory_manifest reconstructed offline from run_artifact_dir_entry (directory deleted from disk)
    id_dir_manifest = uuid.uuid4()
    entry_bytes = b"part-0-bytes"
    entry_sha = digest_bytes(entry_bytes)

    # 2. dlt_hot_table in global_tables.households
    id_dlt = uuid.uuid4()

    # 3a. v1_full_reverified (file on disk matches recorded full sha256)
    file_full_ok = tmp_path / "full_ok.txt"
    file_full_ok.write_bytes(b"verified-full-bytes")
    full_ok_sha = file_sha256(file_full_ok)
    id_full_reverified = uuid.uuid4()

    # 3b. v1_full where the file on disk was LATER OVERWRITTEN!
    # Must preserve the verified v1_full anchor while NOT recording the overwritten file path.
    file_full_overwritten = tmp_path / "full_overwritten.txt"
    original_full_sha = digest_bytes(b"original-full-bytes-before-overwrite")
    file_full_overwritten.write_bytes(b"corrupted-overwritten-bytes")
    id_full_overwritten = uuid.uuid4()

    # 4a & 4b. Two distinct files with the EXACT SAME v1 fast stat hash (same size + mtime_ns),
    # where file A is intact on disk (migrated_fast_match) and file B is overwritten after v1 run (overwritten_cold).
    # Proves we NEVER deduplicate by v1 fast-hash equality!
    file_fast_a = tmp_path / "fast_a.bin"
    file_fast_b = tmp_path / "fast_b.bin"
    payload_same_len = b"fast-match-bytes"  # 16 bytes
    file_fast_a.write_bytes(payload_same_len)
    os.utime(file_fast_a, ns=(safe_mtime_ns, safe_mtime_ns))
    shared_fast_hash = compute_v1_fast_file_hash(len(payload_same_len), safe_mtime_ns)

    # Overwrite file_fast_b with different size/mtime so its fast hash no longer matches shared_fast_hash
    file_fast_b.write_bytes(b"overwritten-with-different-size-and-content")
    os.utime(
        file_fast_b,
        ns=(safe_mtime_ns + 50_000_000_000, safe_mtime_ns + 50_000_000_000),
    )
    id_fast_match_a = uuid.uuid4()
    id_overwritten_b = uuid.uuid4()

    # 4c. Container member artifacts (HDF5 table_path & OMX array_path) matching fast stat hash
    h5_file = tmp_path / "container.h5"
    df_zones = pd.DataFrame({"zone_id": [1, 2], "pop": [100, 250]})
    df_zones.to_hdf(h5_file, key="/zones", mode="w", format="table")
    with h5py.File(h5_file, "a") as h5f:
        mat_grp = h5f.create_group("data")
        mat_ds = mat_grp.create_dataset(
            "skims",
            data=np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64),
        )
        mat_ds.attrs["NA"] = -1.0
    os.utime(h5_file, ns=(safe_mtime_ns, safe_mtime_ns))
    h5_fast_hash = compute_v1_fast_file_hash(h5_file.stat().st_size, safe_mtime_ns)
    id_h5_table = uuid.uuid4()
    id_omx_array = uuid.uuid4()

    # 4d. racy_fast_stat: 0 <= (created_at_ns - mtime_ns) < RACY_STAT_WINDOW_NS (2s)
    file_racy = tmp_path / "racy.bin"
    file_racy.write_bytes(b"racy-lustre-payload")
    os.utime(file_racy, ns=(safe_mtime_ns, safe_mtime_ns))
    racy_fast_hash = compute_v1_fast_file_hash(file_racy.stat().st_size, safe_mtime_ns)
    racy_created_at = datetime.fromtimestamp(
        (safe_mtime_ns + (RACY_STAT_WINDOW_NS // 2)) / 1_000_000_000,
        tz=timezone.utc,
    )
    id_racy = uuid.uuid4()

    # 5a. missing_cold: file does not exist on disk
    missing_path = tmp_path / "deleted_cold.parquet"
    id_missing = uuid.uuid4()

    # 5b. unhashed_v1: v1 row has hash=NULL
    id_unhashed = uuid.uuid4()

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, signature, started_at, created_at, updated_at) "
            "VALUES ('run_verified', 'activitysim', 'completed', 'sig_ver', ?, ?, ?), "
            "       ('run_mixed', 'activitysim', 'completed', 'sig_mix', ?, ?, ?)",
            [
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
            ],
        )

        # run_artifact_dir_entry table for id_dir_manifest
        con.execute(
            "CREATE TABLE run_artifact_dir_entry ("
            "artifact_id VARCHAR, run_id VARCHAR, rel_path VARCHAR, kind VARCHAR, "
            "size BIGINT, sha256 VARCHAR, mtime_ns BIGINT"
            ")"
        )
        con.execute(
            "INSERT INTO run_artifact_dir_entry VALUES (?, 'run_verified', 'subdir/part-0.bin', 'file', ?, ?, 123)",
            [str(id_dir_manifest), len(entry_bytes), entry_sha],
        )

        # global_tables for id_dlt
        con.execute("CREATE SCHEMA global_tables")
        con.execute(
            "CREATE TABLE global_tables._dlt_loads (load_id VARCHAR, status INTEGER)"
        )
        con.execute("INSERT INTO global_tables._dlt_loads VALUES ('load_42', 0)")
        con.execute(
            "CREATE TABLE global_tables.households ("
            "hh_id BIGINT, cars BIGINT, consist_artifact_id VARCHAR, consist_run_id VARCHAR, _dlt_load_id VARCHAR"
            ")"
        )
        con.execute(
            "INSERT INTO global_tables.households VALUES (10, 2, ?, 'run_verified', 'load_42'), (20, 1, ?, 'run_verified', 'load_42')",
            [str(id_dlt), str(id_dlt)],
        )

        artifacts_to_insert = [
            (
                str(id_dir_manifest),
                "dir_bundle",
                str(tmp_path / "missing_dir_on_disk"),
                "directory",
                "a" * 64,
                None,
                None,
                "run_verified",
                json.dumps({"is_dir": True}),
                safe_created_at,
            ),
            (
                str(id_dlt),
                "households",
                "db://households",
                "dlt",
                None,
                None,
                None,
                "run_verified",
                "{}",
                safe_created_at,
            ),
            (
                str(id_full_reverified),
                "full_ok",
                str(file_full_ok),
                "file",
                full_ok_sha,
                None,
                None,
                "run_verified",
                json.dumps({"hash_semantics": "full", "user_tag": "keep_me"}),
                safe_created_at,
            ),
            (
                str(id_full_overwritten),
                "full_overwritten",
                str(file_full_overwritten),
                "file",
                original_full_sha,
                None,
                None,
                "run_verified",
                json.dumps({"hashing_strategy": "full"}),
                safe_created_at,
            ),
            (
                str(id_fast_match_a),
                "fast_a",
                str(file_fast_a),
                "file",
                shared_fast_hash,
                None,
                None,
                "run_verified",
                "{}",
                safe_created_at,
            ),
            (
                str(id_overwritten_b),
                "fast_b",
                str(file_fast_b),
                "file",
                shared_fast_hash,
                None,
                None,
                "run_mixed",
                "{}",
                safe_created_at,
            ),
            (
                str(id_h5_table),
                "zones_tbl",
                str(h5_file),
                "h5_table",
                h5_fast_hash,
                "/zones",
                None,
                "run_verified",
                "{}",
                safe_created_at,
            ),
            (
                str(id_omx_array),
                "skims_mat",
                str(h5_file),
                "omx",
                h5_fast_hash,
                None,
                "/data/skims",
                "run_verified",
                "{}",
                safe_created_at,
            ),
            (
                str(id_racy),
                "racy_art",
                str(file_racy),
                "file",
                racy_fast_hash,
                None,
                None,
                "run_mixed",
                "{}",
                racy_created_at,
            ),
            (
                str(id_missing),
                "missing_art",
                str(missing_path),
                "parquet",
                "b" * 64,
                None,
                None,
                "run_mixed",
                "{}",
                safe_created_at,
            ),
            (
                str(id_unhashed),
                "unhashed_art",
                "",
                "other",
                None,
                None,
                None,
                "run_mixed",
                "{}",
                safe_created_at,
            ),
        ]
        for row in artifacts_to_insert:
            con.execute(
                "INSERT INTO artifact ("
                "id, key, container_uri, driver, hash, table_path, array_path, run_id, meta, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(row),
            )
            con.execute(
                "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
                "VALUES (?, ?, 'output', false)",
                [row[7], row[0]],
            )

    # First check --quick planning (`quick_deferred`)
    quick_plan = plan_migration(db_path, quick=True)
    quick_by_id = {c.artifact_id: c for c in quick_plan.classifications}
    assert quick_by_id[id_dir_manifest].basis == "directory_manifest"
    assert quick_by_id[id_dlt].basis == "dlt_hot_table"
    assert quick_by_id[id_full_reverified].basis == "v1_full"
    assert quick_by_id[id_full_overwritten].basis == "v1_full"
    assert quick_by_id[id_fast_match_a].basis == "quick_deferred"
    assert quick_by_id[id_overwritten_b].basis == "quick_deferred"
    assert quick_by_id[id_racy].basis == "quick_deferred"
    assert quick_by_id[id_missing].basis == "quick_deferred"
    assert quick_by_id[id_unhashed].basis == "unhashed_v1"

    # Now run full migration (`quick=False`)
    result = migrate_v1_to_v2(db_path, quick=False, workers=2)
    assert result.verification.ok is True
    assert result.backup_path is not None and result.backup_path.exists()

    full_by_id = {c.artifact_id: c for c in result.plan.classifications}
    assert full_by_id[id_dir_manifest].basis == "directory_manifest"
    assert full_by_id[id_dir_manifest].outcome == "verified"
    assert full_by_id[id_dir_manifest].anchor.scheme == "sha256:manifest-v1"

    assert full_by_id[id_dlt].basis == "dlt_hot_table"
    assert full_by_id[id_dlt].outcome == "verified"
    expected_dlt_digest = compute_ctd1_digest(
        pd.DataFrame({"hh_id": [10, 20], "cars": [2, 1]}),
        columns=["hh_id", "cars"],
    )
    assert full_by_id[id_dlt].anchor == expected_dlt_digest

    assert full_by_id[id_full_reverified].basis == "v1_full_reverified"
    assert full_by_id[id_full_reverified].anchor == Digest("sha256:file", full_ok_sha)

    assert full_by_id[id_full_overwritten].basis == "v1_full"
    assert full_by_id[id_full_overwritten].anchor == Digest(
        "sha256:file", original_full_sha
    )
    assert full_by_id[id_full_overwritten].resolved_path is None

    # Crucial test: file_fast_a and file_fast_b shared the same v1 fast hash,
    # but file_fast_a is verified while file_fast_b is historical (overwritten_cold)!
    assert full_by_id[id_fast_match_a].basis == "migrated_fast_match"
    assert full_by_id[id_fast_match_a].outcome == "verified"
    assert full_by_id[id_fast_match_a].anchor == Digest(
        "sha256:file", digest_bytes(payload_same_len)
    )

    assert full_by_id[id_overwritten_b].basis == "overwritten_cold"
    assert full_by_id[id_overwritten_b].outcome == "historical"
    assert full_by_id[id_overwritten_b].anchor == Digest(
        "legacy-v1:unverified", shared_fast_hash
    )
    assert full_by_id[id_overwritten_b].anchor != full_by_id[id_fast_match_a].anchor

    # Container members
    assert full_by_id[id_h5_table].basis == "migrated_fast_match"
    assert full_by_id[id_h5_table].anchor == compute_ctd1_digest(
        h5_file, member="/zones"
    )
    assert full_by_id[id_omx_array].basis == "migrated_fast_match"
    assert full_by_id[id_omx_array].anchor == compute_logical_matrix_digest(
        "skims",
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64),
        na_value=-1.0,
    )

    # Racy, missing, unhashed
    assert full_by_id[id_racy].basis == "racy_fast_stat"
    assert full_by_id[id_racy].anchor.kind is Kind.HISTORICAL
    assert full_by_id[id_missing].basis == "missing_cold"
    assert full_by_id[id_missing].anchor.kind is Kind.HISTORICAL
    assert full_by_id[id_unhashed].basis == "unhashed_v1"
    assert full_by_id[id_unhashed].anchor.kind is Kind.HISTORICAL

    # Inspect resulting v2 StoreLedger
    with StoreLedger(db_path, read_only=True) as ledger:
        # run_verified had only verified artifacts -> cache_status='live'
        # run_mixed had historical artifacts -> cache_status='historical', signature=NULL
        run_rows = dict(
            ledger.connection.execute(
                "SELECT id, cache_status FROM main.run"
            ).fetchall()
        )
        assert run_rows["run_verified"] == "live"
        assert run_rows["run_mixed"] == "historical"

        # Overwritten cold file location is marked evicted and never returned by locate()
        overwritten_art = ledger.load_artifact(id_overwritten_b)
        assert overwritten_art is not None
        with pytest.raises(FileNotFoundError):
            locate(overwritten_art, ledger=ledger)
        evicted_rows = ledger.query_location_current(
            anchor=overwritten_art.anchor,
            include_evicted=True,
        )
        assert len(evicted_rows) == 1
        assert evicted_rows[0]["status"] == "evicted"

        # Overwritten v1_full artifact preserves its verified anchor but refuses to locate the overwritten file
        full_over_art = ledger.load_artifact(id_full_overwritten)
        assert full_over_art is not None
        assert full_over_art.anchor == Digest("sha256:file", original_full_sha)
        with pytest.raises(FileNotFoundError):
            locate(full_over_art, ledger=ledger)

        # System metadata keys were stripped from artifact.meta while user keys were kept
        full_ok_art = ledger.load_artifact(id_full_reverified)
        assert full_ok_art is not None
        assert full_ok_art.meta.get("user_tag") == "keep_me"
        assert "hash_semantics" not in full_ok_art.meta


def test_pass_through_artifact_link_parity_and_hybrid_view_on_migrated_v2_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v1_passthrough_and_views.duckdb"
    _init_minimal_v1_db(db_path)

    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_created_at = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )

    cold_parquet = tmp_path / "cold_trips.parquet"
    pd.DataFrame({"trip_id": [1, 2], "dist": [3.5, 7.0]}).to_parquet(
        cold_parquet, index=False
    )
    os.utime(cold_parquet, ns=(safe_mtime_ns, safe_mtime_ns))
    cold_fast_hash = compute_v1_fast_file_hash(
        cold_parquet.stat().st_size, safe_mtime_ns
    )

    id_passthrough = uuid.uuid4()
    id_hot = uuid.uuid4()

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, year, iteration, started_at, created_at, updated_at) "
            "VALUES ('r_pass', 'beam', 'completed', 2025, 1, ?, ?, ?), "
            "       ('r_hot', 'beam', 'completed', 2026, 2, ?, ?, ?)",
            [
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
                safe_created_at,
            ],
        )
        # id_passthrough has run_id='r_pass' (produced by r_pass) AND direction='input' in run_artifact_link
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'trips', ?, 'parquet', ?, 'r_pass', '{}', ?), "
            "       (?, 'trips', 'db://trips', 'parquet', NULL, 'r_hot', '{\"is_ingested\": true}', ?)",
            [
                str(id_passthrough),
                str(cold_parquet),
                cold_fast_hash,
                safe_created_at,
                str(id_hot),
                safe_created_at,
            ],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('r_pass', ?, 'input', false), "
            "       ('r_hot', ?, 'output', false)",
            [str(id_passthrough), str(id_hot)],
        )
        con.execute("CREATE SCHEMA global_tables")
        con.execute(
            "CREATE TABLE global_tables.trips ("
            "trip_id BIGINT, dist DOUBLE, consist_run_id VARCHAR, consist_artifact_id VARCHAR, "
            "consist_year INTEGER, consist_iteration INTEGER"
            ")"
        )
        con.execute(
            "INSERT INTO global_tables.trips VALUES (3, 9.5, 'r_hot', ?, 2026, 2)",
            [str(id_hot)],
        )

    res = migrate_v1_to_v2(db_path)
    assert res.verification.ok is True
    assert res.verification.link_triples_match is True

    with StoreLedger(db_path, read_only=True) as ledger:
        role_row = ledger.connection.execute(
            "SELECT is_input, is_output FROM main.run_artifact_role "
            "WHERE run_id = 'r_pass' AND artifact_id = ?",
            [id_passthrough],
        ).fetchone()
        assert role_row == (True, True)

    # Verify ViewFactory.create_hybrid_view works on the migrated v2 database!
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(db_path),
        auto_migrate=True,
    )
    vf = ViewFactory(tracker)
    assert vf.create_hybrid_view("v_trips_hybrid", "trips") is True
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            "SELECT trip_id, dist, consist_run_id FROM v_trips_hybrid ORDER BY trip_id"
        ).fetchall()
    assert rows == [
        (1, 3.5, "r_pass"),
        (2, 7.0, "r_pass"),
        (3, 9.5, "r_hot"),
    ]


def test_backup_undo_and_force_guard_when_new_v2_runs_recorded(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "undo_test.duckdb"
    _init_minimal_v1_db(db_path)

    now = datetime.now(timezone.utc)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, started_at, created_at, updated_at) "
            "VALUES ('v1_run_1', 'm', 'completed', ?, ?, ?)",
            [now, now, now],
        )

    res = migrate_v1_to_v2(db_path, quick=True)
    assert res.backup_path is not None and res.backup_path.exists()
    assert detect_schema(db_path) is SchemaState.V2

    # Record a new v2 run after migration
    with StoreLedger(db_path) as ledger:
        ledger.record_run(run_id="v2_new_run", model_name="m2", status="completed")

    # Without force=True, undo_migration refuses to lose v2_new_run
    with pytest.raises(ConsistError, match="new v2 run"):
        undo_migration(db_path, force=False)

    # With force=True, undo_migration restores the v1 backup cleanly
    restored = undo_migration(db_path, force=True)
    assert restored == res.backup_path
    assert detect_schema(db_path) is SchemaState.V1


def test_preflight_wal_checkpoint_disk_space_and_stale_lock_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert default_migration_workers() >= 1
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "6")
    assert default_migration_workers() == 6
    monkeypatch.delenv("SLURM_CPUS_PER_TASK", raising=False)

    db_path = tmp_path / "preflight.duckdb"
    _init_minimal_v1_db(db_path)

    # 1. Non-empty .wal file without --checkpoint raises ConsistError
    wal_path = Path(f"{db_path}.wal")
    wal_path.write_bytes(b"unflushed-wal-bytes")
    with pytest.raises(ConsistError, match="WAL"):
        preflight_check_migration(db_path, checkpoint=False)
    wal_path.unlink()

    # 2. Insufficient free disk space raises ConsistError
    DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _p: DiskUsage(total=1000, used=999, free=1),
    )
    with pytest.raises(ConsistError, match="Insufficient free disk space"):
        preflight_check_migration(db_path)
    monkeypatch.undo()

    # 3. Stale lockfile from dead local PID is automatically cleaned up
    lock_path = db_path.with_name(f".{db_path.name}.migrate.lock")
    lock_path.write_text(
        json.dumps(
            {
                "hostname": os.uname().nodename,
                "pid": 999_999_999,
                "started_at_ns": 1,
            }
        ),
        encoding="utf-8",
    )
    with _migration_lock_context(db_path, wait_seconds=1.0) as acquired:
        assert acquired == lock_path
        assert lock_path.exists()
    assert not lock_path.exists()


def test_promote_historical_artifact_fast_recomputed_expected_digest_and_asserted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promote.duckdb"
    _init_minimal_v1_db(db_path)

    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_created_at = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )

    # Create 3 files that are "missing" during --quick migration (or moved to archive),
    # then restored and promoted using the 3 promotion bases.
    restored_a = tmp_path / "archive" / "restored_a.txt"
    restored_b = tmp_path / "archive" / "restored_b.txt"
    restored_c = tmp_path / "archive" / "restored_c.txt"
    restored_a.parent.mkdir(parents=True, exist_ok=True)

    restored_a.write_bytes(b"restored-payload-a")
    os.utime(restored_a, ns=(safe_mtime_ns, safe_mtime_ns))
    fast_hash_a = compute_v1_fast_file_hash(restored_a.stat().st_size, safe_mtime_ns)
    sha_a = file_sha256(restored_a)

    restored_b.write_bytes(b"restored-payload-b-mtime-changed")
    sha_b = file_sha256(restored_b)
    fast_hash_b_old = "c" * 64

    restored_c.write_bytes(b"restored-payload-c-asserted")
    sha_c = file_sha256(restored_c)
    fast_hash_c_old = "d" * 64

    id_a = uuid.uuid4()
    id_b = uuid.uuid4()
    id_c = uuid.uuid4()

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, started_at, created_at, updated_at) "
            "VALUES ('r_hist', 'urbansim', 'completed', ?, ?, ?)",
            [safe_created_at, safe_created_at, safe_created_at],
        )
        for art_id, key, fast_h in (
            (id_a, "art_a", fast_hash_a),
            (id_b, "art_b", fast_hash_b_old),
            (id_c, "art_c", fast_hash_c_old),
        ):
            con.execute(
                "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
                "VALUES (?, ?, ?, 'file', ?, 'r_hist', '{}', ?)",
                [
                    str(art_id),
                    key,
                    str(tmp_path / "missing_dir" / f"{key}.txt"),
                    fast_h,
                    safe_created_at,
                ],
            )

    migrate_v1_to_v2(db_path, quick=True)

    # 1. Promote id_a via v1_fast_recomputed (stat_v1_fast_file_hash matches original v1.hash)
    hist_a, ver_a, basis_a = promote_historical_artifact(
        db_path,
        id_a,
        path=restored_a,
    )
    assert basis_a == "v1_fast_recomputed"
    assert hist_a == Digest("legacy-v1:unverified", fast_hash_a)
    assert ver_a == Digest("sha256:file", sha_a)

    with StoreLedger(db_path, read_only=True) as ledger:
        art_a = ledger.load_artifact(id_a)
        assert art_a is not None
        assert art_a.identity.anchor == ver_a
        assert art_a.recorded_anchor == hist_a
        assert locate(art_a, ledger=ledger).path == restored_a.resolve()

    # 2. Promote id_b via caller_supplied_match (--expected-digest)
    with pytest.raises(ConsistError, match="digest mismatch"):
        promote_historical_artifact(
            db_path,
            "art_b",
            path=restored_b,
            expected_digest=f"sha256:file:{'0' * 64}",
        )
    hist_b, ver_b, basis_b = promote_historical_artifact(
        db_path,
        "art_b",
        path=restored_b,
        expected_digest=f"sha256:file:{sha_b}",
    )
    assert basis_b == "caller_supplied_match"
    assert ver_b == Digest("sha256:file", sha_b)

    # 3. Promote id_c: fails without --asserted because mtime/size does not match fast_hash_c_old
    with pytest.raises(ConsistError, match="--asserted"):
        promote_historical_artifact(
            db_path,
            f"legacy-v1:unverified:{fast_hash_c_old}",
            path=restored_c,
            asserted=False,
        )
    hist_c, ver_c, basis_c = promote_historical_artifact(
        db_path,
        f"legacy-v1:unverified:{fast_hash_c_old}",
        path=restored_c,
        asserted=True,
        asserted_by="alice",
    )
    assert basis_c == "asserted"
    assert ver_c == Digest("sha256:file", sha_c)

    with StoreLedger(db_path, read_only=True) as ledger:
        art_c = ledger.load_artifact(id_c)
        assert art_c is not None
        # Asserted promotion keeps artifact.identity.anchor as Kind.HISTORICAL while allowing locate()!
        assert art_c.identity.anchor == hist_c
        assert art_c.identity.anchor.kind is Kind.HISTORICAL
        assert locate(art_c, ledger=ledger).path == restored_c.resolve()


def test_open_v2_store_auto_migrate_and_cli_commands(tmp_path: Path) -> None:
    db_path = tmp_path / "cli_migrate.duckdb"
    _init_minimal_v1_db(db_path)

    sample_file = tmp_path / "input.csv"
    sample_file.write_text("id,val\n1,10\n", encoding="utf-8")
    sample_sha = file_sha256(sample_file)
    art_id = uuid.uuid4()

    now = datetime.now(timezone.utc)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, started_at, created_at, updated_at) "
            "VALUES ('r1', 'demo', 'completed', ?, ?, ?)",
            [now, now, now],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'input_csv', ?, 'csv', ?, 'r1', '{}', ?)",
            [str(art_id), str(sample_file), "e" * 64, now],
        )

    # 1. open_v2_store(auto_migrate=False) and Tracker(..., auto_migrate=False) raise DatabaseMigrationRequired
    with pytest.raises(DatabaseMigrationRequired) as exc_info:
        open_v2_store(db_path, auto_migrate=False)
    assert exc_info.value.run_count == 1
    assert exc_info.value.artifact_count == 1

    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=tmp_path / "runs", db_path=str(db_path), auto_migrate=False)

    # 2. CLI --dry-run leaves database on v1
    runner = CliRunner()
    res_dry = runner.invoke(app, ["migrate", str(db_path), "--dry-run"])
    assert res_dry.exit_code == 0, res_dry.output
    assert "DRY RUN" in res_dry.output
    assert detect_schema(db_path) is SchemaState.V1

    # 3. CLI --quick upgrades database to v2
    res_quick = runner.invoke(app, ["migrate", str(db_path), "--quick"])
    assert res_quick.exit_code == 0, res_quick.output
    assert "QUICK MIGRATION" in res_quick.output
    assert detect_schema(db_path) is SchemaState.V2

    # 4. CLI promote upgrades the deferred historical artifact
    res_promo = runner.invoke(
        app,
        [
            "promote",
            str(db_path),
            "input_csv",
            "--path",
            str(sample_file),
            "--expected-digest",
            f"sha256:file:{sample_sha}",
        ],
    )
    assert res_promo.exit_code == 0, res_promo.output
    assert "caller_supplied_match" in res_promo.output

    # 5. CLI `consist migrate undo <db>` restores v1
    res_undo = runner.invoke(app, ["migrate", "undo", str(db_path)])
    assert res_undo.exit_code == 0, res_undo.output
    assert detect_schema(db_path) is SchemaState.V1

    # 6. open_v2_store(auto_migrate="quick") automatically upgrades to v2 and returns a StoreLedger
    with open_v2_store(db_path, auto_migrate="quick") as ledger:
        assert detect_schema(db_path) is SchemaState.V2
        loaded = ledger.load_artifact(art_id)
        assert loaded is not None
    assert verify_migration(
        next(tmp_path.glob("cli_migrate.v1-backup-*.duckdb")),
        db_path,
    ).ok


def test_quick_migration_v1_full_records_declared_observation_for_locate(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quick_v1_full.duckdb"
    _init_minimal_v1_db(db_path)

    intact_file = tmp_path / "intact.txt"
    intact_file.write_bytes(b"intact-v1-full-payload")
    intact_sha = file_sha256(intact_file)
    id_intact = uuid.uuid4()

    overwritten_file = tmp_path / "overwritten.txt"
    original_sha = digest_bytes(b"original-v1-full-payload")
    overwritten_file.write_bytes(b"corrupted-overwritten-payload")
    id_overwritten = uuid.uuid4()

    now = datetime.now(timezone.utc)
    full_meta = json.dumps({"hashing_strategy": "full"})
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, started_at, created_at, updated_at) "
            "VALUES ('r_quick', 'demo', 'completed', ?, ?, ?)",
            [now, now, now],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'intact_full', ?, 'file', ?, 'r_quick', ?, ?), "
            "       (?, 'overwritten_full', ?, 'file', ?, 'r_quick', ?, ?)",
            [
                str(id_intact),
                str(intact_file),
                intact_sha,
                full_meta,
                now,
                str(id_overwritten),
                str(overwritten_file),
                original_sha,
                full_meta,
                now,
            ],
        )

    res = migrate_v1_to_v2(db_path, quick=True)
    assert res.verification.ok is True
    assert res.plan.counts_by_basis.get("v1_full") == 2

    with StoreLedger(db_path, read_only=True) as ledger:
        art_intact = ledger.load_artifact(id_intact)
        assert art_intact is not None
        assert art_intact.identity.anchor == Digest("sha256:file", intact_sha)
        assert locate(art_intact, ledger=ledger).path == intact_file.resolve()

        art_overwritten = ledger.load_artifact(id_overwritten)
        assert art_overwritten is not None
        assert art_overwritten.identity.anchor == Digest("sha256:file", original_sha)
        with pytest.raises(FileNotFoundError):
            locate(art_overwritten, ledger=ledger)


def test_h5_table_sibling_tables_classified_as_v1_full_reverified(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "h5_siblings.duckdb"
    _init_minimal_v1_db(db_path)

    h5_path = tmp_path / "model_store.h5"
    df_households = pd.DataFrame(
        {"hh_id": [1, 2, 3], "income": [50_000, 75_000, 90_000]}
    )
    df_persons = pd.DataFrame({"person_id": [10, 20], "hh_id": [1, 2], "age": [34, 41]})
    df_households.to_hdf(h5_path, key="/2010/households", mode="w", format="fixed")
    df_persons.to_hdf(h5_path, key="/2010/persons", mode="a", format="fixed")

    with h5py.File(h5_path, "r") as h5f:
        hh_table_hash = _hash_h5_dataset(h5f["/2010/households"], chunk_rows=None)
        p_table_hash = _hash_h5_dataset(h5f["/2010/persons"], chunk_rows=None)
    assert hh_table_hash is not None
    assert p_table_hash is not None

    # Set container mtime well in the past, and simulate sibling table 1 being logged
    # BEFORE sibling table 2 modified the same .h5 container later in the run.
    safe_mtime_ns = 1_700_000_000_000_000_000
    os.utime(h5_path, ns=(safe_mtime_ns, safe_mtime_ns))
    created_at_t1 = datetime.fromtimestamp(
        (safe_mtime_ns - 5_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )
    created_at_t2 = datetime.fromtimestamp(
        (safe_mtime_ns + 5_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )

    id_hh = uuid.uuid4()
    id_persons = uuid.uuid4()

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, started_at, created_at, updated_at) "
            "VALUES ('r_h5', 'urbansim', 'completed', ?, ?, ?)",
            [created_at_t1, created_at_t1, created_at_t2],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, table_path, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'hh_2010', ?, '/2010/households', 'h5_table', ?, 'r_h5', ?, ?), "
            "       (?, 'persons_2010', ?, '/2010/persons', 'h5_table', NULL, 'r_h5', ?, ?)",
            [
                str(id_hh),
                str(h5_path),
                hh_table_hash,
                json.dumps({"table_hash": hh_table_hash, "table_hash_algo": "sha256"}),
                created_at_t1,
                str(id_persons),
                str(h5_path),
                json.dumps({"table_hash": p_table_hash, "table_hash_algo": "sha256"}),
                created_at_t2,
            ],
        )

    res = migrate_v1_to_v2(db_path, quick=False)
    assert res.verification.ok is True
    by_id = {c.artifact_id: c for c in res.plan.classifications}
    assert by_id[id_hh].outcome == "verified"
    assert by_id[id_hh].basis == "v1_full_reverified"
    assert by_id[id_hh].anchor == compute_ctd1_digest(df_households)
    assert by_id[id_persons].outcome == "verified"
    assert by_id[id_persons].basis == "v1_full_reverified"
    assert by_id[id_persons].anchor == compute_ctd1_digest(df_persons)


def test_promote_conflict_raises_and_view_factory_resolves_promoted_cold_artifact(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "promote_view.duckdb"
    _init_minimal_v1_db(db_path)

    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_created_at = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )

    restored_parquet = tmp_path / "archive" / "trips_restored.parquet"
    restored_parquet.parent.mkdir(parents=True, exist_ok=True)
    df_trips = pd.DataFrame({"trip_id": [10, 20], "dist": [4.2, 8.4]})
    df_trips.to_parquet(restored_parquet, index=False)
    os.utime(restored_parquet, ns=(safe_mtime_ns, safe_mtime_ns))
    fast_hash_trips = compute_v1_fast_file_hash(
        restored_parquet.stat().st_size,
        safe_mtime_ns,
    )

    conflicting_parquet = tmp_path / "archive" / "trips_conflicting.parquet"
    pd.DataFrame({"trip_id": [99], "dist": [99.9]}).to_parquet(
        conflicting_parquet,
        index=False,
    )

    id_trips = uuid.uuid4()
    missing_original_path = tmp_path / "missing_dir" / "trips.parquet"

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, year, iteration, started_at, created_at, updated_at) "
            "VALUES ('r_promo_view', 'activitysim', 'completed', 2025, 1, ?, ?, ?)",
            [safe_created_at, safe_created_at, safe_created_at],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'trips', ?, 'parquet', ?, 'r_promo_view', '{}', ?)",
            [
                str(id_trips),
                str(missing_original_path),
                fast_hash_trips,
                safe_created_at,
            ],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('r_promo_view', ?, 'output', false)",
            [str(id_trips)],
        )

    migrate_v1_to_v2(db_path, quick=True)

    # Promote historical cold artifact to restored_parquet
    hist_anchor, ver_anchor, basis = promote_historical_artifact(
        db_path,
        id_trips,
        path=restored_parquet,
    )
    assert basis == "v1_fast_recomputed"
    assert ver_anchor == Digest("sha256:file", file_sha256(restored_parquet))

    # Re-promoting the same historical anchor to the same digest is idempotent
    hist_same, ver_same, _ = promote_historical_artifact(
        db_path,
        str(hist_anchor),
        path=restored_parquet,
    )
    assert hist_same == hist_anchor
    assert ver_same == ver_anchor

    # Attempting to re-promote the already-promoted historical anchor to a conflicting digest raises ConsistError
    with pytest.raises(ConsistError, match="already promoted"):
        promote_historical_artifact(
            db_path,
            str(hist_anchor),
            path=conflicting_parquet,
            asserted=True,
            asserted_by="alice",
        )

    # ViewFactory resolves the promoted historical cold artifact's location!
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(db_path),
        auto_migrate=True,
    )
    vf = ViewFactory(tracker)
    assert vf.create_hybrid_view("v_trips_promoted", "trips") is True
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            "SELECT trip_id, dist, consist_run_id FROM v_trips_promoted ORDER BY trip_id"
        ).fetchall()
    assert rows == [
        (10, 4.2, "r_promo_view"),
        (20, 8.4, "r_promo_view"),
    ]


def test_pr4_modules_hygiene_no_getattr_or_ponytail() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    pr4_files = [
        repo_root / "src" / "consist" / "migrations" / "v1" / "migrator.py",
        repo_root / "src" / "consist" / "migrations" / "v1" / "reader.py",
        repo_root / "src" / "consist" / "migrations" / "v1" / "fasthash.py",
        repo_root / "src" / "consist" / "migrations" / "v1" / "__init__.py",
        repo_root / "src" / "consist" / "migrations" / "__init__.py",
        repo_root / "src" / "consist" / "store" / "ledger.py",
    ]
    forbidden_calls = {"getattr", "setattr", "hasattr"}
    for file_path in pr4_files:
        source = file_path.read_text(encoding="utf-8")
        assert "ponytail" not in source.lower(), f"Found ponytail in {file_path}"
        tree = ast.parse(source, filename=str(file_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_calls, (
                    f"Forbidden call {node.func.id}() in {file_path}:{node.lineno}"
                )
