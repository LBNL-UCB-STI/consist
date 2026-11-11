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
from tests.helpers.v1_db_builder import create_v1_schema


def _init_minimal_v1_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        create_v1_schema(con)


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
            "CREATE TABLE IF NOT EXISTS run_artifact_dir_entry ("
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


def test_phase2_fasthash_symmetric_racy_and_parent_symlink_directory(
    tmp_path: Path,
) -> None:
    from consist.migrations.v1.fasthash import (
        stat_v1_fast_directory_hash,
        stat_v1_fast_file_hash,
    )

    f = tmp_path / "sample.bin"
    f.write_bytes(b"0123456789")
    fixed_mtime = 1_700_000_000_000_000_000
    os.utime(f, ns=(fixed_mtime, fixed_mtime))

    _, _, racy_plus_1s = stat_v1_fast_file_hash(f, now_ns=fixed_mtime + 1_000_000_000)
    _, _, racy_minus_1s = stat_v1_fast_file_hash(f, now_ns=fixed_mtime - 1_000_000_000)
    _, _, safe_plus_3s = stat_v1_fast_file_hash(f, now_ns=fixed_mtime + 3_000_000_000)
    _, _, safe_minus_3s = stat_v1_fast_file_hash(f, now_ns=fixed_mtime - 3_000_000_000)
    assert racy_plus_1s is True
    assert racy_minus_1s is True
    assert safe_plus_3s is False
    assert safe_minus_3s is False

    real_parent = tmp_path / "real_parent"
    real_dir = real_parent / "bundle"
    real_dir.mkdir(parents=True)
    leaf = real_dir / "part.txt"
    leaf.write_bytes(b"leaf-content")
    os.utime(leaf, ns=(fixed_mtime, fixed_mtime))
    os.utime(real_dir, ns=(fixed_mtime, fixed_mtime))

    sym_parent = tmp_path / "sym_parent"
    sym_parent.symlink_to(real_parent, target_is_directory=True)

    direct_stat = stat_v1_fast_directory_hash(
        real_dir, now_ns=fixed_mtime + 5_000_000_000
    )
    via_parent_sym = stat_v1_fast_directory_hash(
        sym_parent / "bundle", now_ns=fixed_mtime + 5_000_000_000
    )
    assert via_parent_sym.canonical_root == real_dir.resolve()
    assert via_parent_sym.legacy_fast_hex == direct_stat.legacy_fast_hex
    assert via_parent_sym.stat_v2_observation == direct_stat.stat_v2_observation
    assert via_parent_sym.is_racy is False

    # Symlinked root itself is rejected
    sym_root = tmp_path / "sym_bundle"
    sym_root.symlink_to(real_dir, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinked root"):
        stat_v1_fast_directory_hash(sym_root, now_ns=fixed_mtime + 5_000_000_000)

    # Inner symlink inside directory is rejected without following it
    (real_dir / "inner_link.txt").symlink_to(f)
    with pytest.raises(ValueError, match="Symlink encountered"):
        stat_v1_fast_directory_hash(real_dir, now_ns=fixed_mtime + 5_000_000_000)


def test_phase2_migrator_facets_operational_tables_ddl_scoping_and_run_promotion(
    tmp_path: Path,
) -> None:
    from consist.core.identity import compute_sig_v2
    from consist.location import Mount
    from consist.migrations.v1.migrator import (
        PromotionError,
        activate_migration,
        apply_migration,
        promote_historical_run,
    )
    from tests.helpers.v1_db_builder import (
        insert_v1_artifact,
        insert_v1_artifact_kv,
        insert_v1_binding_invocation,
        insert_v1_config_facet,
        insert_v1_coupler_snapshot,
        insert_v1_link,
        insert_v1_run,
        insert_v1_run_config_kv,
        insert_v1_schema_bundle,
    )

    ws_dir = tmp_path / "workspace"
    ws_dir.mkdir()
    snap_dir = tmp_path / "unrelated_snapshot"
    snap_dir.mkdir()

    # Decoy file in snapshot_dir sharing basename with a missing workspace file:
    # Phase 2 forbids basename-only candidate guessing!
    (snap_dir / "missing_same_name.csv").write_bytes(b"decoy-bytes")

    db_path = tmp_path / "phase2_mig.duckdb"
    _init_minimal_v1_db(db_path)

    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_dt = datetime(2025, 2, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Unhashed directory on disk -> must remain Kind.HISTORICAL (basis='unhashed_v1')
    unhashed_dir = ws_dir / "unhashed_bundle"
    unhashed_dir.mkdir()
    (unhashed_dir / "a.parquet").write_bytes(b"dir-bytes")

    # Historical quick-style / missing-at-mig-time input & output files for run promotion
    promo_in_file = ws_dir / "promo_in.csv"
    promo_out_file = ws_dir / "promo_out.csv"
    promo_in_bytes = b"id,val\n1,10\n"
    promo_out_bytes = b"id,val\n1,200\n"
    in_fast_hex = compute_v1_fast_file_hash(len(promo_in_bytes), safe_mtime_ns)
    out_fast_hex = compute_v1_fast_file_hash(len(promo_out_bytes), safe_mtime_ns)

    # Active racy file for racy_fast_stat promotion check
    racy_missing_id = uuid.uuid4()
    racy_body = b"15-byte-racy-ok"
    racy_fast_hex = compute_v1_fast_file_hash(len(racy_body), safe_mtime_ns)
    racy_recorded_dt = datetime.fromtimestamp(
        (safe_mtime_ns + 500_000_000) / 1_000_000_000, tz=timezone.utc
    )
    racy_file = ws_dir / "racy_temp.bin"
    racy_file.write_bytes(racy_body)
    os.utime(racy_file, ns=(safe_mtime_ns, safe_mtime_ns))

    id_unhashed_dir = uuid.uuid4()
    id_decoy_test = uuid.uuid4()
    id_ing_matched = uuid.uuid4()
    id_ing_ambig_1 = uuid.uuid4()
    id_ing_ambig_2 = uuid.uuid4()
    id_parent_art = uuid.uuid4()
    id_child_art = uuid.uuid4()
    id_promo_in = uuid.uuid4()
    id_promo_out = uuid.uuid4()

    with duckdb.connect(str(db_path)) as con:
        insert_v1_config_facet(
            con,
            facet_id="facet_sim_v2",
            namespace="activitysim",
            schema_name="SimConfig",
            schema_version="3",
            facet_json={"sample_rate": 0.25, "seed": 42},
            created_at=safe_dt,
        )
        insert_v1_run(
            con,
            run_id="run_rich",
            model_name="asim",
            status="completed",
            stage="assignment",
            phase="am_peak",
            config_hash="cfg_rich_1",
            git_hash="git_abc123",
            meta={"config_facet_id": "facet_sim_v2", "note": "rich"},
            created_at=safe_dt,
            started_at=safe_dt,
            ended_at=safe_dt,
            updated_at=safe_dt,
        )
        insert_v1_run_config_kv(
            con,
            run_id="run_rich",
            facet_id="facet_sim_v2",
            namespace="activitysim",
            key="iterations",
            value_type="int",
            value_num=5.0,
        )
        insert_v1_run(
            con,
            run_id="run_ambig_dlt",
            model_name="ingester",
            status="completed",
            created_at=safe_dt,
        )
        insert_v1_run(
            con,
            run_id="run_to_promote",
            model_name="calibrator",
            status="completed",
            config_hash="cfg_promo_hash",
            git_hash="code_promo_hash",
            created_at=safe_dt,
        )

        # Operational tables: coupler, binding invocation, artifact_schema*
        insert_v1_coupler_snapshot(
            con,
            run_id="run_rich",
            phase="am_peak",
            slot_key="skims",
            artifact_id=id_parent_art,
            step_ordinal=1,
            updated_at=safe_dt,
        )
        insert_v1_binding_invocation(
            con,
            requested_run_id="run_rich",
            execution_run_id="run_rich",
            cache_outcome="miss",
            binding_json=json.dumps({"inputs": ["in_a"], "outputs": ["skims"]}),
            created_at=safe_dt,
        )
        insert_v1_schema_bundle(
            con,
            schema_id="sch_hh_1",
            summary_json={"table": "households"},
            profile_json={"source": "pydantic"},
            fields=[
                {
                    "ordinal_position": 0,
                    "name": "hh_id",
                    "logical_type": "bigint",
                    "nullable": False,
                }
            ],
            relations=[
                {
                    "from_field": "hh_id",
                    "to_table": "zones",
                    "to_field": "zone_id",
                    "relationship_type": "many_to_one",
                }
            ],
            artifact_id=id_ing_matched,
            run_id="run_rich",
            source="dlt",
            created_at=safe_dt,
        )

        # User schemas with sequences, FK topological order, PK/NOT NULL, and secondary index
        con.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        con.execute("CREATE SCHEMA IF NOT EXISTS _dlt_meta")
        con.execute("CREATE SEQUENCE global_tables.seq_hh START 100")
        con.execute(
            """
            CREATE TABLE global_tables.zones_ref (
                zone_id BIGINT PRIMARY KEY,
                name VARCHAR NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE global_tables.households (
                hh_id BIGINT PRIMARY KEY DEFAULT nextval('global_tables.seq_hh'),
                zone_id BIGINT NOT NULL REFERENCES global_tables.zones_ref(zone_id),
                consist_run_id VARCHAR,
                consist_artifact_id VARCHAR
            )
            """
        )
        con.execute("CREATE INDEX idx_hh_zone ON global_tables.households (zone_id)")
        con.execute(
            """
            CREATE TABLE global_tables.trips_ambig (
                trip_id BIGINT NOT NULL,
                consist_run_id VARCHAR
            )
            """
        )
        con.execute("CREATE TABLE _dlt_meta._dlt_loads (load_id VARCHAR PRIMARY KEY)")
        con.execute("INSERT INTO global_tables.zones_ref VALUES (1, 'CBD')")
        con.execute(
            "INSERT INTO global_tables.households (zone_id, consist_run_id, consist_artifact_id) "
            "VALUES (1, 'run_rich', ?)",
            [str(id_ing_matched)],
        )
        con.execute(
            "INSERT INTO global_tables.trips_ambig VALUES (10, 'run_ambig_dlt'), (20, 'run_ambig_dlt')"
        )
        con.execute("INSERT INTO _dlt_meta._dlt_loads VALUES ('load_001')")

        # Artifacts
        insert_v1_artifact(
            con,
            artifact_id=id_unhashed_dir,
            key="unhashed_dir_key",
            container_uri="workspace://unhashed_bundle",
            driver="directory",
            hash_val=None,
            run_id="run_rich",
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_decoy_test,
            key="decoy_key",
            container_uri="workspace://sub/missing_same_name.csv",
            driver="csv",
            hash_val="f" * 64,
            run_id="run_rich",
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_ing_matched,
            key="households",
            container_uri="workspace://households.parquet",
            driver="parquet",
            hash_val="a" * 64,
            run_id="run_rich",
            meta={"is_ingested": True, "dlt_table_name": "households"},
            created_at=safe_dt,
        )
        insert_v1_artifact_kv(
            con,
            artifact_id=id_ing_matched,
            facet_id="af_hh",
            key_path="year",
            value_type="int",
            value_num=2030.0,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_parent_art,
            key="parent_container",
            container_uri="workspace://missing_parent.h5",
            driver="h5",
            hash_val="b" * 64,
            run_id="run_rich",
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_child_art,
            key="child_member",
            container_uri="workspace://missing_parent.h5",
            table_path="/skims",
            driver="h5_table",
            hash_val="c" * 64,
            run_id="run_rich",
            parent_artifact_id=id_parent_art,
            created_at=safe_dt,
        )
        # Two ingested artifacts in same run sharing table name without consist_artifact_id -> ambiguous!
        insert_v1_artifact(
            con,
            artifact_id=id_ing_ambig_1,
            key="trips_ambig_1",
            container_uri="workspace://t1.parquet",
            driver="parquet",
            hash_val="d" * 64,
            run_id="run_ambig_dlt",
            meta={"is_ingested": True, "dlt_table_name": "trips_ambig"},
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_ing_ambig_2,
            key="trips_ambig_2",
            container_uri="workspace://t2.parquet",
            driver="parquet",
            hash_val="e" * 64,
            run_id="run_ambig_dlt",
            meta={"is_ingested": True, "dlt_table_name": "trips_ambig"},
            created_at=safe_dt,
        )
        # Racy file at migration time -> basis='racy_fast_stat'
        insert_v1_artifact(
            con,
            artifact_id=racy_missing_id,
            key="racy_art",
            container_uri="workspace://racy_temp.bin",
            driver="bin",
            hash_val=racy_fast_hex,
            run_id="run_ambig_dlt",
            created_at=racy_recorded_dt,
        )
        # Historical input & output for run_to_promote (missing during migration, restored before promote)
        insert_v1_artifact(
            con,
            artifact_id=id_promo_in,
            key="promo_in",
            container_uri="workspace://promo_in.csv",
            driver="csv",
            hash_val=in_fast_hex,
            run_id=None,
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_promo_out,
            key="promo_out",
            container_uri="workspace://promo_out.csv",
            driver="csv",
            hash_val=out_fast_hex,
            run_id="run_to_promote",
            created_at=safe_dt,
        )
        insert_v1_link(
            con, run_id="run_to_promote", artifact_id=id_promo_in, direction="input"
        )
        insert_v1_link(
            con, run_id="run_to_promote", artifact_id=id_promo_out, direction="output"
        )

    mounts = [Mount("workspace", ws_dir)]
    plan = plan_migration(
        db_path,
        mounts=mounts,
        snapshot_dirs=[snap_dir],
    )
    cls_by_id = {c.artifact_id: c for c in plan.classifications}
    assert cls_by_id[id_unhashed_dir].outcome == "historical"
    assert cls_by_id[id_unhashed_dir].basis == "unhashed_v1"
    assert cls_by_id[id_unhashed_dir].anchor.kind is Kind.HISTORICAL
    # Decoy in snapshot_dir was NOT matched by basename
    assert cls_by_id[id_decoy_test].basis == "missing_cold"
    # Matched dlt table streamed ctd1:sha256; ambiguous dlt table stayed historical
    assert cls_by_id[id_ing_matched].basis == "dlt_hot_table"
    assert cls_by_id[id_ing_matched].anchor.scheme == "ctd1:sha256"
    assert cls_by_id[id_ing_ambig_1].outcome == "historical"
    assert cls_by_id[id_ing_ambig_2].outcome == "historical"
    assert cls_by_id[racy_missing_id].basis == "racy_fast_stat"

    # Test activate_migration unlinks stale .wal before os.replace
    v2_tmp = db_path.with_name(f"{db_path.name}.v2.tmp")
    apply_migration(db_path, v2_tmp, plan, mounts=mounts)
    ver = verify_migration(db_path, v2_tmp, plan=plan)
    assert ver.ok is True
    stale_wal = Path(f"{db_path}.wal")
    stale_wal.write_bytes(b"corrupt-stale-v1-wal")
    backup_path = activate_migration(db_path, v2_tmp)
    assert backup_path.exists()
    assert not stale_wal.exists()

    with StoreLedger(db_path, mounts=mounts) as ledger:
        # Facet + columns + operational tables migrated
        rich_run = ledger.get_run("run_rich")
        assert rich_run is not None
        assert rich_run.stage == "assignment"
        assert rich_run.phase == "am_peak"
        assert rich_run.git_hash == "git_abc123"
        assert rich_run.meta["facet_namespace"] == "activitysim"
        assert rich_run.meta["facet_schema_version"] == "3"
        facet_payload = rich_run.meta["facet"]
        assert facet_payload["sample_rate"] == 0.25
        assert facet_payload["seed"] == 42
        assert facet_payload["iterations"] == 5
        assert isinstance(facet_payload["iterations"], int)

        child_loaded = ledger.load_artifact(id_child_art)
        assert child_loaded is not None
        assert child_loaded.parent_artifact_id == id_parent_art

        matched_loaded = ledger.load_artifact(id_ing_matched)
        assert matched_loaded is not None
        assert matched_loaded.is_ingested is True
        assert "is_ingested" not in matched_loaded.meta
        assert matched_loaded.meta["facet"]["year"] == 2030
        assert isinstance(matched_loaded.meta["facet"]["year"], int)

        # Ambiguous ingested artifacts ALSO received active db://global_tables/trips_ambig observations
        ambig_loaded = ledger.load_artifact(id_ing_ambig_1)
        assert ambig_loaded is not None
        assert ambig_loaded.is_ingested is True

        assert len(ledger.get_coupler_snapshots("run_rich", phase="am_peak")) == 1
        assert len(ledger.list_binding_invocations(requested_run_id="run_rich")) == 1
        sch = ledger.get_artifact_schema("sch_hh_1")
        assert sch is not None and len(sch[1]) == 1

        # DDL replay preserved sequence, FK constraint, and secondary index
        with ledger.exec_cursor() as cur:
            next_seq = cur.execute("SELECT nextval('global_tables.seq_hh')").fetchone()
            assert next_seq is not None and int(next_seq[0]) >= 101
            idx_rows = cur.execute(
                "SELECT index_name FROM duckdb_indexes() "
                "WHERE schema_name = 'global_tables' AND table_name = 'households'"
            ).fetchall()
            assert ("idx_hh_zone",) in idx_rows
            with pytest.raises(duckdb.Error):
                cur.execute(
                    "INSERT INTO global_tables.households (zone_id) VALUES (999)"
                )

        # Unknown identifier raises PromotionError (a ValueError) without fabricating observations
        with ledger.exec_cursor() as cur:
            obs_count_before = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_location"
            ).fetchone()[0]
        for unknown_target in (
            str(uuid.uuid4()),
            "legacy-v1:unverified:" + "9" * 64,
            "nonexistent_run_or_key",
        ):
            with pytest.raises(ValueError):
                promote_historical_artifact(ledger, unknown_target, mounts=mounts)
        with ledger.exec_cursor() as cur:
            obs_count_after = cur.execute(
                "SELECT COUNT(*) FROM main.artifact_location"
            ).fetchone()[0]
        assert obs_count_after == obs_count_before

        # racy_fast_stat rejected if bytes are missing on disk even with asserted=True
        racy_file.unlink()
        with pytest.raises(PromotionError):
            promote_historical_artifact(
                ledger,
                racy_missing_id,
                asserted=True,
                asserted_by="tester",
                mounts=mounts,
            )
        # Once full bytes exist on disk (outside racy window), full content hash promotes it
        racy_file.write_bytes(racy_body)
        os.utime(racy_file, ns=(safe_mtime_ns, safe_mtime_ns))
        _, racy_ver, _ = promote_historical_artifact(
            ledger, racy_missing_id, mounts=mounts
        )
        assert racy_ver.scheme == "sha256:file"

        # Promote entire historical run via <run-id> -> computes sig-v2 and marks cache_status='live'
        promo_in_file.write_bytes(promo_in_bytes)
        promo_out_file.write_bytes(promo_out_bytes)
        os.utime(promo_in_file, ns=(safe_mtime_ns, safe_mtime_ns))
        os.utime(promo_out_file, ns=(safe_mtime_ns, safe_mtime_ns))

        promo_results = promote_historical_run(ledger, "run_to_promote", mounts=mounts)
        assert len(promo_results) == 2
        promoted_run = ledger.get_run("run_to_promote")
        assert promoted_run is not None
        with ledger.exec_cursor() as cur:
            status_row = cur.execute(
                "SELECT cache_status, signature_scheme FROM main.run WHERE id = 'run_to_promote'"
            ).fetchone()
        assert status_row == ("live", "sig-v2")

        expected_in_anchor = f"sha256:file:{file_sha256(promo_in_file)}"
        exp_input_hash, exp_sig = compute_sig_v2(
            "code_promo_hash",
            "cfg_promo_hash",
            [("promo_in", expected_in_anchor)],
        )
        assert promoted_run.input_hash == exp_input_hash
        assert promoted_run.signature == exp_sig
        cached_hit = ledger.find_cached_run(exp_sig)
        assert cached_hit is not None and cached_hit.id == "run_to_promote"


def test_phase2_audit_part_a2_a3_a7_and_b2_b3_b4_regressions(
    tmp_path: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from consist.core.identity import compute_sig_v2
    from consist.digest import digest_table
    from consist.location import Mount
    from consist.migrations.v1.fasthash import stat_v1_fast_directory_hash
    from consist.migrations.v1.migrator import (
        _stream_ctd1_hex_from_duckdb,
        _strip_catalog_prefix_from_ddl,
        promote_historical_run,
    )
    from consist.migrations.v1.reader import _extract_fk_target
    from tests.helpers.v1_db_builder import (
        insert_v1_artifact,
        insert_v1_link,
        insert_v1_run,
    )

    # --- Part A2: FLOAT/DOUBLE NaN/Inf/NULL ordering parity & nested LIST fallback ---
    float_vals = [
        float("inf"),
        1.0,
        float("nan"),
        None,
        -1.0,
        float("-inf"),
        0.0,
    ]
    for arrow_type, duck_type in ((pa.float32(), "FLOAT"), (pa.float64(), "DOUBLE")):
        tbl = pa.table(
            {
                "val": pa.array(float_vals, type=arrow_type),
                "tie": pa.array([7, 6, 5, 4, 3, 2, 1], type=pa.int64()),
            }
        )
        pq_path = tmp_path / f"nan_order_{duck_type.lower()}.parquet"
        pq.write_table(tbl, pq_path)
        pq_hex = digest_table(pq_path)

        with duckdb.connect(":memory:") as mem_con:
            mem_con.execute(f"CREATE TABLE t_flt (val {duck_type}, tie BIGINT)")
            for v, t in zip(float_vals, [7, 6, 5, 4, 3, 2, 1], strict=True):
                mem_con.execute("INSERT INTO t_flt VALUES (?, ?)", [v, t])
            streamed_hex = _stream_ctd1_hex_from_duckdb(
                mem_con,
                q_schema="main",
                q_table='"t_flt"',
                select_cols=["val", "tie"],
                column_types={"val": duck_type, "tie": "BIGINT"},
                where_sql="",
                params=[],
            )
            assert streamed_hex == Digest.parse(pq_hex).hex

            # Nested LIST column returns None cleanly instead of diverging from Parquet
            mem_con.execute("CREATE TABLE t_nested (id BIGINT, tags BIGINT[])")
            mem_con.execute("INSERT INTO t_nested VALUES (1, [10, 20])")
            assert (
                _stream_ctd1_hex_from_duckdb(
                    mem_con,
                    q_schema="main",
                    q_table='"t_nested"',
                    select_cols=["id", "tags"],
                    column_types={"id": "BIGINT", "tags": "BIGINT[]"},
                    where_sql="",
                    params=[],
                )
                is None
            )

    # digest_table presorted=True skips sort_by on pa.Table, and handles pandas index columns on RecordBatchReader
    unsorted_pa = pa.table({"k": [2, 1], "v": ["b", "a"]})
    sorted_pa = pa.table({"k": [1, 2], "v": ["a", "b"]})
    assert digest_table(unsorted_pa, presorted=False) == digest_table(
        sorted_pa, presorted=True
    )
    assert digest_table(unsorted_pa, presorted=True) != digest_table(
        sorted_pa, presorted=True
    )

    df_indexed = pd.DataFrame({"val": [10, 20]}, index=pd.Index([100, 200], name="idx"))
    tbl_with_idx = pa.Table.from_pandas(df_indexed, preserve_index=True)
    reader_with_idx = pa.RecordBatchReader.from_batches(
        tbl_with_idx.schema, tbl_with_idx.to_batches()
    )
    assert digest_table(reader_with_idx, presorted=True) == digest_table(
        tbl_with_idx, presorted=True
    )

    # --- Part A3: Multi-dot DB catalog prefix, sequence regex no-backtracking, CHECK string literal safety, escaped "" FK ---
    rewritten_sql = _strip_catalog_prefix_from_ddl(
        'CREATE TABLE "my-db"."global_tables"."hh" ('
        'id BIGINT DEFAULT nextval(\'"my-db"."global_tables"."seq_hh"\'), '
        "tag VARCHAR CHECK (tag <> 'my-db.global_tables.literal') -- my-db.global_tables.comment\n"
        ")",
        catalog_names=["my-db"],
        known_schemas=["global_tables"],
    )
    assert '"my-db"."global_tables"."hh"' not in rewritten_sql
    assert '"global_tables"."hh"' in rewritten_sql
    assert 'nextval(\'"global_tables"."seq_hh"\')' in rewritten_sql
    assert "'my-db.global_tables.literal'" in rewritten_sql
    assert _extract_fk_target(
        'FOREIGN KEY (pid) REFERENCES "my""ns"."my""parent"("id""col")',
        default_schema="global_tables",
    ) == ('my"ns', 'my"parent')

    multi_dot_db = tmp_path / "sim.2025.duckdb"
    _init_minimal_v1_db(multi_dot_db)
    ws_dir = tmp_path / "ws_a3"
    ws_dir.mkdir()
    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_dt = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000, tz=timezone.utc
    )

    id_list_dlt = uuid.uuid4()
    id_dep_art = uuid.uuid4()
    dep_file = ws_dir / "dep_cfg.json"
    dep_bytes = b'{"mode":"transit"}'
    dep_file.write_bytes(dep_bytes)
    os.utime(dep_file, ns=(safe_mtime_ns, safe_mtime_ns))
    dep_fast = compute_v1_fast_file_hash(len(dep_bytes), safe_mtime_ns)

    id_p_in = uuid.uuid4()
    p_in_file = ws_dir / "run_a_in.txt"
    p_in_bytes = b"run-a-input-payload"
    p_in_fast = compute_v1_fast_file_hash(len(p_in_bytes), safe_mtime_ns)

    with duckdb.connect(str(multi_dot_db)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        con.execute("CREATE SEQUENCE global_tables.seq_hh START 100")
        con.execute(
            'CREATE TABLE global_tables.zones_parent ("id""col" BIGINT PRIMARY KEY)'
        )
        con.execute(
            "CREATE TABLE global_tables.households ("
            "id BIGINT PRIMARY KEY DEFAULT nextval('global_tables.seq_hh'), "
            'parent_id BIGINT REFERENCES global_tables.zones_parent("id""col"), '
            "tag VARCHAR CHECK (tag <> 'sim.global_tables.keep_me')"
            ")"
        )
        con.execute("INSERT INTO global_tables.zones_parent VALUES (7)")
        con.execute(
            "INSERT INTO global_tables.households (parent_id, tag) VALUES (7, 'ok')"
        )
        # DLT table with LIST column -> stays historical cleanly
        con.execute(
            "CREATE TABLE global_tables.nested_trips ("
            "trip_id BIGINT, modes VARCHAR[], consist_run_id VARCHAR, consist_artifact_id VARCHAR"
            ")"
        )
        con.execute(
            "INSERT INTO global_tables.nested_trips VALUES (1, ['walk', 'bus'], 'run_nested', ?)",
            [str(id_list_dlt)],
        )

        insert_v1_run(
            con,
            run_id="run_nested",
            model_name="nested_sim",
            status="completed",
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_list_dlt,
            key="nested_trips",
            container_uri="db://global_tables/nested_trips",
            driver="duckdb",
            hash_val="9" * 64,
            run_id="run_nested",
            meta={"is_ingested": True, "dlt_table_name": "nested_trips"},
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_dep_art,
            key="dep_cfg",
            container_uri="workspace://dep_cfg.json",
            driver="json",
            hash_val=dep_fast,
            run_id=None,
            created_at=safe_dt,
        )
        insert_v1_link(
            con,
            run_id="run_nested",
            artifact_id=id_dep_art,
            direction="depends_on",
        )

        # Part A7 setup: run_a (missing input at mig time, promoted later) vs unrelated 0-artifact marker run_b
        insert_v1_run(
            con,
            run_id="run_a",
            model_name="model_a",
            status="completed",
            config_hash="cfg_a",
            git_hash="git_a",
            created_at=safe_dt,
        )
        insert_v1_artifact(
            con,
            artifact_id=id_p_in,
            key="in_slot",
            container_uri="workspace://run_a_in.txt",
            driver="txt",
            hash_val=p_in_fast,
            run_id=None,
            created_at=safe_dt,
        )
        insert_v1_link(con, run_id="run_a", artifact_id=id_p_in, direction="input")
        insert_v1_run(
            con,
            run_id="run_b_empty_marker",
            model_name="marker",
            status="completed",
            config_hash=None,
            git_hash=None,
            created_at=safe_dt,
        )

    mounts = [Mount("workspace", ws_dir)]
    mig_res = migrate_v1_to_v2(multi_dot_db, mounts=mounts, quick=False)
    assert mig_res.verification.ok is True

    cls_map = {c.artifact_id: c for c in mig_res.plan.classifications}
    assert cls_map[id_list_dlt].outcome == "historical"
    assert cls_map[id_list_dlt].anchor.kind is Kind.HISTORICAL

    with StoreLedger(multi_dot_db, mounts=mounts) as ledger:
        with ledger.exec_cursor() as cur:
            # Sequence nextval works in migrated DB
            cur.execute(
                "INSERT INTO global_tables.households (parent_id, tag) VALUES (7, 'second')"
            )
            rows_hh = cur.execute(
                "SELECT id, tag FROM global_tables.households ORDER BY id"
            ).fetchall()
            assert len(rows_hh) == 2
            assert rows_hh[1][1] == "second"

            # CHECK string literal 'sim.global_tables.keep_me' was NOT stripped!
            with pytest.raises(duckdb.ConstraintException):
                cur.execute(
                    "INSERT INTO global_tables.households (parent_id, tag) "
                    "VALUES (7, 'sim.global_tables.keep_me')"
                )

            # Part A7: depends_on direction preserved in main.run_binding
            dep_dirs = cur.execute(
                "SELECT direction, slot FROM main.run_binding WHERE run_id = 'run_nested'"
            ).fetchall()
            assert ("depends_on", "dep_cfg") in dep_dirs

            # Unrelated 0-artifact marker run_b starts historical
            run_b_before = cur.execute(
                "SELECT cache_status, signature FROM main.run WHERE id = 'run_b_empty_marker'"
            ).fetchone()
            assert run_b_before == ("historical", None)

        # Restore run_a's input and promote run_a -> run_b_empty_marker MUST remain historical!
        p_in_file.write_bytes(p_in_bytes)
        os.utime(p_in_file, ns=(safe_mtime_ns, safe_mtime_ns))
        promote_historical_run(ledger, "run_a", mounts=mounts)

        with ledger.exec_cursor() as cur:
            run_a_after = cur.execute(
                "SELECT cache_status, signature_scheme FROM main.run WHERE id = 'run_a'"
            ).fetchone()
            assert run_a_after == ("live", "sig-v2")
            run_b_after = cur.execute(
                "SELECT cache_status, signature FROM main.run WHERE id = 'run_b_empty_marker'"
            ).fetchone()
            assert run_b_after == ("historical", None)

    # Part A7: compute_sig_v2 rejects Kind.HISTORICAL / unregistered anchors and distinguishes ordinal > 0
    with pytest.raises(ValueError):
        compute_sig_v2("code_1", "cfg_1", [("slot", f"historical:v1:{'c' * 64}")])
    for bad_hist in (
        f"legacy-v1:unverified:{'a' * 64}",
        str(cls_map[id_list_dlt].anchor),
    ):
        with pytest.raises(ValueError, match="verified input anchors"):
            compute_sig_v2("code_1", "cfg_1", [("slot", bad_hist)])

    a1 = f"sha256:file:{'1' * 64}"
    a2 = f"sha256:file:{'2' * 64}"
    assert compute_sig_v2("c", "k", [("s", a1)]) == compute_sig_v2(
        "c", "k", [("s", 0, a1)]
    )
    assert compute_sig_v2("c", "k", [("s", 0, a1), ("s", 1, a2)]) != compute_sig_v2(
        "c", "k", [("s", 0, a1), ("s", 0, a2)]
    )

    # Part B4: stat_v1_fast_directory_hash recursive os.scandir walk + symlink racy flag
    scan_root = tmp_path / "fast_dir_scan"
    (scan_root / "sub").mkdir(parents=True)
    f_one = scan_root / "a.txt"
    f_two = scan_root / "sub" / "b.txt"
    f_one.write_bytes(b"alpha")
    f_two.write_bytes(b"bravo-bytes")
    os.utime(f_one, ns=(safe_mtime_ns, safe_mtime_ns))
    os.utime(f_two, ns=(safe_mtime_ns, safe_mtime_ns))
    os.utime(scan_root / "sub", ns=(safe_mtime_ns, safe_mtime_ns))
    os.utime(scan_root, ns=(safe_mtime_ns, safe_mtime_ns))

    dir_stat_clean = stat_v1_fast_directory_hash(
        scan_root, now_ns=safe_mtime_ns + 10_000_000_000
    )
    assert dir_stat_clean.entry_count == 2
    assert dir_stat_clean.is_racy is False
    assert len(dir_stat_clean.legacy_fast_hex) == 64

    os.symlink(f_one, scan_root / "sub" / "sym.txt")
    with pytest.raises(ValueError, match="Symlink"):
        stat_v1_fast_directory_hash(scan_root, now_ns=safe_mtime_ns + 10_000_000_000)


def test_pr4_modules_hygiene_no_getattr_or_ponytail() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    pr4_files = [
        repo_root / "src" / "consist" / "core" / "identity.py",
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
