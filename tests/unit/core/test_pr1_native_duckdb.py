from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import duckdb
import numpy as np
import pandas as pd
import pytest
from sqlmodel import Field, SQLModel

import consist
from consist.core import identity as identity_mod
from consist.core.maintenance import DatabaseMaintenance
from consist.core.netcdf_views import NetCdfMetadataView
from consist.core.openmatrix_views import OpenMatrixMetadataView
from consist.core.persistence import (
    DatabaseManager,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
    TransactionAbortedError,
    tracked_relation_count,
)
from consist.core.spatial_views import SpatialMetadataView
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def test_missing_gitpython_degrades_cleanly_in_repo_and_tmp_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "git", None)
    monkeypatch.setattr(identity_mod, "git", identity_mod._UNSET)

    # 1. Plain directory without a git repo
    plain_dir = tmp_path / "plain_workspace"
    plain_dir.mkdir()
    monkeypatch.chdir(plain_dir)

    tracker_plain = Tracker(
        run_dir=plain_dir / "runs",
        db_path=str(plain_dir / "prov.duckdb"),
    )
    try:
        with tracker_plain.start_run("no_git_plain", model="demo") as t:
            assert t.current_consist is not None
            assert isinstance(t.current_consist.run.git_hash, str)
    finally:
        tracker_plain.close()

    # 2. Real initialized git repository when gitpython is missing
    repo_dir = tmp_path / "git_workspace"
    repo_dir.mkdir()
    subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    (repo_dir / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(
        ["git", "add", "README.md"], cwd=repo_dir, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "commit", "-m", "init"], cwd=repo_dir, check=True, capture_output=True
    )
    monkeypatch.chdir(repo_dir)

    tracker_repo = Tracker(
        run_dir=repo_dir / "runs",
        db_path=str(repo_dir / "prov.duckdb"),
    )
    try:
        with tracker_repo.start_run("no_gitpython_in_repo", model="demo") as t:
            assert t.current_consist is not None
            git_hash = t.current_consist.run.git_hash
            assert isinstance(git_hash, str)
            assert len(git_hash) >= 7
    finally:
        tracker_repo.close()


def test_poison_safe_reentrant_transaction_rolls_back_when_inner_caught(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "tx_poison.duckdb"))
    try:
        # Nested visibility inside active transaction
        with db.transaction() as outer_cur:
            db.sync_run(Run(id="outer_ok", model_name="demo", status="completed"))
            with db.transaction() as inner_cur:
                assert inner_cur is outer_cur
                visible = db.get_run("outer_ok")
                assert visible is not None
                db.sync_run(Run(id="inner_ok", model_name="demo", status="completed"))

        assert db.get_run("outer_ok") is not None
        assert db.get_run("inner_ok") is not None

        # Inner failure caught by outer caller still poisons and rolls back outer transaction
        with pytest.raises(TransactionAbortedError):
            with db.transaction():
                db.sync_run(
                    Run(id="must_rollback", model_name="demo", status="completed")
                )
                try:
                    with db.transaction():
                        db.sync_run(
                            Run(
                                id="inner_boom_row",
                                model_name="demo",
                                status="completed",
                            )
                        )
                        raise ValueError("simulated inner failure")
                except ValueError:
                    pass

        assert db.get_run("must_rollback") is None
        assert db.get_run("inner_boom_row") is None
        assert db._tx_depth == 0
        assert db._tx_poisoned is False
    finally:
        db.close()


def test_borrow_for_dlt_rejects_active_tx_and_restores_search_path(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "borrow_dlt.duckdb"))
    try:
        with db.transaction():
            with pytest.raises(RuntimeError, match="active transaction"):
                with db.borrow_for_dlt():
                    pass

        with db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")

        with pytest.raises(ValueError, match="dlt pipeline failed"):
            with db.borrow_for_dlt() as conn:
                conn.execute("SET search_path = 'global_tables'")
                raise ValueError("dlt pipeline failed")

        row = db.fetchone("SELECT current_setting('search_path')")
        assert row is not None
        assert str(row[0]) == "main"
    finally:
        db.close()


@pytest.mark.parametrize(
    "bad_sql",
    [
        "",
        "   ",
        "CREATE TABLE evil (x INT)",
        "COPY (SELECT 1) TO 'evil.csv'",
        "ATTACH ':memory:' AS evil_db",
        "CALL duckdb_tables()",
        "PRAGMA table_info('run')",
        "WITH src AS (SELECT 'x' AS id) INSERT INTO run (id, status, model_name, started_at, created_at, updated_at) SELECT id, 'completed', 'm', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM src",
        "SELECT 1; DROP TABLE run",
    ],
)
def test_validate_select_only_sql_guards_all_read_helpers(
    tmp_path: Path,
    bad_sql: str,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "guard.duckdb"),
    )
    try:
        with pytest.raises(ValueError):
            tracker.run_query(bad_sql)
        with pytest.raises(ValueError):
            tracker.query_df(bad_sql)
        with pytest.raises(ValueError):
            tracker.query_rows(bad_sql)
    finally:
        tracker.close()


def test_suspend_for_subprocesses_releases_locks_blocks_threads_and_restores_temp_views(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "suspend.duckdb"
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))
    try:
        with tracker.start_run("seed_run", model="demo"):
            p = tmp_path / "runs" / "item.parquet"
            pd.DataFrame({"val": [1, 2, 3]}).to_parquet(p, index=False)
            tracker.log_artifact(p, key="items")

        assert tracker.db is not None
        # Open a tracked relation_query() cursor + Ibis cursor + temp view before suspending
        rel = tracker.db.relation_query("SELECT * FROM main.run")
        assert tracked_relation_count() >= 1
        _ = tracker.ibis_connection().table("run").count().execute()
        tracker.db.register_temp_view(
            "temp_seed_runs",
            "CREATE OR REPLACE TEMP VIEW temp_seed_runs AS SELECT id FROM main.run",
        )

        thread_entered = threading.Event()
        thread_finished = threading.Event()
        thread_result: list[int] = []

        def _concurrent_reader() -> None:
            thread_entered.set()
            rows = tracker.query_rows("SELECT COUNT(*) FROM temp_seed_runs")
            thread_result.append(int(rows[0]))
            thread_finished.set()

        worker: threading.Thread | None = None
        with tracker.db.suspend_for_subprocesses():
            assert tracked_relation_count() == 0
            worker = threading.Thread(target=_concurrent_reader)
            worker.start()
            assert thread_entered.wait(timeout=2.0)
            time.sleep(0.05)
            # Concurrent thread must remain blocked while suspended
            assert not thread_finished.is_set()

            # Child OS process must be able to open the .duckdb file read-write!
            child_code = (
                "import duckdb, sys; "
                "con = duckdb.connect(sys.argv[1], read_only=False); "
                "cnt = con.execute('SELECT COUNT(*) FROM main.run').fetchone()[0]; "
                "con.close(); "
                "assert cnt == 1"
            )
            proc = subprocess.run(
                [sys.executable, "-c", child_code, str(db_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert proc.returncode == 0, proc.stderr
            _ = rel

        assert worker is not None
        worker.join(timeout=5.0)
        assert thread_finished.is_set()
        assert thread_result == [1]
    finally:
        tracker.close()


def test_schema_version_check_raises_database_migration_required(
    tmp_path: Path,
) -> None:
    assert issubclass(DatabaseMigrationRequired, IncompatibleSchemaError)

    # 1. Explicit version=1 consist_schema DB
    v1_path = tmp_path / "v1.duckdb"
    con = duckdb.connect(str(v1_path))
    con.execute("CREATE TABLE consist_schema (version INTEGER PRIMARY KEY)")
    con.execute("INSERT INTO consist_schema VALUES (1)")
    con.close()

    expected_v1 = (
        f"Database uses Consist v0.4 schema (version=1). "
        f"Run 'consist migrate {v1_path}'."
    )
    with pytest.raises(DatabaseMigrationRequired) as exc_info:
        DatabaseManager(str(v1_path))
    assert str(exc_info.value) == expected_v1

    # 2. Legacy unversioned DB with a `run` table
    legacy_path = tmp_path / "legacy.duckdb"
    con_legacy = duckdb.connect(str(legacy_path))
    con_legacy.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY, status VARCHAR)")
    con_legacy.close()

    expected_legacy = (
        f"Database uses Consist v0.4 schema (version=1). "
        f"Run 'consist migrate {legacy_path}'."
    )
    with pytest.raises(DatabaseMigrationRequired) as exc_legacy:
        DatabaseManager(str(legacy_path))
    assert str(exc_legacy.value) == expected_legacy


def test_b2_export_rejects_self_symlink_and_hardlink_before_unlink(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "b2_source.duckdb"
    db = DatabaseManager(str(db_path))
    maint = DatabaseMaintenance(db, run_dir=tmp_path / "runs")
    try:
        db.sync_run(Run(id="r1", model_name="demo", status="completed"))
        symlink_target = tmp_path / "b2_symlink.duckdb"
        symlink_target.symlink_to(db_path)
        hardlink_target = tmp_path / "b2_hardlink.duckdb"
        os.link(db_path, hardlink_target)

        for alias in (db_path, symlink_target, hardlink_target):
            with pytest.raises(ValueError):
                maint.export("r1", alias, include_data=False, include_snapshots=False)
            assert db_path.exists()
            assert db.get_run("r1") is not None
    finally:
        db.close()


def test_b3_purge_preserves_shared_containers_unmounted_uris_and_surviving_coupler_snapshots(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    inputs_dir = tmp_path / "external_inputs"
    inputs_dir.mkdir()
    db_path = tmp_path / "b3.duckdb"

    shared_h5 = run_dir / "shared.h5"
    shared_h5.write_bytes(b"shared-hdf5-container")
    mounted_file = inputs_dir / "custom_mounted.parquet"
    mounted_file.write_bytes(b"mounted-data")
    private_out = run_dir / "private_purge.parquet"
    private_out.write_bytes(b"purge-me")

    db = DatabaseManager(str(db_path))
    try:
        db.sync_run(Run(id="purge_run", model_name="demo", status="completed"))
        db.sync_run(Run(id="survive_run", model_name="demo", status="completed"))

        art_h5_purge = Artifact(
            key="h5_member_a",
            container_uri=f"workspace://{shared_h5.name}",
            table_path="/group_a",
            driver="h5_table",
            hash="a" * 64,
            run_id="purge_run",
        )
        art_h5_keep = Artifact(
            key="h5_member_b",
            container_uri=f"workspace://{shared_h5.name}",
            table_path="/group_b",
            driver="h5_table",
            hash="b" * 64,
            run_id="survive_run",
        )
        art_mounted = Artifact(
            key="custom_mount_out",
            container_uri="inputs://custom_mounted.parquet",
            driver="parquet",
            hash="c" * 64,
            run_id="purge_run",
        )
        art_private = Artifact(
            key="private_out",
            container_uri=f"workspace://{private_out.name}",
            driver="parquet",
            hash="d" * 64,
            run_id="purge_run",
        )

        db.sync_artifact(art_h5_purge, run_id="purge_run", direction="output")
        db.sync_artifact(art_h5_keep, run_id="survive_run", direction="output")
        db.sync_artifact(art_mounted, run_id="purge_run", direction="output")
        db.sync_artifact(art_private, run_id="purge_run", direction="output")

        now = datetime.now(timezone.utc)
        with db.transaction() as cur:
            cur.execute(
                """
                INSERT INTO main.run_coupler_snapshot (
                    run_id, phase, role_key, scenario_run_id, step_ordinal, artifact_id, updated_at
                )
                VALUES (?, 'step', 'k1', ?, 0, ?, ?), (?, 'step', 'k2', ?, 0, ?, ?)
                """,
                [
                    "purge_run",
                    "purge_run",
                    str(art_private.id),
                    now,
                    "survive_run",
                    "survive_run",
                    str(art_h5_keep.id),
                    now,
                ],
            )

        # Maintenance initialized with mounts={} (unconfigured custom mount)
        maint = DatabaseMaintenance(db, run_dir=run_dir, mounts={})
        res = maint.purge(
            "purge_run",
            include_children=False,
            delete_files=True,
            delete_ingested_data=False,
            dry_run=False,
        )
        assert res.executed is True
        assert shared_h5.exists(), (
            "Shared HDF5 container must not be unlinked while another member survives"
        )
        assert mounted_file.exists(), (
            "Unresolved custom-mount URI must not unlink external files"
        )
        assert not private_out.exists(), "Unshared workspace output should be unlinked"

        surviving_snapshots = db.query_rows(
            "SELECT scenario_run_id, role_key FROM main.run_coupler_snapshot"
        )
        assert surviving_snapshots == [("survive_run", "k2")]
    finally:
        db.close()


def test_b14_find_runs_coerces_numpy_scalars_and_pushes_down_sql_limit(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=str(tmp_path / "b14.duckdb"))
    try:
        assert tracker.db is not None
        for idx in range(5):
            tracker.db.sync_run(
                Run(
                    id=f"run_{idx}",
                    model_name="sim",
                    status="completed",
                    meta={"step": 3, "ratio": 0.5, "idx": idx},
                )
            )
        tracker.db.sync_run(
            Run(
                id="run_other",
                model_name="sim",
                status="completed",
                meta={"step": 9, "ratio": 0.25},
            )
        )

        matched = tracker.find_runs(
            metadata={"step": np.int64(3), "ratio": np.float64(0.5)},
            limit=2,
            offset=1,
        )
        assert len(matched) == 2
        assert all(r.meta["step"] == 3 and r.meta["ratio"] == 0.5 for r in matched)
    finally:
        tracker.close()


def test_b16_readonly_temp_views_str_config_query_null_artifact_id_fallback_and_relation_close(
    tmp_path: Path,
) -> None:
    xr = pytest.importorskip("xarray")

    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = tmp_path / "b16.duckdb"

    writer = Tracker(run_dir=run_dir, db_path=str(db_path))
    try:
        # 1. Create a cold parquet artifact + scalar 0-dim NetCDF artifact
        with writer.start_run("run_b16", model="demo", year=2030):
            pq_path = run_dir / "trips.parquet"
            pd.DataFrame(
                {"trip_id": [1, 2, 3, 4], "dist": [1.5, 2.5, 3.5, 4.5]}
            ).to_parquet(pq_path, index=False)
            pq_art = writer.log_artifact(pq_path, key="trips")

            nc_path = run_dir / "scalar.nc"
            ds = xr.Dataset({"global_mean": xr.DataArray(42.5)})
            ds.to_netcdf(nc_path)
            nc_art = writer.log_artifact(nc_path, key="climate_scalar", driver="netcdf")
            writer.ingest(nc_art)

        # Verify scalar 0-dim NetCDF metadata view works when __dims/__shape child tables are absent
        nc_df = writer.netcdf_metadata("climate_scalar").get_variables("climate_scalar")
        assert len(nc_df) == 1
        assert nc_df.iloc[0]["variable_name"] == "global_mean"
        assert list(nc_df.iloc[0]["dims"]) == []
        assert list(nc_df.iloc[0]["shape"]) == []

        # Verify consist.to_df(consist.load(art).limit(2), close=True) closes root tracked cursor
        chained_rel = consist.load(pq_art, tracker=writer).limit(2)
        assert consist.active_relation_count() == 1
        df_limited = consist.to_df(chained_rel, close=True)
        assert len(df_limited) == 2
        assert consist.active_relation_count() == 0

        # Seed config table + link table for str target config_run_query / config_run_rows
        assert writer.db is not None
        with writer.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                """
                CREATE TABLE global_tables.asim_coeffs (
                    content_hash VARCHAR,
                    coefficient_name VARCHAR,
                    value DOUBLE
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE global_tables.asim_link (
                    run_id VARCHAR,
                    table_name VARCHAR,
                    content_hash VARCHAR
                )
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.asim_coeffs VALUES
                ('h1', 'asc_transit', -1.25),
                ('h2', 'other_table_coeff', 9.99)
                """
            )
            cur.execute(
                """
                INSERT INTO global_tables.asim_link VALUES
                ('run_b16', 'asim_coeffs', 'h1'),
                ('run_b16', 'other_coeffs', 'h2')
                """
            )

            # Seed hot tables with consist_artifact_id IS NULL (consist_run_id fallback)
            # across hybrid views, netcdf_views, spatial_views, and openmatrix_views
            hot_only_run = Run(
                id="run_null_art", model_name="demo", status="completed", year=2035
            )
            writer.db.sync_run(hot_only_run)
            for key, driver in (
                ("legacy_hot", "parquet"),
                ("legacy_nc", "netcdf"),
                ("legacy_sp", "geojson"),
                ("legacy_omx", "openmatrix"),
            ):
                writer.db.sync_artifact(
                    Artifact(
                        key=key,
                        container_uri=f"workspace://{key}.{driver}",
                        driver=driver,
                        hash="f" * 64,
                        run_id="run_null_art",
                        meta={"is_ingested": True},
                    ),
                    run_id="run_null_art",
                    direction="output",
                )

            cur.execute(
                """
                CREATE TABLE global_tables.legacy_hot (
                    val INTEGER,
                    consist_run_id VARCHAR,
                    consist_artifact_id VARCHAR
                )
                """
            )
            cur.execute(
                "INSERT INTO global_tables.legacy_hot VALUES (77, 'run_null_art', NULL)"
            )

            cur.execute(
                """
                CREATE TABLE global_tables.legacy_nc (
                    _dlt_id VARCHAR,
                    variable_name VARCHAR,
                    variable_type VARCHAR,
                    dtype VARCHAR,
                    attributes VARCHAR,
                    consist_run_id VARCHAR,
                    consist_artifact_id VARCHAR
                )
                """
            )
            cur.execute(
                "INSERT INTO global_tables.legacy_nc VALUES ('n1', 'temp', 'data', 'float32', '{}', 'run_null_art', NULL)"
            )

            cur.execute(
                """
                CREATE TABLE global_tables.legacy_sp (
                    _dlt_id VARCHAR,
                    crs VARCHAR,
                    feature_count BIGINT,
                    geometry_column VARCHAR,
                    consist_run_id VARCHAR,
                    consist_artifact_id VARCHAR
                )
                """
            )
            cur.execute(
                "INSERT INTO global_tables.legacy_sp VALUES ('s1', 'EPSG:4326', 10, 'geometry', 'run_null_art', NULL)"
            )

            cur.execute(
                """
                CREATE TABLE global_tables.legacy_omx (
                    _dlt_id VARCHAR,
                    matrix_name VARCHAR,
                    dtype VARCHAR,
                    n_rows BIGINT,
                    n_cols BIGINT,
                    attributes VARCHAR,
                    consist_run_id VARCHAR,
                    consist_artifact_id VARCHAR
                )
                """
            )
            cur.execute(
                "INSERT INTO global_tables.legacy_omx VALUES ('m1', 'am_skim', 'float64', 5, 5, '{}', 'run_null_art', NULL)"
            )

        spec = writer.config_run_query("asim_coeffs", link_table="asim_link")
        assert "table_name" in spec.sql
        cfg_rows = writer.config_run_rows("asim_coeffs", link_table="asim_link")
        assert len(cfg_rows) == 1
        assert cfg_rows[0][0] == "run_b16"
        assert cfg_rows[0][1]["coefficient_name"] == "asc_transit"

        # Verify consist_artifact_id IS NULL fallback across all 4 view modules
        writer.create_view("v_legacy_hot", "legacy_hot")
        hot_rows = writer.query_rows("SELECT val, consist_run_id FROM v_legacy_hot")
        assert hot_rows == [(77, "run_null_art")]

        assert len(NetCdfMetadataView(writer).get_variables("legacy_nc")) == 1
        assert len(SpatialMetadataView(writer).get_metadata("legacy_sp")) == 1
        assert len(OpenMatrixMetadataView(writer).get_matrices("legacy_omx")) == 1
    finally:
        writer.close()

    class TripSchema(SQLModel, table=True):
        __tablename__ = "trips"
        __table_args__ = {"extend_existing": True}
        trip_id: int = Field(primary_key=True)
        dist: float

    # Verify read_only=True creates TEMP VIEWs in create_view() and ibis_view() without write errors
    ro_db = DatabaseManager(str(db_path), read_only=True)
    ro_tracker = Tracker(run_dir=run_dir, db_path=None, schemas=[TripSchema])
    ro_tracker.db = ro_db
    ro_tracker._hot_data_db = ro_db
    try:
        ro_tracker.create_view("v_ro_trips", "trips")
        cnt = ro_tracker.query_rows("SELECT COUNT(*) FROM v_ro_trips")[0]
        assert int(cnt) == 4

        ibis_tbl = ro_tracker.ibis_view(TripSchema)
        assert int(ibis_tbl.count().execute()) == 4
    finally:
        ro_db.close()
