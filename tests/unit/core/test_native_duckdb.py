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
    _validate_select_only_sql,
    close_tracked_relation_tokens,
    extract_tracked_relation_tokens,
    lookup_live_db_by_path,
    tracked_relation_count,
)
from consist.core.spatial_views import SpatialMetadataView
from consist.core.tracker import Tracker
from consist.integrations.containers import api as containers_api
from consist.integrations.ibis import close_tracker_ibis_connection
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
            db.sync_run(
                Run(
                    id="outer_ok",
                    model_name="demo",
                    config_hash=None,
                    git_hash=None,
                    status="completed",
                )
            )
            with db.transaction() as inner_cur:
                assert inner_cur is outer_cur
                visible = db.get_run("outer_ok")
                assert visible is not None
                db.sync_run(
                    Run(
                        id="inner_ok",
                        model_name="demo",
                        config_hash=None,
                        git_hash=None,
                        status="completed",
                    )
                )

        assert db.get_run("outer_ok") is not None
        assert db.get_run("inner_ok") is not None

        # Inner failure caught by outer caller still poisons and rolls back outer transaction
        with pytest.raises(TransactionAbortedError):
            with db.transaction():
                db.sync_run(
                    Run(
                        id="must_rollback",
                        model_name="demo",
                        config_hash=None,
                        git_hash=None,
                        status="completed",
                    )
                )
                try:
                    with db.transaction():
                        db.sync_run(
                            Run(
                                id="inner_boom_row",
                                model_name="demo",
                                config_hash=None,
                                git_hash=None,
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

        row = db.connection.execute("SELECT current_setting('search_path')").fetchone()
        assert row is not None
        assert str(row[0]) == "main"
        schema_row = db.fetchone("SELECT current_schema()")
        assert schema_row == ("main",)
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
        db.sync_run(
            Run(
                id="r1",
                model_name="demo",
                config_hash=None,
                git_hash=None,
                status="completed",
            )
        )
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
        db.sync_run(
            Run(
                id="purge_run",
                model_name="demo",
                config_hash=None,
                git_hash=None,
                status="completed",
            )
        )
        db.sync_run(
            Run(
                id="survive_run",
                model_name="demo",
                config_hash=None,
                git_hash=None,
                status="completed",
            )
        )

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
                    config_hash=None,
                    git_hash=None,
                    status="completed",
                    meta={"step": 3, "ratio": 0.5, "idx": idx},
                )
            )
        tracker.db.sync_run(
            Run(
                id="run_other",
                model_name="sim",
                config_hash=None,
                git_hash=None,
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
        loaded_rel = consist.load(pq_art, tracker=writer)
        assert isinstance(loaded_rel, duckdb.DuckDBPyRelation)
        chained_rel = loaded_rel.limit(2)
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
                id="run_null_art",
                model_name="demo",
                config_hash=None,
                git_hash=None,
                status="completed",
                year=2035,
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
        close_tracker_ibis_connection(ro_tracker)
        ro_db.close()


def test_config_run_query_and_select_only_sql_guards(tmp_path: Path) -> None:
    db_path = tmp_path / "sql_guard.duckdb"
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))
    try:
        with tracker.start_run("guarded_run", model="asim"):
            pass
        assert tracker.db is not None
        with tracker.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.asim_coeffs (content_hash VARCHAR, val DOUBLE)"
            )
            cur.execute(
                "CREATE TABLE global_tables.asim_link (run_id VARCHAR, table_name VARCHAR, content_hash VARCHAR)"
            )
            cur.execute("INSERT INTO global_tables.asim_coeffs VALUES ('h1', 1.5)")
            cur.execute(
                "INSERT INTO global_tables.asim_link VALUES ('guarded_run', 'asim_coeffs', 'h1')"
            )

        inj_where = ("1=1); DELETE FROM main.run; SELECT 1 WHERE (1=1", [])
        with pytest.raises(ValueError, match="read-only SELECT"):
            tracker.config_run_query(
                "asim_coeffs", link_table="asim_link", where=inj_where
            )
        with pytest.raises(ValueError, match="read-only SELECT"):
            tracker.config_run_rows(
                "asim_coeffs", link_table="asim_link", where=inj_where
            )

        assert tracker.get_run("guarded_run") is not None

        for bad_stmt in (
            "SELECT * FROM (",
            "DESCRIBE main.run",
            "SUMMARIZE main.run",
            "UNPIVOT main.run ON id",
            "PIVOT main.run ON status USING COUNT(*)",
            "PRAGMA database_list",
        ):
            with pytest.raises(ValueError):
                _validate_select_only_sql(bad_stmt)
            with pytest.raises(ValueError):
                tracker.run_query(bad_stmt)
    finally:
        tracker.close()


def test_stale_temp_view_replay_does_not_brick_metadata_or_user_queries(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stale_tv.duckdb"
    cold_parquet = tmp_path / "runs" / "cold_trips.parquet"
    writer = Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))
    try:
        with writer.start_run("run_tv", model="demo"):
            pd.DataFrame({"x": [10, 20]}).to_parquet(cold_parquet, index=False)
            writer.log_artifact(
                cold_parquet, key="cold_trips", profile_file_schema=True
            )
    finally:
        writer.close()

    ro_tracker = Tracker(
        run_dir=tmp_path / "runs", db_path=str(db_path), read_only=True
    )
    try:
        ro_tracker.create_view("v_cold_trips", "cold_trips")
        assert ro_tracker.query_rows("SELECT COUNT(*) FROM v_cold_trips") == [2]

        # Remove cold Parquet backing file so any eager view validation against it would fail
        cold_parquet.unlink()

        run_row = ro_tracker.get_run("run_tv")
        assert run_row is not None and run_row.id == "run_tv"
        assert len(ro_tracker.find_runs(model="demo")) == 1
        assert ro_tracker.query_rows("SELECT 42") == [42]
    finally:
        ro_tracker.close()


def test_coexisting_readonly_and_writable_managers_suspend_all_for_subprocess(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "coexist.duckdb"
    seed = DatabaseManager(str(db_path))
    seed.upsert_run(
        Run(
            id="seed_r",
            model_name="m",
            config_hash=None,
            git_hash=None,
            status="completed",
        )
    )
    seed.close()

    # Order 1: read_only opened BEFORE writable in the same OS process
    ro_first = DatabaseManager(str(db_path), read_only=True)
    assert lookup_live_db_by_path(db_path) is ro_first
    rw_second = DatabaseManager(str(db_path), read_only=False)
    try:
        assert lookup_live_db_by_path(db_path) is rw_second
        assert ro_first.get_run("seed_r") is not None

        with rw_second.suspend_for_subprocesses():
            child = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import duckdb; "
                        f"c = duckdb.connect({str(db_path)!r}, read_only=False); "
                        'c.execute("INSERT INTO run (id, status, model_name, tags, cacheable, meta, started_at, created_at, updated_at) '
                        "VALUES ('child_r1', 'completed', 'm', '[]', TRUE, '{}', now(), now(), now())\"); "
                        "c.close()"
                    ),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            assert child.returncode == 0, child.stderr

        assert rw_second.get_run("child_r1") is not None
        assert ro_first.get_run("child_r1") is not None
    finally:
        ro_first.close()
        rw_second.close()

    # Order 2: writable opened BEFORE read_only in the same OS process
    rw_first = DatabaseManager(str(db_path), read_only=False)
    ro_second = DatabaseManager(str(db_path), read_only=True)
    try:
        with ro_second.suspend_for_subprocesses():
            child2 = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import duckdb; "
                        f"c = duckdb.connect({str(db_path)!r}, read_only=False); "
                        "c.close()"
                    ),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            assert child2.returncode == 0, child2.stderr
        assert rw_first.get_run("seed_r") is not None
        assert ro_second.get_run("seed_r") is not None
    finally:
        ro_second.close()
        rw_first.close()


def test_run_container_suspends_db_for_real_subprocess_and_supports_in_process_callbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "runs"
    out_dir = run_dir / "container_out"
    out_dir.mkdir(parents=True)
    db_path = tmp_path / "container_lock.duckdb"

    tracker = Tracker(
        run_dir=run_dir,
        db_path=str(db_path),
        mounts={"runs": str(run_dir)},
    )
    try:
        in_parquet = run_dir / "in.parquet"
        pd.DataFrame({"v": [1, 2, 3]}).to_parquet(in_parquet, index=False)
        with tracker.start_run("prep_in", model="prep"):
            in_art = tracker.log_artifact(in_parquet, key="in_tbl", direction="output")

        class SubprocBackend:
            def resolve_image_digest(self, image: str) -> str:
                return "sha256:subproc123"

            def run(
                self,
                image: str,
                command: list[str],
                volumes: dict[str, str],
                env: dict[str, str],
                working_dir: str | None = None,
                **kwargs: object,
            ) -> bool:
                # External OS process must be able to open db_path read-only without lock failure
                proc = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import duckdb; "
                            f"c = duckdb.connect({str(db_path)!r}, read_only=True); "
                            "assert c.execute('SELECT COUNT(*) FROM run').fetchone()[0] >= 1; "
                            "c.close()"
                        ),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                assert proc.returncode == 0, proc.stderr
                pd.DataFrame({"ok": [1]}).to_parquet(
                    out_dir / "sub.parquet", index=False
                )
                return True

        monkeypatch.setattr(
            containers_api, "DockerBackend", lambda pull_latest=False: SubprocBackend()
        )
        res1 = containers_api.run_container(
            tracker=tracker,
            run_id="container_subproc",
            image="alpine:latest",
            command=["echo", "hi"],
            volumes={str(out_dir): "/out"},
            inputs=[in_art],
            outputs={"sub_out": str(out_dir / "sub.parquet")},
            backend_type="docker",
        )
        assert not res1.cache_hit
        assert "sub_out" in res1.artifacts

        # In-process Python mock backend invoking consist.load_df(tracker=tracker) on the same thread
        class InProcMockBackend:
            def resolve_image_digest(self, image: str) -> str:
                return "sha256:inproc456"

            def run(
                self,
                image: str,
                command: list[str],
                volumes: dict[str, str],
                env: dict[str, str],
                working_dir: str | None = None,
                **kwargs: object,
            ) -> bool:
                df = consist.load_df(in_art, tracker=tracker)
                assert len(df) == 3
                df.to_parquet(out_dir / "inproc.parquet", index=False)
                return True

        monkeypatch.setattr(
            containers_api,
            "DockerBackend",
            lambda pull_latest=False: InProcMockBackend(),
        )
        res2 = containers_api.run_container(
            tracker=tracker,
            run_id="container_inproc",
            image="alpine:latest",
            command=["echo", "inproc"],
            volumes={str(out_dir): "/out"},
            inputs=[in_art],
            outputs={"inproc_out": str(out_dir / "inproc.parquet")},
            backend_type="docker",
        )
        assert not res2.cache_hit
        assert "inproc_out" in res2.artifacts
    finally:
        tracker.close()


def test_artifact_member_mutual_exclusion_and_bulk_meta_preserves_member_kind(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "member_kind.duckdb"))
    try:
        both = Artifact(
            key="bad_dual",
            container_uri="inputs://dual.h5",
            driver="h5",
            table_path="/tables/a",
            array_path="/arrays/b",
        )
        with pytest.raises(ValueError, match="both table_path and array_path"):
            db.upsert_artifact(both)

        h5_arr = Artifact(
            key="vel_field",
            container_uri="outputs://sim.h5",
            driver="h5",
            array_path="/fields/velocity",
            meta={"units": "m/s"},
        )
        db.upsert_artifact(h5_arr)
        loaded_before = db.get_artifact(h5_arr.id)
        assert loaded_before is not None
        assert loaded_before.array_path == "/fields/velocity"
        assert loaded_before.table_path is None

        db.replace_artifacts_meta_bulk(
            {h5_arr.id: {"units": "m/s", "recovery_roots": ["/archive/root"]}}
        )
        loaded_after = db.get_artifact(h5_arr.id)
        assert loaded_after is not None
        assert loaded_after.array_path == "/fields/velocity"
        assert loaded_after.table_path is None
        assert loaded_after.meta["recovery_roots"] == ["/archive/root"]
        assert "_consist_member_kind" not in loaded_after.meta
    finally:
        db.close()


def test_snapshot_db_rejects_self_symlink_and_hardlink_aliases_without_destroying_source(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "live_src.duckdb"
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))
    try:
        with tracker.start_run("snap_guard_run", model="demo"):
            pass

        with pytest.raises(ValueError, match="alias"):
            tracker.snapshot_db(db_path)

        sym = tmp_path / "sym_alias.duckdb"
        sym.symlink_to(db_path)
        with pytest.raises(ValueError, match="alias"):
            tracker.snapshot_db(sym)

        hard = tmp_path / "hard_alias.duckdb"
        os.link(db_path, hard)
        with pytest.raises(ValueError, match="alias"):
            tracker.snapshot_db(hard)

        wal_path = Path(f"{db_path}.wal")
        wal_existed = wal_path.exists()
        if not wal_existed:
            wal_path.write_bytes(b"wal")
        try:
            wal_sym = tmp_path / "wal_alias.duckdb"
            wal_sym.symlink_to(wal_path)
            with pytest.raises(ValueError, match="alias"):
                tracker.snapshot_db(wal_sym)
        finally:
            if not wal_existed and wal_path.exists():
                wal_path.unlink()

        assert db_path.exists()
        assert tracker.get_run("snap_guard_run") is not None
    finally:
        tracker.close()


def test_relation_query_in_active_and_poisoned_tx_and_borrow_for_dlt_aborted_tx_recovery(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "tx_rel.duckdb"))
    try:
        with db.transaction() as cur:
            db._upsert_run_locked(
                cur,
                Run(
                    id="uncommitted_r",
                    model_name="m",
                    config_hash=None,
                    git_hash=None,
                    status="running",
                ),
            )
            rel = db.relation_query(
                "SELECT id FROM run WHERE id = ?", ["uncommitted_r"]
            )
            try:
                assert rel.fetchall() == [("uncommitted_r",)]
            finally:
                close_tracked_relation_tokens(extract_tracked_relation_tokens(rel))

        with pytest.raises(TransactionAbortedError):
            with db.transaction():
                try:
                    with db.transaction() as inner:
                        inner.execute("SELECT * FROM missing_table_xyz")
                except duckdb.Error:
                    pass
                with pytest.raises(TransactionAbortedError):
                    db.relation_query("SELECT 1")

        with pytest.raises(duckdb.Error):
            with db.borrow_for_dlt() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("SELECT * FROM missing_dlt_staging_table")

        # Connection must be cleanly rolled back and usable after aborted dlt tx
        db.upsert_run(
            Run(
                id="after_dlt_abort",
                model_name="m",
                config_hash=None,
                git_hash=None,
                status="completed",
            )
        )
        assert db.get_run("after_dlt_abort") is not None
    finally:
        db.close()


def test_api_config_run_query_accepts_tracker_and_rejects_unresolvable_str_link_table(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="without a database connection"):
        consist.config_run_query("asim_coeffs", link_table="asim_link")

    tracker = Tracker(
        run_dir=tmp_path / "runs", db_path=str(tmp_path / "api_cfg_q.duckdb")
    )
    try:
        assert tracker.db is not None
        with tracker.db.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.asim_coeffs (content_hash VARCHAR, k VARCHAR)"
            )
            cur.execute(
                "CREATE TABLE global_tables.asim_link (run_id VARCHAR, table_name VARCHAR, content_hash VARCHAR)"
            )
        spec = consist.config_run_query(
            "asim_coeffs", link_table="asim_link", tracker=tracker
        )
        assert "table_name" in spec.sql
    finally:
        tracker.close()


def test_ibis_temp_view_epoch_refresh_closes_stale_cursors_without_leak(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ibis_leak.duckdb"
    pq_path = tmp_path / "runs" / "trips.parquet"
    writer = Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))
    try:
        with writer.start_run("seed_ibis", model="m"):
            pd.DataFrame({"trip_id": [1, 2], "dist": [3.5, 4.5]}).to_parquet(
                pq_path, index=False
            )
            writer.log_artifact(pq_path, key="trips", profile_file_schema=True)
    finally:
        writer.close()

    ro_tracker = Tracker(
        run_dir=tmp_path / "runs", db_path=str(db_path), read_only=True
    )
    try:
        assert ro_tracker.db is not None
        for idx in range(10):
            ro_tracker.create_view(f"v_trips_{idx}", "trips")
            conn = ro_tracker.ibis_connection()
            assert int(conn.table(f"v_trips_{idx}").count().execute()) == 2
            assert len(ro_tracker.db._open_cursors) == 1

        close_tracker_ibis_connection(ro_tracker)
        assert len(ro_tracker.db._open_cursors) == 0
    finally:
        ro_tracker.close()


def test_cache_lookup_uses_art_index_scan_nested_metadata_pushdown_and_signature_prefetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs", db_path=str(tmp_path / "perf_scale.duckdb")
    )
    try:
        assert tracker.db is not None
        db = tracker.db

        # 1. Verify DuckDB ART Index Scan on find_matching_run & find_run_by_signature
        with db.transaction() as cur:
            cur.execute(
                """
                INSERT INTO run (
                    id, status, model_name, config_hash, git_hash, input_hash,
                    signature, cacheable, started_at, created_at, updated_at
                )
                SELECT
                    'idx_r_' || i::VARCHAR,
                    'completed',
                    'sim',
                    'cfg_' || i::VARCHAR,
                    'git_0',
                    'inp_' || i::VARCHAR,
                    'sig_' || i::VARCHAR,
                    TRUE,
                    TIMESTAMPTZ '2026-01-01 00:00:00+00',
                    TIMESTAMPTZ '2026-01-01 00:00:00+00',
                    TIMESTAMPTZ '2026-01-01 00:00:00+00'
                FROM range(5000) t(i)
                """
            )
        explain_match = "\n".join(
            str(r[1])
            for r in db.fetchall(
                "EXPLAIN ANALYZE WITH cand AS MATERIALIZED ("
                "SELECT id, status, cacheable, config_hash, input_hash, git_hash, created_at "
                "FROM run r WHERE r.config_hash = ?"
                ") SELECT * FROM cand r "
                "WHERE r.input_hash = ? AND r.git_hash = ? "
                "AND r.status = 'completed' AND r.cacheable = TRUE "
                "ORDER BY r.created_at DESC, r.id DESC LIMIT 1",
                ["cfg_2500", "inp_2500", "git_0"],
            )
        )
        assert "Index Scan" in explain_match

        explain_sig = "\n".join(
            str(r[1])
            for r in db.fetchall(
                "EXPLAIN ANALYZE WITH cand AS MATERIALIZED ("
                "SELECT id, status, cacheable, signature, created_at "
                "FROM run r WHERE r.signature = ?"
                ") SELECT * FROM cand r "
                "WHERE r.status = 'completed' AND r.cacheable = TRUE "
                "ORDER BY r.created_at DESC, r.id DESC LIMIT 1",
                ["sig_2500"],
            )
        )
        assert "Index Scan" in explain_sig
        assert db.find_matching_run("cfg_2500", "inp_2500", "git_0") is not None
        assert db.find_run_by_signature("sig_2500") is not None

        # 2. Verify nested metadata SQL pushdown (exact object keys + ordered array + null vs missing)
        db.upsert_run(
            Run(
                id="meta_exact",
                model_name="meta_m",
                config_hash=None,
                git_hash=None,
                status="completed",
                meta={"nested": {"b": [2, 3], "a": 1, "n": None}},
            )
        )
        db.upsert_run(
            Run(
                id="meta_wrong_order",
                model_name="meta_m",
                config_hash=None,
                git_hash=None,
                status="completed",
                meta={"nested": {"a": 1, "b": [3, 2], "n": None}},
            )
        )
        db.upsert_run(
            Run(
                id="meta_extra_key",
                model_name="meta_m",
                config_hash=None,
                git_hash=None,
                status="completed",
                meta={"nested": {"a": 1, "b": [2, 3], "n": None, "extra": 9}},
            )
        )
        db.upsert_run(
            Run(
                id="meta_missing_null_key",
                model_name="meta_m",
                config_hash=None,
                git_hash=None,
                status="completed",
                meta={"nested": {"a": 1, "b": [2, 3]}},
            )
        )

        clauses: list[str] = []
        params: list[object] = []
        needs_client = db.append_run_tag_and_metadata_clauses(
            clauses,
            params,
            metadata={"nested": {"a": 1, "b": [2, 3], "n": None}},
        )
        assert needs_client is False
        matched = db.find_runs(
            model="meta_m",
            metadata={"nested": {"a": 1, "b": [2, 3], "n": None}},
            limit=1,
        )
        assert [r.id for r in matched] == ["meta_exact"]

        # 3. Verify _prefetch_run_signatures batches 20 upstream inputs into 1 query
        upstream_arts: list[Artifact] = []
        for idx in range(20):
            p = tmp_path / "runs" / f"up_{idx}.parquet"
            pd.DataFrame({"v": [idx]}).to_parquet(p, index=False)
            with tracker.start_run(f"up_run_{idx}", model="up"):
                art = tracker.log_artifact(p, key=f"k_{idx}", direction="output")
                upstream_arts.append(art)

        # Clear in-memory cache so downstream run must fetch all 20 signatures from DuckDB
        tracker._run_signature_cache.clear()
        calls: list[list[str]] = []
        orig_get_sigs = db.get_run_signatures

        def _spy_get_sigs(run_ids: list[str]) -> dict[str, str]:
            calls.append(list(run_ids))
            return orig_get_sigs(run_ids)

        monkeypatch.setattr(db, "get_run_signatures", _spy_get_sigs)
        with tracker.start_run(
            "downstream_consumer", model="down", inputs=upstream_arts
        ):
            pass

        assert len(calls) == 1
        assert len(calls[0]) == 20
    finally:
        tracker.close()
