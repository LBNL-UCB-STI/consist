from __future__ import annotations

from datetime import datetime, timezone
import gc
import json
import os
from pathlib import Path
from unittest.mock import patch
import uuid

import click
import duckdb
import pandas as pd
from pydantic import BaseModel
import pytest
from typer.testing import CliRunner

import consist
from consist.api import active_relation_count
from consist.artifact import Artifact
from consist.cli import ConsistShell, app, get_tracker
from consist.core.lineage import LineageService
from consist.core.maintenance import DatabaseMaintenance
from consist.core.openlineage import OpenLineageEmitter, OpenLineageOptions
from consist.core.persistence import (
    ConsistError,
    DatabaseManager,
    DatabaseMigrationRequired,
)
from consist.core.queries import execute_config_run_rows
from consist.core.tracker import Tracker
from consist.core.views import ViewFactory
from consist.digest import Digest, Identity
from consist.integrations.beam.config_adapter import (
    BeamConfigAdapter,
    BeamConfigOverrides,
    BeamLaunchBundleMember,
)
from consist.integrations.containers.api import _container_signature
from consist.integrations.containers.models import ContainerDefinition
from consist.migrations.v1.fasthash import compute_v1_fast_file_hash
from consist.migrations.v1.migrator import migrate_v1_to_v2


def _init_minimal_v1_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE consist_schema (version INTEGER)")
        con.execute("INSERT INTO consist_schema VALUES (1)")
        con.execute(
            "CREATE TABLE run ("
            "id VARCHAR PRIMARY KEY, parent_run_id VARCHAR, status VARCHAR, "
            "model_name VARCHAR, description VARCHAR, year INTEGER, iteration INTEGER, "
            "tags JSON, config_hash VARCHAR, git_hash VARCHAR, input_hash VARCHAR, "
            "signature VARCHAR, meta JSON, started_at TIMESTAMP, ended_at TIMESTAMP, "
            "created_at TIMESTAMP, updated_at TIMESTAMP"
            ")"
        )
        con.execute(
            "CREATE TABLE artifact ("
            "id UUID PRIMARY KEY, key VARCHAR, container_uri VARCHAR, table_path VARCHAR, "
            "array_path VARCHAR, driver VARCHAR, hash VARCHAR, run_id VARCHAR, "
            "parent_artifact_id UUID, meta JSON, created_at TIMESTAMP"
            ")"
        )
        con.execute(
            "CREATE TABLE run_artifact_link ("
            "run_id VARCHAR, artifact_id UUID, direction VARCHAR, is_implicit BOOLEAN"
            ")"
        )


def test_deprecated_views_stubs_and_query_df_query_rows_guards(tmp_path: Path) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "provenance.duckdb",
    )
    try:
        with consist.use_tracker(tracker):
            with pytest.warns(DeprecationWarning, match="register_views"):
                with pytest.raises(ConsistError, match="create_view"):
                    consist.register_views()

            with pytest.warns(DeprecationWarning, match="view"):
                with pytest.raises(ConsistError, match="query_df"):
                    consist.view("trips")

            # Read-only enforcement on query_df and query_rows
            with pytest.raises(ValueError, match="SELECT"):
                consist.query_df("CREATE TABLE forbidden_tbl (x INT)")

            class DummyModel(BaseModel):
                val: int

            with pytest.raises(ValueError, match="SELECT"):
                consist.query_rows("DROP VIEW IF EXISTS nope", model=DummyModel)

            rows = consist.query_rows(
                "SELECT ? AS val",
                params=[42],
                model=DummyModel,
            )
            assert rows == [DummyModel(val=42)]
    finally:
        tracker.close()


def test_hybrid_and_grouped_view_hot_dedup_exogenous_and_archive_fallback(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "runs"
    tracker = Tracker(
        run_dir=run_dir,
        db_path=tmp_path / "views.duckdb",
        mounts={"archive": str(archive_dir)},
    )
    try:
        # 1. Exogenous artifact (producing run_id IS NULL) visible via LEFT JOIN main.run
        exog_pq = tmp_path / "exog_trips.parquet"
        pd.DataFrame({"trip_id": [1], "dist": [1.5]}).to_parquet(exog_pq, index=False)

        with tracker.start_run("step_1", "sim", year=2025, iteration=1):
            exog_art = tracker.log_artifact(exog_pq, key="trips", direction="input")
            assert exog_art.run_id is None

            # 2. Hot+cold artifact: exists on disk AND ingested into global_tables.trips!
            hot_pq = run_dir / "hot_trips.parquet"
            hot_df = pd.DataFrame({"trip_id": [2, 3], "dist": [4.0, 6.0]})
            hot_df.to_parquet(hot_pq, index=False)
            hot_art = tracker.log_artifact(
                hot_pq,
                key="trips",
                direction="output",
                profile_file_schema=True,
                facet={"scenario": "base"},
                facet_index=True,
            )
            ingested_art = tracker.ingest(hot_art, data=hot_df)
            assert ingested_art.is_ingested is True
            assert hot_pq.exists()  # Still on disk too — must NOT double-count!

        # 3. Cold artifact archived with scratch copy deleted: resolves via locate_current!
        with tracker.start_run("step_2", "sim", year=2026, iteration=1):
            moved_pq = run_dir / "moved_trips.parquet"
            moved_df = pd.DataFrame({"trip_id": [4], "dist": [9.0]})
            moved_df.to_parquet(moved_pq, index=False)
            moved_art = tracker.log_artifact(
                moved_pq,
                key="trips",
                direction="output",
                profile_file_schema=True,
                facet={"scenario": "alt"},
                facet_index=True,
            )

        tracker.archive_artifact(moved_art, archive_dir, mode="move")
        assert not moved_pq.exists()

        # Also verify consist.load_df from hot DB works alongside open writable StoreLedger
        hot_pq.unlink()
        recovered_df = consist.load_df(
            ingested_art,
            tracker=tracker,
            db_fallback="always",
        )
        assert sorted(recovered_df["trip_id"].tolist()) == [2, 3]
        # Restore hot_pq on disk to explicitly prove ViewFactory does NOT double-count!
        hot_df.to_parquet(hot_pq, index=False)

        vf = ViewFactory(tracker)
        assert vf.create_hybrid_view("v_trips_all", "trips") is True
        df_hybrid = tracker.query_df(
            "SELECT trip_id, dist, consist_run_id FROM v_trips_all ORDER BY trip_id"
        )
        assert list(df_hybrid["trip_id"]) == [1, 2, 3, 4]
        assert df_hybrid.loc[df_hybrid["trip_id"] == 1, "consist_run_id"].isna().all()
        assert list(
            df_hybrid.loc[df_hybrid["trip_id"].isin([2, 3]), "consist_run_id"]
        ) == ["step_1", "step_1"]
        assert list(df_hybrid.loc[df_hybrid["trip_id"] == 4, "consist_run_id"]) == [
            "step_2"
        ]

        # Grouped hybrid view also deduplicates hot+cold rows!
        schema_id = moved_art.schema_id
        assert schema_id is not None
        assert (
            vf.create_grouped_hybrid_view(
                view_name="v_trips_grouped",
                schema_id=schema_id,
                attach_facets=["scenario"],
            )
            is True
        )
        df_grouped = tracker.query_df(
            "SELECT trip_id, facet_scenario FROM v_trips_grouped ORDER BY trip_id"
        )
        assert list(df_grouped["trip_id"]) == [2, 3, 4]
        assert list(df_grouped["facet_scenario"]) == ["base", "base", "alt"]
    finally:
        tracker.close()


def test_maintenance_lineage_openlineage_and_schema_export(tmp_path: Path) -> None:
    db_main = tmp_path / "main.duckdb"
    run_dir = tmp_path / "runs"
    events_log = tmp_path / "openlineage.jsonl"

    tracker = Tracker(run_dir=run_dir, db_path=db_main)
    try:
        emitter = OpenLineageEmitter(
            OpenLineageOptions(enabled=True, namespace="consist", path=events_log)
        )
        in_pq = tmp_path / "zones.parquet"
        pd.DataFrame({"zone_id": [1, 2], "pop": [100, 200]}).to_parquet(
            in_pq, index=False
        )

        with tracker.start_run("r_keep", "urbansim", year=2025):
            z_in = tracker.log_artifact(in_pq, key="zones", direction="input")
            out_keep = run_dir / "keep_out.parquet"
            pd.DataFrame({"zone_id": [1, 2], "jobs": [50, 80]}).to_parquet(
                out_keep, index=False
            )
            a_keep = tracker.log_artifact(
                out_keep,
                key="jobs",
                direction="output",
                profile_file_schema=True,
            )

        run_keep = tracker.get_run("r_keep")
        assert run_keep is not None
        emitter.emit_complete(run_keep, inputs=[z_in], outputs=[a_keep])
        assert events_log.exists() and events_log.read_text(encoding="utf-8").strip()

        with tracker.start_run("r_purge", "urbansim", year=2026):
            tracker.log_artifact(a_keep, key="jobs_in", direction="input")
            out_purge = run_dir / "purge_out.parquet"
            pd.DataFrame({"zone_id": [1], "val": [9.5]}).to_parquet(
                out_purge, index=False
            )
            tracker.log_artifact(out_purge, key="purge_val", direction="output")

        # Lineage traversal over main.run_binding
        lineage = LineageService(tracker)
        tree = lineage.get_lineage(a_keep.id)
        assert tree is not None
        assert tree["producing_run"]["run"].id == "r_keep"

        # Schema SQLModel code export over StoreLedger
        assert a_keep.schema_id is not None
        code_str = tracker.export_schema_sqlmodel(schema_id=a_keep.schema_id)
        assert "class " in code_str and "SQLModel" in code_str

        assert tracker.db is not None
        maint = DatabaseMaintenance(tracker.db, run_dir)
        report = maint.inspect()
        assert report.total_runs == 2
        doc = maint.doctor()
        assert doc.zombie_run_ids == []

        # Snapshot
        snap_path = tmp_path / "snap.duckdb"
        maint.snapshot(snap_path)
        assert snap_path.exists()

        # Export subset containing only r_keep
        export_path = tmp_path / "subset.duckdb"
        exp_res = maint.export(
            "r_keep",
            export_path,
            include_data=True,
            include_snapshots=False,
            include_children=False,
        )
        assert exp_res.run_ids == ["r_keep"]

        # Purge r_purge with prune_files=True: deletes r_purge & unreferenced purge_out,
        # while keeping shared a_keep!
        purge_res = maint.purge(
            "r_purge",
            include_children=False,
            prune_files=True,
            dry_run=False,
        )
        assert purge_res.executed is True
        assert tracker.get_run("r_purge") is None
        assert tracker.get_run("r_keep") is not None
        assert not out_purge.exists()
        assert out_keep.exists()

        # Merge subset into a fresh v2 tracker, and verify v1 merge guard
        v1_shard = tmp_path / "v1_shard.duckdb"
        _init_minimal_v1_db(v1_shard)
        with pytest.raises(DatabaseMigrationRequired):
            maint.merge(v1_shard, auto_migrate=False)
        maint.merge(v1_shard, auto_migrate=True)

        fresh_tracker = Tracker(
            run_dir=tmp_path / "fresh_runs",
            db_path=tmp_path / "merged.duckdb",
        )
        try:
            assert fresh_tracker.db is not None
            fresh_maint = DatabaseMaintenance(fresh_tracker.db, tmp_path / "fresh_runs")
            merge_res = fresh_maint.merge(export_path)
            assert "r_keep" in merge_res.runs_merged
            assert fresh_tracker.get_run("r_keep") is not None

            fresh_maint.fix_status("r_keep", "failed", reason="manual override")
            updated_run = fresh_tracker.get_run("r_keep")
            assert updated_run is not None and updated_run.status == "failed"
            fresh_maint.compact()
        finally:
            fresh_tracker.close()
    finally:
        tracker.close()


def test_container_declared_outputs_affect_signature_and_virtualize(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "containers.duckdb",
    )
    try:
        base_def = ContainerDefinition(
            image="alpine:3.19",
            command=["echo", "hi"],
            backend="docker",
            environment={},
            volumes={},
            image_digest="sha256:1111",
        )
        sig_a = _container_signature(
            defn=base_def.model_copy(
                update={
                    "declared_output_specs": [
                        ("out_a", "workspace://a.parquet", "parquet")
                    ]
                }
            ),
            inputs=[],
            tracker=tracker,
        )
        sig_b = _container_signature(
            defn=base_def.model_copy(
                update={
                    "declared_output_specs": [
                        ("out_a", "workspace://b.parquet", "parquet")
                    ]
                }
            ),
            inputs=[],
            tracker=tracker,
        )
        assert sig_a != sig_b
    finally:
        tracker.close()


def test_cli_read_only_migration_error_lock_pid_and_promote_run(
    tmp_path: Path,
) -> None:
    runner = CliRunner()

    # 1. Unmigrated v1 DB -> actionable DatabaseMigrationRequired message
    v1_db = tmp_path / "legacy_v1.duckdb"
    _init_minimal_v1_db(v1_db)
    safe_mtime_ns = 1_700_000_000_000_000_000
    safe_ts = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )
    cold_file = tmp_path / "trips.parquet"
    pd.DataFrame({"id": [1, 2]}).to_parquet(cold_file, index=False)
    os.utime(cold_file, ns=(safe_mtime_ns, safe_mtime_ns))
    fast_h = compute_v1_fast_file_hash(cold_file.stat().st_size, safe_mtime_ns)
    art_id = uuid.uuid4()
    with duckdb.connect(str(v1_db)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, signature, started_at, created_at, updated_at) "
            "VALUES ('hist_run_1', 'beam', 'completed', 'sig_hist_1', ?, ?, ?)",
            [safe_ts, safe_ts, safe_ts],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'trips', ?, 'parquet', ?, 'hist_run_1', '{}', ?)",
            [str(art_id), str(cold_file), fast_h, safe_ts],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('hist_run_1', ?, 'output', false)",
            [str(art_id)],
        )

    res_v1 = runner.invoke(app, ["runs", "--db-path", str(v1_db)])
    assert res_v1.exit_code != 0
    assert "consist migrate" in click.unstyle(res_v1.output)

    # 2. DuckDB lock conflict -> extracts PID and mentions consist.json + snapshot_db
    v2_db = tmp_path / "locked_v2.duckdb"
    t_init = Tracker(run_dir=tmp_path / "r", db_path=v2_db)
    t_init.close()
    fake_lock_err = duckdb.IOException(
        'IO Error: Could not set lock on file "/tmp/x.duckdb": '
        "Conflicting lock is held in /usr/bin/python3 (PID 98765) by user test."
    )
    with patch("consist.cli.Tracker", side_effect=fake_lock_err):
        res_lock = runner.invoke(app, ["runs", "--db-path", str(v2_db)])
    assert res_lock.exit_code != 0
    lock_out = click.unstyle(res_lock.output)
    assert "98765" in lock_out
    assert "consist.json" in lock_out
    assert "snapshot_db" in lock_out

    # 3. CLI migrate (dry-run, quick, undo, already_v2, error) -> promote -> exec --dry-run
    res_mig_missing = runner.invoke(app, ["migrate", str(tmp_path / "missing.duckdb")])
    assert res_mig_missing.exit_code != 0
    assert "Migration Error:" in click.unstyle(res_mig_missing.output)

    res_dry = runner.invoke(app, ["migrate", str(v1_db), "--dry-run"])
    assert res_dry.exit_code == 0, res_dry.output
    assert "DRY RUN" in click.unstyle(res_dry.output)

    res_quick = runner.invoke(app, ["migrate", str(v1_db), "--quick"])
    assert res_quick.exit_code == 0, res_quick.output
    assert "QUICK MIGRATION" in click.unstyle(res_quick.output)

    res_undo = runner.invoke(app, ["migrate", "undo", str(v1_db)])
    assert res_undo.exit_code == 0, res_undo.output
    assert "Restored v1 database" in click.unstyle(res_undo.output)

    res_quick_again = runner.invoke(app, ["migrate", str(v1_db), "--quick"])
    assert res_quick_again.exit_code == 0, res_quick_again.output

    res_already = runner.invoke(app, ["migrate", str(v1_db)])
    assert res_already.exit_code == 0, res_already.output
    assert "already on Consist v2 schema" in " ".join(
        click.unstyle(res_already.output).split()
    )

    res_runs = runner.invoke(app, ["runs", "--db-path", str(v1_db)])
    assert res_runs.exit_code == 0
    assert "hist_run_1" in click.unstyle(res_runs.output)

    res_promo_err = runner.invoke(app, ["promote", str(v1_db), "nonexistent_target"])
    assert res_promo_err.exit_code != 0
    assert "Promotion Error:" in click.unstyle(res_promo_err.output)

    res_promo = runner.invoke(
        app,
        ["promote", "hist_run_1", "--db-path", str(v1_db)],
    )
    assert res_promo.exit_code == 0, res_promo.output

    with duckdb.connect(str(v1_db), read_only=True) as con:
        row = con.execute(
            "SELECT signature_scheme, cache_status FROM main.run WHERE id = 'hist_run_1'"
        ).fetchone()
    assert row == ("sig-v2", "live")
    assert isinstance(consist.Artifact, type) and consist.Artifact is Artifact

    sugg_file = tmp_path / "suggested_arg.txt"
    sugg_file.write_text("arg", encoding="utf-8")
    res_exec_dry = runner.invoke(
        app,
        [
            "exec",
            "--dry-run",
            "--db-path",
            str(v1_db),
            "--run-dir",
            str(tmp_path / "r"),
            "--outputs-dir",
            str(tmp_path / "out_dir"),
            "-i",
            f"trips={cold_file}",
            "-o",
            f"out={tmp_path / 'out.parquet'}",
            "--",
            "python",
            str(sugg_file),
        ],
    )
    assert res_exec_dry.exit_code == 0, res_exec_dry.output
    exec_dry_out = click.unstyle(res_exec_dry.output)
    assert "Consist Exec Plan (Dry Run)" in exec_dry_out
    assert "Declared Inputs" in exec_dry_out
    assert "Declared Outputs" in exec_dry_out


def test_a1_readonly_export_and_nested_tx_guard(tmp_path: Path) -> None:
    db_path = tmp_path / "a1.duckdb"
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        with tracker.start_run("r_exp", "sim", year=2025):
            pq = run_dir / "out.parquet"
            pd.DataFrame({"x": [1, 2]}).to_parquet(pq, index=False)
            tracker.log_artifact(pq, key="out", direction="output")

        assert tracker.db is not None
        maint_rw = DatabaseMaintenance(tracker.db, run_dir)
        nested_shard = tmp_path / "nested_fail.duckdb"
        with maint_rw.ledger.transaction():
            with pytest.raises(ConsistError, match="transaction"):
                maint_rw.export(
                    "r_exp",
                    nested_shard,
                    include_data=False,
                    include_snapshots=False,
                    include_children=False,
                )
    finally:
        tracker.close()

    # Export on a read_only=True StoreLedger
    ro_db = DatabaseManager(str(db_path), read_only=True)
    try:
        ro_maint = DatabaseMaintenance(ro_db, run_dir)
        ro_shard = tmp_path / "ro_export.duckdb"
        res = ro_maint.export(
            "r_exp",
            ro_shard,
            include_data=True,
            include_snapshots=False,
            include_children=False,
        )
        assert res.run_ids == ["r_exp"]
        assert ro_shard.exists()
    finally:
        ro_db.close()

    # CLI `consist db export` also opens read-only and succeeds
    cli_shard = tmp_path / "cli_export.duckdb"
    runner = CliRunner()
    cli_res = runner.invoke(
        app,
        [
            "db",
            "export",
            "r_exp",
            "--out",
            str(cli_shard),
            "--db-path",
            str(db_path),
        ],
    )
    assert cli_res.exit_code == 0, cli_res.output
    assert cli_shard.exists()


def test_a2_beam_launch_bundle_scheme_qualified_identity(tmp_path: Path) -> None:
    db_path = tmp_path / "beam.duckdb"
    run_dir = tmp_path / "runs"
    cfg_root = tmp_path / "beam_cfg"
    cfg_root.mkdir(parents=True, exist_ok=True)
    (cfg_root / "beam.conf").write_text(
        'beam.agentsim.simulationName = "test_sim"\n',
        encoding="utf-8",
    )
    member_file = tmp_path / "rates.csv"
    member_file.write_text("zone,rate\n1,2.5\n", encoding="utf-8")

    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        with tracker.start_run("prep_member", "beam"):
            member_art = tracker.log_artifact(
                member_file, key="rates", direction="input"
            )
        assert member_art.hash.startswith("sha256:file:")

        adapter = BeamConfigAdapter(primary_config=Path("beam.conf"))
        durable_root = tmp_path / "durable"

        # 1. Cold creation of launch bundle with scheme-qualified v2 Artifact member
        out_1 = tmp_path / "launch_1"
        bundle_1 = adapter.materialize_bundle(
            tracker=tracker,
            base_root_dirs=[cfg_root],
            overrides=BeamConfigOverrides(),
            staged_members=[
                BeamLaunchBundleMember(source=member_art, destination="extra/rates.csv")
            ],
            output_dir=out_1,
            durable_root=durable_root,
        )
        assert bundle_1.artifact.identity.anchor.scheme == "sha256:manifest-v1"
        assert (out_1 / "extra" / "rates.csv").read_text(
            encoding="utf-8"
        ) == "zone,rate\n1,2.5\n"
        staged_prov = bundle_1.artifact.meta["bundle_staged_members"][0]
        assert len(staged_prov["source_hash"]) == 64
        assert ":" not in staged_prov["source_hash"]

        # 2. Warm reuse from durable bundle hits _beam_launch_bundle_directory_manifest
        out_2 = tmp_path / "launch_2"
        bundle_2 = adapter.materialize_bundle(
            tracker=tracker,
            base_root_dirs=[cfg_root],
            overrides=BeamConfigOverrides(),
            staged_members=[
                BeamLaunchBundleMember(source=member_art, destination="extra/rates.csv")
            ],
            output_dir=out_2,
            durable_root=durable_root,
        )
        assert bundle_2.artifact.id == bundle_1.artifact.id
        assert (out_2 / "extra" / "rates.csv").exists()

        # 3. Unverified legacy artifact member is rejected
        unverified_art = Artifact(
            id=uuid.uuid4(),
            key="legacy_rates",
            driver="csv",
            identity=Identity(anchor=Digest("legacy-v1:unverified", "0" * 64)),
        )
        with pytest.raises(ValueError, match="sha256:file"):
            adapter.materialize_bundle(
                tracker=tracker,
                base_root_dirs=[cfg_root],
                overrides=BeamConfigOverrides(),
                staged_members=[
                    BeamLaunchBundleMember(
                        source=unverified_art, destination="extra/legacy.csv"
                    )
                ],
                output_dir=tmp_path / "launch_bad",
                durable_root=durable_root,
            )
    finally:
        tracker.close()


def test_a3_shared_cold_path_does_not_square_rows(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=tmp_path / "a3.duckdb")
    try:
        shared_pq = tmp_path / "shared_trips.parquet"
        pd.DataFrame({"trip_id": [1, 2, 3]}).to_parquet(shared_pq, index=False)

        with tracker.start_run("run_1", "sim", year=2025):
            art_1 = tracker.log_artifact(
                shared_pq,
                key="shared_trips",
                direction="output",
                profile_file_schema=True,
                facet={"scenario": "s1"},
                facet_index=True,
            )
        with tracker.start_run("run_2", "sim", year=2026):
            art_2 = tracker.log_artifact(
                shared_pq,
                key="shared_trips",
                direction="output",
                profile_file_schema=True,
                facet={"scenario": "s2"},
                facet_index=True,
            )
        assert art_1.id != art_2.id

        vf = ViewFactory(tracker)
        assert vf.create_hybrid_view("v_shared_hybrid", "shared_trips") is True
        df_hybrid = tracker.query_df(
            "SELECT trip_id, consist_run_id FROM v_shared_hybrid ORDER BY consist_run_id, trip_id"
        )
        assert len(df_hybrid) == 6
        assert list(df_hybrid["consist_run_id"]) == [
            "run_1",
            "run_1",
            "run_1",
            "run_2",
            "run_2",
            "run_2",
        ]

        assert art_1.schema_id is not None
        assert (
            vf.create_grouped_hybrid_view(
                view_name="v_shared_grouped",
                schema_id=art_1.schema_id,
                attach_facets=["scenario"],
            )
            is True
        )
        df_grouped = tracker.query_df(
            "SELECT trip_id, facet_scenario FROM v_shared_grouped ORDER BY facet_scenario, trip_id"
        )
        assert len(df_grouped) == 6
        assert list(df_grouped["facet_scenario"]) == [
            "s1",
            "s1",
            "s1",
            "s2",
            "s2",
            "s2",
        ]
    finally:
        tracker.close()


def test_a4_v1_migrated_hot_table_evolution_no_duplication_and_extra_hot_cols(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=tmp_path / "a4.duckdb")
    try:
        pq_v1 = run_dir / "ev_v1.parquet"
        pd.DataFrame({"ev_id": [10]}).to_parquet(pq_v1, index=False)
        with tracker.start_run("r_v1", "sim", year=2024):
            art_v1 = tracker.log_artifact(
                pq_v1,
                key="events",
                direction="output",
                profile_file_schema=True,
                is_ingested=True,
            )

        pq_v2 = run_dir / "ev_v2.parquet"
        pd.DataFrame({"ev_id": [20]}).to_parquet(pq_v2, index=False)
        with tracker.start_run("r_v2", "sim", year=2025):
            art_v2 = tracker.log_artifact(
                pq_v2,
                key="events",
                direction="output",
                profile_file_schema=True,
                is_ingested=True,
            )

        pq_v3 = run_dir / "ev_v3.parquet"
        pd.DataFrame({"ev_id": [30]}).to_parquet(pq_v3, index=False)
        with tracker.start_run("r_v3", "sim", year=2026):
            art_v3 = tracker.log_artifact(
                pq_v3,
                key="events",
                direction="output",
                dlt_table_name="custom_events",
                is_ingested=True,
            )

        assert tracker.ledger is not None
        with tracker.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            # Evolved table: v1 row has NULL consist_artifact_id; v2 row has populated consist_artifact_id
            cur.execute(
                "CREATE TABLE global_tables.events ("
                "ev_id INTEGER, consist_run_id VARCHAR, consist_artifact_id VARCHAR"
                ")"
            )
            cur.execute(
                "INSERT INTO global_tables.events VALUES (10, 'r_v1', NULL), (20, 'r_v2', ?)",
                [str(art_v2.id)],
            )
            # Extra hot table WITHOUT consist_run_id column
            cur.execute(
                "CREATE TABLE global_tables.custom_events ("
                "ev_id INTEGER, consist_artifact_id VARCHAR"
                ")"
            )
            cur.execute(
                "INSERT INTO global_tables.custom_events VALUES (30, ?)",
                [str(art_v3.id)],
            )

        vf = ViewFactory(tracker)
        assert vf.create_hybrid_view("v_events_all", "events") is True
        df_all = tracker.query_df("SELECT ev_id FROM v_events_all ORDER BY ev_id")
        assert list(df_all["ev_id"]) == [10, 20, 30]

        assert art_v1.schema_id is not None
        assert (
            vf.create_grouped_hybrid_view(
                view_name="v_events_grouped",
                schema_id=art_v1.schema_id,
            )
            is True
        )
        df_grp = tracker.query_df("SELECT ev_id FROM v_events_grouped ORDER BY ev_id")
        assert list(df_grp["ev_id"]) == [10, 20]
    finally:
        tracker.close()


def test_a5_purge_preserves_disk_file_referenced_via_anchor_promotion(
    tmp_path: Path,
) -> None:
    v1_db = tmp_path / "promo_purge.duckdb"
    _init_minimal_v1_db(v1_db)

    shared_file = tmp_path / "runs" / "shared_out.parquet"
    shared_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"k": [1]}).to_parquet(shared_file, index=False)

    safe_mtime_ns = 1_700_000_000_000_000_000
    os.utime(shared_file, ns=(safe_mtime_ns, safe_mtime_ns))
    safe_ts = datetime.fromtimestamp(
        (safe_mtime_ns + 10_000_000_000) / 1_000_000_000,
        tz=timezone.utc,
    )
    fast_h = compute_v1_fast_file_hash(shared_file.stat().st_size, safe_mtime_ns)
    hist_art_id = uuid.uuid4()

    with duckdb.connect(str(v1_db)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, signature, started_at, created_at, updated_at) "
            "VALUES ('hist_keep', 'sim', 'completed', 'sig_h', ?, ?, ?)",
            [safe_ts, safe_ts, safe_ts],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'shared_out', ?, 'parquet', ?, 'hist_keep', '{}', ?)",
            [str(hist_art_id), str(shared_file), fast_h, safe_ts],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('hist_keep', ?, 'output', false)",
            [str(hist_art_id)],
        )

    migrate_v1_to_v2(v1_db, quick=True)
    # Promote hist_keep -> inserts anchor_promotion(historical_anchor -> verified_anchor)
    # while main.artifact for hist_art_id keeps historical_anchor!
    runner = CliRunner()
    promo_res = runner.invoke(app, ["promote", "hist_keep", "--db-path", str(v1_db)])
    assert promo_res.exit_code == 0, promo_res.output

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=v1_db)
    try:
        with tracker.start_run("live_purge", "sim", year=2026):
            tracker.log_artifact(shared_file, key="shared_out", direction="output")

        assert tracker.db is not None
        maint = DatabaseMaintenance(tracker.db, tmp_path / "runs")
        purge_res = maint.purge(
            "live_purge",
            include_children=False,
            prune_files=True,
            dry_run=False,
        )
        assert purge_res.executed is True
        assert tracker.get_run("live_purge") is None
        assert tracker.get_run("hist_keep") is not None
        assert shared_file.exists()
    finally:
        tracker.close()


def test_a6_rebuild_from_json_legacy_v04_snapshot_historical_isolation(
    tmp_path: Path,
) -> None:
    json_dir = tmp_path / "snapshots"
    json_dir.mkdir(parents=True, exist_ok=True)
    art_id_1 = str(uuid.uuid4())
    art_id_2 = str(uuid.uuid4())

    snap_1 = {
        "run": {
            "id": "legacy_run_null_hash",
            "model_name": "urbansim",
            "status": "completed",
            "signature": "sig_legacy_1",
            "meta": {"stage": "step1"},
        },
        "inputs": [],
        "outputs": [
            {
                "id": art_id_1,
                "key": "households",
                "container_uri": "outputs://hh.parquet",
                "driver": "parquet",
                "hash": None,
                "run_id": "legacy_run_null_hash",
                "meta": {},
            }
        ],
        "config": {"beta": 0.5},
    }
    snap_2 = {
        "run": {
            "id": "legacy_run_hex_hash",
            "model_name": "urbansim",
            "status": "completed",
            "signature": "sig_legacy_2",
            "meta": {},
        },
        "inputs": [],
        "outputs": [
            {
                "id": art_id_2,
                "key": "persons",
                "container_uri": "outputs://persons.parquet",
                "driver": "parquet",
                "hash": "a" * 64,
                "run_id": "legacy_run_hex_hash",
                "meta": {},
            }
        ],
        "config": {"beta": 0.75},
    }
    (json_dir / "snap_1.json").write_text(json.dumps(snap_1), encoding="utf-8")
    (json_dir / "snap_2.json").write_text(json.dumps(snap_2), encoding="utf-8")

    db_path = tmp_path / "rebuilt.duckdb"
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)
    try:
        assert tracker.db is not None
        maint = DatabaseMaintenance(tracker.db, tmp_path / "runs")
        res_min = maint.rebuild_from_json(json_dir, mode="minimal", dry_run=False)
        assert res_min.errors == []
        assert res_min.runs_inserted == 2

        res_full = maint.rebuild_from_json(json_dir, mode="full", dry_run=False)
        assert res_full.errors == []

        assert tracker.ledger is not None
        with tracker.ledger.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT id, cache_status, signature_scheme FROM main.run ORDER BY id"
            ).fetchall()
        assert rows == [
            ("legacy_run_hex_hash", "historical", "legacy-v1"),
            ("legacy_run_null_hash", "historical", "legacy-v1"),
        ]
    finally:
        tracker.close()


def test_a7_purge_and_merge_clear_store_ledger_location_memos(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=tmp_path / "a7_main.duckdb")
    shard_tracker = Tracker(
        run_dir=tmp_path / "shard_runs", db_path=tmp_path / "a7_shard.duckdb"
    )
    try:
        with tracker.start_run("r_del", "sim"):
            pq = run_dir / "del.parquet"
            pd.DataFrame({"v": [1]}).to_parquet(pq, index=False)
            tracker.log_artifact(pq, key="del", direction="output")

        with shard_tracker.start_run("r_shard", "sim"):
            spq = tmp_path / "shard_runs" / "s.parquet"
            pd.DataFrame({"v": [2]}).to_parquet(spq, index=False)
            shard_tracker.log_artifact(spq, key="s", direction="output")
        shard_tracker.close()

        assert tracker.ledger is not None and tracker.db is not None
        tracker.ledger._memoized_observations[("a1", "b1")] = "obs1"
        tracker.ledger._memoized_locations["dummy_anchor"] = {"dummy_loc"}
        assert len(tracker.ledger._memoized_observations) > 0
        assert len(tracker.ledger._memoized_locations) > 0

        maint = DatabaseMaintenance(tracker.db, run_dir)
        maint.purge("r_del", dry_run=False)
        assert len(tracker.ledger._memoized_observations) == 0
        assert len(tracker.ledger._memoized_locations) == 0

        tracker.ledger._memoized_observations[("a", "b")] = "obs"
        tracker.ledger._memoized_locations["dummy_anchor_2"] = {"dummy_loc_2"}
        maint.merge(tmp_path / "a7_shard.duckdb")
        assert len(tracker.ledger._memoized_observations) == 0
        assert len(tracker.ledger._memoized_locations) == 0
    finally:
        shard_tracker.close()
        tracker.close()


def test_a8_shell_readonly_mutating_passthrough_and_preview_no_relation_leak(
    tmp_path: Path,
) -> None:
    assert {"admission", "migrate", "promote"} <= set(ConsistShell._CLI_ROOT_COMMANDS)

    db_path = tmp_path / "shell.duckdb"
    run_dir = tmp_path / "runs"
    pq = tmp_path / "table.parquet"
    pd.DataFrame({"col_a": [10, 20], "col_b": ["x", "y"]}).to_parquet(pq, index=False)
    writer = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        with writer.start_run("r_shell", "sim"):
            art = writer.log_artifact(pq, key="table_key", direction="output")
    finally:
        writer.close()

    # CLI preview closes relation connection
    gc.collect()
    baseline_relations = active_relation_count()
    runner = CliRunner()
    prev_res = runner.invoke(
        app, ["preview", str(art.id), "--db-path", str(db_path), "--rows", "1"]
    )
    assert prev_res.exit_code == 0, prev_res.output
    assert active_relation_count() == baseline_relations

    # ConsistShell opens read_only=True, temporarily releases lock for mutating CLI subcommands,
    # and cleans up relation connections on preview / schema-profile
    ro_tracker = get_tracker(str(db_path), read_only=True)
    shell = ConsistShell(ro_tracker)
    try:
        assert shell.tracker.read_only is True
        shell.onecmd("db compact")
        assert shell.tracker.read_only is True
        assert shell.tracker.ledger is not None

        shell.onecmd(f"preview {art.id} --rows 1")
        assert active_relation_count() == baseline_relations

        shell.onecmd(f"schema-profile {art.id}")
        assert active_relation_count() == baseline_relations
    finally:
        shell.tracker.close()


def test_a9_config_run_rows_table_name_tuple_and_string_table_run_id_collision(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "a9.duckdb")
    try:
        assert tracker.ledger is not None
        with tracker.ledger.transaction() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute(
                "CREATE TABLE global_tables.my_cfg ("
                "content_hash VARCHAR, run_id VARCHAR, alpha INTEGER"
                ")"
            )
            cur.execute(
                "CREATE TABLE global_tables.my_link ("
                "run_id VARCHAR, table_name VARCHAR, content_hash VARCHAR"
                ")"
            )
            cur.execute(
                "INSERT INTO global_tables.my_cfg VALUES ('h1', 'cfg_internal_run_id', 7)"
            )
            cur.execute(
                "INSERT INTO global_tables.my_link VALUES ('actual_run_1', 'my_cfg', 'h1')"
            )

        # 1. 2-tuple (table_name_str, col_name_str) in columns=...
        col_vals = execute_config_run_rows(
            tracker,
            "my_cfg",
            link_table="my_link",
            columns=("my_cfg", "alpha"),
        )
        assert col_vals == [7]

        # 2. String-table columns=None preserves t.run_id without `run_id_1` alias collision
        full_rows = execute_config_run_rows(
            tracker,
            "my_cfg",
            link_table="my_link",
            columns=None,
        )
        assert full_rows == [
            (
                "actual_run_1",
                {
                    "content_hash": "h1",
                    "run_id": "cfg_internal_run_id",
                    "alpha": 7,
                },
            )
        ]
    finally:
        tracker.close()


def test_a10_merge_skip_scopes_schema_ids_and_migrate_undo_missing_arg(
    tmp_path: Path,
) -> None:
    # 1. CLI `consist migrate undo` without `<db_path>` gives clear usage error
    runner = CliRunner()
    undo_res = runner.invoke(app, ["migrate", "undo"])
    assert undo_res.exit_code != 0
    assert "Missing '<db_path>'" in click.unstyle(undo_res.output)

    # 2. `DatabaseMaintenance.merge(..., conflict="skip")` does not copy schemas of skipped runs
    main_db = tmp_path / "main_a10.duckdb"
    shard_db = tmp_path / "shard_a10.duckdb"

    main_tr = Tracker(run_dir=tmp_path / "main_runs", db_path=main_db)
    try:
        with main_tr.start_run("r_dup", "sim"):
            pass
    finally:
        main_tr.close()

    shard_tr = Tracker(run_dir=tmp_path / "shard_runs", db_path=shard_db)
    try:
        with shard_tr.start_run("r_dup", "sim", year=2024):
            pq_dup = tmp_path / "shard_runs" / "dup.parquet"
            pd.DataFrame({"only_in_skipped_dup": [1]}).to_parquet(pq_dup, index=False)
            art_dup = shard_tr.log_artifact(
                pq_dup,
                key="dup",
                direction="output",
                profile_file_schema=True,
            )
            skipped_schema_id = art_dup.schema_id
            assert skipped_schema_id is not None
            assert shard_tr.ledger is not None
            with shard_tr.ledger.transaction() as cur:
                cur.execute(
                    "INSERT INTO main.artifact_schema "
                    "(id, summary_json, profile_version, created_at) "
                    "VALUES ('schema_exclusively_skipped', '{\"table_name\":\"dup_extra\"}', 1, CURRENT_TIMESTAMP)"
                )
                cur.execute(
                    "INSERT INTO main.artifact_schema_observation "
                    "(id, artifact_id, schema_id, run_id, source, observed_at) "
                    "VALUES (?, ?, 'schema_exclusively_skipped', 'r_dup', 'file', CURRENT_TIMESTAMP)",
                    [uuid.uuid4(), art_dup.id],
                )

        with shard_tr.start_run("r_new", "sim", year=2025):
            pq_new = tmp_path / "shard_runs" / "new.parquet"
            pd.DataFrame({"kept_col": ["hello"]}).to_parquet(pq_new, index=False)
            art_new = shard_tr.log_artifact(
                pq_new,
                key="new",
                direction="output",
                profile_file_schema=True,
            )
            kept_schema_id = art_new.schema_id
            assert kept_schema_id is not None
    finally:
        shard_tr.close()

    main_tr2 = Tracker(run_dir=tmp_path / "main_runs", db_path=main_db)
    try:
        assert main_tr2.db is not None and main_tr2.ledger is not None
        maint = DatabaseMaintenance(main_tr2.db, tmp_path / "main_runs")
        merge_res = maint.merge(shard_db, conflict="skip")
        assert merge_res.runs_skipped == ["r_dup"]
        assert merge_res.runs_merged == ["r_new"]

        with main_tr2.ledger.exec_cursor() as cur:
            schema_ids_in_main = {
                str(r[0])
                for r in cur.execute("SELECT id FROM main.artifact_schema").fetchall()
            }
        assert kept_schema_id in schema_ids_in_main
        assert skipped_schema_id not in schema_ids_in_main
        assert "schema_exclusively_skipped" not in schema_ids_in_main
    finally:
        main_tr2.close()
