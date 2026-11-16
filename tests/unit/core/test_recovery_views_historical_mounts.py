from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import pytest

from consist.core.tracker import Tracker
from consist.core.tracker_recovery import resolve_verified_local_path


def test_login_node_view_and_recovery_resolves_archive_via_historical_mount(
    tmp_path: Path,
) -> None:
    compute_ws = tmp_path / "compute_local_scratch"
    login_tmp = tmp_path / "login_tmp"
    shared_scratch = tmp_path / "global_shared_scratch"
    db_path = tmp_path / "prov.duckdb"

    compute_ws.mkdir(parents=True, exist_ok=True)
    login_tmp.mkdir(parents=True, exist_ok=True)
    shared_scratch.mkdir(parents=True, exist_ok=True)

    # 1. Compute node runs a run producing out.parquet, archived to scratch via mode="move"
    compute_tracker = Tracker(
        run_dir=compute_ws,
        db_path=str(db_path),
        mounts={"scratch": str(shared_scratch)},
    )
    try:
        out_file = compute_ws / "out.parquet"
        pd.DataFrame({"x": [10, 20], "label": ["a", "b"]}).to_parquet(out_file)

        with compute_tracker.start_run("step_sim", model="test"):
            art = compute_tracker.log_output(out_file, key="out")

        # Archive with mode="move" so workspace file is removed
        archive_root = shared_scratch / "consist-recovery"
        compute_tracker.archive_run_outputs(
            "step_sim",
            archive_root=archive_root,
            mode="move",
        )
        assert not out_file.exists()
    finally:
        compute_tracker.close()

    # 2. Login node inspects the DB with NO mounts passed (mounts={"scratch": ...} not provided)
    login_tracker = Tracker(
        run_dir=login_tmp,
        db_path=str(db_path),
        read_only=True,
    )
    try:
        loaded_art = login_tracker.get_artifact(art.id)
        assert loaded_art is not None

        # resolve_verified_local_path should resolve via historical mounts recorded on the run/DB
        resolved = resolve_verified_local_path(
            login_tracker, loaded_art, allow_recovery_roots=True
        )
        assert resolved is not None
        assert resolved.exists()
        assert str(shared_scratch.resolve()) in str(resolved.resolve())

        # create_view should resolve the cold record and return the rows
        login_tracker.create_view("v_out", "out")
        df = login_tracker.query_df("SELECT * FROM v_out")
        assert len(df) == 2
    finally:
        login_tracker.close()


def test_db_fallback_resolves_hyphenated_archive_mount_across_sessions(
    tmp_path: Path,
) -> None:
    compute_ws = tmp_path / "compute_ws"
    shared_scratch = tmp_path / "lrc_shared_scratch"
    login_tmp = tmp_path / "login_tmp"
    db_path = tmp_path / "prov.duckdb"

    compute_ws.mkdir(parents=True, exist_ok=True)
    shared_scratch.mkdir(parents=True, exist_ok=True)
    login_tmp.mkdir(parents=True, exist_ok=True)

    # Step runs without lrc-scratch mount; archival runs in a session with lrc-scratch mount
    step_tracker = Tracker(run_dir=compute_ws, db_path=str(db_path))
    try:
        out_file = compute_ws / "trips.parquet"
        pd.DataFrame({"trip_id": [1, 2, 3]}).to_parquet(out_file)
        with step_tracker.start_run("step_only", model="test"):
            art = step_tracker.log_output(out_file, key="trips")
    finally:
        step_tracker.close()

    archive_tracker = Tracker(
        run_dir=compute_ws,
        db_path=str(db_path),
        mounts={"lrc-scratch": str(shared_scratch)},
    )
    try:
        archive_tracker.archive_run_outputs(
            "step_only",
            archive_root=shared_scratch / "consist-recovery",
            mode="move",
        )
    finally:
        archive_tracker.close()

    login_tracker = Tracker(
        run_dir=login_tmp,
        db_path=str(db_path),
        read_only=True,
    )
    try:
        loaded = login_tracker.get_artifact(art.id)
        assert loaded is not None
        resolved = resolve_verified_local_path(
            login_tracker, loaded, allow_recovery_roots=True
        )
        assert resolved is not None and resolved.exists()
        login_tracker.create_view("v_trips", "trips")
        df = login_tracker.query_df("SELECT * FROM v_trips")
        assert len(df) == 3
    finally:
        login_tracker.close()


def test_mount_scheme_mismatch_guard_does_not_misresolve_archive(
    tmp_path: Path,
) -> None:
    compute_ws = tmp_path / "runs"
    inputs_dir = tmp_path / "inputs_dir"
    shared_scratch = tmp_path / "shared_scratch"
    db_path = tmp_path / "prov.duckdb"

    compute_ws.mkdir(parents=True, exist_ok=True)
    inputs_dir.mkdir(parents=True, exist_ok=True)
    shared_scratch.mkdir(parents=True, exist_ok=True)

    in_file = inputs_dir / "in.parquet"
    pd.DataFrame({"val": [1, 2, 3]}).to_parquet(in_file)

    tracker = Tracker(
        run_dir=compute_ws,
        db_path=str(db_path),
        mounts={"inputs": str(inputs_dir), "scratch": str(shared_scratch)},
    )
    try:
        with tracker.start_run("run_1", model="test"):
            # Artifact is on inputs:// mount
            art = tracker.log_input(in_file, key="in")
        assert art.meta.get("mount_scheme") == "inputs"
        assert art.meta.get("mount_root") == str(inputs_dir.resolve())

        # Archive to scratch mount
        archive_root = shared_scratch / "recovery"
        tracker.archive_artifact(art, archive_root, mode="copy")
        # Unlink original input file to force resolving via recovery location
        in_file.unlink()
    finally:
        tracker.close()

    # Now open tracker with only workspace (NO mounts)
    inspect_tracker = Tracker(
        run_dir=tmp_path / "inspect",
        db_path=str(db_path),
        read_only=True,
    )
    try:
        loaded = inspect_tracker.get_artifact(art.id)
        assert loaded is not None

        resolved = resolve_verified_local_path(
            inspect_tracker, loaded, allow_recovery_roots=True
        )
        assert resolved is not None
        assert resolved.exists()
        # Must resolve to shared_scratch, NOT under inputs_dir
        assert str(shared_scratch.resolve()) in str(resolved.resolve())
        assert str(inputs_dir.resolve()) not in str(resolved.resolve())
    finally:
        inspect_tracker.close()


def test_missing_file_warning_diagnostic_in_view_factory(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ws = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    ws.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    out_file = ws / "out.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out_file)

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.start_run("run_warn", model="test"):
            art = tracker.log_output(out_file, key="out", profile_file_schema=True)
        tracker.archive_artifact(art, archive_dir, mode="copy")

        # Now genuinely delete both primary and archive copies
        out_file.unlink()
        for p in archive_dir.glob("**/*.parquet"):
            p.unlink()

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            tracker.create_grouped_view(
                view_name="v_missing",
                schema_id=art.meta["schema_id"],
                missing_files="warn",
            )

        # Verify warning log message mentions both primary path and checked location URI
        warning_records = [
            r
            for r in caplog.records
            if "[Consist Warning] Skipping missing artifact" in r.message
        ]
        assert len(warning_records) >= 1
        msg = warning_records[0].message
        assert "checked locations:" in msg
    finally:
        tracker.close()


def test_historical_mount_prefers_existing_host_mount_over_nonexistent_container_mount(
    tmp_path: Path,
) -> None:
    compute_ws = tmp_path / "compute_ws"
    host_scratch = tmp_path / "host_scratch"
    login_tmp = tmp_path / "login_tmp"
    db_path = tmp_path / "prov.duckdb"

    compute_ws.mkdir(parents=True, exist_ok=True)
    host_scratch.mkdir(parents=True, exist_ok=True)
    login_tmp.mkdir(parents=True, exist_ok=True)

    # Run 1 on host: records host_scratch
    t1 = Tracker(
        run_dir=compute_ws,
        db_path=str(db_path),
        mounts={"shared": str(host_scratch)},
    )
    try:
        out_file = compute_ws / "data.parquet"
        pd.DataFrame({"v": [42]}).to_parquet(out_file)
        with t1.start_run("host_run", model="test"):
            art = t1.log_output(out_file, key="data")
        t1.archive_artifact(
            art,
            host_scratch / "consist-recovery",
            mode="move",
        )
    finally:
        t1.close()

    # Run 2 later in container: records a container path that doesn't exist on host
    t2 = Tracker(
        run_dir=compute_ws,
        db_path=str(db_path),
        mounts={"shared": "/nonexistent_container/scratch"},
    )
    try:
        with t2.start_run("container_run", model="test"):
            pass
    finally:
        t2.close()

    # Login node: without mounts passed, must pick existing host_scratch rather than nonexistent container path
    login_tracker = Tracker(
        run_dir=login_tmp,
        db_path=str(db_path),
        read_only=True,
    )
    try:
        loaded = login_tracker.get_artifact(art.id)
        assert loaded is not None
        resolved = resolve_verified_local_path(
            login_tracker, loaded, allow_recovery_roots=True
        )
        assert resolved is not None and resolved.exists()
        assert str(host_scratch) in str(resolved)
    finally:
        login_tracker.close()
