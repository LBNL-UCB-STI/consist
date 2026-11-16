from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

from consist.core.maintenance import DatabaseMaintenance
from consist.core.tracker import Tracker


def test_named_mount_prepend_source_mount_root_dedup(tmp_path: Path) -> None:
    workspace = tmp_path / "runs"
    outputs_dir = tmp_path / "outputs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_file = outputs_dir / "households.parquet"
    pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_parquet(out_file)

    tracker = Tracker(
        run_dir=workspace,
        db_path=str(db_path),
        mounts={"outputs": str(outputs_dir)},
    )
    try:
        with tracker.start_run("run_1", model="test"):
            art = tracker.log_output(out_file, key="households")

        # Prior to archival: 1 row in artifact_location
        con = duckdb.connect(str(db_path))
        try:
            rows_before = con.execute(
                "SELECT location_id, site, uri, status FROM artifact_location WHERE artifact_id = ?",
                [str(art.id)],
            ).fetchall()
            assert len(rows_before) == 1
            assert rows_before[0][1] == "external"
            assert rows_before[0][2] == "outputs://households.parquet"
            assert rows_before[0][3] == "active"
        finally:
            con.close()

        # Now archive with mode="copy", append=True, prepend_source_mount_root=True (default)
        tracker.archive_artifact(
            art,
            archive_dir,
            mode="copy",
            append=True,
            prepend_source_mount_root=True,
        )

        con = duckdb.connect(str(db_path))
        try:
            rows_after = con.execute(
                "SELECT location_id, site, uri, status FROM artifact_location WHERE artifact_id = ? AND uri = ?",
                [str(art.id), "outputs://households.parquet"],
            ).fetchall()
            # There should be exactly 1 row for outputs://households.parquet, with site='archive' and status='active'
            assert len(rows_after) == 1, f"Expected 1 row, got: {rows_after}"
            assert rows_after[0][1] == "archive"
            assert rows_after[0][3] == "active"

            all_active = con.execute(
                "SELECT site, uri FROM artifact_location WHERE artifact_id = ? AND status = 'active'",
                [str(art.id)],
            ).fetchall()
            # 1 row for outputs://households.parquet (archive) + 1 row for archive:// (archive)
            assert len(all_active) == 2
        finally:
            con.close()

        roots = tracker.artifact_recovery_roots(art)
        assert len(roots) == 2
        assert str(outputs_dir.resolve()) in [str(Path(r).resolve()) for r in roots]
        assert str(archive_dir.resolve()) in [str(Path(r).resolve()) for r in roots]
    finally:
        tracker.close()


def test_advisory_promoted_to_archive_single_row(tmp_path: Path) -> None:
    workspace = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    workspace.mkdir(parents=True, exist_ok=True)
    out_file = workspace / "data.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out_file)

    tracker = Tracker(run_dir=workspace, db_path=str(db_path))
    try:
        with tracker.start_run("run_adv", model="test"):
            art = tracker.log_output(out_file, key="data")

        # Register advisory recovery root
        tracker.set_artifact_recovery_roots(art, [str(archive_dir)], site="advisory")

        con = duckdb.connect(str(db_path))
        try:
            adv_rows = con.execute(
                "SELECT location_id, site, uri, status, witness_json FROM artifact_location "
                "WHERE artifact_id = ? AND site = 'advisory'",
                [str(art.id)],
            ).fetchall()
            assert len(adv_rows) == 1
            adv_uri = adv_rows[0][2]
        finally:
            con.close()

        # Now archive to that exact root (promotes to archive with witness)
        tracker.archive_artifact(art, archive_dir, mode="copy")

        con = duckdb.connect(str(db_path))
        try:
            rows_after = con.execute(
                "SELECT location_id, site, uri, status, witness_json FROM artifact_location "
                "WHERE artifact_id = ? AND uri = ?",
                [str(art.id), adv_uri],
            ).fetchall()
            # MUST be exactly 1 row at this (artifact_id, uri), promoted to archive and active
            assert len(rows_after) == 1, f"Expected 1 row, got {rows_after}"
            assert rows_after[0][1] == "archive"
            assert rows_after[0][3] == "active"
            assert rows_after[0][4] is not None
        finally:
            con.close()
    finally:
        tracker.close()


def test_sync_artifact_with_recovery_roots_does_not_demote_archive(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    workspace.mkdir(parents=True, exist_ok=True)
    out_file = workspace / "data.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out_file)

    tracker = Tracker(run_dir=workspace, db_path=str(db_path))
    try:
        with tracker.start_run("run_sync", model="test"):
            art = tracker.log_output(out_file, key="data")

        tracker.archive_artifact(art, archive_dir, mode="copy")

        # Now sync_artifact with recovery_roots containing archive_dir
        tracker.persistence.sync_artifact(
            art, direction="output", recovery_roots=[str(archive_dir)]
        )

        con = duckdb.connect(str(db_path))
        try:
            rows = con.execute(
                "SELECT location_id, site, uri, status FROM artifact_location WHERE artifact_id = ?",
                [str(art.id)],
            ).fetchall()
            # Should have 1 workspace row and 1 archive row (NOT an extra advisory row or demoted archive)
            assert len(rows) == 2, f"Expected 2 rows, got {rows}"
            sites = {r[1] for r in rows}
            assert sites == {"workspace", "archive"}
            assert all(r[3] == "active" for r in rows)
        finally:
            con.close()
    finally:
        tracker.close()


def test_evicted_tombstones_preserved_when_no_active_row(tmp_path: Path) -> None:
    workspace = tmp_path / "runs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    workspace.mkdir(parents=True, exist_ok=True)
    out_file = workspace / "data.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out_file)

    tracker = Tracker(run_dir=workspace, db_path=str(db_path))
    try:
        with tracker.start_run("run_evict", model="test"):
            art = tracker.log_output(out_file, key="data")

        tracker.archive_artifact(art, archive_dir, mode="copy")
        # Now evict primary
        tracker.evict_artifact(art)

        assert tracker.db is not None
        assert tracker.db.has_evicted_artifact_location(artifact_id=art.id)

        con = duckdb.connect(str(db_path))
        try:
            evicted_rows = con.execute(
                "SELECT location_id, site, status FROM artifact_location WHERE artifact_id = ? AND status = 'evicted'",
                [str(art.id)],
            ).fetchall()
            assert len(evicted_rows) >= 1
        finally:
            con.close()
    finally:
        tracker.close()


def test_merge_db_deduplicates_advisory_and_archive(tmp_path: Path) -> None:
    db1_path = tmp_path / "db1.duckdb"
    db2_path = tmp_path / "db2.duckdb"
    ws = tmp_path / "ws"
    ws.mkdir(parents=True, exist_ok=True)

    out_file = ws / "out.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out_file)

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # In db1: log output and set advisory recovery root
    t1 = Tracker(run_dir=ws, db_path=str(db1_path))
    try:
        with t1.start_run("run_main", model="test"):
            art1 = t1.log_output(out_file, key="out")
        t1.set_artifact_recovery_roots(art1, [str(archive_dir)], site="advisory")
    finally:
        t1.close()

    # In db2: separate run archiving that artifact to archive_dir
    t2 = Tracker(run_dir=ws, db_path=str(db2_path))
    try:
        with t2.start_run("run_archive", model="test"):
            art2 = t2.log_input(art1, key="in")
            t2.archive_artifact(art2, archive_dir, mode="copy")
    finally:
        t2.close()

    # Now merge db2 into db1
    from consist.core.persistence import DatabaseManager

    db1 = DatabaseManager(db1_path)
    try:
        maint = DatabaseMaintenance(db=db1, run_dir=ws)
        maint.merge(db2_path, conflict="skip", include_snapshots=False)
    finally:
        db1.close()

    con = duckdb.connect(str(db1_path))
    try:
        # Check rows for archive_dir URI
        rec_rows = con.execute(
            "SELECT location_id, site, uri, status FROM artifact_location "
            "WHERE artifact_id = ? AND site IN ('archive', 'advisory')",
            [str(art1.id)],
        ).fetchall()
        assert len(rec_rows) == 1, f"Expected 1 recovery row, got: {rec_rows}"
        assert rec_rows[0][1] == "archive"
        assert rec_rows[0][3] == "active"
    finally:
        con.close()


def test_secondary_archive_after_source_mount_overwrite(tmp_path: Path) -> None:
    workspace = tmp_path / "runs"
    outputs_dir = tmp_path / "outputs"
    archive_1 = tmp_path / "archive_1"
    archive_2 = tmp_path / "archive_2"
    db_path = tmp_path / "prov.duckdb"

    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_file = outputs_dir / "households.parquet"
    pd.DataFrame({"id": [1], "val": ["D1"]}).to_parquet(out_file)

    tracker = Tracker(
        run_dir=workspace,
        db_path=str(db_path),
        mounts={"outputs": str(outputs_dir)},
    )
    try:
        # Run 1: produce D1 and copy to archive_1
        with tracker.start_run("run_1", model="test", config={"version": 1}):
            art1 = tracker.log_output(out_file, key="households")
        tracker.archive_artifact(art1, archive_1, mode="copy")

        # Run 2: overwrite outputs://households.parquet with D2
        # Set mtime back 10s to avoid 2s racy file window
        pd.DataFrame({"id": [2], "val": ["D2"]}).to_parquet(out_file)
        import os
        import time

        now = time.time()
        os.utime(out_file, (now - 10, now - 10))

        with tracker.start_run("run_2", model="test", config={"version": 2}):
            art2 = tracker.log_output(out_file, key="households")
            assert art2.id != art1.id

        # Now archive art1 to archive_2 (mode="copy")
        # source_path should be taken from archive_1 (valid D1), NOT outputs_dir (overwritten with D2)
        tracker.archive_artifact(art1, archive_2, mode="copy")

        import consist

        loaded_df = consist.load(art1, tracker=tracker)
        if isinstance(loaded_df, duckdb.DuckDBPyRelation):
            loaded_df = loaded_df.df()
        elif not isinstance(loaded_df, pd.DataFrame):
            loaded_df = pd.DataFrame(loaded_df)
        assert loaded_df["val"].tolist() == ["D1"]

        con = duckdb.connect(str(db_path))
        try:
            # art1's row at outputs://households.parquet must be stale because run_2 overwrote it
            rows = con.execute(
                "SELECT location_id, site, uri, status FROM artifact_location "
                "WHERE artifact_id = ? AND uri = 'outputs://households.parquet'",
                [str(art1.id)],
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][3] == "stale"

            # Both archives exist and are active
            arch_rows = con.execute(
                "SELECT site, uri, status FROM artifact_location "
                "WHERE artifact_id = ? AND site = 'archive'",
                [str(art1.id)],
            ).fetchall()
            assert len(arch_rows) == 2
        finally:
            con.close()
    finally:
        tracker.close()


def test_witness_preserved_on_external_to_advisory_promotion(tmp_path: Path) -> None:
    from consist.core.tracker_recovery import resolve_verified_local_path

    workspace = tmp_path / "runs"
    outputs_dir = tmp_path / "outputs"
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "prov.duckdb"

    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_file = outputs_dir / "data.parquet"
    pd.DataFrame({"val": [100]}).to_parquet(out_file)

    tracker = Tracker(
        run_dir=workspace,
        db_path=str(db_path),
        mounts={"outputs": str(outputs_dir)},
    )
    try:
        with tracker.start_run("run_1", model="test"):
            art = tracker.log_output(out_file, key="data", hashing_strategy="fast")

        # Before archival, external location has a witness
        con = duckdb.connect(str(db_path))
        try:
            rows_before = con.execute(
                "SELECT site, witness_json FROM artifact_location WHERE artifact_id = ?",
                [str(art.id)],
            ).fetchall()
            assert len(rows_before) == 1
            assert rows_before[0][1] is not None
        finally:
            con.close()

        # Archive with mode="copy"
        tracker.archive_artifact(art, archive_dir, mode="copy")

        # Local output is untouched, resolve with allow_recovery_roots=False must succeed
        resolved = resolve_verified_local_path(tracker, art, allow_recovery_roots=False)
        assert resolved is not None
        assert resolved.resolve() == out_file.resolve()
    finally:
        tracker.close()
