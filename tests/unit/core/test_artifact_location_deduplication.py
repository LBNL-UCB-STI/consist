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
