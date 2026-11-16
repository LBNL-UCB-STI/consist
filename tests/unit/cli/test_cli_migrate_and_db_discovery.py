"""Unit tests for migration UX progress reporting, bounded DB auto-discovery, and mount-aware validate."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from typer.testing import CliRunner
import duckdb
import pytest

from consist.cli import app, find_db_path, find_project_root_and_db
from consist.core.migrate import migrate_database
from consist.core.tracker import Tracker
from consist.digest import Digest
from consist.models import Artifact


def _create_v04_database(db_path: Path, *, num_artifacts: int = 3) -> dict[str, str]:
    now_ts = datetime.now(timezone.utc)
    aid_list: list[uuid.UUID] = []
    run_id = "test_run_01"

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "CREATE TABLE consist_schema ("
            "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR)"
        )
        con.execute("INSERT INTO consist_schema VALUES (1, ?, '0.4.0')", [now_ts])

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

        con.execute("CREATE SCHEMA global_tables")
        con.execute("CREATE TABLE global_tables.heavy_data (idx INTEGER, val VARCHAR)")
        con.execute(
            "INSERT INTO global_tables.heavy_data VALUES (1, 'alpha'), (2, 'beta')"
        )
        con.execute("CREATE TABLE custom_data (c_id INTEGER)")
        con.execute("INSERT INTO custom_data VALUES (42)")

        con.execute("CREATE VIEW v_heavy AS SELECT * FROM global_tables.heavy_data")

        for idx in range(num_artifacts):
            aid = uuid.uuid4()
            aid_list.append(aid)
            art_file = db_path.parent / f"art_{idx}.txt"
            art_file.write_text(f"content_{idx}\n", encoding="utf-8")
            con.execute(
                "INSERT INTO artifact VALUES (?, ?, ?, 'txt', NULL, NULL, NULL, NULL, ?, NULL, NULL, '{}', ?)",
                [aid, f"art_key_{idx}", str(art_file), run_id, now_ts],
            )
            con.execute(
                "INSERT INTO run_artifact_link VALUES (?, ?, 'output', FALSE)",
                [run_id, aid],
            )

        con.execute(
            "INSERT INTO run VALUES ("
            "?, NULL, 'completed', 'demo_model', 'description', 2024, 1, "
            "'[]', 'cfg123', 'git123', 'in123', 'sig123', NULL, '{}', "
            "?, ?, ?, ?)",
            [run_id, now_ts, now_ts, now_ts, now_ts],
        )

    return {"run_id": run_id, "first_aid": str(aid_list[0])}


def test_find_project_root_and_db_probing_order(tmp_path: Path) -> None:
    root = tmp_path / "proj"
    sub = root / "subdir" / "nested"
    sub.mkdir(parents=True)

    # 1. Probe .consist/provenance.duckdb
    consist_dir = root / ".consist"
    consist_dir.mkdir(parents=True)
    db1 = consist_dir / "provenance.duckdb"
    db1.write_text("", encoding="utf-8")

    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == db1

    # 2. Probe .consist/snapshots/latest/provenance.duckdb when db1 is removed
    db1.unlink()
    latest_dir = consist_dir / "snapshots" / "latest"
    latest_dir.mkdir(parents=True)
    db2 = latest_dir / "provenance.duckdb"
    db2.write_text("", encoding="utf-8")

    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == db2

    # 3. Probe root/provenance.duckdb when snapshots db is removed
    db2.unlink()
    db3 = root / "provenance.duckdb"
    db3.write_text("", encoding="utf-8")

    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == db3

    # 4. Probe consist.json when root db is removed
    db3.unlink()
    external_db = tmp_path / "custom_location" / "provenance.duckdb"
    external_db.parent.mkdir(parents=True)
    external_db.write_text("", encoding="utf-8")

    consist_json = root / "consist.json"
    consist_json.write_text(json.dumps({"db_path": str(external_db)}), encoding="utf-8")

    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == external_db

    # Relative db_path inside consist.json
    rel_db = root / "sub_db.duckdb"
    rel_db.write_text("", encoding="utf-8")
    consist_json.write_text(json.dumps({"db_path": "sub_db.duckdb"}), encoding="utf-8")

    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == rel_db

    # 5. Probe .consist directory fallback when consist.json is removed
    consist_json.unlink()
    rel_db.unlink()
    r, d = find_project_root_and_db(sub)
    assert r == root
    assert d == db1


def test_find_db_path_with_start_dir_and_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    start_dir = tmp_path / "custom_start"
    start_dir.mkdir()
    db_in_start = start_dir / "provenance.duckdb"
    db_in_start.write_text("", encoding="utf-8")

    # When start_dir provided, it discovers db_in_start
    assert find_db_path(start_dir=start_dir) == str(db_in_start)

    # When explicit path provided, explicit path takes highest precedence
    explicit = tmp_path / "explicit.duckdb"
    assert find_db_path(str(explicit), start_dir=start_dir) == str(explicit)

    # When CONSIST_DB env var set, env takes precedence over start_dir
    env_db = tmp_path / "env.duckdb"
    monkeypatch.setenv("CONSIST_DB", str(env_db))
    assert find_db_path(start_dir=start_dir) == str(env_db)
    monkeypatch.delenv("CONSIST_DB", raising=False)

    # Subdirectory probes in start_dir (.consist/provenance.duckdb)
    db_in_start.unlink()
    c_dir = start_dir / ".consist"
    c_dir.mkdir()
    c_db = c_dir / "provenance.duckdb"
    c_db.write_text("", encoding="utf-8")
    assert find_db_path(start_dir=start_dir) == str(c_db)

    # .consist/snapshots/latest/provenance.duckdb
    c_db.unlink()
    snap_db = c_dir / "snapshots" / "latest" / "provenance.duckdb"
    snap_db.parent.mkdir(parents=True)
    snap_db.write_text("", encoding="utf-8")
    assert find_db_path(start_dir=start_dir) == str(snap_db)


def test_cli_commands_wire_run_dir_db_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "isolated_run_dir"
    run_dir.mkdir()

    tracker = Tracker(run_dir=run_dir, db_path=str(run_dir / "provenance.duckdb"))
    with tracker.start_run("sample_run_id", "demo_model"):
        test_file = run_dir / "output.txt"
        test_file.write_text("sample output\n", encoding="utf-8")
        tracker.log_artifact(test_file, key="sample_art", direction="output")
    tracker.close()

    # Move working directory to a clean empty directory with no DB
    other_dir = tmp_path / "unrelated_empty_dir"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)
    monkeypatch.delenv("CONSIST_DB", raising=False)

    # 1. runs --run-dir
    res_runs = runner.invoke(app, ["runs", "--run-dir", str(run_dir), "--json"])
    assert res_runs.exit_code == 0, res_runs.output
    runs_data = json.loads(res_runs.stdout)
    assert len(runs_data) >= 1
    assert runs_data[0]["id"] == "sample_run_id"

    # 2. summary --run-dir
    res_summary = runner.invoke(app, ["summary", "--run-dir", str(run_dir)])
    assert res_summary.exit_code == 0, res_summary.output
    assert "sample_run_id" in res_summary.stdout or "demo_model" in res_summary.stdout

    # 3. show --run-dir
    res_show = runner.invoke(app, ["show", "sample_run_id", "--run-dir", str(run_dir)])
    assert res_show.exit_code == 0, res_show.output
    assert "sample_run_id" in res_show.stdout

    # 4. lineage --run-dir
    res_lineage = runner.invoke(
        app, ["lineage", "sample_art", "--run-dir", str(run_dir)]
    )
    assert res_lineage.exit_code == 0, res_lineage.output
    assert "sample_art" in res_lineage.stdout

    # 5. validate --run-dir
    res_val = runner.invoke(app, ["validate", "--run-dir", str(run_dir)])
    assert res_val.exit_code == 0, res_val.output
    assert "All artifacts validated successfully" in res_val.stdout


def test_migrate_dry_run_fast_and_progress_callback(tmp_path: Path) -> None:
    db_path = tmp_path / "v04_database.duckdb"
    _create_v04_database(db_path, num_artifacts=4)

    progress_events: list[tuple[str, int, int]] = []

    def on_progress(phase: str, current: int, total: int) -> None:
        progress_events.append((phase, current, total))

    report = migrate_database(
        db_path,
        dry_run=True,
        progress_callback=on_progress,
    )

    assert report.dry_run is True
    assert report.already_current is False
    assert report.runs_migrated == 1
    assert report.artifacts_migrated == 4
    assert report.global_tables_copied == 1
    assert report.views_copied == 1
    assert not Path(f"{db_path}.v04.bak").exists()

    # Verify progress events cover all migration phases
    phases = [p for p, _, _ in progress_events]
    assert "Inspecting tables and schemas" in phases
    assert "Resolving artifacts" in phases
    assert "Promoting run identities" in phases
    assert "Copying views" in phases
    assert "Finalizing migration" in phases

    # Check artifact progress monotonicity and completion
    art_events = [(c, t) for p, c, t in progress_events if p == "Resolving artifacts"]
    assert len(art_events) == 4
    assert art_events[-1] == (4, 4)


def test_migrate_cli_non_json_progress_and_output(tmp_path: Path) -> None:
    runner = CliRunner()
    db_path = tmp_path / "v04_cli_migrate.duckdb"
    _create_v04_database(db_path, num_artifacts=2)

    res = runner.invoke(app, ["migrate", str(db_path), "--dry-run"])
    assert res.exit_code == 0, res.output
    assert "Dry run complete" in res.stdout
    assert db_path.name in res.stdout


def test_validate_distinguishes_unmounted_schemes_from_missing_files(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    db_path = tmp_path / "validate_distinguish.duckdb"
    run_dir = tmp_path / "val_run_dir"
    run_dir.mkdir()

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    assert tracker.db is not None

    # 1. Genuinely missing file (local path that does not exist)
    missing_local_file = run_dir / "deleted_file.csv"
    aid_local = uuid.uuid4()
    art_local = Artifact(
        id=aid_local,
        key="deleted_local",
        uri=str(missing_local_file),
        driver="csv",
        run_id="run_1",
        digest=Digest("sha256:file", "11" * 32),
    )
    tracker.db.sync_artifact(art_local)

    # 2. Unmounted remote mount scheme (inputs://data.csv) where inputs is not in tracker.mounts
    aid_unmounted = uuid.uuid4()
    art_unmounted = Artifact(
        id=aid_unmounted,
        key="unmounted_data",
        uri="inputs://data.csv",
        driver="csv",
        run_id="run_1",
        digest=Digest("sha256:file", "22" * 32),
    )
    tracker.db.sync_artifact(art_unmounted)
    tracker.close()

    res = runner.invoke(app, ["validate", "--db-path", str(db_path)])
    assert res.exit_code == 0, res.output
    assert "Found 2 missing artifacts:" in res.stdout
    assert "missing on disk" in res.stdout
    assert "unmounted scheme" in res.stdout
    assert "inputs" in res.stdout
    assert "unconfigured mount scheme" in res.stdout


def test_validate_fix_does_not_mark_unmounted_scheme_missing(tmp_path: Path) -> None:
    runner = CliRunner()
    db_path = tmp_path / "validate_fix.duckdb"
    run_dir = tmp_path / "fix_run_dir"
    run_dir.mkdir()

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    assert tracker.db is not None

    # 1. Genuinely missing file
    missing_local = run_dir / "really_gone.parquet"
    aid_local = uuid.uuid4()
    art_local = Artifact(
        id=aid_local,
        key="really_gone",
        uri=str(missing_local),
        driver="parquet",
        run_id="run_2",
        digest=Digest("sha256:file", "aa" * 32),
    )
    tracker.db.sync_artifact(art_local)

    # 2. Unmounted scheme
    aid_unmounted = uuid.uuid4()
    art_unmounted = Artifact(
        id=aid_unmounted,
        key="unmounted_remote",
        uri="remote://warehouse.parquet",
        driver="parquet",
        run_id="run_2",
        digest=Digest("sha256:file", "bb" * 32),
    )
    tracker.db.sync_artifact(art_unmounted)
    tracker.close()

    res = runner.invoke(app, ["validate", "--db-path", str(db_path), "--fix"])
    assert res.exit_code == 0, res.output

    # Inspect metadata after fix
    verify_tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    refreshed_local = verify_tracker.get_artifact(aid_local)
    refreshed_unmounted = verify_tracker.get_artifact(aid_unmounted)
    assert refreshed_local is not None
    assert refreshed_unmounted is not None

    # Genuinely missing file MUST have missing_on_disk = True
    assert refreshed_local.meta.get("missing_on_disk") is True
    # Unmounted scheme MUST NOT have missing_on_disk = True
    assert "missing_on_disk" not in refreshed_unmounted.meta
    verify_tracker.close()


def test_validate_recovers_via_verified_artifact_locations(tmp_path: Path) -> None:
    runner = CliRunner()
    db_path = tmp_path / "validate_location_recovery.duckdb"
    run_dir = tmp_path / "loc_run_dir"
    alt_dir = tmp_path / "alt_dir"
    run_dir.mkdir()
    alt_dir.mkdir()

    from consist.digest import compute_digest

    real_file = alt_dir / "actual_data.csv"
    real_file.write_text("col1,col2\n1,2\n", encoding="utf-8")
    file_digest = compute_digest(real_file, driver="csv")

    aid = uuid.uuid4()
    art = Artifact(
        id=aid,
        key="relocated_art",
        uri="./actual_data.csv",
        driver="csv",
        digest=file_digest,
    )

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    assert tracker.db is not None
    tracker.db.sync_artifact(art)

    # Add verified archive recovery location pointing to alt_dir
    tracker.set_artifact_recovery_roots(
        art, [alt_dir], site="archive", verified_roots=[alt_dir]
    )
    tracker.close()

    res = runner.invoke(app, ["validate", "--db-path", str(db_path), "--trust-db"])
    assert res.exit_code == 0, res.output
    assert "All artifacts validated successfully" in res.stdout
