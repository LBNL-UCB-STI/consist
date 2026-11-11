from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid

import duckdb
import pytest

import consist
from consist.core.persistence import (
    DatabaseManager,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
)
from consist.core.tracker import Tracker
from consist.store.schema import V1_ONLY_TABLE_NAMES, init_v2_schema
from tests.helpers.v1_db_builder import (
    create_v1_schema,
    insert_v1_artifact,
    insert_v1_link,
    insert_v1_run,
)


def test_new_v2_database_stamps_consist_schema_version_2_and_meta(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2_provenance.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        with db.ledger.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT version, applied_at, consist_version FROM main.consist_schema"
            ).fetchall()
            meta_rows = dict(
                cur.execute("SELECT key, value FROM main.consist_meta").fetchall()
            )
    finally:
        db.close()

    assert len(rows) == 1
    version, applied_at, consist_version = rows[0]
    assert version == 2
    assert applied_at is not None
    assert isinstance(consist_version, str) and len(consist_version) > 0
    assert "db_uid" in meta_rows
    assert uuid.UUID(str(meta_rows["db_uid"]))
    assert "created_utc" in meta_rows


def test_opening_future_schema_database_raises_incompatible_schema_error_before_mutation(
    tmp_path: Path,
) -> None:
    assert consist.IncompatibleSchemaError is IncompatibleSchemaError

    db_path = tmp_path / "future_v3_provenance.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE consist_schema (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL,
                consist_version VARCHAR NOT NULL
            )
            """
        )
        con.execute(
            "INSERT INTO consist_schema VALUES (?, ?, ?)",
            [3, datetime.now(timezone.utc), "0.99.0"],
        )
        con.execute(
            """
            CREATE TABLE future_provenance_node (
                node_id UUID PRIMARY KEY,
                payload VARCHAR NOT NULL
            )
            """
        )

    with pytest.raises(IncompatibleSchemaError, match="newer schema version"):
        DatabaseManager(str(db_path))

    with pytest.raises(consist.IncompatibleSchemaError, match="newer schema version"):
        DatabaseManager(str(db_path), read_only=True)

    with pytest.raises(consist.IncompatibleSchemaError, match="newer schema version"):
        Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))

    with duckdb.connect(str(db_path), read_only=True) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
    assert tables == {"consist_schema", "future_provenance_node"}


def test_opening_v1_database_requires_migration_by_default_and_migrates_when_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert consist.DatabaseMigrationRequired is DatabaseMigrationRequired
    monkeypatch.delenv("CONSIST_AUTO_MIGRATE", raising=False)

    data_file = tmp_path / "out.parquet"
    data_file.write_bytes(b"parquet-v1-test-bytes")

    def _populate_v1_db(path: Path) -> None:
        with duckdb.connect(str(path)) as con:
            create_v1_schema(con)
            con.execute(
                "CREATE TABLE consist_schema ("
                "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR"
                ")"
            )
            con.execute("INSERT INTO consist_schema VALUES (1, now(), '0.4.2')")
            insert_v1_run(con, run_id="v1_run_1", model_name="demo_model")
            art_id = insert_v1_artifact(
                con,
                key="out_table",
                container_uri=str(data_file),
                driver="parquet",
                hash_val="a" * 64,
                run_id="v1_run_1",
            )
            insert_v1_link(
                con,
                run_id="v1_run_1",
                artifact_id=art_id,
                direction="output",
            )

    db_path = tmp_path / "existing_v1.duckdb"
    _populate_v1_db(db_path)

    with duckdb.connect(str(db_path), read_only=True) as con:
        v1_tables_before = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }

    with pytest.raises(DatabaseMigrationRequired, match="consist migrate") as exc_info:
        DatabaseManager(str(db_path))
    assert exc_info.value.run_count == 1
    assert exc_info.value.artifact_count == 1

    with pytest.raises(DatabaseMigrationRequired, match="consist migrate"):
        DatabaseManager(str(db_path), read_only=True)

    with pytest.raises(DatabaseMigrationRequired, match="consist migrate"):
        Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))

    # Verify failed v1 opens did not mutate any v1 tables or stamp v2 objects.
    with duckdb.connect(str(db_path), read_only=True) as con:
        v1_tables_after = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        versions = [
            row[0]
            for row in con.execute("SELECT version FROM consist_schema").fetchall()
        ]
    assert v1_tables_after == v1_tables_before
    assert versions == [1]

    # Explicit auto_migrate=True upgrades the database to v2 and preserves runs.
    db_migrated = DatabaseManager(str(db_path), auto_migrate=True)
    try:
        migrated_run = db_migrated.get_run("v1_run_1")
        assert migrated_run is not None
        assert migrated_run.model_name == "demo_model"
        with db_migrated.ledger.exec_cursor() as cur:
            v2_versions = [
                row[0]
                for row in cur.execute(
                    "SELECT version FROM main.consist_schema"
                ).fetchall()
            ]
        assert v2_versions == [2]
    finally:
        db_migrated.close()

    backups = list(tmp_path.glob("existing_v1.v1-backup-*.duckdb"))
    assert len(backups) == 1

    # CONSIST_AUTO_MIGRATE=1 also upgrades automatically when opening via Tracker.
    env_db_path = tmp_path / "env_auto_migrate_v1.duckdb"
    _populate_v1_db(env_db_path)
    monkeypatch.setenv("CONSIST_AUTO_MIGRATE", "1")
    tracker = Tracker(run_dir=tmp_path / "runs_env", db_path=str(env_db_path))
    try:
        assert tracker.get_run("v1_run_1") is not None
    finally:
        tracker.close()


def test_v2_compatibility_and_v1_tripwire_views_are_installed_and_protected(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "views_check.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        with db.ledger.exec_cursor() as cur:
            base_tables = {
                str(row[0])
                for row in cur.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
                ).fetchall()
            }
            views = {
                str(row[0])
                for row in cur.execute(
                    "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
                ).fetchall()
            }

            expected_compat_views = {
                "run_artifact_link",
                "run_config_kv",
                "artifact_kv",
                "artifact_location_current",
            }
            assert expected_compat_views.issubset(views)
            assert expected_compat_views.isdisjoint(base_tables)

            tripwire_names = set(V1_ONLY_TABLE_NAMES)
            assert tripwire_names.issubset(views)
            assert tripwire_names.isdisjoint(base_tables)

            for v1_view in V1_ONLY_TABLE_NAMES:
                rel = cur.execute(f'SELECT * FROM main."{v1_view}"')
                assert [col[0] for col in (rel.description or ())] == [
                    "upgrade_to_consist_0_5"
                ]
                assert rel.fetchall() == []
                with pytest.raises(duckdb.Error):
                    cur.execute(f'INSERT INTO main."{v1_view}" VALUES (?)', ["x"])

            with pytest.raises(duckdb.Error):
                cur.execute(
                    "INSERT INTO main.run_artifact_link VALUES ('r1', ?, 'output', false)",
                    [uuid.uuid4()],
                )
    finally:
        db.close()


def test_read_only_missing_or_empty_db_does_not_stamp_schema_and_v2_init_is_idempotent(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "does_not_exist.duckdb"
    with pytest.raises(FileNotFoundError):
        DatabaseManager(str(missing_path), read_only=True)
    assert not missing_path.exists()

    empty_path = tmp_path / "zero_byte.duckdb"
    empty_path.write_bytes(b"")
    with pytest.raises(IncompatibleSchemaError):
        DatabaseManager(str(empty_path), read_only=True)
    assert empty_path.stat().st_size == 0

    db_path = tmp_path / "idempotent_v2.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        with db.ledger.exec_cursor() as cur:
            uid_before = cur.execute(
                "SELECT value FROM main.consist_meta WHERE key = 'db_uid'"
            ).fetchone()[0]
    finally:
        db.close()

    with duckdb.connect(str(db_path)) as con:
        init_v2_schema(con)
        init_v2_schema(con)
        versions = con.execute("SELECT version FROM main.consist_schema").fetchall()
        uid_after = con.execute(
            "SELECT value FROM main.consist_meta WHERE key = 'db_uid'"
        ).fetchone()[0]

    assert versions == [(2,)]
    assert uid_after == uid_before
