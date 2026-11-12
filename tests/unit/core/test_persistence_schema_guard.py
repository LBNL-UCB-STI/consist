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


def test_new_v2_database_stamps_consist_schema_version_2_and_db_uid(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2_provenance.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        rows = db.fetchall(
            "SELECT version, db_uid, updated_at FROM main.consist_schema"
        )
    finally:
        db.close()

    assert len(rows) == 1
    version, db_uid, updated_at = rows[0]
    assert version == 2
    assert uuid.UUID(str(db_uid))
    assert updated_at is not None


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
                db_uid VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        con.execute(
            "INSERT INTO consist_schema VALUES (?, ?, ?)",
            [3, str(uuid.uuid4()), datetime.now(timezone.utc)],
        )
        con.execute(
            """
            CREATE TABLE future_provenance_node (
                node_id UUID PRIMARY KEY,
                payload VARCHAR NOT NULL
            )
            """
        )

    with pytest.raises(IncompatibleSchemaError, match="newer than supported version 2"):
        DatabaseManager(str(db_path))

    with pytest.raises(
        consist.IncompatibleSchemaError, match="newer than supported version 2"
    ):
        DatabaseManager(str(db_path), read_only=True)

    with pytest.raises(
        consist.IncompatibleSchemaError, match="newer than supported version 2"
    ):
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


def test_opening_v1_or_unversioned_database_raises_migration_required(
    tmp_path: Path,
) -> None:
    assert consist.DatabaseMigrationRequired is DatabaseMigrationRequired
    assert issubclass(DatabaseMigrationRequired, IncompatibleSchemaError)

    v1_path = tmp_path / "existing_v1.duckdb"
    with duckdb.connect(str(v1_path)) as con:
        con.execute(
            "CREATE TABLE consist_schema ("
            "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR"
            ")"
        )
        con.execute("INSERT INTO consist_schema VALUES (1, now(), '0.4.2')")
        con.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY)")

    expected_msg = (
        f"Database uses Consist v0.4 schema (version=1). "
        f"Run 'consist migrate {v1_path}'."
    )
    with pytest.raises(DatabaseMigrationRequired) as exc_info:
        DatabaseManager(str(v1_path))
    assert str(exc_info.value) == expected_msg

    with pytest.raises(DatabaseMigrationRequired):
        DatabaseManager(str(v1_path), read_only=True)

    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=tmp_path / "runs", db_path=str(v1_path))

    unversioned_path = tmp_path / "unversioned_v04.duckdb"
    with duckdb.connect(str(unversioned_path)) as con:
        con.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY)")

    with pytest.raises(DatabaseMigrationRequired) as exc_unversioned:
        DatabaseManager(str(unversioned_path))
    assert str(exc_unversioned.value) == (
        f"Database uses Consist v0.4 schema (version=1). "
        f"Run 'consist migrate {unversioned_path}'."
    )


def test_read_only_missing_db_does_not_create_file_and_init_schema_is_idempotent(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "does_not_exist.duckdb"
    with pytest.raises((FileNotFoundError, duckdb.IOException)):
        DatabaseManager(str(missing_path), read_only=True)
    assert not missing_path.exists()

    db_path = tmp_path / "idempotent_v2.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        row_before = db.fetchone("SELECT version, db_uid FROM consist_schema")
        assert row_before is not None
        db.init_schema()
        db.init_schema()
        rows_after = db.fetchall("SELECT version, db_uid FROM consist_schema")
        assert rows_after == [row_before]
    finally:
        db.close()
