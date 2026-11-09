from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest
from sqlalchemy import inspect
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel, create_engine

import consist
from consist.core.persistence import DatabaseManager, IncompatibleSchemaError
from consist.core.tracker import Tracker
from consist.models.run import RunArtifactLink


def test_new_v1_database_stamps_consist_schema_version_1(tmp_path: Path) -> None:
    db_path = tmp_path / "v1_provenance.duckdb"
    db = DatabaseManager(str(db_path))

    with db.engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT version, applied_at, consist_version FROM consist_schema"
        ).fetchall()

    assert len(rows) == 1
    version, applied_at, consist_version = rows[0]
    assert version == 1
    assert applied_at is not None
    assert isinstance(consist_version, str) and len(consist_version) > 0


def test_opening_v2_database_raises_incompatible_schema_error_before_mutation(
    tmp_path: Path,
) -> None:
    assert consist.IncompatibleSchemaError is IncompatibleSchemaError

    db_path = tmp_path / "v2_provenance.duckdb"
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
            [2, datetime.now(timezone.utc), "0.5.0"],
        )
        con.execute(
            """
            CREATE TABLE run_binding (
                run_id VARCHAR NOT NULL,
                direction VARCHAR NOT NULL,
                slot VARCHAR NOT NULL,
                ordinal INTEGER NOT NULL DEFAULT 0,
                artifact_id UUID NOT NULL,
                anchor VARCHAR NOT NULL,
                PRIMARY KEY (run_id, direction, slot, ordinal)
            )
            """
        )

    with pytest.raises(
        IncompatibleSchemaError,
        match=r'schema version 2.*pip install -U "consist>=0\.5"',
    ):
        DatabaseManager(str(db_path))

    with pytest.raises(
        consist.IncompatibleSchemaError,
        match=r'schema version 2.*pip install -U "consist>=0\.5"',
    ):
        DatabaseManager(str(db_path), read_only=True)

    with pytest.raises(
        consist.IncompatibleSchemaError,
        match=r'schema version 2.*pip install -U "consist>=0\.5"',
    ):
        Tracker(run_dir=tmp_path / "runs", db_path=str(db_path))

    # Verify that no v1 tables (such as `run`, `artifact`, `artifact_kv`) were created.
    with duckdb.connect(str(db_path), read_only=True) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
    assert tables == {"consist_schema", "run_binding"}


def test_duckdb_engine_introspection_and_create_all_with_run_artifact_link_view(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "view_introspection.duckdb"
    engine = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE run_binding (
                run_id VARCHAR NOT NULL,
                artifact_id UUID NOT NULL,
                direction VARCHAR NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE VIEW run_artifact_link AS
            SELECT
                run_id,
                artifact_id,
                CASE WHEN bool_or(direction IN ('input', 'depends_on')) THEN 'input' ELSE 'output' END AS direction
            FROM run_binding
            GROUP BY run_id, artifact_id
            """
        )

    inspector = inspect(engine)
    assert inspector.has_table("run_artifact_link") is True
    assert "run_artifact_link" not in inspector.get_table_names()
    assert "run_artifact_link" in inspector.get_view_names()

    # SQLModel.metadata.create_all uses has_table() when checkfirst=True,
    # so it skips creating a table over the existing SQL VIEW.
    SQLModel.metadata.create_all(
        engine,
        tables=[SQLModel.metadata.tables[RunArtifactLink.__tablename__]],
    )

    inspector_after = inspect(engine)
    assert "run_artifact_link" not in inspector_after.get_table_names()
    assert "run_artifact_link" in inspector_after.get_view_names()


def test_unpatched_v041_behavior_against_v2_database_fails_without_partial_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify what an unpatched v0.4.1 process (without the consist_schema guard) does on a v2 DB."""
    from sqlalchemy.exc import ProgrammingError
    from consist.models.artifact import Artifact
    from consist.models.run import Run

    db_path = tmp_path / "v2_full_schema.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE consist_schema (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL,
                consist_version VARCHAR NOT NULL
            );
            INSERT INTO consist_schema VALUES (2, now(), '0.5.0');
            CREATE TABLE run (
                id VARCHAR PRIMARY KEY,
                model_name VARCHAR NOT NULL,
                description VARCHAR,
                year INTEGER,
                iteration INTEGER,
                parent_run_id VARCHAR,
                status VARCHAR NOT NULL,
                code_hash VARCHAR,
                config_hash VARCHAR,
                input_hash VARCHAR,
                signature VARCHAR,
                signature_scheme VARCHAR,
                cache_status VARCHAR NOT NULL,
                tags JSON,
                meta JSON NOT NULL,
                started_at TIMESTAMPTZ,
                ended_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE artifact (
                id UUID PRIMARY KEY,
                key VARCHAR NOT NULL,
                driver VARCHAR NOT NULL,
                anchor VARCHAR NOT NULL,
                run_id VARCHAR,
                schema_id VARCHAR,
                meta JSON NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE run_binding (
                run_id VARCHAR NOT NULL,
                direction VARCHAR NOT NULL,
                occurrence_index INTEGER NOT NULL,
                slot VARCHAR NOT NULL,
                ordinal INTEGER NOT NULL DEFAULT 0,
                artifact_id UUID NOT NULL,
                role VARCHAR,
                anchor VARCHAR NOT NULL,
                contract_scheme VARCHAR,
                sig_scheme VARCHAR,
                sig_hex VARCHAR,
                PRIMARY KEY (run_id, direction, slot, ordinal)
            );
            CREATE VIEW run_artifact_link AS
            SELECT
                run_id,
                artifact_id,
                CASE WHEN bool_or(direction IN ('input', 'depends_on')) THEN 'input' ELSE 'output' END AS direction
            FROM run_binding
            GROUP BY run_id, artifact_id;
            """
        )

    # Simulate unpatched v0.4.1 by disabling the forward-schema guard.
    monkeypatch.setattr(
        DatabaseManager,
        "_check_compatible_schema_version",
        lambda self: None,
    )

    # 1. Default init_schema (`SQLModel.metadata.create_all`) fails on FK type mismatch
    # (`artifact.id` UUID vs v1 `artifact_schema_observation.artifact_id` CHAR(36)/VARCHAR)
    # and rolls back the entire transaction without creating v1 tables.
    with pytest.raises(ProgrammingError, match="incompatible types"):
        DatabaseManager(str(db_path))

    with duckdb.connect(str(db_path), read_only=True) as con:
        remaining_objects = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
    assert remaining_objects == {
        "consist_schema",
        "run",
        "artifact",
        "run_binding",
        "run_artifact_link",
    }

    # 2. If schema init is skipped (`skip_schema_init=True`), attempting to write a v1
    # Run or Artifact fails on the initial SELECT before any row is mutated.
    db_no_init = DatabaseManager(str(db_path), skip_schema_init=True)
    with pytest.raises(ProgrammingError, match="git_hash"):
        db_no_init.sync_run(
            Run(
                id="r1",
                model_name="m",
                status="running",
                config_hash="c",
                git_hash="g",
            ),
            require_success=True,
        )

    art = Artifact(
        key="k",
        container_uri="workspace://a.parquet",
        driver="parquet",
        hash="a" * 64,
    )
    db_no_init.sync_artifact(art, "r1", "output")

    with duckdb.connect(str(db_path), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM run").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM artifact").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM run_binding").fetchone()[0] == 0


def test_v05_upgrade_notice_emitted_once_per_existing_v1_db_and_silenced_by_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recwarn: pytest.WarningsRecorder,
) -> None:
    monkeypatch.delenv("CONSIST_SHOW_V05_NOTICE", raising=False)
    monkeypatch.delenv("CONSIST_SILENCE_V05_NOTICE", raising=False)
    db_path = tmp_path / "existing_v1.duckdb"

    # By default (without CONSIST_SHOW_V05_NOTICE=1), opening a new or existing v1 DB is silent.
    DatabaseManager(str(db_path))
    DatabaseManager(str(db_path))
    default_warnings = [
        w
        for w in recwarn.list
        if issubclass(w.category, FutureWarning) and "Consist 0.5" in str(w.message)
    ]
    assert len(default_warnings) == 0

    # When CONSIST_SHOW_V05_NOTICE=1 is enabled, opening an existing v1 database emits the FutureWarning once.
    monkeypatch.setenv("CONSIST_SHOW_V05_NOTICE", "1")
    with pytest.warns(
        FutureWarning,
        match=r"Consist 0\.5 will require a one-time upgrade.*CONSIST_SILENCE_V05_NOTICE=1",
    ):
        DatabaseManager(str(db_path))

    # Re-opening the same database does not spam a second FutureWarning.
    recwarn.clear()
    DatabaseManager(str(db_path))
    repeated = [
        w
        for w in recwarn.list
        if issubclass(w.category, FutureWarning) and "Consist 0.5" in str(w.message)
    ]
    assert len(repeated) == 0

    # Setting CONSIST_SILENCE_V05_NOTICE=1 suppresses the warning on another existing DB.
    db_path_silenced = tmp_path / "silenced_v1.duckdb"
    monkeypatch.delenv("CONSIST_SHOW_V05_NOTICE", raising=False)
    DatabaseManager(str(db_path_silenced))
    monkeypatch.setenv("CONSIST_SHOW_V05_NOTICE", "1")
    monkeypatch.setenv("CONSIST_SILENCE_V05_NOTICE", "1")
    recwarn.clear()
    DatabaseManager(str(db_path_silenced))
    silenced = [
        w
        for w in recwarn.list
        if issubclass(w.category, FutureWarning) and "Consist 0.5" in str(w.message)
    ]
    assert len(silenced) == 0


def test_read_only_and_skip_schema_init_do_not_stamp_consist_schema_and_insert_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pre_v042_v1.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY)")

    db_ro = DatabaseManager(str(db_path), read_only=True)
    db_ro._ensure_consist_schema_v1()
    db_ro.dispose()

    db_skip = DatabaseManager(str(db_path), skip_schema_init=True)
    db_skip._ensure_consist_schema_v1()
    db_skip.dispose()

    with duckdb.connect(str(db_path), read_only=True) as con:
        has_schema_table = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'consist_schema'"
        ).fetchone()[0]
    assert has_schema_table == 0

    # Standard writable open stamps version=1 and repeated _ensure_consist_schema_v1
    # calls are idempotent via ON CONFLICT (version) DO NOTHING.
    db_rw = DatabaseManager(str(db_path))
    db_rw._ensure_consist_schema_v1()
    db_rw._ensure_consist_schema_v1()
    with db_rw.engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT version FROM consist_schema").fetchall()
    assert rows == [(1,)]
    db_rw.dispose()
