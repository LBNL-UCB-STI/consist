from __future__ import annotations

import ast
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any
import uuid

import duckdb
import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel, create_engine

import consist
from consist.core.identity import IdentityManager
from consist.core.persistence import (
    ConsistError,
    DatabaseManager,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
)
from consist.digest import Digest, Identity
from consist.location import (
    FileStat,
    Location,
    Observation,
    lookup_live_ledger_by_path,
    unregister_live_ledger,
)
from consist.migrations.v1 import (
    RACY_STAT_WINDOW_NS,
    V1DatabaseReader,
    compute_v1_fast_file_hash,
    stat_v1_fast_file_hash,
)
from consist.models.artifact import Artifact as V1ORMArtifact
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.config_facet import ConfigFacet
from consist.models.run import Run as V1ORMRun, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV
from consist.store import (
    SchemaState,
    StoreLedger,
    V1_ONLY_TABLE_NAMES,
    V2_EXPECTED_TABLE_COLUMNS,
    V2_SCHEMA_VERSION,
    V2_TABLE_NAMES,
    V2_VIEW_NAMES,
    _migration_workdir,
    _resolve_auto_migrate,
    detect_schema,
    init_v2_schema,
    inspect_v1_summary,
)


def test_init_v2_schema_creates_all_tables_views_macro_and_tripwires(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2.duckdb"
    explicit_uid = str(uuid.uuid4())
    with duckdb.connect(str(db_path)) as con:
        returned_uid = init_v2_schema(
            con,
            db_uid=explicit_uid,
            consist_version="0.5.0-test",
        )
        assert returned_uid == explicit_uid

        # Calling init_v2_schema again is idempotent and preserves the existing db_uid.
        second_uid = init_v2_schema(
            con,
            db_uid=str(uuid.uuid4()),
            consist_version="0.5.0-other",
        )
        assert second_uid == explicit_uid

        tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        views = {
            str(r[0])
            for r in con.execute(
                "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
            ).fetchall()
        }

        assert tables == set(V2_TABLE_NAMES)
        assert set(V2_EXPECTED_TABLE_COLUMNS.keys()) == set(V2_TABLE_NAMES)
        assert "ledger_applied" not in tables
        assert set(V2_VIEW_NAMES) <= views
        assert set(V1_ONLY_TABLE_NAMES) <= views

        for table_name, expected_cols in V2_EXPECTED_TABLE_COLUMNS.items():
            actual_cols = {
                str(col_name): str(dtype)
                for col_name, dtype in con.execute(
                    "SELECT column_name, data_type FROM duckdb_columns() "
                    "WHERE schema_name = 'main' AND table_name = ? "
                    "ORDER BY column_index",
                    [table_name],
                ).fetchall()
            }
            assert actual_cols == expected_cols, f"Column mismatch on {table_name}"

        schema_rows = con.execute(
            "SELECT version, consist_version FROM consist_schema"
        ).fetchall()
        assert schema_rows == [(V2_SCHEMA_VERSION, "0.5.0-test")]

        meta_rows = dict(con.execute("SELECT key, value FROM consist_meta").fetchall())
        assert meta_rows["db_uid"] == explicit_uid
        assert "created_utc" in meta_rows


def test_init_v2_schema_rolls_back_atomically_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "interrupted_init.duckdb"

    def _boom(_con: duckdb.DuckDBPyConnection) -> None:
        raise RuntimeError("simulated mid-init failure before consist_schema stamp")

    monkeypatch.setattr("consist.store.schema.install_v1_tripwires", _boom)

    with duckdb.connect(str(db_path)) as con:
        with pytest.raises(RuntimeError, match="simulated mid-init failure"):
            init_v2_schema(con)

        tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        assert tables == set()


def _consist_v1_main_sqlmodel_tables() -> list[Any]:
    """Return only Consist's built-in `consist.models.*` main-schema SQLModel tables."""
    import consist.core.persistence  # noqa: F401

    tables: dict[str, Any] = {}
    queue = list(SQLModel.__subclasses__())
    while queue:
        cls = queue.pop()
        queue.extend(cls.__subclasses__())
        if not cls.__module__.startswith("consist.models"):
            continue
        table = cls.__dict__.get("__table__")
        if table is not None and table.schema is None:
            tables[table.name] = table
    return list(tables.values())


def test_every_v1_sqlmodel_main_table_is_covered_by_v2_or_tripwires() -> None:
    main_sqlmodel_tables = {t.name for t in _consist_v1_main_sqlmodel_tables()}
    assert len(main_sqlmodel_tables) >= 8
    covered = set(V2_TABLE_NAMES) | set(V2_VIEW_NAMES) | set(V1_ONLY_TABLE_NAMES)
    missing = main_sqlmodel_tables - covered
    assert missing == set()


def test_version_matrix_tripwire_defense_against_real_v2_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "real_v2.duckdb"
    with duckdb.connect(str(db_path)) as con:
        init_v2_schema(con)
        baseline_tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        baseline_views = {
            str(r[0])
            for r in con.execute(
                "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
            ).fetchall()
        }

    # 1. v0.4.2's DatabaseManager raises IncompatibleSchemaError before any DDL/writes.
    with pytest.raises(IncompatibleSchemaError, match="schema version 2"):
        DatabaseManager(str(db_path))

    # 2. Simulated unpatched v0.4.1 (`SQLModel.metadata.create_all(engine)` without guard):
    # `inspect(engine).has_table()` returns True for every v1 table because of the
    # tripwire views, so `create_all` executes zero CREATE TABLE statements.
    engine = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)
    try:
        inspector = inspect(engine)
        for v1_table in V1_ONLY_TABLE_NAMES:
            assert inspector.has_table(v1_table) is True
            assert v1_table in inspector.get_view_names()
            assert v1_table not in inspector.get_table_names()

        main_v1_tables = _consist_v1_main_sqlmodel_tables()
        SQLModel.metadata.create_all(engine, tables=main_v1_tables)

        # Any attempt to query/insert v1 columns on a tripwire view or v2 table fails at bind time.
        with engine.connect() as conn:
            with pytest.raises(
                ProgrammingError, match="upgrade_to_consist_0_5|Binder Error"
            ):
                conn.exec_driver_sql(
                    "SELECT id FROM config_facet WHERE namespace = 'a'"
                )
            with pytest.raises(ProgrammingError):
                conn.exec_driver_sql("INSERT INTO artifact_kv VALUES ('id')")
    finally:
        engine.dispose()

    # Verify v0.4.1 skip_schema_init sync_run / sync_artifact also fail without mutating rows.
    monkeypatch.setattr(
        DatabaseManager,
        "_check_compatible_schema_version",
        lambda self: None,
    )
    db_no_init = DatabaseManager(str(db_path), skip_schema_init=True)
    try:
        with pytest.raises(
            (ProgrammingError, IntegrityError),
            match="facet|cache_status|Binder Error|NOT NULL",
        ):
            db_no_init.sync_run(
                V1ORMRun(
                    id="r1",
                    model_name="m",
                    status="running",
                    config_hash="c",
                    git_hash="g",
                ),
                require_success=True,
            )
        db_no_init.sync_artifact(
            V1ORMArtifact(
                key="k",
                container_uri="workspace://x.parquet",
                driver="parquet",
                hash="a" * 64,
            ),
            "r1",
            "output",
        )
    finally:
        db_no_init.dispose()

    # 3. Simulated older v0.3.x metadata (subset of SQLModel tables without
    # `artifact_schema_observation`) ALSO skips CREATE TABLE for all V1_ONLY_TABLE_NAMES
    # and fails on INSERT into a view.
    v03_tables = [
        SQLModel.metadata.tables[V1ORMRun.__tablename__],
        SQLModel.metadata.tables[V1ORMArtifact.__tablename__],
        SQLModel.metadata.tables[RunArtifactLink.__tablename__],
        SQLModel.metadata.tables[ConfigFacet.__tablename__],
        SQLModel.metadata.tables[RunConfigKV.__tablename__],
        SQLModel.metadata.tables[ArtifactFacet.__tablename__],
        SQLModel.metadata.tables[ArtifactKV.__tablename__],
    ]
    engine_v03 = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)
    try:
        SQLModel.metadata.create_all(engine_v03, tables=v03_tables)
        with engine_v03.begin() as conn:
            with pytest.raises(
                ProgrammingError,
                match="not an? table|Cannot insert|view|upgrade_to_consist_0_5|Binder Error",
            ):
                conn.exec_driver_sql(
                    "INSERT INTO config_facet VALUES ('f1', 'ns', 's', '1', '{}', now())"
                )
    finally:
        engine_v03.dispose()

    with duckdb.connect(str(db_path), read_only=True) as con:
        after_tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        after_views = {
            str(r[0])
            for r in con.execute(
                "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
            ).fetchall()
        }
        assert after_tables == baseline_tables
        assert after_views == baseline_views
        assert con.execute("SELECT count(*) FROM run").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM artifact").fetchone()[0] == 0


def test_detect_schema_all_states_and_live_ledger_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 1. ABSENT
    absent_path = tmp_path / "does_not_exist.duckdb"
    assert detect_schema(absent_path) is SchemaState.ABSENT

    # 2. INTERRUPTED (.migrate.d exists)
    interrupted_path = tmp_path / "interrupted.duckdb"
    _migration_workdir(interrupted_path).mkdir(parents=True)
    assert detect_schema(interrupted_path) is SchemaState.INTERRUPTED

    # 3. V1 pre-0.4.2 (no consist_schema table, artifact has container_uri + hash)
    v1_old_path = tmp_path / "v1_old.duckdb"
    with duckdb.connect(str(v1_old_path)) as con:
        con.execute(
            "CREATE TABLE artifact (id VARCHAR, container_uri VARCHAR, hash VARCHAR)"
        )
    assert detect_schema(v1_old_path) is SchemaState.V1

    # 4. V1 0.4.2 (with consist_schema.version = 1)
    v1_042_path = tmp_path / "v1_042.duckdb"
    db_v1 = DatabaseManager(str(v1_042_path))
    db_v1.dispose()
    assert detect_schema(v1_042_path) is SchemaState.V1

    # 5. V2 + in-process live ledger lookup avoiding read_only=True connection conflict
    v2_path = tmp_path / "v2_live.duckdb"
    with StoreLedger(v2_path) as ledger:
        assert lookup_live_ledger_by_path(v2_path) is ledger
        # Unregistering a different object for the same db_path does not evict `ledger`
        other_sentinel = object()
        unregister_live_ledger(
            other_sentinel,
            instance_key="other-key",
            db_path=v2_path,
        )
        assert lookup_live_ledger_by_path(v2_path) is ledger
        # While writable StoreLedger is open, detect_schema hits `_live_ledgers` first
        # and does not fail with a DuckDB read_only=True connection-config conflict.
        assert detect_schema(v2_path) is SchemaState.V2
    assert lookup_live_ledger_by_path(v2_path) is None
    assert detect_schema(v2_path) is SchemaState.V2

    # 6. FUTURE (consist_schema.version = 3)
    future_path = tmp_path / "future.duckdb"
    with duckdb.connect(str(future_path)) as con:
        con.execute(
            "CREATE TABLE consist_schema (version INTEGER, applied_at TIMESTAMPTZ, consist_version VARCHAR)"
        )
        con.execute("INSERT INTO consist_schema VALUES (3, now(), '0.6.0')")
    assert detect_schema(future_path) is SchemaState.FUTURE

    # 7. UNKNOWN (unrecognized schema or v2-like artifact table missing consist_schema)
    unknown_path = tmp_path / "unknown.duckdb"
    with duckdb.connect(str(unknown_path)) as con:
        con.execute("CREATE TABLE artifact (id UUID, anchor VARCHAR, driver VARCHAR)")
    assert detect_schema(unknown_path) is SchemaState.UNKNOWN

    # 8. UNREADABLE (corrupt/non-DuckDB file header bytes)
    unreadable_path = tmp_path / "corrupt.duckdb"
    unreadable_path.write_bytes(b"not-a-valid-duckdb-file-header")
    assert detect_schema(unreadable_path) is SchemaState.UNREADABLE

    # 9. LOCKED (simulated DuckDB IOException lock error)
    locked_path = tmp_path / "locked.duckdb"
    locked_path.write_bytes(b"placeholder")

    def _raise_lock(*_args: object, **_kwargs: object) -> duckdb.DuckDBPyConnection:
        raise duckdb.IOException(
            'IO Error: Could not set lock on file "locked.duckdb": Conflicting lock is held'
        )

    monkeypatch.setattr(duckdb, "connect", _raise_lock)
    assert detect_schema(locked_path) is SchemaState.LOCKED


def test_resolve_auto_migrate_and_database_migration_required_formatting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert consist.ConsistError is ConsistError
    assert consist.DatabaseMigrationRequired is DatabaseMigrationRequired
    assert issubclass(DatabaseMigrationRequired, ConsistError)

    monkeypatch.delenv("CONSIST_AUTO_MIGRATE", raising=False)
    assert _resolve_auto_migrate(None) == "off"
    assert _resolve_auto_migrate(False) == "off"
    assert _resolve_auto_migrate(True) == "full"
    assert _resolve_auto_migrate("quick") == "quick"
    assert _resolve_auto_migrate("1") == "full"
    assert _resolve_auto_migrate("0") == "off"

    monkeypatch.setenv("CONSIST_AUTO_MIGRATE", "quick")
    assert _resolve_auto_migrate(None) == "quick"

    monkeypatch.setenv("CONSIST_AUTO_MIGRATE", "invalid")
    with pytest.raises(ValueError, match="CONSIST_AUTO_MIGRATE='invalid'"):
        _resolve_auto_migrate(None)

    db_path = tmp_path / "provenance.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            "CREATE TABLE run (id VARCHAR, created_at TIMESTAMPTZ); "
            "CREATE TABLE artifact (id VARCHAR, container_uri VARCHAR, hash VARCHAR);"
        )
        con.execute(
            "INSERT INTO run VALUES ('r1', '2026-10-30T12:00:00Z'), ('r2', '2026-10-29T08:00:00Z');"
        )
        con.execute(
            "INSERT INTO artifact VALUES ('a1', 'workspace://x', 'h1'), ('a2', 'workspace://y', 'h2'), ('a3', 'workspace://z', 'h3');"
        )

    err = inspect_v1_summary(db_path)
    assert err.db_path == db_path
    assert err.run_count == 2
    assert err.artifact_count == 3
    assert err.last_written == "2026-10-30"
    msg = str(err)
    assert "Consist 0.5 can't open this database yet." in msg
    assert f"Database:  {db_path}" in msg
    assert "Contents:  2 runs, 3 artifacts (last written 2026-10-30)" in msg
    assert f"consist migrate {db_path}" in msg
    assert (
        "Tracker(..., auto_migrate=True)    or    export CONSIST_AUTO_MIGRATE=1" in msg
    )
    assert 'pip install "consist<0.5"' in msg


def test_place_supersession_and_monotonic_location_events(tmp_path: Path) -> None:
    db_path = tmp_path / "locations.duckdb"
    anchor_a = Digest("sha256:file", "a" * 64)
    anchor_b = Digest("sha256:file", "b" * 64)
    id_a = Identity(anchor=anchor_a)
    id_b = Identity(anchor=anchor_b)
    witness_a = FileStat(size=100, mtime_ns=1_000, ctime_ns=1_000, ino=11, racy=False)
    witness_b = FileStat(size=200, mtime_ns=2_000, ctime_ns=2_000, ino=12, racy=False)

    with StoreLedger(db_path) as ledger:
        # 1. Observe anchor_A at workspace://out.parquet and archive://run1/out.parquet
        obs_a_ws = ledger.record_observation(
            Observation(
                location=Location(uri="workspace://out.parquet"),
                identity=id_a,
                access_driver="parquet",
                witness=witness_a,
                basis="computed",
                site="lawrencium",
                observed_at_ns=100,
            )
        )
        obs_a_arch = ledger.record_observation(
            Observation(
                location=Location(uri="archive://run1/out.parquet"),
                identity=id_a,
                access_driver="parquet",
                witness=witness_a,
                basis="computed",
                site="lawrencium",
                observed_at_ns=110,
            )
        )

        # 2. Observe anchor_B overwriting workspace://out.parquet at a later timestamp
        obs_b_ws = ledger.record_observation(
            Observation(
                location=Location(uri="workspace://out.parquet"),
                identity=id_b,
                access_driver="parquet",
                witness=witness_b,
                basis="computed",
                site="lawrencium",
                observed_at_ns=200,
            )
        )

        # Verify locate_current(['anchor_A']) returns ONLY archive://run1/out.parquet
        loc_a = ledger.locate_current([anchor_a])
        assert [r["uri"] for r in loc_a] == ["archive://run1/out.parquet"]
        assert loc_a[0]["observation_id"] == obs_a_arch
        assert loc_a[0]["witness"] == witness_a

        # Verify locate_current(['anchor_B']) returns workspace://out.parquet
        loc_b = ledger.locate_current([anchor_b])
        assert [r["uri"] for r in loc_b] == ["workspace://out.parquet"]
        assert loc_b[0]["observation_id"] == obs_b_ws
        assert loc_b[0]["witness"] == witness_b

        # Verify artifact_location_current view also reflects place supersession
        cur_a = ledger.query_location_current(anchor=anchor_a)
        assert [r["uri"] for r in cur_a] == ["archive://run1/out.parquet"]
        assert obs_a_ws not in {r["observation_id"] for r in cur_a}

        # 3. Verify monotonic location_event status_rank transitions:
        # pending_transfer (0) -> active (1) -> evicted (2), even if a late lower-rank
        # event arrives with a higher timestamp.
        ledger.record_location_event(
            observation_id=obs_a_arch,
            status="pending_transfer",
            transfer_ref="tx-1",
            at_ns=120,
        )
        assert (
            ledger.query_location_current(anchor=anchor_a)[0]["status"]
            == "pending_transfer"
        )

        ledger.record_location_event(
            observation_id=obs_a_arch,
            status="active",
            transfer_ref="tx-1",
            at_ns=130,
        )
        assert ledger.query_location_current(anchor=anchor_a)[0]["status"] == "active"

        # Out-of-order lower-rank event with later timestamp (at_ns=140) does NOT downgrade 'active' (rank 1)
        ledger.record_location_event(
            observation_id=obs_a_arch,
            status="pending_transfer",
            transfer_ref="tx-old",
            at_ns=140,
        )
        assert ledger.query_location_current(anchor=anchor_a)[0]["status"] == "active"

        # Evicted (rank 2) supersedes active (rank 1) even with an earlier at_ns
        ledger.record_location_event(
            observation_id=obs_a_arch,
            status="evicted",
            at_ns=135,
        )
        assert ledger.query_location_current(anchor=anchor_a)[0]["status"] == "evicted"
        assert ledger.locate_current([anchor_a], include_evicted=False) == []
        assert len(ledger.locate_current([anchor_a], include_evicted=True)) == 1


def test_run_artifact_link_and_run_artifact_role_passthrough_views(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bindings.duckdb"
    passthrough_id = uuid.uuid4()
    output_only_id = uuid.uuid4()
    anchor = Digest("sha256:file", "c" * 64)

    with StoreLedger(db_path) as ledger:
        ledger.record_run(run_id="run_1", model_name="sim")
        ledger.record_artifact(
            artifact_id=passthrough_id,
            key="households",
            driver="parquet",
            anchor=anchor,
        )
        ledger.record_artifact(
            artifact_id=output_only_id,
            key="trips",
            driver="parquet",
            anchor=anchor,
        )

        # Bind `passthrough_id` as BOTH input and output of `run_1`
        ledger.record_run_binding(
            run_id="run_1",
            direction="input",
            occurrence_index=0,
            slot="households_in",
            artifact_id=passthrough_id,
            anchor=anchor,
        )
        ledger.record_run_binding(
            run_id="run_1",
            direction="output",
            occurrence_index=0,
            slot="households_out",
            artifact_id=passthrough_id,
            anchor=anchor,
        )
        ledger.record_run_binding(
            run_id="run_1",
            direction="output",
            occurrence_index=1,
            slot="trips",
            artifact_id=output_only_id,
            anchor=anchor,
        )

        link_rows = ledger.connection.execute(
            "SELECT run_id, artifact_id, direction FROM run_artifact_link "
            "WHERE run_id = 'run_1'"
        ).fetchall()
        link_by_art = {row[1]: row[2] for row in link_rows}
        assert len(link_rows) == 2
        assert link_by_art[passthrough_id] == "input"
        assert link_by_art[output_only_id] == "output"

        role_rows = ledger.connection.execute(
            "SELECT artifact_id, is_input, is_output FROM run_artifact_role "
            "WHERE run_id = 'run_1'"
        ).fetchall()
        role_by_art = {row[0]: (bool(row[1]), bool(row[2])) for row in role_rows}
        assert role_by_art[passthrough_id] == (True, True)
        assert role_by_art[output_only_id] == (False, True)


def test_store_ledger_digest_conflict_routing_and_readonly_in_memory_overlay(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "overlay_and_conflict.duckdb"
    anchor_a = Digest("sha256:file", "1" * 64)
    anchor_b = Digest("sha256:file", "2" * 64)
    ctd_good = Digest("ctd1:sha256", "3" * 64)
    ctd_bad = Digest("ctd1:sha256", "4" * 64)

    with StoreLedger(db_path) as rw_ledger:
        rw_ledger.record_content(anchor_a, "file")
        assert (
            rw_ledger.record_content_digest(anchor_a, ctd_good, basis="computed")
            is True
        )
        # Idempotent re-insert of identical digest succeeds
        assert (
            rw_ledger.record_content_digest(anchor_a, ctd_good, basis="computed")
            is True
        )
        # Conflicting digest for (anchor_a, ctd1:sha256) is routed to content_digest_conflict
        # without overwriting the kept value!
        assert (
            rw_ledger.record_content_digest(
                anchor_a, ctd_bad, basis="computed", source="worker-2"
            )
            is False
        )

        kept = rw_ledger.connection.execute(
            "SELECT value FROM content_digest WHERE anchor = ? AND scheme = 'ctd1:sha256'",
            [str(anchor_a)],
        ).fetchone()
        assert kept == (ctd_good.value,)

        conflicts = rw_ledger.connection.execute(
            "SELECT anchor, scheme, kept_value, rejected_value, source FROM content_digest_conflict"
        ).fetchall()
        assert conflicts == [
            (str(anchor_a), "ctd1:sha256", ctd_good.value, ctd_bad.value, "worker-2")
        ]

        obs_initial = rw_ledger.record_observation(
            Observation(
                location=Location(uri="workspace://table.parquet"),
                identity=Identity(anchor=anchor_a, claims=(ctd_good,)),
                access_driver="parquet",
                witness=None,
                basis="computed",
                site="laptop",
                observed_at_ns=100,
            )
        )

    # Open StoreLedger in read_only=True mode and verify in-memory session overlay
    with StoreLedger(db_path, read_only=True) as ro_ledger:
        core_claim = Digest("core_state/v1:sha256", "5" * 64)
        core_conflict = Digest("core_state/v1:sha256", "6" * 64)

        assert ro_ledger.record_content_digest(anchor_a, core_claim) is True
        assert ro_ledger.record_content_digest(anchor_a, core_conflict) is False
        assert len(ro_ledger.overlay_digest_conflicts) == 1

        identity_in_session = ro_ledger.get_identity(anchor_a)
        assert identity_in_session.get("ctd1:sha256") == ctd_good
        assert identity_in_session.get("core_state/v1:sha256") == core_claim

        # Record a new observation in the read-only session that supersedes workspace://table.parquet
        ro_ledger.record_observation(
            Observation(
                location=Location(uri="workspace://table.parquet"),
                identity=Identity(anchor=anchor_b),
                access_driver="parquet",
                witness=None,
                basis="computed",
                site="laptop",
                observed_at_ns=200,
            )
        )
        # Also record an archive location for anchor_a in the read-only overlay, then evict it
        obs_ro_arch = ro_ledger.record_observation(
            Observation(
                location=Location(uri="archive://table.parquet"),
                identity=Identity(anchor=anchor_a),
                access_driver="parquet",
                witness=None,
                basis="computed",
                site="laptop",
                observed_at_ns=150,
            )
        )
        assert [r["uri"] for r in ro_ledger.locate_current([anchor_a])] == [
            "archive://table.parquet"
        ]
        assert [r["uri"] for r in ro_ledger.locate_current([anchor_b])] == [
            "workspace://table.parquet"
        ]

        ro_ledger.record_location_event(
            observation_id=obs_ro_arch, status="evicted", at_ns=210
        )
        assert ro_ledger.locate_current([anchor_a], include_evicted=False) == []

    # Verify zero .ledger.d sidecar files were created and the on-disk DB was not mutated
    assert not db_path.with_name(f"{db_path.name}.ledger.d").exists()
    with StoreLedger(db_path, read_only=True) as fresh_ro:
        assert fresh_ro.get_identity(anchor_a).get("core_state/v1:sha256") is None
        loc_on_disk = fresh_ro.locate_current([anchor_a])
        assert len(loc_on_disk) == 1
        assert loc_on_disk[0]["observation_id"] == obs_initial


def test_v1_database_reader_across_minimal_v01_and_full_v042_schemas(
    tmp_path: Path,
) -> None:
    # 1. Minimal v0.1-style schema (no consist_schema, no artifact_content, minimal columns)
    v01_path = tmp_path / "v01_minimal.duckdb"
    art_v01_id = uuid.uuid4()
    with duckdb.connect(str(v01_path)) as con:
        con.execute(
            """
            CREATE TABLE run (
                id VARCHAR PRIMARY KEY,
                model_name VARCHAR,
                status VARCHAR,
                started_at TIMESTAMP
            );
            CREATE TABLE artifact (
                id VARCHAR PRIMARY KEY,
                key VARCHAR,
                uri VARCHAR,
                driver VARCHAR,
                hash VARCHAR
            );
            CREATE TABLE run_artifact_link (
                run_id VARCHAR,
                artifact_id VARCHAR,
                direction VARCHAR
            );
            INSERT INTO run VALUES ('run_v01', 'urbansim', 'completed', '2025-01-15 10:00:00');
            """
        )
        con.execute(
            "INSERT INTO artifact VALUES (?, 'parcels', 'inputs://parcels.csv', 'csv', ?)",
            [str(art_v01_id), "f" * 64],
        )
        con.execute(
            "INSERT INTO run_artifact_link VALUES ('run_v01', ?, 'input')",
            [str(art_v01_id)],
        )

    with V1DatabaseReader(v01_path) as reader:
        inv = reader.inventory()
        assert inv.consist_schema_version is None
        assert inv.run_count == 1
        assert inv.artifact_count == 1
        assert inv.link_count == 1
        assert inv.artifact_content_count is None
        assert inv.dir_entry_count is None
        assert inv.dlt_tables == ()

        runs = list(reader.iter_runs())
        assert len(runs) == 1
        assert runs[0].id == "run_v01"
        assert runs[0].model_name == "urbansim"
        assert runs[0].git_hash is None
        assert runs[0].signature is None

        arts = list(reader.iter_artifacts())
        assert len(arts) == 1
        assert arts[0].id == art_v01_id
        assert arts[0].container_uri == "inputs://parcels.csv"
        assert arts[0].hash == "f" * 64
        assert arts[0].content_id is None

        links = list(reader.iter_links())
        assert len(links) == 1
        assert links[0].run_id == "run_v01"
        assert links[0].artifact_id == art_v01_id
        assert links[0].direction == "input"
        assert links[0].is_implicit is False
        assert list(reader.iter_dir_entries()) == []

    # 2. Full v0.4.2 schema with artifact_content, run_artifact_dir_entry, and global_tables
    v042_path = tmp_path / "v042_full.duckdb"
    db = DatabaseManager(str(v042_path))
    db.dispose()

    art_id = uuid.uuid4()
    content_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    with duckdb.connect(str(v042_path)) as con:
        con.execute(
            "INSERT INTO run (id, model_name, status, config_hash, git_hash, signature, "
            "tags, meta, started_at, created_at, updated_at, stage, phase) "
            "VALUES ('r_full', 'activitysim', 'completed', 'cfg1', 'git1', 'sig1', "
            "'[\"prod\"]', '{\"owner\":\"test\"}', ?, ?, ?, 's1', 'p1')",
            [now, now, now],
        )
        con.execute(
            "INSERT INTO artifact_content (id, content_hash, driver, created_at, meta) "
            "VALUES (?, ?, 'parquet', ?, '{\"rows\":10}')",
            [str(content_id), "e" * 64, now],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, content_id, "
            "run_id, meta, created_at) "
            "VALUES (?, 'households', 'workspace://hh.parquet', 'parquet', ?, ?, 'r_full', "
            '\'{"recovery_roots":["/archive"],"hash_semantics":{"source":"full"}}\', ?)',
            [str(art_id), "e" * 64, str(content_id), now],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('r_full', ?, 'output', true)",
            [str(art_id)],
        )
        con.execute(
            "CREATE TABLE run_artifact_dir_entry ("
            "artifact_id VARCHAR, run_id VARCHAR, rel_path VARCHAR, kind VARCHAR, "
            "size BIGINT, sha256 VARCHAR, mtime_ns BIGINT"
            ")"
        )
        con.execute(
            "INSERT INTO run_artifact_dir_entry VALUES (?, 'r_full', 'part-0.parquet', 'file', 512, ?, 999)",
            [str(art_id), "d" * 64],
        )
        con.execute("CREATE SCHEMA global_tables")
        con.execute(
            "CREATE TABLE global_tables._dlt_loads (load_id VARCHAR, status INTEGER)"
        )
        con.execute("INSERT INTO global_tables._dlt_loads VALUES ('load_1', 0)")
        con.execute(
            "CREATE TABLE global_tables.households ("
            "hh_id INTEGER, consist_artifact_id VARCHAR, consist_run_id VARCHAR"
            ")"
        )
        con.execute(
            "INSERT INTO global_tables.households VALUES (1, ?, 'r_full')",
            [str(art_id)],
        )

    with V1DatabaseReader(v042_path) as reader:
        inv = reader.inventory()
        assert inv.consist_schema_version == 1
        assert inv.run_count == 1
        assert inv.artifact_count == 1
        assert inv.link_count == 1
        assert inv.artifact_content_count == 1
        assert inv.dir_entry_count == 1
        assert len(inv.dlt_tables) == 2

        dlt_by_name = {t.table_name: t for t in inv.dlt_tables}
        assert dlt_by_name["_dlt_loads"].is_dlt_internal is True
        hh_info = dlt_by_name["households"]
        assert hh_info.is_dlt_internal is False
        assert hh_info.row_count == 1
        assert hh_info.has_consist_artifact_id is True
        assert hh_info.consist_artifact_id_type == "VARCHAR"
        assert hh_info.has_consist_run_id is True

        art_rec = next(reader.iter_artifacts())
        assert art_rec.id == art_id
        assert art_rec.content_id == content_id
        assert art_rec.content_hash == "e" * 64
        assert art_rec.content_meta == {"rows": 10}
        assert art_rec.recovery_roots == ["/archive"]
        assert art_rec.hash_semantics == {"source": "full"}

        dir_entries = list(reader.iter_dir_entries())
        assert len(dir_entries) == 1
        assert dir_entries[0].artifact_id == art_id
        assert dir_entries[0].rel_path == "part-0.parquet"
        assert dir_entries[0].size == 512
        assert dir_entries[0].sha256 == "d" * 64


def test_v1_fasthash_parity_and_racy_window(tmp_path: Path) -> None:
    sample = tmp_path / "fast.bin"
    sample.write_bytes(b"hello-consist-fasthash")
    os.utime(sample, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))

    im_fast = IdentityManager(project_root=str(tmp_path), hashing_strategy="fast")
    expected = im_fast.compute_file_checksum(sample)

    digest_racy, st, is_racy = stat_v1_fast_file_hash(
        sample,
        v1_recorded_ns=1_700_000_000_000_000_000 + RACY_STAT_WINDOW_NS - 1,
    )
    assert digest_racy == expected
    assert compute_v1_fast_file_hash(st.st_size, st.st_mtime_ns) == expected
    assert is_racy is True

    digest_safe, _, is_racy_safe = stat_v1_fast_file_hash(
        sample,
        v1_recorded_ns=1_700_000_000_000_000_000 + RACY_STAT_WINDOW_NS,
    )
    assert digest_safe == expected
    assert is_racy_safe is False


def test_fasthash_is_never_imported_outside_consist_migrations() -> None:
    src_root = Path(__file__).resolve().parents[3] / "src" / "consist"
    violations: list[str] = []

    for py_file in sorted(src_root.rglob("*.py")):
        rel = py_file.relative_to(src_root)
        if rel.parts[0] == "migrations":
            continue
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "fasthash" in alias.name or alias.name.startswith(
                        "consist.migrations.v1.fasthash"
                    ):
                        violations.append(f"{rel}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                imported_names = {a.name for a in node.names}
                if (
                    "fasthash" in mod
                    or "fasthash" in imported_names
                    or "compute_v1_fast_file_hash" in imported_names
                    or "stat_v1_fast_file_hash" in imported_names
                ):
                    violations.append(
                        f"{rel}: from {mod} import {sorted(imported_names)}"
                    )

    assert violations == []


def test_phase1_v1_db_builder_and_store_ledger_startup_guards(
    tmp_path: Path,
) -> None:
    from consist.store.detect import DatabaseMigrationRequired
    from tests.helpers.v1_db_builder import (
        create_v1_global_table,
        create_v1_schema,
        insert_v1_artifact,
        insert_v1_artifact_content,
        insert_v1_artifact_facet,
        insert_v1_artifact_kv,
        insert_v1_binding_invocation,
        insert_v1_config_facet,
        insert_v1_coupler_snapshot,
        insert_v1_dir_entry,
        insert_v1_link,
        insert_v1_run,
        insert_v1_run_config_kv,
        insert_v1_schema_bundle,
    )

    v1_db = tmp_path / "built_v1.duckdb"
    with duckdb.connect(str(v1_db)) as con:
        create_v1_schema(con)
        insert_v1_run(
            con,
            run_id="r_v1",
            model_name="activitysim",
            year=2025,
            iteration=1,
            stage="main",
            phase="calib",
            git_hash="abc1234",
            signature="sig_r_v1",
            facet={"sample_rate": 0.1},
        )
        cid = insert_v1_artifact_content(con, content_hash="1" * 64)
        aid = insert_v1_artifact(
            con,
            key="trips",
            container_uri="workspace://trips.parquet",
            hash_val="1" * 64,
            content_id=cid,
            run_id="r_v1",
            schema_id="sch_1",
        )
        insert_v1_link(con, run_id="r_v1", artifact_id=aid, direction="output")
        insert_v1_dir_entry(con, artifact_id=aid, rel_path="part.parquet", size=128)
        insert_v1_config_facet(
            con,
            facet_id="cf_1",
            namespace="activitysim",
            facet_json={"sample_rate": 0.1},
        )
        insert_v1_run_config_kv(
            con,
            run_id="r_v1",
            facet_id="cf_1",
            namespace="activitysim",
            key="sample_rate",
            value_type="float",
            value_num=0.1,
        )
        insert_v1_artifact_facet(
            con,
            facet_id="af_1",
            namespace="activitysim",
            facet_json={"rows": 50},
        )
        insert_v1_artifact_kv(
            con,
            artifact_id=aid,
            facet_id="af_1",
            key_path="rows",
            value_type="int",
            value_num=50.0,
        )
        insert_v1_schema_bundle(
            con,
            schema_id="sch_1",
            summary_json={"n_cols": 1},
            fields=[{"name": "trip_id", "logical_type": "BIGINT"}],
            artifact_id=aid,
            run_id="r_v1",
        )
        insert_v1_coupler_snapshot(
            con,
            run_id="r_v1",
            phase="end",
            slot_key="trips",
            artifact_id=aid,
        )
        insert_v1_binding_invocation(
            con,
            requested_run_id="r_v1_req",
            execution_run_id="r_v1",
            cache_outcome="miss",
        )
        create_v1_global_table(
            con,
            "trips",
            [{"trip_id": 1, "consist_run_id": "r_v1", "consist_artifact_id": str(aid)}],
        )

    assert detect_schema(v1_db) is SchemaState.V1
    with V1DatabaseReader(v1_db) as reader:
        inv = reader.inventory()
        assert inv.run_count == 1
        assert inv.artifact_count == 1
        assert inv.link_count == 1
        assert len(inv.dlt_tables) == 1

    # StoreLedger unconditionally rejects V1 DBs with DatabaseMigrationRequired
    with pytest.raises(DatabaseMigrationRequired):
        StoreLedger(v1_db)

    # StoreLedger rejects UNKNOWN and FUTURE schemas with IncompatibleSchemaError
    unknown_db = tmp_path / "unknown.duckdb"
    with duckdb.connect(str(unknown_db)) as con:
        con.execute("CREATE TABLE arbitrary_foreign_table (x INTEGER)")
    assert detect_schema(unknown_db) is SchemaState.UNKNOWN
    with pytest.raises(IncompatibleSchemaError):
        StoreLedger(unknown_db)

    future_db = tmp_path / "future.duckdb"
    with duckdb.connect(str(future_db)) as con:
        con.execute(
            "CREATE TABLE consist_schema (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ, consist_version VARCHAR)"
        )
        con.execute("INSERT INTO consist_schema VALUES (99, now(), '9.9.9')")
    assert detect_schema(future_db) is SchemaState.FUTURE
    with pytest.raises(IncompatibleSchemaError):
        StoreLedger(future_db)


def test_phase1_store_ledger_transactions_overlay_basis_memo_and_container_supersession(
    tmp_path: Path,
) -> None:
    import threading
    from consist.digest import Digest, Identity
    from consist.location import FileStat, Location, MemberOf, Observation

    db_path = tmp_path / "ledger_p1.duckdb"
    h5_file = tmp_path / "pipeline.h5"
    h5_file.write_bytes(b"initial-h5-bytes")
    base_mtime = h5_file.stat().st_mtime_ns
    safe_obs_1 = base_mtime + 5_000_000_000

    with StoreLedger(db_path) as ledger:
        # 1. Re-entrant transaction commit, cursor() guard, seal() + export_run_manifest() inside tx,
        # and rollback clearing _memoized_observations / _memoized_locations
        from consist.engine import PendingOutput, seal
        from consist.store.detect import ConsistError
        from consist.store.manifest import export_run_manifest

        seal_f1 = tmp_path / "tx_seal_1.csv"
        seal_f2 = tmp_path / "tx_seal_2.csv"
        seal_f1.write_text("a,b\n1,2\n", encoding="utf-8")
        seal_f2.write_text("x,y\n3,4\n", encoding="utf-8")

        with ledger.transaction() as outer_cur:
            with pytest.raises(ConsistError, match="exec_cursor"):
                ledger.cursor()
            ledger.record_run(run_id="tx_ok", model_name="m1")
            sealed_1 = seal(
                [PendingOutput(role="out1", path=seal_f1, driver="csv", key="out1")],
                ledger=ledger,
                run_id="tx_ok",
            )
            sealed_2 = seal(
                [PendingOutput(role="out2", path=seal_f2, driver="csv", key="out2")],
                ledger=ledger,
                run_id="tx_ok",
            )
            assert set(sealed_1) == {"out1"}
            assert set(sealed_2) == {"out2"}
            tx_manifest = export_run_manifest(ledger, "tx_ok")
            assert {o.slot for o in tx_manifest.outputs} == {"out1", "out2"}
            with ledger.transaction() as inner_cur:
                assert inner_cur is outer_cur
                ledger.record_run(run_id="tx_ok_2", model_name="m1")
        assert ledger.get_run("tx_ok") is not None
        assert ledger.get_run("tx_ok_2") is not None

        rb_anchor = Digest("sha256:file", "f" * 64)
        with pytest.raises(RuntimeError, match="boom"):
            with ledger.transaction():
                ledger.record_run(run_id="tx_rolled_back", model_name="m1")
                ledger.record_observation(
                    Observation(
                        location=Location(uri=h5_file.resolve().as_uri()),
                        identity=Identity(rb_anchor),
                        access_driver="file",
                        witness=FileStat.capture(h5_file, observed_at_ns=safe_obs_1),
                        basis="computed",
                        site="local",
                        observed_at_ns=safe_obs_1,
                    )
                )
                assert len(ledger.memoized_candidates(rb_anchor)) == 1
                ledger.memoize_location(rb_anchor, "sentinel_loc", site="local")
                assert len(ledger._memoized_observations) > 0
                assert len(ledger._memoized_locations) > 0
                raise RuntimeError("boom")
        assert ledger.get_run("tx_rolled_back") is None
        assert ledger._memoized_observations == {}
        assert ledger._memoized_locations == {}
        assert ledger.memoized_candidates(rb_anchor) == ()

        # Multi-threaded RLock safety
        errors: list[Exception] = []

        def _worker(idx: int) -> None:
            try:
                with ledger.transaction():
                    ledger.record_run(run_id=f"thr_run_{idx}", model_name="mt")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(ledger.find_runs(model="mt")) == 6

        # 2. Digest basis widening: 'computed' supersedes conflicting 'declared' / 'trusted_foreign'
        anchor = Digest("sha256:file", "a" * 64)
        foreign_ctd = Digest("ctd1:sha256", "b" * 64)
        computed_ctd = Digest("ctd1:sha256", "c" * 64)
        ledger.record_identity(
            Identity(anchor=anchor, claims=(foreign_ctd,)),
            basis="trusted_foreign",
            anchor_basis="computed",
        )
        assert ledger.get_identity(anchor).get("ctd1:sha256") == foreign_ctd

        assert (
            ledger.record_content_digest(
                anchor,
                computed_ctd,
                basis="computed",
                source="local_verify",
            )
            is True
        )
        assert ledger.get_identity(anchor).get("ctd1:sha256") == computed_ctd
        with ledger.cursor() as cur:
            conflict_row = cur.execute(
                "SELECT kept_value, rejected_value, source FROM main.content_digest_conflict "
                "WHERE anchor = ? AND scheme = 'ctd1:sha256'",
                [str(anchor)],
            ).fetchone()
        assert conflict_row == ("c" * 64, "b" * 64, "local_verify")

        # 3. _memoized_observations < 4 sentinel caching (including empty () and 1 item) + invalidation
        unseen_anchor = Digest("sha256:file", "d" * 64)
        assert ledger.memoized_candidates(unseen_anchor) == ()
        assert ledger._memoized_observations.get(str(unseen_anchor)) == ()

        obs_single = Observation(
            location=Location(uri=h5_file.resolve().as_uri()),
            identity=Identity(unseen_anchor),
            access_driver="file",
            witness=FileStat.capture(h5_file, observed_at_ns=safe_obs_1),
            basis="computed",
            site="local",
            observed_at_ns=safe_obs_1,
        )
        obs_id = ledger.record_observation(obs_single)
        assert str(unseen_anchor) not in ledger._memoized_observations
        cands_1 = ledger.memoized_candidates(unseen_anchor)
        assert len(cands_1) == 1
        assert ledger._memoized_observations.get(str(unseen_anchor)) == cands_1
        # Eviction invalidates memo!
        ledger.record_location_event(observation_id=obs_id, status="evicted")
        assert str(unseen_anchor) not in ledger._memoized_observations
        assert ledger.memoized_candidates(unseen_anchor) == ()

        # 4. Container-over-member NOT EXISTS supersession in SQL locate_current
        member_anchor = Digest("ctd1:sha256", "1" * 64)
        container_anchor_1 = Digest("sha256:file", "2" * 64)
        container_anchor_2 = Digest("sha256:file", "3" * 64)
        container_uri = "workspace://pipeline.h5"

        parent_stat_1 = FileStat(
            size=100, mtime_ns=1000, ctime_ns=1000, ino=1, racy=False
        )
        parent_stat_2 = FileStat(
            size=250, mtime_ns=2000, ctime_ns=2000, ino=1, racy=False
        )
        ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member=None),
                identity=Identity(container_anchor_1),
                access_driver="hdf5",
                witness=parent_stat_1,
                basis="computed",
                site="local",
                observed_at_ns=10_000,
            )
        )
        ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member="/households"),
                identity=Identity(member_anchor),
                access_driver="h5_table",
                witness=MemberOf(parent=parent_stat_1, member="/households"),
                basis="computed",
                site="local",
                observed_at_ns=10_001,
            )
        )
        assert len(ledger.locate_current([member_anchor])) == 1

        # Overwrite container at t=20_000 with new witness -> member observation is superseded!
        ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member=None),
                identity=Identity(container_anchor_2),
                access_driver="hdf5",
                witness=parent_stat_2,
                basis="computed",
                site="local",
                observed_at_ns=20_000,
            )
        )
        assert ledger.locate_current([member_anchor]) == []

        # Member row whose witness_json lacks 'parent' (e.g. FileStat instead of MemberOf)
        # MUST ALSO be superseded in SQL locate_current & artifact_location_current (3-valued NULL fix)!
        member_no_parent_anchor = Digest("ctd1:sha256", "8" * 64)
        ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member="/legacy_member"),
                identity=Identity(member_no_parent_anchor),
                access_driver="h5_table",
                witness=parent_stat_1,
                basis="migrated_v1",
                site="local",
                observed_at_ns=15_000,
            )
        )
        assert ledger.locate_current([member_no_parent_anchor]) == []
        assert ledger.query_location_current(anchor=member_no_parent_anchor) == []

        # Record a fresh member at t=25_000 so we can also test read_only overlay supersession!
        member_anchor_2 = Digest("ctd1:sha256", "4" * 64)
        ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member="/persons"),
                identity=Identity(member_anchor_2),
                access_driver="h5_table",
                witness=MemberOf(parent=parent_stat_2, member="/persons"),
                basis="computed",
                site="local",
                observed_at_ns=25_000,
            )
        )
        assert len(ledger.locate_current([member_anchor_2])) == 1

    # 5. Read-only StoreLedger: overlay allows observations/events/digests; mutators raise PermissionError
    with StoreLedger(db_path, read_only=True) as ro_ledger:
        assert len(ro_ledger.locate_current([member_anchor_2])) == 1
        parent_stat_3 = FileStat(
            size=999, mtime_ns=3000, ctime_ns=3000, ino=2, racy=False
        )
        ro_ledger.record_observation(
            Observation(
                location=Location(uri=container_uri, member=None),
                identity=Identity(Digest("sha256:file", "5" * 64)),
                access_driver="hdf5",
                witness=parent_stat_3,
                basis="computed",
                site="local",
                observed_at_ns=30_000,
            )
        )
        # Overlay container observation supersedes older DB member observation!
        assert ro_ledger.locate_current([member_anchor_2]) == []

        with pytest.raises(PermissionError):
            ro_ledger.record_run(run_id="ro_fail", model_name="m")
        with pytest.raises(PermissionError):
            ro_ledger.record_artifact(
                artifact_id=uuid.uuid4(),
                key="k",
                driver="csv",
                anchor=Digest("sha256:file", "6" * 64),
            )
        with pytest.raises(PermissionError):
            ro_ledger.record_promotion(
                Digest("legacy-v1:unverified", "7" * 64),
                Digest("sha256:file", "6" * 64),
                basis="asserted",
            )
        with pytest.raises(PermissionError):
            ro_ledger.record_coupler_snapshots(
                run_id="tx_ok",
                phase="end",
                artifacts={},
            )
        with pytest.raises(PermissionError):
            ro_ledger.record_binding_invocation(
                requested_run_id="a",
                execution_run_id="b",
                cache_outcome="hit",
                binding_json="{}",
            )


def test_phase1_store_ledger_domain_crud_facets_and_select_only_queries(
    tmp_path: Path,
) -> None:
    from pydantic import BaseModel
    from consist.digest import Digest, Identity
    from consist.models.artifact_schema import (
        ArtifactSchema,
        ArtifactSchemaField,
        ArtifactSchemaObservation,
        ArtifactSchemaRelation,
    )
    from consist.models.run import Run

    class _RunSummaryRow(BaseModel):
        id: str
        model_name: str

    db_path = tmp_path / "domain_crud.duckdb"
    with StoreLedger(db_path) as ledger:
        # 1. upsert_run, get_run, find_runs, find_cached_run, and run_config_kv view
        run_obj = Run(
            id="run_live_1",
            model_name="beam",
            status="completed",
            year=2030,
            iteration=2,
            stage="sim",
            phase="iter",
            config_hash="cfg_1",
            input_hash="in_1",
            git_hash="git_1",
            signature="sig_beam_1",
            tags=["baseline", "calib"],
            meta={
                "facet": {"pop_scale": 0.25, "transit_enabled": True, "region": "bay"},
                "facet_namespace": "beam_ns",
            },
        )
        ledger.upsert_run(run_obj)
        loaded_run = ledger.get_run("run_live_1")
        assert loaded_run is not None
        assert loaded_run.id == "run_live_1"
        assert loaded_run.stage == "sim"
        assert loaded_run.phase == "iter"
        assert loaded_run.git_hash == "git_1"
        assert loaded_run.tags == ["baseline", "calib"]

        # find_cached_run matches ONLY cache_status='live' AND status='completed'
        cached = ledger.find_cached_run("sig_beam_1", model_name="beam")
        assert cached is not None and cached.id == "run_live_1"

        ledger.record_run(
            run_id="run_hist_1",
            model_name="beam",
            status="completed",
            cache_status="historical",
            signature="sig_beam_hist",
        )
        assert ledger.find_cached_run("sig_beam_hist", model_name="beam") is None

        found = ledger.find_runs(
            model="beam",
            year=2030,
            stage="sim",
            tags=["baseline"],
            facet={"region": "bay", "transit_enabled": True},
        )
        assert [r.id for r in found] == ["run_live_1"]

        # Verify run_config_kv LATERAL json_each view
        kv_df = ledger.query_df(
            "SELECT namespace, key, value_type, value_num, value_bool, value_str "
            "FROM run_config_kv WHERE run_id = ? ORDER BY key",
            ["run_live_1"],
        )
        assert list(kv_df["key"]) == ["pop_scale", "region", "transit_enabled"]
        assert list(kv_df["namespace"].unique()) == ["beam_ns"]

        # > 500 facet leaves -> facet_kv is NULL
        huge_facet = {f"k_{i}": i for i in range(505)}
        ledger.record_run(
            run_id="run_huge_facet",
            model_name="beam",
            meta={"facet": huge_facet},
        )
        assert (
            len(
                ledger.query_df(
                    "SELECT * FROM run_config_kv WHERE run_id = 'run_huge_facet'"
                )
            )
            == 0
        )

        # Nested dicts, JSON list leaves, dotted keys, and {"facet": None, "config_facet": ...} non-colliding facet_id
        ledger.record_run(
            run_id="run_nested_facet_1",
            model_name="ml_sim",
            meta={
                "facet": None,
                "config_facet": {
                    "nested": {"lr": 0.01, "layers": [64, 128]},
                    "top_list": ["a", "b"],
                },
            },
        )
        ledger.record_run(
            run_id="run_nested_facet_2",
            model_name="ml_sim",
            meta={
                "facet": None,
                "config_facet": {
                    "nested": {"lr": 0.05, "layers": [32]},
                    "top_list": ["c"],
                },
            },
        )
        facet_ids_df = ledger.query_df(
            "SELECT DISTINCT run_id, facet_id FROM run_config_kv "
            "WHERE run_id IN ('run_nested_facet_1', 'run_nested_facet_2') ORDER BY run_id"
        )
        assert len(facet_ids_df) == 2
        assert facet_ids_df.iloc[0]["facet_id"] != facet_ids_df.iloc[1]["facet_id"]

        assert [
            r.id
            for r in ledger.find_runs(
                model="ml_sim",
                facet={
                    "nested": {"lr": 0.01, "layers": [64, 128]},
                    "top_list": ["a", "b"],
                },
            )
        ] == ["run_nested_facet_1"]
        assert [
            r.id
            for r in ledger.find_runs(
                model="ml_sim",
                facet={"nested.lr": 0.05},
            )
        ] == ["run_nested_facet_2"]
        # SQL LIMIT / OFFSET pushdown when tags and metadata are empty
        paged_1 = ledger.find_runs(model="ml_sim", limit=1, offset=0)
        paged_2 = ledger.find_runs(model="ml_sim", limit=1, offset=1)
        assert len(paged_1) == 1 and len(paged_2) == 1
        assert {paged_1[0].id, paged_2[0].id} == {
            "run_nested_facet_1",
            "run_nested_facet_2",
        }

        # 2. Artifacts, parent/child lineage, update_artifact_meta, artifact_kv, get_artifacts_for_run
        parent_id = uuid.uuid4()
        child_id = uuid.uuid4()
        d_parent = Digest("sha256:file", "1" * 64)
        d_child = Digest("ctd1:sha256", "2" * 64)
        ledger.record_identity(Identity(d_parent))
        ledger.record_identity(Identity(d_child), repr_kind="table")
        ledger.record_artifact(
            artifact_id=parent_id,
            key="container_h5",
            driver="hdf5",
            anchor=d_parent,
            run_id="run_live_1",
            meta={"facet": {"tables": 2}, "facet_namespace": "h5_ns"},
        )
        ledger.record_artifact(
            artifact_id=child_id,
            key="households",
            driver="h5_table",
            anchor=d_child,
            run_id="run_live_1",
            parent_artifact_id=parent_id,
            meta={"custom": "initial"},
        )
        ledger.record_run_binding(
            run_id="run_live_1",
            direction="input",
            occurrence_index=0,
            slot="container_h5",
            artifact_id=parent_id,
            anchor=d_parent,
        )
        ledger.record_run_binding(
            run_id="run_live_1",
            direction="output",
            occurrence_index=0,
            slot="households",
            artifact_id=child_id,
            anchor=d_child,
        )

        assert ledger.get_artifact(parent_id) is not None
        assert ledger.get_artifact(str(d_child)) is not None
        children = ledger.get_child_artifacts(parent_id)
        assert [c.id for c in children] == [child_id]

        updated_child = ledger.update_artifact_meta(
            child_id,
            {"custom": "replaced", "facet": {"zone_count": 1454}},
            merge=False,
            schema_id="sch_hh",
        )
        assert updated_child.meta["custom"] == "replaced"
        assert updated_child.schema_id == "sch_hh"

        art_kv_df = ledger.query_df(
            "SELECT key_path, value_num FROM artifact_kv WHERE artifact_id = ?",
            [child_id],
        )
        assert len(art_kv_df) == 1
        assert art_kv_df.iloc[0]["key_path"] == "zone_count"
        assert float(art_kv_df.iloc[0]["value_num"]) == 1454.0

        # Verify get_artifacts_for_run uses batched queries (never calling load_artifact in an N+1 loop)
        unbound_id = uuid.uuid4()
        hist_d = Digest("legacy-v1:unverified", "9" * 64)
        ver_d = Digest("sha256:file", "7" * 64)
        ledger.record_identity(Identity(hist_d), repr_kind="historical")
        ledger.record_identity(Identity(ver_d), repr_kind="file")
        ledger.record_promotion(hist_d, ver_d, basis="caller_supplied_match")
        ledger.record_artifact(
            artifact_id=unbound_id,
            key="promoted_extra",
            driver="parquet",
            anchor=hist_d,
            run_id="run_live_1",
        )
        ledger._memoized_observations.clear()
        orig_load_artifact = ledger.load_artifact

        def _forbid_n_plus_1(*_a: object, **_kw: object) -> None:
            raise AssertionError("get_artifacts_for_run must not call load_artifact")

        ledger.load_artifact = _forbid_n_plus_1  # type: ignore[method-assign]
        try:
            run_arts = ledger.get_artifacts_for_run("run_live_1")
        finally:
            ledger.load_artifact = orig_load_artifact  # type: ignore[method-assign]
        assert set(run_arts.inputs.keys()) == {"container_h5"}
        assert set(run_arts.outputs.keys()) == {"households", "promoted_extra"}
        assert run_arts.outputs["promoted_extra"].anchor == ver_d
        assert run_arts.outputs["promoted_extra"].recorded_anchor == hist_d

        # 3. Operational tables: binding invocations, coupler snapshots, artifact schemas
        inv_id = ledger.record_binding_invocation(
            requested_run_id="run_req_2",
            execution_run_id="run_live_1",
            cache_source_run_id="run_live_1",
            cache_outcome="hit",
            binding_json='{"sig":"sig_beam_1"}',
        )
        invs = ledger.list_binding_invocations(requested_run_id="run_req_2")
        assert len(invs) == 1 and invs[0].id == inv_id

        ledger.record_coupler_snapshots(
            scenario_run_id="scen_1",
            run_id="run_live_1",
            phase="post_step",
            step_ordinal=3,
            artifacts={"hh": updated_child},
        )
        snaps = ledger.get_coupler_snapshots("run_live_1", phase="post_step")
        assert len(snaps) == 1
        assert snaps[0].role_key == "hh"
        assert snaps[0].artifact_id == child_id

        schema_row = ArtifactSchema(
            id="sch_hh",
            profile_version=1,
            summary_json={"rows": 100},
            profile_json={"columns": ["hh_id"]},
        )
        field_row = ArtifactSchemaField(
            schema_id="sch_hh",
            ordinal_position=1,
            name="hh_id",
            logical_type="BIGINT",
            nullable=False,
        )
        rel_row = ArtifactSchemaRelation(
            schema_id="sch_hh",
            from_field="hh_id",
            to_table="persons",
            to_field="hh_id",
        )
        obs_row = ArtifactSchemaObservation(
            artifact_id=child_id,
            schema_id="sch_hh",
            run_id="run_live_1",
            source="duckdb",
            sample_rows=100,
        )
        ledger.upsert_artifact_schema(
            schema_row,
            [field_row],
            relations=[rel_row],
            observation=obs_row,
        )
        fetched_bundle = ledger.get_artifact_schema("sch_hh")
        assert fetched_bundle is not None
        assert fetched_bundle[0].id == "sch_hh"
        assert [f.name for f in fetched_bundle[1]] == ["hh_id"]
        assert len(ledger.list_artifact_schemas(artifact_id=child_id)) == 1

        # 4. query_rows typed validation & SELECT-only enforcement
        rows = ledger.query_rows(
            "SELECT id, model_name FROM run WHERE model_name = ? ORDER BY id",
            ["beam"],
            model=_RunSummaryRow,
        )
        assert [r.id for r in rows] == ["run_hist_1", "run_huge_facet", "run_live_1"]

        for forbidden_sql in (
            "DELETE FROM run",
            "INSERT INTO consist_meta VALUES ('k', 'v')",
            "SELECT 1; DROP TABLE run",
            "CREATE TABLE sneaky (x INT)",
        ):
            with pytest.raises(ValueError, match="SELECT"):
                ledger.query_df(forbidden_sql)
            with pytest.raises(ValueError, match="SELECT"):
                ledger.query_rows(forbidden_sql, model=_RunSummaryRow)
