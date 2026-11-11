from __future__ import annotations

from contextlib import contextmanager

import duckdb
import pytest

from consist.core.db_runtime import DatabaseRuntimeOps, is_retryable_db_error
from consist.core.persistence import DatabaseManager, ProvenanceWriter
from consist.core.stores import HotDataStore, MetadataStore
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaRelation,
)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Database is LOCKED by another process", True),
        ("IO Error while opening file", True),
        ("resource is already active", True),
        ("connection is already open", True),
        ("another connection has the file", True),
        ("another process holds the lock", True),
        ("IO Error: database does not exist", False),
        ("IO Error: no such file or directory", False),
        ("constraint violation", False),
        ("syntax error near SELECT", False),
    ],
)
def test_is_retryable_db_error_matches_known_markers(
    message: str, expected: bool
) -> None:
    assert is_retryable_db_error(message) is expected


def test_query_df_rejects_non_select_and_multi_statement_sql() -> None:
    db = DatabaseManager(":memory:")
    try:
        with pytest.raises(ValueError, match="non-empty SELECT"):
            db.query_df("   ")

        with pytest.raises(ValueError, match="single read-only SELECT"):
            db.query_df("DELETE FROM main.run")

        with pytest.raises(ValueError, match="single read-only SELECT"):
            db.query_df("SELECT 1; DROP TABLE main.run;")
    finally:
        db.close()


def test_hot_data_store_rejects_unsafe_table_name() -> None:
    db = DatabaseManager(":memory:")
    try:
        meta_store = MetadataStore(db=db)
        hot_store = HotDataStore(db_path=":memory:", metadata_store=meta_store)
        assert hot_store.ingest_cache_hit("run; DROP TABLE run;", "abc123") is False
    finally:
        db.close()


def _build_retry_only_db() -> DatabaseManager:
    db = DatabaseManager.__new__(DatabaseManager)
    db._runtime_ops = DatabaseRuntimeOps(db)
    db._lock_retries = 5
    db._lock_base_sleep_seconds = 0.01
    db._lock_max_sleep_seconds = 0.05
    return db


def test_execute_with_retry_retries_retryable_duckdb_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _build_retry_only_db()

    calls: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise duckdb.IOException("IO Error: conflicting lock from another process")
        return "ok"

    monkeypatch.setattr("consist.core.db_runtime.random.uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr(
        "consist.core.db_runtime.time.sleep", lambda s: sleeps.append(s)
    )

    result = db.execute_with_retry(flaky, operation_name="test_retry")
    assert result == "ok"
    assert len(calls) == 3
    assert len(sleeps) == 2


def test_execute_with_retry_does_not_retry_non_retryable_duckdb_error() -> None:
    db = _build_retry_only_db()
    calls: list[int] = []

    def fail_fast() -> None:
        calls.append(1)
        raise duckdb.ConstraintException("Constraint Error: constraint violation")

    with pytest.raises(duckdb.ConstraintException):
        db.execute_with_retry(fail_fast, retries=3)

    assert len(calls) == 1


def test_apply_physical_fks_quotes_valid_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = DatabaseManager(":memory:")
    try:
        with db.ledger.exec_cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute("CREATE TABLE global_tables.parents (id INTEGER PRIMARY KEY)")
            cur.execute("CREATE TABLE global_tables.children (parent_id INTEGER)")

        db.persist_artifact_schema(
            ArtifactSchema(
                id="schema_children",
                profile_version=1,
                summary_json={
                    "table_schema": "global_tables",
                    "table_name": "children",
                },
            ),
            fields=[
                ArtifactSchemaField(
                    schema_id="schema_children",
                    ordinal_position=1,
                    name="parent_id",
                    logical_type="INTEGER",
                )
            ],
            relations=[
                ArtifactSchemaRelation(
                    schema_id="schema_children",
                    from_field="parent_id",
                    to_table="parents",
                    to_field="id",
                )
            ],
        )

        alter_statements: list[str] = []
        original_exec_cursor = db.ledger.exec_cursor

        class _CursorProxy:
            def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
                self._inner = inner

            def execute(self, sql: str, parameters: object = None):
                if sql.lstrip().upper().startswith("ALTER TABLE"):
                    alter_statements.append(sql)
                    return self._inner
                if parameters is None:
                    return self._inner.execute(sql)
                return self._inner.execute(sql, parameters)

        @contextmanager
        def intercepting_exec_cursor():
            with original_exec_cursor() as real_cur:
                yield _CursorProxy(real_cur)

        monkeypatch.setattr(db.ledger, "exec_cursor", intercepting_exec_cursor)

        applied = db.apply_physical_fks()

        assert applied == [
            "global_tables.children(parent_id) -> global_tables.parents(id)"
        ]
        assert len(alter_statements) == 1
        assert alter_statements[0] == (
            'ALTER TABLE "global_tables"."children" '
            'ADD FOREIGN KEY ("parent_id") '
            'REFERENCES "global_tables"."parents"("id")'
        )
    finally:
        db.close()


def test_apply_physical_fks_skips_unmatched_or_unsafe_table_targets() -> None:
    db = DatabaseManager(":memory:")
    try:
        with db.ledger.exec_cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
            cur.execute("CREATE TABLE global_tables.children (parent_id INTEGER)")

        db.persist_artifact_schema(
            ArtifactSchema(
                id="schema_unsafe_fk",
                profile_version=1,
                summary_json={"table_name": "children"},
            ),
            fields=[
                ArtifactSchemaField(
                    schema_id="schema_unsafe_fk",
                    ordinal_position=1,
                    name="parent_id",
                    logical_type="INTEGER",
                )
            ],
            relations=[
                ArtifactSchemaRelation(
                    schema_id="schema_unsafe_fk",
                    from_field="parent_id",
                    to_table="parents;DROP TABLE run;--",
                    to_field="id",
                )
            ],
        )

        applied = db.apply_physical_fks()
        assert applied == []

        with db.ledger.exec_cursor() as cur:
            run_table_exists = cur.execute(
                "SELECT COUNT(*) FROM duckdb_tables() "
                "WHERE schema_name = 'main' AND table_name = 'run'"
            ).fetchone()
        assert run_table_exists == (1,)
    finally:
        db.close()


def test_write_record_json_does_not_use_predictable_tmp_targets(tracker) -> None:
    with tracker.start_run("run_secure_snapshot_tmp", "demo") as t:
        record = t.current_consist
        assert record is not None

        per_run_dir = t.fs.run_dir / "consist_runs"
        per_run_dir.mkdir(parents=True, exist_ok=True)
        per_run_target = per_run_dir / "run_secure_snapshot_tmp.json"
        per_run_predictable_tmp = per_run_target.with_suffix(".tmp")
        per_run_predictable_tmp.write_text("sentinel", encoding="utf-8")

        latest_target = t.fs.run_dir / "consist.json"
        latest_predictable_tmp = latest_target.with_suffix(".tmp")
        latest_predictable_tmp.write_text("sentinel", encoding="utf-8")

        writer = ProvenanceWriter(t)
        writer._write_record_json(record)

        assert per_run_target.exists()
        assert latest_target.exists()
        assert per_run_predictable_tmp.read_text(encoding="utf-8") == "sentinel"
        assert latest_predictable_tmp.read_text(encoding="utf-8") == "sentinel"
