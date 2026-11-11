from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import duckdb
import pytest

from consist.core.db_runtime import DatabaseRuntimeOps
from consist.core.persistence import DatabaseManager
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV


def _build_retry_only_db() -> DatabaseManager:
    db = DatabaseManager.__new__(DatabaseManager)
    db._lock_retries = 20
    db._lock_base_sleep_seconds = 0.1
    db._lock_max_sleep_seconds = 2.0
    db._runtime_ops = DatabaseRuntimeOps(db)
    return db


def test_execute_with_retry_retries_on_lock(monkeypatch) -> None:
    """
    execute_with_retry should retry lock/IO errors and eventually return.
    """
    db = _build_retry_only_db()
    calls: List[int] = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise duckdb.IOException("database is locked")
        return "ok"

    monkeypatch.setattr("consist.core.db_runtime.time.sleep", lambda _s: None)
    assert db.execute_with_retry(flaky, retries=3) == "ok"
    assert len(calls) == 3


def test_execute_with_retry_raises_on_non_lock_errors() -> None:
    """
    execute_with_retry should not retry non-lock errors (e.g., constraint violations).
    """
    db = _build_retry_only_db()
    calls: List[int] = []

    def fail_fast():
        calls.append(1)
        raise duckdb.ConstraintException("constraint violation")

    with pytest.raises(duckdb.ConstraintException):
        db.execute_with_retry(fail_fast, retries=3)
    assert len(calls) == 1


def test_execute_with_retry_raises_on_constraint_operational_error() -> None:
    """
    Constraint-like OperationalError should not be retried.
    """
    db = _build_retry_only_db()
    calls: List[int] = []

    def fail_fast():
        calls.append(1)
        raise duckdb.OperationalError("constraint violation")

    with pytest.raises(duckdb.OperationalError):
        db.execute_with_retry(fail_fast, retries=3)
    assert len(calls) == 1


def test_execute_with_retry_retries_on_active_connection_lock(monkeypatch) -> None:
    """
    execute_with_retry should retry connection-lock errors from DuckDB.
    """
    db = _build_retry_only_db()
    calls: List[int] = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise duckdb.IOException(
                "IO Error: Could not set lock on file: conflicting lock"
            )
        return "ok"

    monkeypatch.setattr("consist.core.db_runtime.time.sleep", lambda _s: None)
    assert db.execute_with_retry(flaky, retries=3) == "ok"
    assert len(calls) == 3


def test_sync_run_facet_kv_upserts_idempotently_on_repeat_sync(tmp_path) -> None:
    """
    Re-syncing a Run with updated facet values in Schema v2 replaces facet_kv cleanly
    without duplicate rows in main.run_config_kv.
    """
    db = DatabaseManager(str(tmp_path / "provenance.duckdb"))
    try:
        now = datetime.now(timezone.utc)
        run = Run(
            id="run_1",
            model_name="model",
            status="completed",
            meta={
                "facet": {"param": "alpha"},
                "config_facet_id": "facet_a",
                "config_facet_namespace": "model",
            },
            created_at=now,
            started_at=now,
            ended_at=now,
        )
        db.sync_run(run)

        run.meta["facet"] = {"param": "beta"}
        db.sync_run(run)

        rows = db.query_rows(
            "SELECT * FROM main.run_config_kv WHERE run_id = ?",
            ["run_1"],
            model=RunConfigKV,
        )
        assert len(rows) == 1
        assert rows[0].key == "param"
        assert rows[0].value_str == "beta"
    finally:
        db.close()


def test_database_manager_uses_configured_retry_defaults(tmp_path) -> None:
    db = DatabaseManager(
        str(tmp_path / "provenance_retry_defaults.duckdb"),
        lock_retries=7,
        lock_base_sleep_seconds=0.3,
        lock_max_sleep_seconds=1.1,
    )
    try:
        assert db._lock_retries == 7
        assert db._lock_base_sleep_seconds == 0.3
        assert db._lock_max_sleep_seconds == 1.1
    finally:
        db.close()
