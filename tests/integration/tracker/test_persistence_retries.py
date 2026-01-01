from __future__ import annotations

import logging
from typing import List

import pytest
from sqlalchemy.exc import DatabaseError, OperationalError

from consist.core.persistence import DatabaseManager
from consist.models.run_config_kv import RunConfigKV


def test_execute_with_retry_retries_on_lock(monkeypatch) -> None:
    """
    execute_with_retry should retry lock/IO errors and eventually return.
    """
    db = DatabaseManager.__new__(DatabaseManager)
    calls: List[int] = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise OperationalError("stmt", {}, Exception("database is locked"))
        return "ok"

    monkeypatch.setattr("time.sleep", lambda _s: None)
    assert db.execute_with_retry(flaky, retries=3) == "ok"
    assert len(calls) == 3


def test_execute_with_retry_raises_on_non_lock_errors() -> None:
    """
    execute_with_retry should not retry non-lock errors (e.g., constraint violations).
    """
    db = DatabaseManager.__new__(DatabaseManager)
    calls: List[int] = []

    def fail_fast():
        calls.append(1)
        raise DatabaseError("stmt", {}, Exception("constraint violation"))

    with pytest.raises(DatabaseError):
        db.execute_with_retry(fail_fast, retries=3)
    assert len(calls) == 1


def test_execute_with_retry_raises_on_constraint_operational_error() -> None:
    """
    Constraint-like OperationalError should not be retried.
    """
    db = DatabaseManager.__new__(DatabaseManager)
    calls: List[int] = []

    def fail_fast():
        calls.append(1)
        raise OperationalError("stmt", {}, Exception("constraint violation"))

    with pytest.raises(OperationalError):
        db.execute_with_retry(fail_fast, retries=3)
    assert len(calls) == 1


def test_insert_run_config_kv_bulk_warns_on_duplicate_keys(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Duplicate RunConfigKV inserts should warn and not crash.
    """
    caplog.set_level(logging.WARNING)
    db = DatabaseManager(str(tmp_path / "provenance.db"))

    row_a = RunConfigKV(
        run_id="run_1",
        facet_id="facet_a",
        namespace="model",
        key="param",
        value_type="str",
        value_str="alpha",
    )
    row_b = RunConfigKV(
        run_id="run_1",
        facet_id="facet_a",
        namespace="model",
        key="param",
        value_type="str",
        value_str="beta",
    )

    db.insert_run_config_kv_bulk([row_a, row_b])

    assert any(
        "Failed to insert run config kv rows" in record.message
        for record in caplog.records
    )
