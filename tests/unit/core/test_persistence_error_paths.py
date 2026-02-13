from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError, OperationalError

from consist.core.persistence import (
    DatabaseManager,
    _is_retryable_db_error,
    load_json_safe,
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
        ("constraint violation", False),
        ("syntax error near SELECT", False),
    ],
)
def test_is_retryable_db_error_matches_known_markers(message: str, expected: bool) -> None:
    assert _is_retryable_db_error(message) is expected


def test_load_json_safe_raises_value_error_for_invalid_json() -> None:
    with pytest.raises(ValueError, match="Invalid JSON"):
        load_json_safe("{not-valid-json")


def test_load_json_safe_raises_value_error_for_excessive_nesting_depth() -> None:
    deeply_nested = "[" * 51 + "0" + "]" * 51
    with pytest.raises(ValueError, match="JSON nesting depth exceeds limit"):
        load_json_safe(deeply_nested)


def test_table_has_column_returns_false_for_unsafe_table_name() -> None:
    db = DatabaseManager.__new__(DatabaseManager)
    db.engine = object()

    assert db._table_has_column(table_name="run; DROP TABLE run;", column_name="id") is False


def test_execute_with_retry_retries_retryable_operational_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = DatabaseManager.__new__(DatabaseManager)
    db._lock_retries = 5
    db._lock_base_sleep_seconds = 0.01
    db._lock_max_sleep_seconds = 0.05

    calls: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise OperationalError(
                "SELECT 1",
                {},
                Exception("IO Error: conflicting lock from another process"),
            )
        return "ok"

    monkeypatch.setattr("consist.core.persistence.random.uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr("consist.core.persistence.time.sleep", lambda s: sleeps.append(s))

    result = db.execute_with_retry(flaky, operation_name="test_retry")
    assert result == "ok"
    assert len(calls) == 3
    assert len(sleeps) == 2


def test_execute_with_retry_does_not_retry_non_retryable_db_error() -> None:
    db = DatabaseManager.__new__(DatabaseManager)
    calls: list[int] = []

    def fail_fast() -> None:
        calls.append(1)
        raise DatabaseError("INSERT", {}, Exception("constraint violation"))

    with pytest.raises(DatabaseError):
        db.execute_with_retry(fail_fast, retries=3)

    assert len(calls) == 1
