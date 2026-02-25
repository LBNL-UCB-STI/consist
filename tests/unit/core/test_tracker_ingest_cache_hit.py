from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from consist.core.tracker import Tracker


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, result_row):
        self.result_row = result_row
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def exec_driver_sql(self, statement: str, params: tuple[str, ...]) -> _Result:
        self.calls.append((statement, params))
        return _Result(self.result_row)


class _Engine:
    def __init__(self, connection: _Connection):
        self.connection = connection

    @contextmanager
    def begin(self):
        yield self.connection


def test_ingest_cache_hit_quotes_table_name() -> None:
    tracker = Tracker.__new__(Tracker)
    connection = _Connection(result_row=(1,))
    tracker.db = SimpleNamespace(engine=_Engine(connection))

    result = tracker._ingest_cache_hit("safe_table", "abc123")

    assert result is True
    assert len(connection.calls) == 1
    statement, params = connection.calls[0]
    assert statement.startswith('SELECT 1 FROM "global_tables"."safe_table"')
    assert params == ("abc123",)


def test_ingest_cache_hit_rejects_unsafe_table_name_before_sql() -> None:
    tracker = Tracker.__new__(Tracker)
    connection = _Connection(result_row=(1,))
    tracker.db = SimpleNamespace(engine=_Engine(connection))

    result = tracker._ingest_cache_hit("safe_table;DROP TABLE run;--", "abc123")

    assert result is False
    assert connection.calls == []
