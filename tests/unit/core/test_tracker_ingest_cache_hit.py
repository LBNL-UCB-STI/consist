from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from consist.core.tracker import Tracker


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Cursor:
    def __init__(self, result_row):
        self.result_row = result_row
        self.calls: list[tuple[str, list[str]]] = []

    def execute(self, statement: str, params: list[str]) -> _Result:
        self.calls.append((statement, params))
        return _Result(self.result_row)


class _Ledger:
    def __init__(self, cursor: _Cursor):
        self.cursor = cursor

    @contextmanager
    def exec_cursor(self):
        yield self.cursor


def test_ingest_cache_hit_quotes_table_name() -> None:
    tracker = Tracker.__new__(Tracker)
    cursor = _Cursor(result_row=(1,))
    tracker.db = SimpleNamespace(ledger=_Ledger(cursor))

    result = tracker._ingest_cache_hit("safe_table", "abc123")

    assert result is True
    assert len(cursor.calls) == 1
    statement, params = cursor.calls[0]
    assert statement.startswith('SELECT 1 FROM "global_tables"."safe_table"')
    assert params == ["abc123"]


def test_ingest_cache_hit_rejects_unsafe_table_name_before_sql() -> None:
    tracker = Tracker.__new__(Tracker)
    cursor = _Cursor(result_row=(1,))
    tracker.db = SimpleNamespace(ledger=_Ledger(cursor))

    result = tracker._ingest_cache_hit("safe_table;DROP TABLE run;--", "abc123")

    assert result is False
    assert cursor.calls == []
