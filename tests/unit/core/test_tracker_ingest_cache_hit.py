from __future__ import annotations

from typing import Any

from consist.core.tracker import Tracker


class _DbStub:
    def __init__(self, result_row: Any) -> None:
        self.result_row = result_row
        self.calls: list[tuple[str, Any]] = []
        self.column_checks: list[tuple[str, str, str]] = []

    def has_table_column(
        self,
        *,
        table_name: str,
        column_name: str,
        table_schema: str = "main",
    ) -> bool:
        self.column_checks.append((table_schema, table_name, column_name))
        return True

    def fetchone(self, statement: str, params: Any = None) -> Any:
        self.calls.append((statement, params))
        return self.result_row


def test_ingest_cache_hit_quotes_table_name() -> None:
    tracker = Tracker.__new__(Tracker)
    db = _DbStub(result_row=(1,))
    tracker.metadata_store = None
    tracker.hot_data_store = None
    tracker.db = db

    result = tracker._ingest_cache_hit("safe_table", "abc123")

    assert result is True
    assert db.column_checks == [("global_tables", "safe_table", "content_hash")]
    assert len(db.calls) == 1
    statement, params = db.calls[0]
    assert statement.startswith('SELECT 1 FROM "global_tables"."safe_table"')
    assert params == ["abc123"]


def test_ingest_cache_hit_rejects_unsafe_table_name_before_sql() -> None:
    tracker = Tracker.__new__(Tracker)
    db = _DbStub(result_row=(1,))
    tracker.metadata_store = None
    tracker.hot_data_store = None
    tracker.db = db

    result = tracker._ingest_cache_hit("safe_table;DROP TABLE run;--", "abc123")

    assert result is False
    assert db.column_checks == []
    assert db.calls == []
