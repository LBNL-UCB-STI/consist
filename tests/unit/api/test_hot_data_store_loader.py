from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

from consist.api import _load_from_db, load
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact


def test_load_from_db_uses_hot_data_store_db_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class _Relation:
        def limit(self, nrows: int):
            captured["nrows"] = nrows
            return self

    class _Connection:
        def sql(self, query: str):
            captured["query"] = query
            return _Relation()

    def _fake_connect(path: str, read_only: bool = False):
        captured["db_path"] = path
        captured["read_only"] = read_only
        return _Connection()

    monkeypatch.setattr("consist.api.duckdb.connect", _fake_connect)

    hot_db_path = tmp_path / "hot.duckdb"
    artifact = Artifact(
        id=uuid4(),
        key="fallback_key",
        container_uri=str(tmp_path / "missing.csv"),
        driver="csv",
        meta={"is_ingested": True, "dlt_table_name": "My Table"},
    )
    tracker = cast(
        Tracker,
        SimpleNamespace(
            db_path=None,
            hot_data_store=SimpleNamespace(db_path=hot_db_path),
        ),
    )

    _load_from_db(artifact, tracker, nrows=5)

    assert captured["db_path"] == str(hot_db_path)
    assert captured["read_only"] is True
    assert captured["nrows"] == 5
    assert 'FROM global_tables."My Table"' in str(captured["query"])


def test_load_allows_hot_data_store_without_tracker_engine(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sentinel = object()
    resolved_path = str(tmp_path / "missing.csv")
    artifact = Artifact(
        id=uuid4(),
        key="fallback_key",
        container_uri=resolved_path,
        driver="csv",
        meta={"is_ingested": True, "dlt_table_name": "My Table"},
    )
    tracker = cast(
        Tracker,
        SimpleNamespace(
            hot_data_store=SimpleNamespace(
                db_path=str(tmp_path / "hot.duckdb"),
            ),
            engine=None,
            db_path=None,
            current_consist=None,
            resolve_uri=lambda _uri: resolved_path,
        ),
    )

    monkeypatch.setattr("consist.api._load_from_db", lambda *_args, **_kwargs: sentinel)

    result = load(artifact, tracker=tracker, db_fallback="always")

    assert result is sentinel


def test_load_falls_back_to_tracker_db_path_when_hot_store_absent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class _Relation:
        def limit(self, nrows: int):
            captured["nrows"] = nrows
            return self

    class _Connection:
        def sql(self, query: str):
            captured["query"] = query
            return _Relation()

    def _fake_connect(path: str, read_only: bool = False):
        captured["db_path"] = path
        captured["read_only"] = read_only
        return _Connection()

    monkeypatch.setattr("consist.api.duckdb.connect", _fake_connect)

    db_path = str(tmp_path / "legacy-fallback.duckdb")
    artifact = Artifact(
        id=uuid4(),
        key="legacy_key",
        container_uri=str(tmp_path / "missing.csv"),
        driver="csv",
        meta={"is_ingested": True, "dlt_table_name": "legacy_table"},
    )
    tracker = cast(
        Tracker,
        SimpleNamespace(
            db_path=db_path,
            hot_data_store=None,
            current_consist=None,
            resolve_uri=lambda _uri: str(tmp_path / "missing.csv"),
        ),
    )

    load(artifact, tracker=tracker, db_fallback="always", nrows=7)

    assert captured["db_path"] == db_path
    assert captured["read_only"] is True
    assert captured["nrows"] == 7
    assert "legacy_table" in str(captured["query"])
