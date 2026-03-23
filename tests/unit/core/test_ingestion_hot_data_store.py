from __future__ import annotations

from types import SimpleNamespace

import pytest

from consist.core.ingestion import ingest_artifact
from consist.models.artifact import Artifact
from consist.models.run import Run


def _run() -> Run:
    return Run(id="run_1", model_name="model", config_hash=None, git_hash=None)


def _artifact() -> Artifact:
    return Artifact(
        key="table",
        container_uri="inputs://table.csv",
        driver="csv",
    )


def _tracker(
    *,
    hot_data_store,
    db_path,
    engine,
):
    return SimpleNamespace(
        hot_data_store=hot_data_store,
        db_path=db_path,
        engine=engine,
        current_consist=None,
        db=None,
        _dlt_lock_retries=1,
        _dlt_lock_base_sleep_seconds=0.01,
        _dlt_lock_max_sleep_seconds=0.02,
        artifact_schemas=SimpleNamespace(profile_ingested_table=lambda **_kwargs: None),
        resolve_uri=lambda uri: uri,
    )


def test_ingest_artifact_prefers_hot_data_store_path_and_dispose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _HotDataStore:
        db_path = "/tmp/hot-data.duckdb"
        engine = object()

        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose_engine(self) -> None:
            self.dispose_calls += 1

    hot_data_store = _HotDataStore()

    def _fake_ingest_artifact(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}, "table"

    monkeypatch.setattr(
        "consist.integrations.dlt_loader.ingest_artifact",
        _fake_ingest_artifact,
    )

    tracker = _tracker(
        hot_data_store=hot_data_store,
        db_path=None,
        engine=None,
    )
    result = ingest_artifact(
        tracker=tracker,
        artifact=_artifact(),
        data=[{"value": 1}],
        schema=None,
        run=_run(),
        profile_schema=False,
    )

    assert result == {"status": "ok"}
    assert captured["db_path"] == "/tmp/hot-data.duckdb"
    assert hot_data_store.dispose_calls == 1


def test_ingest_artifact_falls_back_to_tracker_db_path_and_engine_dispose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _Engine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def dispose(self) -> None:
            self.dispose_calls += 1

    engine = _Engine()

    def _fake_ingest_artifact(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}, "table"

    monkeypatch.setattr(
        "consist.integrations.dlt_loader.ingest_artifact",
        _fake_ingest_artifact,
    )

    tracker = _tracker(
        hot_data_store=None,
        db_path="/tmp/fallback.duckdb",
        engine=engine,
    )
    ingest_artifact(
        tracker=tracker,
        artifact=_artifact(),
        data=[{"value": 1}],
        schema=None,
        run=_run(),
        profile_schema=False,
    )

    assert captured["db_path"] == "/tmp/fallback.duckdb"
    assert engine.dispose_calls == 1


def test_ingest_artifact_requires_hot_data_db_path() -> None:
    tracker = _tracker(hot_data_store=None, db_path=None, engine=None)

    with pytest.raises(RuntimeError, match="db_path is not configured"):
        ingest_artifact(
            tracker=tracker,
            artifact=_artifact(),
            data=[{"value": 1}],
            schema=None,
            run=_run(),
            profile_schema=False,
        )
