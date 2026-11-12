from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from consist.core.ingestion import ingest_artifact
from consist.core.persistence import DatabaseManager
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
    db,
):
    return SimpleNamespace(
        hot_data_store=hot_data_store,
        db_path=db_path,
        current_consist=None,
        db=db,
        _dlt_lock_retries=1,
        _dlt_lock_base_sleep_seconds=0.01,
        _dlt_lock_max_sleep_seconds=0.02,
        artifact_schemas=SimpleNamespace(profile_ingested_table=lambda **_kwargs: None),
        resolve_uri=lambda uri: uri,
    )


def test_ingest_artifact_prefers_hot_data_store_path_and_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    hot_db_path = tmp_path / "hot-data.duckdb"
    with DatabaseManager(str(hot_db_path)) as hot_db:
        hot_data_store = SimpleNamespace(
            db_path=str(hot_db_path),
            db=hot_db,
        )

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
            db=None,
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
        assert captured["db_path"] == str(hot_db_path)
        assert captured["db"] is hot_db


def test_ingest_artifact_falls_back_to_tracker_db_path_and_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fallback_path = tmp_path / "fallback.duckdb"
    with DatabaseManager(str(fallback_path)) as fallback_db:

        def _fake_ingest_artifact(**kwargs):
            captured.update(kwargs)
            return {"status": "ok"}, "table"

        monkeypatch.setattr(
            "consist.integrations.dlt_loader.ingest_artifact",
            _fake_ingest_artifact,
        )

        tracker = _tracker(
            hot_data_store=None,
            db_path=str(fallback_path),
            db=fallback_db,
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
        assert captured["db_path"] == str(fallback_path)
        assert captured["db"] is fallback_db


def test_ingest_artifact_requires_hot_data_db_path() -> None:
    tracker = _tracker(hot_data_store=None, db_path=None, db=None)

    with pytest.raises(RuntimeError, match="db_path is not configured"):
        ingest_artifact(
            tracker=tracker,
            artifact=_artifact(),
            data=[{"value": 1}],
            schema=None,
            run=_run(),
            profile_schema=False,
        )
