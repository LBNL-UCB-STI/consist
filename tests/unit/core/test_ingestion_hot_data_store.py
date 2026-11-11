from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from consist.artifact import Artifact
from consist.core.ingestion import ingest_artifact
from consist.models.run import Run
from consist.store.ledger import StoreLedger
from tests.helpers.artifact_factory import make_artifact


def _run() -> Run:
    return Run(id="run_1", model_name="model", config_hash=None, git_hash=None)


def _artifact() -> Artifact:
    return make_artifact(
        key="table",
        container_uri="inputs://table.csv",
        driver="csv",
        hash="a" * 64,
    )


def _tracker(
    *,
    hot_data_store,
    db_path,
    ledger,
):
    return SimpleNamespace(
        hot_data_store=hot_data_store,
        db_path=db_path,
        ledger=ledger,
        site_context=None,
        current_consist=None,
        last_run=None,
        db=None,
        _dlt_lock_retries=1,
        _dlt_lock_base_sleep_seconds=0.01,
        _dlt_lock_max_sleep_seconds=0.02,
        artifact_schemas=SimpleNamespace(profile_ingested_table=lambda **_kwargs: None),
        resolve_uri=lambda uri: uri,
    )


def test_ingest_artifact_prefers_hot_data_store_path_and_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    hot_db = tmp_path / "hot-data.duckdb"
    with StoreLedger(hot_db) as hot_ledger:
        hot_data_store = SimpleNamespace(
            db_path=str(hot_db),
            ledger=hot_ledger,
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
            ledger=None,
        )
        result = ingest_artifact(
            tracker=tracker,
            artifact=_artifact(),
            data=[{"value": 1}],
            schema=None,
            run=_run(),
            profile_schema=False,
        )

        assert isinstance(result, Artifact)
        assert result.is_ingested is True
        assert captured["db_path"] == str(hot_db)
        assert captured["ledger"] is hot_ledger


def test_ingest_artifact_falls_back_to_tracker_db_path_and_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fallback_db = tmp_path / "fallback.duckdb"
    with StoreLedger(fallback_db) as ledger:

        def _fake_ingest_artifact(**kwargs):
            captured.update(kwargs)
            return {"status": "ok"}, "table"

        monkeypatch.setattr(
            "consist.integrations.dlt_loader.ingest_artifact",
            _fake_ingest_artifact,
        )

        tracker = _tracker(
            hot_data_store=None,
            db_path=str(fallback_db),
            ledger=ledger,
        )
        result = ingest_artifact(
            tracker=tracker,
            artifact=_artifact(),
            data=[{"value": 1}],
            schema=None,
            run=_run(),
            profile_schema=False,
        )

        assert isinstance(result, Artifact)
        assert result.is_ingested is True
        assert captured["db_path"] == str(fallback_db)
        assert captured["ledger"] is ledger


def test_ingest_artifact_requires_hot_data_db_path() -> None:
    tracker = _tracker(hot_data_store=None, db_path=None, ledger=None)

    with pytest.raises(RuntimeError, match="db_path is not configured"):
        ingest_artifact(
            tracker=tracker,
            artifact=_artifact(),
            data=[{"value": 1}],
            schema=None,
            run=_run(),
            profile_schema=False,
        )
