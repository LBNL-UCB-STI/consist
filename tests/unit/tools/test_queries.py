from __future__ import annotations

from sqlmodel import Session
import pytest

from consist.tools import queries


def test_get_summary_empty_db(tracker) -> None:
    """
    Empty DB summary should return zero counts and empty model distribution.
    """
    with Session(tracker.engine) as session:
        summary = queries.get_summary(session)

    assert summary["total_runs"] == 0
    assert summary["completed_runs"] == 0
    assert summary["failed_runs"] == 0
    assert summary["total_artifacts"] == 0
    assert summary["models_distribution"] == []


def test_get_artifact_preview_missing_artifact(tracker, monkeypatch) -> None:
    """
    Missing artifacts should return None.
    """
    monkeypatch.setattr(tracker, "get_artifact", lambda *_args, **_kwargs: None)
    assert queries.get_artifact_preview(tracker, "missing") is None


def test_get_artifact_preview_file_not_found(tracker, monkeypatch, tmp_path) -> None:
    """
    File-not-found errors should propagate.
    """
    df_path = tmp_path / "data.csv"
    df_path.write_text("a,b\n1,2\n", encoding="utf-8")

    with tracker.start_run("run", model="model"):
        art = tracker.log_artifact(df_path, key="data", direction="output")

    monkeypatch.setattr(tracker, "get_artifact", lambda *_args, **_kwargs: art)

    def _raise(_artifact, **_kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr("consist.load", _raise)

    with pytest.raises(FileNotFoundError):
        queries.get_artifact_preview(tracker, "data")


def test_get_artifact_preview_non_dataframe_returns_none(
    tracker, monkeypatch, tmp_path
) -> None:
    """
    Non-DataFrame preview data should return None.
    """
    df_path = tmp_path / "data.csv"
    df_path.write_text("a,b\n1,2\n", encoding="utf-8")

    with tracker.start_run("run", model="model"):
        art = tracker.log_artifact(df_path, key="data", direction="output")

    monkeypatch.setattr(tracker, "get_artifact", lambda *_args, **_kwargs: art)
    monkeypatch.setattr("consist.load", lambda *_args, **_kwargs: {"not": "df"})

    assert queries.get_artifact_preview(tracker, "data") is None


def test_get_runs_filters_invalid_tags_and_status(tracker) -> None:
    """
    Invalid tag/status combinations should return empty results.
    """
    with tracker.start_run("run_ok", model="model", tags=["alpha", "shared"]):
        pass

    try:
        with tracker.start_run("run_failed", model="model", tags=["beta"]):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    with Session(tracker.engine) as session:
        matches = queries.get_runs(
            session, tags=["alpha", "shared"], status="completed"
        )
        assert [run.id for run in matches] == ["run_ok"]
        assert queries.get_runs(session, tags=["missing"], status="completed") == []
        assert queries.get_runs(session, tags=["alpha"], status="failed") == []
        assert queries.get_runs(session, status="bogus") == []
