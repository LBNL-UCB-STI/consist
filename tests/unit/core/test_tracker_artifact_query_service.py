from __future__ import annotations

from pathlib import Path
import uuid

import pytest

from consist.models.artifact import Artifact, get_tracker_ref
from consist.models.run import Run


def test_artifact_query_service_prefers_current_run_artifacts_by_uri(
    monkeypatch: pytest.MonkeyPatch,
    tracker,
) -> None:
    service = tracker._artifact_queries
    input_path = tracker.run_dir / "query_input.csv"
    input_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run("query_service_run", "demo"):
        artifact = tracker.log_artifact(input_path, key="input", direction="input")

        if tracker.db is not None:
            monkeypatch.setattr(
                tracker.db,
                "get_artifact_by_uri",
                lambda *args, **kwargs: pytest.fail(
                    "DB lookup should not run when the artifact is present in "
                    "current_consist."
                ),
            )

        found = service.get_artifact_by_uri(artifact.container_uri)

    assert found is artifact


def test_artifact_query_service_normalizes_run_filters_and_attaches_tracker(
    monkeypatch: pytest.MonkeyPatch,
    tracker,
) -> None:
    service = tracker._artifact_queries
    producer = Run(id="producer-run", model_name="demo", config_hash=None, git_hash=None)
    consumer = Run(id="consumer-run", model_name="demo", config_hash=None, git_hash=None)
    artifact = Artifact(
        id=uuid.uuid4(),
        key="result",
        container_uri="outputs://result.csv",
        driver="csv",
    )
    captured: dict[str, object] = {}

    if tracker.db is None:
        pytest.skip("Tracker fixture should provide a DB-backed tracker.")

    def fake_find_artifacts(*, creator, consumer, key, limit):
        captured.update(
            creator=creator,
            consumer=consumer,
            key=key,
            limit=limit,
        )
        return [artifact]

    monkeypatch.setattr(tracker.db, "find_artifacts", fake_find_artifacts)

    results = service.find_artifacts(
        creator=producer,
        consumer=consumer,
        key="result",
        limit=7,
    )

    assert results == [artifact]
    assert captured == {
        "creator": producer.id,
        "consumer": consumer.id,
        "key": "result",
        "limit": 7,
    }
    tracker_ref = get_tracker_ref(artifact)
    assert tracker_ref is not None
    assert tracker_ref() is tracker
