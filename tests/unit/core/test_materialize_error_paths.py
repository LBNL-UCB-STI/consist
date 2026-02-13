from pathlib import Path
from types import SimpleNamespace

import pytest

from consist.core.materialize import (
    materialize_artifacts,
    materialize_artifacts_from_sources,
    materialize_ingested_artifact_from_db,
)
from consist.models.artifact import Artifact


def _artifact(*, key: str, driver: str = "csv", is_ingested: bool = False) -> Artifact:
    return Artifact(
        key=key,
        container_uri=f"./{key}.{driver}",
        driver=driver,
        meta={"is_ingested": is_ingested},
    )


def test_materialize_artifacts_blocks_symlink_destination_when_resolution_is_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("x\n1\n", encoding="utf-8")

    target = tmp_path / "target.csv"
    target.write_text("x\n2\n", encoding="utf-8")
    destination = tmp_path / "dest.csv"
    destination.symlink_to(target)

    # Keep paths un-resolved so the symlink guard sees the link path directly.
    monkeypatch.setattr(Path, "resolve", lambda self: self)

    tracker = SimpleNamespace(resolve_uri=lambda _uri: str(source))
    artifact = _artifact(key="symlinked")

    with pytest.raises(ValueError, match="Symlink detected in destination path"):
        materialize_artifacts(
            tracker=tracker, items=[(artifact, destination)], on_missing="raise"
        )


def test_materialize_artifacts_refuses_overwrite_on_destination_type_mismatch(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    destination = tmp_path / "destination"
    destination.mkdir()

    tracker = SimpleNamespace(resolve_uri=lambda _uri: str(source))
    artifact = _artifact(key="type_mismatch")

    with pytest.raises(RuntimeError, match="Destination type mismatch"):
        materialize_artifacts(
            tracker=tracker, items=[(artifact, destination)], on_missing="raise"
        )


def test_materialize_artifacts_from_sources_raises_for_allowed_base_violation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    allowed_base = tmp_path / "sandbox"
    allowed_base.mkdir()
    outside_destination = tmp_path / "outside.csv"
    artifact = _artifact(key="outside")

    with pytest.raises(ValueError, match="outside allowed base"):
        materialize_artifacts_from_sources(
            items=[(artifact, source, outside_destination)],
            allowed_base=allowed_base,
            on_missing="raise",
        )


def test_materialize_ingested_artifact_from_db_requires_tracker_engine(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", is_ingested=True)
    tracker = SimpleNamespace(engine=None)

    with pytest.raises(RuntimeError, match="tracker has no DB engine"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_rejects_unsupported_driver(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_json", driver="json", is_ingested=True)
    tracker = SimpleNamespace(engine=object())

    with pytest.raises(ValueError, match="Only csv/parquet artifacts"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.json",
        )
