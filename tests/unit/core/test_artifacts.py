from pathlib import Path
from unittest.mock import MagicMock

import pytest

from consist.core.artifacts import ArtifactManager
from consist.models.artifact import Artifact
from consist.models.artifact import ArtifactContent


@pytest.fixture
def mock_tracker():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "mock_hash"
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = ArtifactContent(
        content_hash="mock_hash", driver="csv"
    )
    return tracker


def test_create_artifact_stateless(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(path="/tmp/test.csv", key="test", run_id="run_ABC")

    assert art.run_id == "run_ABC"
    assert art.key == "test"
    assert art.hash == "mock_hash"


def test_create_artifact_uses_precomputed_hash(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(
        path="/tmp/test.csv",
        key="test",
        run_id="run_ABC",
        content_hash="precomputed_hash",
    )

    assert art.hash == "precomputed_hash"
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_dir_uses_precomputed_hash(mock_tracker, tmp_path):
    manager = ArtifactManager(mock_tracker)
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()

    art = manager.create_artifact(
        path=str(data_dir),
        key="dir_artifact",
        run_id="run_ABC",
        content_hash="precomputed_hash",
    )

    assert art.hash == "precomputed_hash"
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_rejects_mismatched_validated_hash(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    with pytest.raises(ValueError, match="content_hash does not match"):
        manager.create_artifact(
            path="/tmp/test.csv",
            key="test",
            run_id="run_ABC",
            content_hash="different_hash",
            validate_content_hash=True,
        )

    mock_tracker.identity.compute_file_checksum.assert_called()


def test_create_artifact_hashes_once_for_reuse_and_validation(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.compute_file_checksum.return_value = "mock_hash"
    tracker.db.find_latest_artifact_at_uri.return_value = None
    tracker.db.find_latest_artifact_by_hash.return_value = None
    tracker.db.get_or_create_artifact_content.return_value = ArtifactContent(
        content_hash="mock_hash", driver="csv"
    )

    manager = ArtifactManager(tracker)
    output_path = tmp_path / "output.csv"
    output_path.write_text("a,b\n1,2\n", encoding="utf-8")

    art = manager.create_artifact(
        path=output_path,
        key="output",
        run_id="run_ABC",
        direction="output",
        reuse_if_unchanged=True,
        validate_content_hash=True,
    )

    assert art.hash == "mock_hash"
    assert tracker.identity.compute_file_checksum.call_count == 1


def test_create_artifacts_share_content_id_when_hashes_match(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {"outputs": str(tmp_path)}
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
    content_row = ArtifactContent(content_hash="shared_hash", driver="parquet")
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = content_row

    manager = ArtifactManager(tracker)

    path_a = tmp_path / "a.parquet"
    path_a.write_text("a", encoding="utf-8")
    path_b = tmp_path / "b.parquet"
    path_b.write_text("b", encoding="utf-8")

    art_a = manager.create_artifact(
        path=path_a,
        key="shared",
        run_id="run_a",
        content_hash="shared_hash",
        driver="parquet",
    )
    art_b = manager.create_artifact(
        path=path_b,
        key="shared",
        run_id="run_b",
        content_hash="shared_hash",
        driver="parquet",
    )

    assert art_a.id != art_b.id
    assert art_a.run_id == "run_a"
    assert art_b.run_id == "run_b"
    assert art_a.content_id == art_b.content_id == content_row.id


def test_attach_content_id_uses_artifact_driver_when_arg_missing():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "hash_from_obj"
    content_row = ArtifactContent(content_hash="hash_from_obj", driver="parquet")
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = content_row

    manager = ArtifactManager(tracker)

    existing = Artifact(
        key="k",
        container_uri="inputs://a.parquet",
        driver="parquet",
        hash="hash_from_obj",
        run_id="run_x",
    )

    out = manager.create_artifact(path=existing, run_id="run_x")

    assert out.content_id is not None
    tracker.db.get_or_create_artifact_content.assert_called_with(
        content_hash="hash_from_obj", driver="parquet"
    )
