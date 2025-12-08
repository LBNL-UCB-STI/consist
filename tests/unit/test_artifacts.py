import pytest
from unittest.mock import MagicMock
from pathlib import Path
from consist.core.artifacts import ArtifactManager
from consist.models.artifact import Artifact


@pytest.fixture
def mock_tracker():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity._compute_file_checksum.return_value = "mock_hash"
    return tracker


def test_create_artifact_stateless(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(
        path="/tmp/test.csv",
        key="test",
        run_id="run_ABC"
    )

    assert art.run_id == "run_ABC"
    assert art.key == "test"
    assert art.hash == "mock_hash"