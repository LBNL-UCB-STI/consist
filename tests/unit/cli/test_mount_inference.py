from __future__ import annotations

from pathlib import Path

from consist import Tracker
from consist.cli import _ensure_tracker_mounts_for_artifact


def test_cli_infers_mounts_from_run_metadata(tmp_path: Path) -> None:
    inputs_root = tmp_path / "inputs_root"
    inputs_root.mkdir(parents=True)
    data_path = inputs_root / "data.csv"
    data_path.write_text("a,b\n1,2\n", encoding="utf-8")

    db_path = tmp_path / "provenance.duckdb"

    tracker = Tracker(
        run_dir=tmp_path / "producer",
        db_path=str(db_path),
        mounts={"inputs": str(inputs_root)},
    )
    with tracker.start_run(run_id="producer", model="test"):
        logged = tracker.log_artifact(data_path, key="data", direction="input")

    inspector = Tracker(run_dir=tmp_path / "inspector", db_path=str(db_path))
    artifact = inspector.get_artifact(logged.id)
    assert artifact is not None

    _ensure_tracker_mounts_for_artifact(inspector, artifact, trust_db=True)

    assert inspector.mounts["inputs"] == str(inputs_root.resolve())
    assert inspector.resolve_uri(artifact.container_uri) == str(data_path.resolve())
