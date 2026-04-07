from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from consist.core.tracker import Tracker
from consist.core.tracker_config import TrackerConfig
from consist.models.run import Run


def test_tracker_from_config_matches_direct_constructor(tmp_path: Path) -> None:
    mounts_root = tmp_path / "inputs"
    mounts_root.mkdir(parents=True, exist_ok=True)
    db_path = str(tmp_path / "provenance.duckdb")

    def run_subdir(run: Run) -> str:
        return f"custom/{run.id}"

    config = TrackerConfig(
        run_dir=tmp_path / "runs",
        db_path=db_path,
        mounts={"inputs": str(mounts_root)},
        project_root=str(tmp_path),
        hashing_strategy="fast",
        cache_epoch=7,
        access_mode="analysis",
        run_subdir_fn=run_subdir,
        allow_external_paths=True,
        openlineage_enabled=False,
        openlineage_namespace="consist-tests",
    )

    from_config_tracker = Tracker.from_config(config)
    direct_tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=db_path,
        mounts={"inputs": str(mounts_root)},
        project_root=str(tmp_path),
        hashing_strategy="fast",
        cache_epoch=7,
        access_mode="analysis",
        run_subdir_fn=run_subdir,
        allow_external_paths=True,
        openlineage_enabled=False,
        openlineage_namespace="consist-tests",
    )

    sample_run = Run(
        id="run-123",
        model_name="demo",
        config_hash=None,
        git_hash=None,
    )

    assert from_config_tracker.run_dir == direct_tracker.run_dir
    assert from_config_tracker.db_path == direct_tracker.db_path
    assert from_config_tracker.mounts == direct_tracker.mounts
    assert from_config_tracker.access_mode == direct_tracker.access_mode
    assert from_config_tracker._cache_epoch == direct_tracker._cache_epoch
    assert (
        from_config_tracker.allow_external_paths == direct_tracker.allow_external_paths
    )
    assert from_config_tracker._run_subdir_fn is run_subdir
    assert direct_tracker._run_subdir_fn is run_subdir
    assert (
        from_config_tracker.identity.project_root
        == direct_tracker.identity.project_root
    )
    assert (
        from_config_tracker.identity.hashing_strategy
        == direct_tracker.identity.hashing_strategy
    )
    assert from_config_tracker.run_artifact_dir(
        sample_run
    ) == direct_tracker.run_artifact_dir(sample_run)


def test_tracker_config_validation_for_required_and_typed_fields(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError):
        TrackerConfig.model_validate({})

    with pytest.raises(ValidationError):
        TrackerConfig(
            run_dir=tmp_path,
            access_mode="invalid",
        )
