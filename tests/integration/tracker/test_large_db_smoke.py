"""CI-friendly large-DB smoke test for Consist query and write paths."""

from __future__ import annotations

import pytest
from sqlmodel import Session, func, select

from tests.large_db_helpers import (
    _count_artifacts_keyset,
    _lineage_run_depth,
    _seed_base_dataset,
    _seed_deep_lineage,
    _seed_diff_runs,
    _seed_wide_lineage,
)
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


@pytest.mark.timeout(30)
def test_large_db_smoke(tmp_path):
    total_runs = 2_000
    runs_per_scenario = 50
    lineage_depth = 10
    wide_inputs = 10
    config_keys = 200
    chunk_size = 500
    write_runs = 5

    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = tmp_path / "smoke_scale.duckdb"

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))

    with Session(tracker.engine) as session:
        sample = _seed_base_dataset(
            session,
            total_runs=total_runs,
            runs_per_scenario=runs_per_scenario,
            chunk_size=chunk_size,
        )
        deep_artifact_id = _seed_deep_lineage(session, depth=lineage_depth)
        wide_artifact_id = _seed_wide_lineage(session, input_count=wide_inputs)
        diff_run_a, diff_run_b, expected_changes = _seed_diff_runs(
            session, num_keys=config_keys
        )

    scale_sizes = sample["scale_sizes"]
    scale_100k = scale_sizes["scale_100k"]
    cache_tuple = sample["cache_tuple"]
    scenario_id = sample["scenario_id"]
    lookup_key = sample["lookup_key"]
    lookup_hash = sample["lookup_hash"]

    runs = tracker.find_runs(tags=["scale_100k"], limit=scale_100k)
    assert len(runs) == scale_100k

    scenario_runs = tracker.find_runs(
        parent_id=scenario_id, limit=min(runs_per_scenario, total_runs)
    )
    assert scenario_runs

    meta_runs = tracker.find_runs(metadata={"region": "west"}, limit=scale_100k)
    assert meta_runs

    cached = tracker.find_matching_run(*cache_tuple)
    assert cached is not None

    deep_lineage = tracker.get_artifact_lineage(deep_artifact_id)
    assert deep_lineage is not None
    assert _lineage_run_depth(deep_lineage) >= lineage_depth

    wide_lineage = tracker.get_artifact_lineage(wide_artifact_id)
    assert wide_lineage is not None
    producing = wide_lineage.get("producing_run", {})
    inputs = producing.get("inputs") if isinstance(producing, dict) else []
    assert isinstance(inputs, list)
    assert len(inputs) == wide_inputs

    diff = tracker.diff_runs(diff_run_a, diff_run_b)
    changes = diff.get("changes") if isinstance(diff, dict) else None
    assert isinstance(changes, dict)
    assert len(changes) == expected_changes

    by_key = tracker.get_artifact(lookup_key)
    assert by_key is not None

    expected_artifacts = total_runs + lineage_depth + wide_inputs + 1

    with Session(tracker.engine) as session:
        by_hash = session.exec(
            select(Artifact).where(Artifact.hash == lookup_hash)
        ).first()
        assert by_hash is not None
        assert by_hash.id == by_key.id

        paginated_count = _count_artifacts_keyset(session, batch_size=250)
        assert paginated_count == expected_artifacts

        initial_run_count = session.exec(select(func.count()).select_from(Run)).one()

    input_path = run_dir / "write_input.txt"
    input_path.write_text("seed", encoding="utf-8")

    for i in range(write_runs):
        run_id = f"write_run_{i:03d}"
        output_path = run_dir / f"write_output_{i:03d}.txt"
        with tracker.start_run(
            run_id=run_id,
            model="write_phase",
            tags=["write_phase"],
            config={"seed": i},
            inputs=[input_path],
            parent_run_id="write_scenario",
            iteration=i,
        ):
            output_path.write_text(f"payload {i}", encoding="utf-8")
            tracker.log_artifact(
                output_path,
                key=f"write_output_{i:03d}",
            )

    with Session(tracker.engine) as session:
        final_run_count = session.exec(select(func.count()).select_from(Run)).one()

    assert final_run_count - initial_run_count == write_runs
