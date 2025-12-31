"""
Large-DB stress tests for Consist.

These tests intentionally create a large number of runs/artifacts to exercise
query performance at scale. They are marked as `heavy` and are not intended for CI.

Environment overrides:
- CONSIST_STRESS_TOTAL_RUNS (default: 100000)
- CONSIST_STRESS_RUNS_PER_SCENARIO (default: 100)
- CONSIST_STRESS_LINEAGE_DEPTH (default: 25)
- CONSIST_STRESS_WIDE_INPUTS (default: 50)
- CONSIST_STRESS_CONFIG_KEYS (default: 2000)
- CONSIST_STRESS_CHUNK_SIZE (default: 5000)
- CONSIST_STRESS_WRITE_RUNS (default: 200)
- CONSIST_STRESS_PROFILE_PATH (default: temp file in pytest tmp_path)
"""

from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict

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

METRIC_DESCRIPTIONS = {
    "seed.base_dataset": "Populate the base run/artifact tables for scale testing.",
    "seed.lineage.deep": "Create a deep, single-input lineage chain.",
    "seed.lineage.wide": "Create a run with many input artifacts (wide lineage).",
    "seed.config.diff_runs": "Insert run_config_kv rows used for diff_runs.",
    "find_runs.scale_10k": "find_runs(tag=scale_10k) over ~10k runs.",
    "find_runs.scale_50k": "find_runs(tag=scale_50k) over ~50k runs.",
    "find_runs.scale_100k": "find_runs(tag=scale_100k) over ~100k runs.",
    "find_runs.tags+model+status": "find_runs with tags + model + status filters.",
    "find_runs.parent_id": "find_runs filtering by parent_run_id (scenario).",
    "find_runs.metadata": "find_runs with JSON metadata filter (client-side).",
    "cache.find_matching_run": "cache lookup using config/input/git hashes.",
    "lineage.deep": "get_artifact_lineage on deep dependency chain.",
    "lineage.wide": "get_artifact_lineage on wide dependency graph.",
    "diff_runs.config_kv": "diff_runs across run_config_kv facets.",
    "artifact.lookup.key": "get_artifact by key (most recent).",
    "artifact.lookup.hash": "direct DB lookup by artifact hash.",
    "scenarios.group_by_parent_run": "CLI scenario query grouped by parent_run_id.",
    "validate.select_all_artifacts": "select all artifacts (validate command baseline).",
    "validate.paginated": "select all artifacts using keyset pagination.",
    "db.size_mb": "DuckDB file size in megabytes.",
    "write.append_runs": "Append new runs via Tracker.start_run/log_artifact.",
    "write.run.total.avg": "Per-run total time (avg) for the write phase.",
    "write.run.total.p50": "Per-run total time (p50) for the write phase.",
    "write.run.total.p95": "Per-run total time (p95) for the write phase.",
    "write.run.begin_run.avg": "Per-run begin_run time (avg) during write phase.",
    "write.run.begin_run.p50": "Per-run begin_run time (p50) during write phase.",
    "write.run.begin_run.p95": "Per-run begin_run time (p95) during write phase.",
    "write.run.log_artifact.avg": "Per-run log_artifact time (avg) during write phase.",
    "write.run.log_artifact.p50": "Per-run log_artifact time (p50) during write phase.",
    "write.run.log_artifact.p95": "Per-run log_artifact time (p95) during write phase.",
    "write.run.end_run.avg": "Per-run end_run time (avg) during write phase.",
    "write.run.end_run.p50": "Per-run end_run time (p50) during write phase.",
    "write.run.end_run.p95": "Per-run end_run time (p95) during write phase.",
    "write.run.file_write.avg": "Per-run output file write time (avg) during write phase.",
    "write.run.file_write.p50": "Per-run output file write time (p50) during write phase.",
    "write.run.file_write.p95": "Per-run output file write time (p95) during write phase.",
}

PARAM_DESCRIPTIONS = {
    "total_runs": "Total runs seeded into the base dataset.",
    "runs_per_scenario": "Runs per scenario (parent_run_id).",
    "lineage_depth": "Number of chained runs in the deep lineage test.",
    "wide_inputs": "Number of input artifacts in the wide lineage test.",
    "config_keys": "Number of config keys per run for diff_runs.",
    "chunk_size": "Batch size for bulk inserts into DuckDB.",
    "write_runs": "Number of new runs appended via Tracker APIs.",
}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value


@contextmanager
def _timed(label: str, metrics: Dict[str, float]):
    start = time.perf_counter()
    yield
    metrics[label] = time.perf_counter() - start


def _emit_stdout(message: str) -> None:
    stream = getattr(sys, "__stdout__", None)
    if stream is None:
        print(message)
        return
    stream.write(message + "\n")
    stream.flush()


def _summarize_samples(samples: list[float]) -> Dict[str, float]:
    if not samples:
        return {}
    data = sorted(samples)
    n = len(data)
    avg = sum(data) / n

    def pick(q: float) -> float:
        return data[int((n - 1) * q)]

    return {"avg": avg, "p50": pick(0.5), "p95": pick(0.95)}


@pytest.mark.heavy
def test_large_db_scale_queries(tmp_path):
    """
    Builds a large provenance DB and runs representative queries to surface scale issues.
    """
    total_runs = max(1, _env_int("CONSIST_STRESS_TOTAL_RUNS", 100_000))
    runs_per_scenario = max(
        1, _env_int("CONSIST_STRESS_RUNS_PER_SCENARIO", 100)
    )
    lineage_depth = max(
        1, _env_int("CONSIST_STRESS_LINEAGE_DEPTH", 25)
    )
    wide_inputs = max(
        1, _env_int("CONSIST_STRESS_WIDE_INPUTS", 50)
    )
    config_keys = max(
        1, _env_int("CONSIST_STRESS_CONFIG_KEYS", 2_000)
    )
    chunk_size = max(100, _env_int("CONSIST_STRESS_CHUNK_SIZE", 5_000))
    write_runs = max(0, _env_int("CONSIST_STRESS_WRITE_RUNS", 200))

    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = tmp_path / "large_scale.duckdb"

    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))

    metrics: Dict[str, float] = {}

    with Session(tracker.engine) as session:
        with _timed("seed.base_dataset", metrics):
            sample = _seed_base_dataset(
                session,
                total_runs=total_runs,
                runs_per_scenario=runs_per_scenario,
                chunk_size=chunk_size,
            )

        with _timed("seed.lineage.deep", metrics):
            deep_artifact_id = _seed_deep_lineage(session, depth=lineage_depth)

        with _timed("seed.lineage.wide", metrics):
            wide_artifact_id = _seed_wide_lineage(session, input_count=wide_inputs)

        with _timed("seed.config.diff_runs", metrics):
            diff_run_a, diff_run_b, expected_changes = _seed_diff_runs(
                session, num_keys=config_keys
            )

    cache_tuple = sample["cache_tuple"]
    scenario_id = sample["scenario_id"]
    lookup_key = sample["lookup_key"]
    lookup_hash = sample["lookup_hash"]
    scale_sizes = sample["scale_sizes"]

    with _timed("find_runs.scale_10k", metrics):
        runs_10k = tracker.find_runs(
            tags=["scale_10k"], limit=scale_sizes["scale_10k"]
        )
    assert len(runs_10k) == scale_sizes["scale_10k"]

    with _timed("find_runs.scale_50k", metrics):
        runs_50k = tracker.find_runs(
            tags=["scale_50k"], limit=scale_sizes["scale_50k"]
        )
    assert len(runs_50k) == scale_sizes["scale_50k"]

    with _timed("find_runs.scale_100k", metrics):
        runs_100k = tracker.find_runs(
            tags=["scale_100k"], limit=scale_sizes["scale_100k"]
        )
    assert len(runs_100k) == scale_sizes["scale_100k"]

    with _timed("find_runs.tags+model+status", metrics):
        filtered = tracker.find_runs(
            tags=["scale_100k", "tagged"],
            model="model_2",
            status="completed",
            limit=min(5_000, scale_sizes["scale_100k"]),
        )
    assert filtered

    expected_scenario_runs = min(runs_per_scenario, total_runs)

    with _timed("find_runs.parent_id", metrics):
        scenario_runs = tracker.find_runs(
            parent_id=scenario_id, limit=expected_scenario_runs
        )
    assert len(scenario_runs) == expected_scenario_runs

    with _timed("find_runs.metadata", metrics):
        meta_runs = tracker.find_runs(
            metadata={"region": "west"}, limit=scale_sizes["scale_100k"]
        )
    assert meta_runs

    with _timed("cache.find_matching_run", metrics):
        cached = tracker.find_matching_run(*cache_tuple)
    assert cached is not None

    with _timed("lineage.deep", metrics):
        deep_lineage = tracker.get_artifact_lineage(deep_artifact_id)
    assert deep_lineage is not None
    assert _lineage_run_depth(deep_lineage) >= lineage_depth

    with _timed("lineage.wide", metrics):
        wide_lineage = tracker.get_artifact_lineage(wide_artifact_id)
    assert wide_lineage is not None
    producing = wide_lineage.get("producing_run", {})
    inputs = producing.get("inputs") if isinstance(producing, dict) else []
    assert isinstance(inputs, list)
    assert len(inputs) == wide_inputs

    with _timed("diff_runs.config_kv", metrics):
        diff = tracker.diff_runs(diff_run_a, diff_run_b)
    changes = diff.get("changes") if isinstance(diff, dict) else None
    assert isinstance(changes, dict)
    assert len(changes) == expected_changes

    with _timed("artifact.lookup.key", metrics):
        by_key = tracker.get_artifact(lookup_key)
    assert by_key is not None

    with Session(tracker.engine) as session:
        with _timed("artifact.lookup.hash", metrics):
            by_hash = session.exec(
                select(Artifact).where(Artifact.hash == lookup_hash)
            ).first()
        assert by_hash is not None
        assert by_hash.id == by_key.id

        with _timed("scenarios.group_by_parent_run", metrics):
            query = (
                select(
                    Run.parent_run_id.label("scenario_id"),
                    func.count(Run.id).label("run_count"),
                    func.min(Run.created_at).label("first_run"),
                    func.max(Run.created_at).label("last_run"),
                )
                .where(Run.parent_run_id.is_not(None))
                .group_by(Run.parent_run_id)
                .order_by(func.max(Run.created_at).desc())
                .limit(sample["scenario_count"])
            )
            scenario_rows = session.exec(query).all()
        assert len(scenario_rows) == sample["scenario_count"]

        with _timed("validate.select_all_artifacts", metrics):
            all_artifacts = session.exec(select(Artifact)).all()
        expected_artifacts = (
            total_runs + lineage_depth + wide_inputs + 1
        )
        assert len(all_artifacts) == expected_artifacts

        with _timed("validate.paginated", metrics):
            paginated_count = _count_artifacts_keyset(
                session, batch_size=1000
            )
        assert paginated_count == expected_artifacts

        if write_runs:
            initial_run_count = session.exec(
                select(func.count()).select_from(Run)
            ).one()

    if write_runs:
        write_dir = run_dir / "write_phase"
        write_dir.mkdir(exist_ok=True)
        input_path = write_dir / "input.txt"
        input_path.write_text("seed", encoding="utf-8")
        begin_times: list[float] = []
        log_times: list[float] = []
        end_times: list[float] = []
        file_write_times: list[float] = []
        total_times: list[float] = []

        with _timed("write.append_runs", metrics):
            for i in range(write_runs):
                run_id = f"write_run_{i:06d}"
                output_path = write_dir / f"output_{i:06d}.txt"
                run_started = False
                total_start = time.perf_counter()
                try:
                    t0 = time.perf_counter()
                    tracker.begin_run(
                        run_id=run_id,
                        model="write_phase",
                        tags=["write_phase", f"group_{i % 10}"],
                        config={"seed": i, "group": i % 10},
                        inputs=[input_path],
                        parent_run_id="write_scenario",
                        iteration=i,
                    )
                    begin_times.append(time.perf_counter() - t0)
                    run_started = True

                    t0 = time.perf_counter()
                    output_path.write_text(f"payload {i}", encoding="utf-8")
                    file_write_times.append(time.perf_counter() - t0)

                    t0 = time.perf_counter()
                    tracker.log_artifact(
                        output_path,
                        key=f"write_output_{i:06d}",
                    )
                    log_times.append(time.perf_counter() - t0)

                    t0 = time.perf_counter()
                    tracker.end_run(status="completed")
                    end_times.append(time.perf_counter() - t0)
                except Exception as exc:
                    if run_started:
                        tracker.end_run(status="failed", error=exc)
                    raise
                finally:
                    total_times.append(time.perf_counter() - total_start)

        for label, stats in (
            ("write.run.total", _summarize_samples(total_times)),
            ("write.run.begin_run", _summarize_samples(begin_times)),
            ("write.run.log_artifact", _summarize_samples(log_times)),
            ("write.run.end_run", _summarize_samples(end_times)),
            ("write.run.file_write", _summarize_samples(file_write_times)),
        ):
            for stat, value in stats.items():
                metrics[f"{label}.{stat}"] = value

        with Session(tracker.engine) as session:
            final_run_count = session.exec(
                select(func.count()).select_from(Run)
            ).one()
        assert final_run_count - initial_run_count == write_runs

    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    metrics["db.size_mb"] = db_size_mb

    profile = {
        "params": {
            "total_runs": total_runs,
            "runs_per_scenario": runs_per_scenario,
            "lineage_depth": lineage_depth,
            "wide_inputs": wide_inputs,
            "config_keys": config_keys,
            "chunk_size": chunk_size,
            "write_runs": write_runs,
        },
        "metrics": metrics,
        "param_descriptions": PARAM_DESCRIPTIONS,
        "metric_descriptions": METRIC_DESCRIPTIONS,
    }

    profile_path_env = os.getenv("CONSIST_STRESS_PROFILE_PATH")
    if profile_path_env:
        profile_path = Path(profile_path_env)
    else:
        profile_path = tmp_path / "large_db_profile.json"

    profile_path.write_text(
        json.dumps(profile, indent=2, sort_keys=True), encoding="utf-8"
    )
    summary_lines = [
        "[stress.large_db] profile summary",
        f"  total_runs={total_runs} runs_per_scenario={runs_per_scenario}",
        f"  lineage_depth={lineage_depth} wide_inputs={wide_inputs}",
        f"  config_keys={config_keys} chunk_size={chunk_size}",
        f"  write_runs={write_runs}",
        f"  db_size_mb={metrics.get('db.size_mb', 0.0):,.3f}",
    ]
    for label, duration in sorted(metrics.items()):
        summary_lines.append(
            f"  {label}: {duration:,.3f}s"
        )
    _emit_stdout("\n".join(summary_lines))
    _emit_stdout(
        f"[stress.large_db] profile written to {profile_path}"
    )
