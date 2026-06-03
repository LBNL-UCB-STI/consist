#!/usr/bin/env python3
"""Profile Consist metadata hot paths and emit a JSON baseline report."""

from __future__ import annotations

import argparse
import itertools
import json
import platform
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from consist.core.tracker import Tracker  # noqa: E402

MODEL_NAME = "metadata_hot_paths"
SEED_INPUT_COUNT = 2
SEED_OUTPUT_COUNT = 1
SEED_CONFIG_KEYS = 8


@dataclass(frozen=True)
class ReferenceRun:
    run_id: str
    config_hash: str
    input_hash: str
    git_hash: str


def _default_out_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return ROOT / "benchmarks" / "results" / stamp / "metadata_hot_paths.json"


def _timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * quantile))
    return ordered[index]


def _summarize_samples(samples_ms: list[float]) -> dict[str, float]:
    return {
        "min": min(samples_ms),
        "max": max(samples_ms),
        "mean": statistics.fmean(samples_ms),
        "median": statistics.median(samples_ms),
        "p95": _percentile(samples_ms, 0.95),
    }


def _measure_samples(
    *,
    repeats: int,
    warmup: int,
    sample: Callable[[], None],
) -> dict[str, Any]:
    for _ in range(warmup):
        sample()

    samples_ms: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        sample()
        samples_ms.append((time.perf_counter() - started) * 1000.0)

    return {
        "samples_ms": samples_ms,
        "summary_ms": _summarize_samples(samples_ms),
    }


def _write_text_file(path: Path, *, label: str, lines: int = 4) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [f"{label} line {i}" for i in range(lines)]
    path.write_text("\n".join(payload) + "\n", encoding="utf-8")


def _make_config(prefix: str, *, config_keys: int, sample_index: int) -> dict[str, Any]:
    config = {
        "profile_case": prefix,
        "sample_index": sample_index,
    }
    for i in range(config_keys):
        config[f"{prefix}_config_{i:03d}"] = f"{prefix}_value_{sample_index:03d}_{i:03d}"
    return config


def _build_reference_artifacts(
    base_dir: Path,
    *,
    input_count: int,
    output_count: int,
) -> tuple[list[Path], dict[str, Path]]:
    input_dir = base_dir / "inputs"
    output_dir = base_dir / "outputs"

    input_paths = []
    for index in range(input_count):
        path = input_dir / f"input_{index:03d}.txt"
        _write_text_file(path, label=f"input_{index:03d}")
        input_paths.append(path)

    output_paths: dict[str, Path] = {}
    for index in range(output_count):
        key = f"output_{index:03d}"
        path = output_dir / f"{key}.txt"
        _write_text_file(path, label=key)
        output_paths[key] = path

    return input_paths, output_paths


def _make_tracker(run_dir: Path, db_path: Path) -> Tracker:
    return Tracker(run_dir=run_dir, db_path=str(db_path))


def _seed_background_runs(
    tracker: Tracker,
    *,
    input_paths: list[Path],
    output_path: Path,
    seed_runs: int,
) -> None:
    for index in range(seed_runs):
        run_id = f"seed_{index:05d}"
        with tracker.start_run(
            run_id,
            model=MODEL_NAME,
            config=_make_config("seed", config_keys=SEED_CONFIG_KEYS, sample_index=index),
            inputs=input_paths[:SEED_INPUT_COUNT],
            cache_mode="overwrite",
        ):
            tracker.log_artifact(
                output_path,
                key="seed_output",
                direction="output",
            )


def _seed_reference_run(
    tracker: Tracker,
    *,
    run_id: str,
    prefix: str,
    input_paths: list[Path],
    output_paths: dict[str, Path],
    config_keys: int,
) -> ReferenceRun:
    with tracker.start_run(
        run_id,
        model=MODEL_NAME,
        config=_make_config(
            prefix,
            config_keys=config_keys,
            sample_index=0,
        ),
        inputs=input_paths,
        cache_mode="overwrite",
    ):
        tracker.log_artifacts(output_paths, direction="output")
    run = tracker.get_run(run_id)
    if run is None:
        raise RuntimeError(f"Failed to load seeded reference run {run_id!r}.")
    if not run.config_hash or not run.input_hash or not run.git_hash:
        raise RuntimeError(f"Seeded reference run {run_id!r} is missing hashes.")
    return ReferenceRun(
        run_id=run_id,
        config_hash=run.config_hash,
        input_hash=run.input_hash,
        git_hash=run.git_hash,
    )


def _profile_write_path(
    *,
    tracker: Tracker,
    input_paths: list[Path],
    output_paths: dict[str, Path],
    config_keys: int,
    repeats: int,
    warmup: int,
) -> dict[str, Any]:
    sample_counter = itertools.count()

    def _sample() -> dict[str, float]:
        sample_index = next(sample_counter)
        run_id = f"write_sample_{sample_index:05d}"

        started = time.perf_counter()
        tracker.begin_run(
            run_id=run_id,
            model=MODEL_NAME,
            config=_make_config(
                "write",
                config_keys=config_keys,
                sample_index=sample_index,
            ),
            inputs=input_paths,
            cache_mode="overwrite",
        )
        begin_elapsed = (time.perf_counter() - started) * 1000.0

        started = time.perf_counter()
        tracker.log_artifacts(output_paths, direction="output")
        log_elapsed = (time.perf_counter() - started) * 1000.0

        started = time.perf_counter()
        tracker.end_run(status="completed")
        end_elapsed = (time.perf_counter() - started) * 1000.0

        return {
            "begin_run_ms": begin_elapsed,
            "log_outputs_ms": log_elapsed,
            "end_run_ms": end_elapsed,
            "total_ms": begin_elapsed + log_elapsed + end_elapsed,
        }

    samples: list[dict[str, float]] = []
    for _ in range(warmup):
        _sample()
    for _ in range(repeats):
        samples.append(_sample())

    return {
        "samples_ms": samples,
        "summary_ms": {
            key: _summarize_samples([sample[key] for sample in samples])
            for key in samples[0]
        },
    }


def _profile_find_matching_run(
    *,
    tracker_factory: Callable[[], Tracker],
    reference_run: ReferenceRun,
    repeats: int,
    warmup: int,
) -> dict[str, Any]:
    def _sample() -> None:
        tracker = tracker_factory()
        match = tracker.find_matching_run(
            reference_run.config_hash,
            reference_run.input_hash,
            reference_run.git_hash,
        )
        if match is None:
            raise RuntimeError("find_matching_run unexpectedly returned no match.")
        if match.id != reference_run.run_id:
            raise RuntimeError(
                f"find_matching_run returned {match.id!r}, expected {reference_run.run_id!r}."
            )

    return _measure_samples(repeats=repeats, warmup=warmup, sample=_sample)


def _profile_artifact_hydration(
    *,
    tracker_factory: Callable[[], Tracker],
    reference_run: ReferenceRun,
    repeats: int,
    warmup: int,
) -> dict[str, Any]:
    def _sample() -> None:
        tracker = tracker_factory()
        artifacts = tracker.get_artifacts_for_run(reference_run.run_id)
        if not artifacts.outputs:
            raise RuntimeError("Expected hydrated outputs for the reference run.")

    return _measure_samples(repeats=repeats, warmup=warmup, sample=_sample)


def _profile_cache_hit(
    *,
    tracker_factory: Callable[[], Tracker],
    input_paths: list[Path],
    config: dict[str, Any],
    repeats: int,
    warmup: int,
) -> dict[str, Any]:
    sample_counter = itertools.count()

    def _sample() -> None:
        tracker = tracker_factory()
        run_id = f"cache_hit_{next(sample_counter):05d}"
        with tracker.start_run(
            run_id,
            model=MODEL_NAME,
            config=config,
            inputs=input_paths,
            cache_mode="reuse",
            validate_cached_outputs="lazy",
        ):
            pass

    return _measure_samples(repeats=repeats, warmup=warmup, sample=_sample)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile Consist metadata hot paths and write a JSON report."
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Path for the JSON report (default: benchmarks/results/<timestamp>/metadata_hot_paths.json).",
    )
    parser.add_argument(
        "--seed-runs",
        type=int,
        default=200,
        help="Number of background runs to seed before measuring.",
    )
    parser.add_argument(
        "--inputs",
        type=int,
        default=20,
        help="Number of inputs in the reference run.",
    )
    parser.add_argument(
        "--outputs",
        type=int,
        default=15,
        help="Number of outputs in the reference run.",
    )
    parser.add_argument(
        "--config-keys",
        type=int,
        default=200,
        help="Number of config keys in the reference run.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Number of measured samples per hot path.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup samples to discard for each hot path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.seed_runs < 0:
        raise ValueError("--seed-runs must be >= 0.")
    if args.inputs < 1:
        raise ValueError("--inputs must be >= 1.")
    if args.outputs < 1:
        raise ValueError("--outputs must be >= 1.")
    if args.config_keys < 1:
        raise ValueError("--config-keys must be >= 1.")
    if args.repeats < 1:
        raise ValueError("--repeats must be >= 1.")
    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0.")

    out_path = args.out or _default_out_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    base_dir = out_path.parent
    seed_run_dir = base_dir / "seed_runs"
    replay_run_dir = base_dir / "replay_runs"
    db_path = base_dir / "metadata_hot_paths.duckdb"

    seed_tracker = _make_tracker(seed_run_dir, db_path)

    reference_inputs, reference_outputs = _build_reference_artifacts(
        base_dir / "reference_data",
        input_count=args.inputs,
        output_count=args.outputs,
    )
    seed_output_path = next(iter(reference_outputs.values()))

    _seed_background_runs(
        seed_tracker,
        input_paths=reference_inputs,
        output_path=seed_output_path,
        seed_runs=args.seed_runs,
    )
    wide_reference_run = _seed_reference_run(
        seed_tracker,
        run_id="wide_reference_run",
        prefix="wide_reference",
        input_paths=reference_inputs,
        output_paths=reference_outputs,
        config_keys=args.config_keys,
    )

    def replay_tracker_factory() -> Tracker:
        return _make_tracker(replay_run_dir, db_path)

    write_tracker = _make_tracker(base_dir / "write_runs", db_path)
    write_profile = _profile_write_path(
        tracker=write_tracker,
        input_paths=reference_inputs,
        output_paths=reference_outputs,
        config_keys=args.config_keys,
        repeats=args.repeats,
        warmup=args.warmup,
    )

    lookup_profile = _profile_find_matching_run(
        tracker_factory=replay_tracker_factory,
        reference_run=wide_reference_run,
        repeats=args.repeats,
        warmup=args.warmup,
    )
    artifact_profile = _profile_artifact_hydration(
        tracker_factory=replay_tracker_factory,
        reference_run=wide_reference_run,
        repeats=args.repeats,
        warmup=args.warmup,
    )
    cache_hit_profile = _profile_cache_hit(
        tracker_factory=replay_tracker_factory,
        input_paths=reference_inputs,
        config=_make_config(
            "wide_reference",
            config_keys=args.config_keys,
            sample_index=0,
        ),
        repeats=args.repeats,
        warmup=args.warmup,
    )

    report = {
        "benchmark": "metadata_hot_paths",
        "timestamp_utc": _timestamp_utc(),
        "command": [str(part) for part in sys.argv],
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "paths": {
            "out_path": str(out_path),
            "base_dir": str(base_dir),
            "db_path": str(db_path),
            "seed_run_dir": str(seed_run_dir),
            "replay_run_dir": str(replay_run_dir),
        },
        "params": {
            "seed_runs": args.seed_runs,
            "inputs": args.inputs,
            "outputs": args.outputs,
            "config_keys": args.config_keys,
            "repeats": args.repeats,
            "warmup": args.warmup,
            "seed_input_count": SEED_INPUT_COUNT,
            "seed_output_count": SEED_OUTPUT_COUNT,
            "seed_config_keys": SEED_CONFIG_KEYS,
            "reference_config_keys": args.config_keys,
        },
        "database": {
            "size_mb": db_path.stat().st_size / (1024 * 1024),
            "seeded_reference_runs": args.seed_runs + 1,
        },
        "metrics": {
            "write.run.begin_log_end": write_profile,
            "cache.find_matching_run": lookup_profile,
            "cache.get_artifacts_for_run": artifact_profile,
            "cache.hit.start_run": cache_hit_profile,
        },
        "reference_runs": {
            "wide_output": {
                "run_id": wide_reference_run.run_id,
                "config_hash": wide_reference_run.config_hash,
                "input_hash": wide_reference_run.input_hash,
                "git_hash": wide_reference_run.git_hash,
                "output_count": args.outputs,
            },
        },
    }

    out_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    summary = [
        "[metadata-hot-paths] profile summary",
        f"  seeded_runs={args.seed_runs + 1} db_size_mb={report['database']['size_mb']:,.3f}",
        (
            "  cache.find_matching_run.mean_ms="
            f"{lookup_profile['summary_ms']['mean']:.3f}"
        ),
        (
            "  cache.get_artifacts_for_run.mean_ms="
            f"{artifact_profile['summary_ms']['mean']:.3f}"
        ),
        (
            "  cache.hit.start_run.mean_ms="
            f"{cache_hit_profile['summary_ms']['mean']:.3f}"
        ),
        f"  wrote_report={out_path}",
    ]
    print("\n".join(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
