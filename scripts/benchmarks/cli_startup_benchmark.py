#!/usr/bin/env python3
"""Measure Consist CLI startup latency and write a JSON report."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import platform
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any


def _run_once(command: list[str]) -> float:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if completed.returncode != 0:
        command_text = " ".join(command)
        raise RuntimeError(
            f"CLI startup probe failed with exit code {completed.returncode}: "
            f"{command_text}"
        )
    return elapsed_ms


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    index = int(round(0.95 * (len(ordered) - 1)))
    return ordered[index]


def run_benchmark(
    *,
    iterations: int,
    warmup: int,
    module: str,
    cli_args: list[str],
) -> dict[str, Any]:
    command = [sys.executable, "-m", module, *cli_args]

    for _ in range(warmup):
        _run_once(command)

    samples_ms = [_run_once(command) for _ in range(iterations)]

    return {
        "benchmark": "cli_startup",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "command": command,
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "iterations": iterations,
        "warmup_iterations": warmup,
        "samples_ms": samples_ms,
        "summary_ms": {
            "min": min(samples_ms),
            "max": max(samples_ms),
            "mean": statistics.fmean(samples_ms),
            "median": statistics.median(samples_ms),
            "p95": _p95(samples_ms),
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Consist CLI startup latency."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=8,
        help="Number of measured startup runs (default: 8).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Warmup runs before measurements (default: 2).",
    )
    parser.add_argument(
        "--module",
        default="consist.cli",
        help="Python module used for startup probes (default: consist.cli).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional path to write benchmark JSON.",
    )
    parser.add_argument(
        "cli_args",
        nargs="*",
        default=["--help"],
        help="CLI args passed to the module command (default: --help).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1.")
    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0.")

    report = run_benchmark(
        iterations=args.iterations,
        warmup=args.warmup,
        module=args.module,
        cli_args=list(args.cli_args),
    )

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    summary = report["summary_ms"]
    print(
        "CLI startup benchmark "
        f"(n={report['iterations']}, warmup={report['warmup_iterations']}): "
        f"mean={summary['mean']:.2f}ms "
        f"median={summary['median']:.2f}ms "
        f"p95={summary['p95']:.2f}ms"
    )
    if args.out is not None:
        print(f"Wrote benchmark report to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
