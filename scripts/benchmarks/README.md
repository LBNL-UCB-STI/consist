# Benchmark Runner

This folder contains helper scripts for running and plotting Consist benchmarks.

## Benchmark Inventory

Use this table to pick the right benchmark for roadmap work. Keep commands here
runnable and stable; put dated results and decisions in
`docs-internal/2026-06-03-roadmap-benchmark-log.md`.

| Benchmark | Command | Measures | Roadmap use |
|---|---|---|---|
| Metadata hot paths | `python scripts/benchmarks/metadata_hot_path_profile.py` | Cache lookup, artifact/link hydration, cache-hit replay, run/logging persistence | Shared-DB A1 baseline; directs A2/A4+A5 |
| Cache-hit profiler | `python scripts/profile_cache_hits.py` | Focused cache-hit behavior with output validation/access options | Debug cache-hit edge cases before/after A2 |
| Large-DB stress profile | `python -m pytest -m heavy tests/stress/tracker/test_large_db_scale.py` | Scale queries, cache lookup, write hot paths at large DB sizes | Heavy validation before claiming scale improvements |
| CLI startup | `python scripts/benchmarks/cli_startup_benchmark.py` | CLI cold-start latency | CLI UX/regression tracking |

Additional local benchmark runners exist in some worktrees but are not ready for the
public inventory until their helper modules are tracked and smoke-tested.

## Run CLI Startup Benchmark

Use this benchmark to track CLI cold-start latency over time:

```bash
python scripts/benchmarks/cli_startup_benchmark.py --out benchmarks/cli_startup.json
```

Common options:

```bash
python scripts/benchmarks/cli_startup_benchmark.py \
  --iterations 10 \
  --warmup 3 \
  --out benchmarks/cli_startup.json
```

Output:
- JSON benchmark report with per-run timings and summary stats.

## Profile Metadata Hot Paths

This benchmark seeds a small provenance database and measures metadata lookup,
artifact/link hydration, cache-hit replay, and run/logging hot paths with repeatable
sizing parameters.

```bash
python scripts/benchmarks/metadata_hot_path_profile.py \
  --seed-runs 200 \
  --inputs 20 \
  --outputs 15 \
  --config-keys 200 \
  --repeats 5 \
  --warmup 1
```

Output:
- JSON report at `benchmarks/results/<timestamp>/metadata_hot_paths.json`
- Concise summary printed to stdout
- Attribution metric `cache.hit.begin_run_attribution`, which records a separate
  warmed-path cache-hit sample broken down by internal `begin_run` seams:
  config hashing, code identity, input binding, input hashing, cache lookup,
  cache validation, cache hydration, JSON flush, and DB sync.

Record representative A1 runs in
`docs-internal/2026-06-03-roadmap-benchmark-log.md` before starting A2/A4+A5
optimization work.
