# Benchmark Runner

This folder contains helper scripts for running and plotting Consist benchmarks.

## Benchmark Inventory

Use this table to pick the right benchmark for roadmap work. Keep commands here
runnable and stable; put dated results and decisions in
`docs-internal/2026-06-03-roadmap-benchmark-log.md`.

| Benchmark | Command | Measures | Roadmap use |
|---|---|---|---|
| Metadata hot paths | `python scripts/benchmarks/metadata_hot_path_profile.py` | Cache lookup, artifact/link hydration, cache-hit replay, run/logging persistence | Shared-DB A1 baseline; directs A2/A4+A5 |
| Maintenance merge profile | `python scripts/benchmarks/maintenance_merge_profile.py` | Shard export, dry-run merge, and real merge over synthetic production-shaped provenance rows | Maintenance merge scaling; directs temp-table/join consolidation work |
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
  --warmup 1 \
  --tracker-reuse cold \
  --code-identity repo_git
```

Output:
- JSON report at `benchmarks/results/<timestamp>/metadata_hot_paths.json`
- Concise summary printed to stdout
- Attribution metric `cache.hit.begin_run_attribution`, which records a separate
  warmed-path cache-hit sample with `begin_run_ms` and `end_run_ms`, plus
  internal `begin_run` phase seams: validation/option parsing, config hashing,
  code identity, run-state setup, config facet persistence, input binding,
  input hashing, cache lookup, cache validation, cache hydration, materialization,
  JSON flush, and DB sync.
- Input-binding sub-attribution labels include artifact construction,
  bulk latest-artifact URI lookup, single latest-artifact URI lookup, inherited
  metadata application, in-memory attachment, batched JSON flush, and batched
  artifact DB sync.
- DB-internal sub-attribution labels include `db.sync_artifacts.*`,
  `db.sync_run_with_links.*`, and `db.sync_run.*` timings for session/retry,
  artifact add/merge, existing-link checks, run merge, link insertion, and commit.

Useful diagnostic switches:
- `--tracker-reuse cold|warm`: compare fresh tracker/session setup against in-process
  tracker reuse.
- `--code-identity repo_git|callable_module|callable_source`: compare repository Git
  identity against callable-scoped identity modes.

Record representative A1 runs in
`docs-internal/2026-06-03-roadmap-benchmark-log.md` before starting A2/A4+A5
optimization work.

## Profile Maintenance Merge

This benchmark builds synthetic source/target provenance databases, exports a shard,
then times dry-run and real `DatabaseMaintenance.merge(...)` calls. Defaults are small
enough for local iteration but include enough rows to expose production-scale merge
shape: run/artifact/link rows, config KVs, artifact KVs, schema observations, scoped
global tables, and conflict-skipped runs.

```bash
python scripts/benchmarks/maintenance_merge_profile.py \
  --target-runs 100 \
  --shard-runs 200 \
  --conflict-runs 20 \
  --artifacts-per-run 3 \
  --config-kvs-per-run 5 \
  --artifact-kvs-per-artifact 2 \
  --global-tables 2 \
  --global-rows-per-run 5 \
  --repeats 1
```

Output:
- JSON report at `benchmarks/results/<timestamp>/maintenance_merge_profile.json`
- Generated source/target/shard DBs under the report directory's `work/` folder
- Concise stdout summary for export, dry-run merge, and real merge means
- Per-merge attribution under `samples[].merge_attribution_summary_ms`, including
  conflict detection, global-table planning, dry-run counts, total write time, and
  global-table write time. The unlabelled remainder inside `merge.write.total` is
  core provenance table merge SQL.

Useful diagnostic switches:
- Increase `--shard-runs` and `--global-rows-per-run` to approximate production shard
  size without needing production data.
- Set `--conflict-runs 0 --conflict error` to isolate the no-conflict path.
- Use `--include-snapshots` only when filesystem snapshot copy cost is part of the
  question; otherwise keep it off to focus on DB merge behavior.
