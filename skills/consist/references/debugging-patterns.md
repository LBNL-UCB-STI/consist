# Debugging Patterns

## Contents

- Start with the public surface
- Identity and cache-miss debugging
- Scenario and step wiring
- Output hydration and missing files
- Cache-miss input hydration
- Path and mount resolution
- Inspection surfaces
- When to read Consist source

## Start With The Public Surface

- Reproduce with an explicit `Tracker(run_dir=..., db_path=...)`.
- Prefer the smallest failing example on `consist.run(...)`,
  `consist.scenario(...)`, or `tracker.run(...)`.
- Inspect the existing run before editing code:
  `consist show`, `consist artifacts`, `consist preview`, and `consist summary`
  are often enough to localize the problem.
- Treat source spelunking as a last resort after checking the docs, the run
  record, and the relevant artifact metadata.

## Identity And Cache-Miss Debugging

- Verify that all real dependencies live in `inputs=...`, `config=...`, or
  `identity_inputs=[...]`.
- Use `identity_inputs` for overlay directories, config bundles, calibration
  notes, or external files read indirectly by the wrapped tool.
- Inspect the stored identity summary before guessing:

```python
run = tracker.get_run(run_id)
print(run.identity_summary)
```

- Enable timing and signature diagnostics when the failure mode is cache reuse
  or unexpectedly slow cache hits:

```bash
CONSIST_CACHE_TIMING=1 CONSIST_CACHE_DEBUG=1 /path/to/python script.py
```

- If a step should reuse cache across scenarios, keep the host output path
  stable and make the true scenario differences explicit in step config or
  `identity_inputs`.
- Do not delete failed run records just because a later rerun succeeds. Failed
  records often explain missing inputs, schema/profile behavior, and partial
  output capture.

## Scenario And Step Wiring

- Use `scenario(...)` when steps depend on prior artifacts or need shared
  scenario metadata.
- Do not assume scenario header config is inherited by steps. If a step's work
  depends on `year`, `mode`, or other knobs, include them on that step's
  `config=...`.
- Use `consist.ref(...)` or `consist.refs(...)` for explicit step-to-step
  artifact links instead of ad hoc path passing.
- Use `trace(...)` when the Python body must still run on cache hits. Use
  `run(...)` when a cache hit should skip the user code entirely.
- Inside a scenario, prefer `scenario.run(...)` and `scenario.trace(...)` so
  child runs inherit the scenario relationship without manual `parent_run_id`
  threading.

## Output Hydration And Missing Files

- If downstream code expects real files on cache hits, check
  `cache_hydration` first.
- Use `output_paths={...}` when a function returns `None`, when an external tool
  writes files, or when cached outputs must re-materialize to specific host
  paths.
- Use `capture_dir=...` when a tool always writes to a fixed folder and the
  simpler direct-capture surface is enough.
- Use `validate_cached_outputs="eager"` when the problem might be stale or
  missing materialized outputs.
- Use `result.output_path("key")` or `tracker.load_run_output(...)` to verify
  what path or artifact Consist thinks belongs to the run.
- Use `load_df(..., db_fallback="always")` when the artifact was ingested and
  the original file may no longer exist.
- If a tool writes nested directories, prefer top-level
  `consist.capture_outputs(..., recursive=True)` or
  `tracker.capture_outputs(..., recursive=True)`. `_consist_ctx.capture_outputs`
  does not expose `recursive=True`.
- Auto-captured keys come from filename stems. If nested outputs can repeat the
  same basename, log them manually with stable keys instead of relying on
  automatic capture.

## Cache-Miss Input Hydration

- `cache_hydration="inputs-missing"` is cache-miss behavior, not cache-hit
  behavior. By default it restores declared inputs only when their destination
  path is absent.
- For in-place workflows that reuse the same path for different logical input
  versions, opt into stale-input checks with
  `CacheOptions(cache_hydration="inputs-missing", validate_materialized_inputs=True)`.
- With validation enabled, Consist preserves existing destinations unless a
  portable full-content hash proves the path is stale. Missing or stale inputs
  can recover from historical run roots, artifact `recovery_roots`, or ingested
  DB fallback.
- Hash validation intentionally stays conservative: destinations are preserved
  when an artifact has no portable hash, the run did not use full hashing, or
  the local path cannot be read safely.
- DB fallback is for ingested file-like artifacts. Directory inputs and
  non-portable artifacts need a filesystem recovery source.

## Path And Mount Resolution

- Inspect `artifact.container_uri` before assuming the stored path is wrong.
- Use `tracker.resolve_uri(uri)` to translate stored URIs into the current host
  filesystem.
- If runs or outputs moved, prefer explicit CLI flags first:
  `--db-path`, `--run-dir`, and `--mount`.
- Use `--trust-db` only when you intentionally want metadata-based fallback to
  stored run-dir or mount information.
- For trusted local archives, `consist shell --trust-db --db-path <db>` is the
  fastest way to inspect mounted URIs and recover stored mount roots. Add
  explicit `--mount name=/path` when the data root moved or the database came
  from another machine.

## Inspection Surfaces

- CLI:

```bash
consist show RUN_ID --db-path ./provenance.duckdb
consist artifacts RUN_ID --db-path ./provenance.duckdb
consist preview persons --db-path ./provenance.duckdb --run-dir ./runs
consist db doctor --db-path ./provenance.duckdb
```

- Python:

```python
run = consist.find_run(tracker=tracker, parent_id="baseline", year=2030)
artifacts = tracker.get_artifacts_for_run(run.id)
persons = consist.load_df(artifacts.outputs["persons"], tracker=tracker)
```

Use the CLI when the task is inspection-heavy and read-only. Use Python when
the next step is an automated fix, a regression test, or downstream analysis.

## When To Read Consist Source

- Read docs first: `docs/usage-guide.md`, `docs/concepts/caching-and-hydration.md`,
  and `docs/cli-reference.md`.
- Read tests next when you need a known-good pattern for a workflow shape.
  `tests/stress/containers/test_pilates_simulation_containers.py` is the best
  downstream example for scenario headers, identity inputs, container outputs,
  and baseline vs policy analysis.
- If the downstream project uses ActivitySim or BEAM adapters, read the adapter
  docs before assuming the wrapper forgot identity wiring. Adapter metadata and
  config roots may already be merged into run identity.
- Read `src/consist/...` only after the public docs and tests fail to explain
  the behavior or when you are implementing a Consist bugfix.
