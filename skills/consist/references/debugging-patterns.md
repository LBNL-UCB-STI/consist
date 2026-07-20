# Debugging Patterns

## Contents

- Start with the public surface
- Identity and cache-miss debugging
- Prior-run admission debugging
- Scenario and step wiring
- Output hydration and missing files
- Exact historical recovery and bundle validation
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

## Prior-Run Admission Debugging

- Use the public `check_artifact_identity(...)` API or
  `check_admission_reference(...)` when a receiving workflow must compare one
  resolved regular file with one exact input from an explicitly selected,
  completed prior run.
- Treat `AdmissionReport` as evidence, not workflow policy. The downstream
  caller decides whether `verified`, `mismatched`, `unverified`, or `unreadable`
  is fatal, warning-only, or acceptable.
- Use `consist admission doctor` for a read-only CLI diagnosis. Add
  `--require-verified` when the caller—not Consist—needs a nonzero exit for any
  result other than `verified`; use `--expected-file` only for a distinct,
  immutable corroborating copy.
- Do not infer consumer mount or configuration resolution from
  `consumer_path`. `AdmissionReference` records that runtime evidence while
  Consist hashes and audits the resolved `execution_path`.

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
- When a cache hit must either materialize and validate every requested output
  or execute as a normal miss, use:

  ```python
  CacheOptions(
      cache_hydration="outputs-requested",
      cache_hydration_failure="miss",
  )
  ```

  This opt-in policy rejects the candidate on a missing key, unavailable
  source, partial materialization, destination failure, or identity mismatch.
  It is valid only with `outputs-requested`; it is not a substitute for eager
  validation, recovery-root configuration, or restart policy. The default
  warning behavior remains unchanged.

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

## Exact Historical Recovery And Bundle Validation

- For a known completed run and caller-chosen destinations, use
  `hydrate_run_outputs_to_destinations(...)` and inspect every keyed result.
  Use `hydrate_run_outputs(...)` when a layout-preserving target root is enough.
- Keep the destination under `tracker.run_dir` or a configured mount root when
  possible. Use `allow_external_paths=True` only when the downstream workflow
  explicitly requires an external destination.
- Immutable directory and Zarr recovery validates a persisted tree manifest,
  stages the copy, and publishes it atomically. Shapefile recovery validates
  the complete same-stem bundle and publishes it through a clean bundle root.
- Legacy Zarr or Shapefile artifacts without those manifests remain loadable
  from bytes but fail closed for exact archive or hydration. Re-log them under
  the current artifact contract; do not bypass validation or treat a loose
  sidecar copy as an equivalent bundle.
- Consist recovers bytes and validates destinations. The caller owns historical
  run selection, downstream staging, mount translation, and proving that the
  external consumer actually read the materialized path.

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
consist admission doctor --db-path ./provenance.duckdb \
  --expected-run BASELINE_RUN_ID --artifact-key gtfs_feed \
  --file ./inputs/gtfs.zip
```

- Python:

```python
run = consist.find_run(tracker=tracker, parent_id="baseline", year=2030)
artifacts = tracker.get_artifacts_for_run(run.id)
persons = consist.load_df(artifacts.outputs["persons"], tracker=tracker)
```

Use the CLI when the task is inspection-heavy and read-only. Use Python when
the next step is an automated fix, a regression test, or downstream analysis.

`RunContext.canonicalization` and its snapshot/reference values are bounded
observations of adapter-resolved inputs for wrapped Python steps. They can aid
diagnosis, but they do not prove final HOCON/container path resolution or that
an external executable consumed those bytes. Archive output reports are
similarly operational evidence, not a replacement for artifact declarations or
restart policy.

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
