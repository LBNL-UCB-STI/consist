# Troubleshooting Guide

This guide is a symptom index: find the message or behavior, confirm the likely
cause, then apply the shortest fix. For concept definitions, see
[Core Concepts](concepts/overview.md). For deeper topic guides, see:

- [Container Integration](containers-guide.md#error-handling)
- [DLT Loader](dlt-loader-guide.md#common-errors)
- [Mounts & Portability](mounts-and-portability.md#troubleshooting)
- [Database Maintenance](db-maintenance.md)
- [Historical Recovery](guides/historical-recovery.md)

## Database Maintenance

### "I need to inspect or repair the provenance DB"

**Diagnosis:** DB maintenance is safest when you snapshot before mutating and
diagnose before deleting or merging anything.

**Fix:** Use the full runbook in [Database Maintenance](db-maintenance.md). The
short sequence is:

```bash
consist db snapshot --out ./snapshots/provenance.pre-maintenance.duckdb --db-path ./provenance.duckdb
consist db inspect --db-path ./provenance.duckdb
consist db doctor --db-path ./provenance.duckdb
```

Then take one action at a time: `consist db purge --dry-run`, `consist db merge`,
`consist db rebuild`, and finally `consist db compact`.

### "Database locked"

**Diagnosis:** DuckDB allows only one writer at a time. Concurrent Consist
processes can collide during run, artifact, or dlt sync writes.

**Fix:** Prefer sequential writes or separate DB files per process. On shared HPC
filesystems, tune retries before redesigning the workflow:

```bash
export CONSIST_DLT_LOCK_RETRIES=40
export CONSIST_DLT_LOCK_BASE_SLEEP_SECONDS=0.2
export CONSIST_DLT_LOCK_MAX_SLEEP_SECONDS=5.0
export CONSIST_DB_LOCK_RETRIES=40
export CONSIST_DB_LOCK_BASE_SLEEP_SECONDS=0.2
export CONSIST_DB_LOCK_MAX_SLEEP_SECONDS=5.0
```

---

## Run Invocation

### "unexpected keyword argument 'hash_inputs'"

**Diagnosis:** `hash_inputs` is no longer accepted on `run(...)`, `trace(...)`,
or step-level scenario execution surfaces.

**Fix:** Use `identity_inputs=[...]`. Scenario header contexts still route
through `begin_run(...)`, where `hash_inputs` remains a legacy low-level option.

### "unexpected keyword argument 'config_plan'"

**Diagnosis:** `config_plan` is no longer accepted on `run(...)`, `trace(...)`,
or step-level scenario execution surfaces.

**Fix:** Pass `adapter=...` and optional `identity_inputs=[...]`. Scenario
headers do not currently support header-level `adapter=...`.

### "identity_inputs/hash_inputs must be a list of paths."

**Diagnosis:** A scalar string/path or invalid shape was passed where Consist
expects a list.

**Fix:** Pass `identity_inputs=[Path(...)]` or
`identity_inputs=[("label", Path(...))]`.

### "Failed to compute identity input digests ..."

**Diagnosis:** One or more identity input paths are missing or unreadable.

**Fix:** Verify that each path exists and is readable before starting the run.

### "load_inputs=True requires inputs to be a dict." / "input_binding=... requires inputs to be a dict."

**Diagnosis:** Automatic input binding needs named inputs so Consist can match
function parameters.

**Fix:** Pass `inputs={"param_name": path_or_artifact}`. To disable binding, use
`ExecutionOptions(input_binding="none")`.

### "cache_hydration='outputs-requested' requires output_paths."

**Diagnosis:** Requested-output hydration needs explicit destination mappings.

**Fix:** Declare `output_paths={...}` whenever using
`cache_hydration="outputs-requested"`.

### "Tracker.run supports executor='python' or 'container'."

**Diagnosis:** The executor value is not supported.

**Fix:** Use `ExecutionOptions(executor="python")` or
`ExecutionOptions(executor="container", container={...})`.

### "cache_options.code_identity callable modes require executor='python'."

**Diagnosis:** Callable code identity modes require Python callable execution.

**Fix:** Switch to `executor="python"` or use `code_identity="repo_git"` for
container runs.

### "executor='container' requires output_paths."

**Diagnosis:** Container runs cannot infer outputs from Python return values.

**Fix:** Provide explicit output mappings with `output_paths={key: path}`.

### "Scenario input string did not resolve ..."

**Diagnosis:** The string matched neither a Coupler key nor an existing
filesystem path.

**Fix:** Pass a real path, or use `consist.refs(...)` between scenario steps on
the preferred execution path.

---

## Cache And Provenance

### "Consist has N active DuckDB relations..."

**Diagnosis:** Relations returned by `consist.load(...)` for tabular artifacts
keep a DuckDB connection open until closed.

**Fix:** Prefer `consist.load_df(...)` when a pandas DataFrame is enough, or use
`consist.load_relation(...)` as a context manager. For intentional high fan-out,
raise `CONSIST_RELATION_WARN_THRESHOLD`.

### "Old DBs no longer load after the Relation-first refactor"

**Diagnosis:** The artifact schema changed: `uri` became `container_uri`, child
artifacts now use first-class `table_path`, `array_path`, and
`parent_artifact_id` fields, and `meta["table_path"]` is no longer canonical.

**Fix:** Reset old local DBs and re-run workflows:

```bash
rm ./provenance.duckdb
rm ./test_db.duckdb
```

Then update code to use `artifact.container_uri`, `artifact.table_path`, and
`artifact.parent_artifact_id`. Use `tracker.get_child_artifacts(...)` and
`tracker.get_parent_artifact(...)` for container traversal.

### "Cache hit but output files are missing"

**Diagnosis:** Consist found matching provenance metadata, but default cache hits
do not copy bytes back to disk.

**Fix:** Opt into hydration:

```python
from consist import CacheOptions

result = tracker.run(
    fn=my_function,
    inputs={...},
    outputs=["results"],
    cache_options=CacheOptions(cache_hydration="outputs-all"),
)
```

For a prior run, archive mirror, or restart workspace, use
`tracker.hydrate_run_outputs(...)`; see
[Historical Recovery](guides/historical-recovery.md).

### "I want to know why this run missed cache"

**Diagnosis:** `cache_hit=False` means the signature did not match, but the
reason is recorded on the completed run.

**Fix:** Inspect `run.meta["cache_miss_explanation"]`:

```python
run = tracker.get_run("my_run_id")
explanation = run.meta.get("cache_miss_explanation", {})

print(explanation.get("reason"))
print(explanation.get("candidate_run_id"))
print(explanation.get("details", {}))
```

Use `reason`, `candidate_run_id`, `matched_components`,
`mismatched_components`, and `details` to identify config, input, code, or
candidate-output drift.

### "Same inputs/config but cache not found"

**Diagnosis:** A cache-key component changed. Common causes are code changes,
config type drift such as `0` versus `0.0`, changed input fingerprints, changed
`model` / `year` / `iteration`, or dependency changes.

**Fix:** Compare the run's `identity_summary` and
`run.meta["cache_miss_explanation"]`. For manual checks, compute the relevant
code, config, and input hashes with `tracker.identity`.

### "How do I clear/reset cache?"

**Diagnosis:** You either want to remove all history or keep history while
forcing one run to execute again.

**Fix:** Delete the DB only when you want a full reset:

```bash
rm ./provenance.duckdb
```

To keep history but force execution, use
`CacheOptions(cache_mode="overwrite")`.

---

## Mounts And Paths

### "Container runs but /inputs is empty"

**Diagnosis:** A host mount path does not exist, is relative when Docker needs an
absolute path, or is unreadable.

**Fix:** Verify host paths with `Path(...).exists()`, pass absolute mount paths,
check permissions, and debug with a direct `docker run -v ... ls` command. If
Consist rejects paths outside configured mounts, add the mount to the `Tracker`
or pass `strict_mounts=False` to `run_container()`.

### "URI resolution failed"

**Diagnosis:** The URI scheme is unknown, the mount was not registered, or the
artifact points at bytes that are no longer reachable from this machine.

**Fix:** Register the mount, fix the URI scheme, or hydrate from a recovery root.
For portable path rules, see [Mounts & Portability](mounts-and-portability.md).

### "Working directory changed between runs"

**Diagnosis:** Relative paths were resolved from different working directories,
so the recorded provenance path no longer points at the same bytes.

**Fix:** Prefer tracker-managed output paths, absolute mount-backed paths, or
configured mount roots. For archived historical bytes, record
`artifact.meta["recovery_roots"]` instead of depending on a mutable cwd.

---

## Data And Schema

### "Schema mismatch during ingestion"

**Diagnosis:** The file columns, dtypes, or table layout changed after the first
ingest or no longer match the expected schema.

**Fix:** Inspect the actual file, schema metadata, and dlt loader logs. For
loader-specific cases, see [DLT Loader](dlt-loader-guide.md#common-errors).

### "Null in non-optional field"

**Diagnosis:** Input data contains nulls where the table schema requires a value.

**Fix:** Clean or fill the source data before ingestion, or update the schema if
null is a valid domain value.

### "Duplicate primary keys"

**Diagnosis:** The source table has repeated keys for a field Consist or the
loader treats as unique.

**Fix:** Deduplicate upstream or change the declared key. Do not suppress the
error until you know whether duplicate rows are legitimate.

### "Can't query across runs"

**Diagnosis:** The target records were never ingested, live in a different DB, or
use incompatible schema/table names.

**Fix:** Confirm the run IDs in the active DB, inspect artifact metadata, and
query only ingested tables or relations opened through Consist.

---

## Container Execution

### "Container execution failed"

**Diagnosis:** The image, command, environment, mount, or output contract failed.

**Fix:** Re-run the equivalent `docker run` command with the same mounts and
environment, then compare the container's expected outputs against
`output_paths`.

### "Output files not found after container"

**Diagnosis:** The container completed but did not write the declared files where
Consist expected them.

**Fix:** Check the container working directory, mount targets, file names, and
permissions. Container runs must declare explicit `output_paths`.

### "Image pull failed"

**Diagnosis:** The registry, tag, credentials, or network path is unavailable.

**Fix:** Pull the image manually first, verify credentials, and pin an available
tag for reproducible workflow runs.

### "Permission denied in container"

**Diagnosis:** The container user cannot read inputs or write mounted outputs.

**Fix:** Adjust host permissions, write to a mounted output directory owned by
the executing user, or configure the container user to match the host workflow.

---

## Performance

### "Runs are very slow"

**Diagnosis:** Slow runs usually come from hashing large inputs, copying cached
outputs, container startup, DB contention, or expensive fallback recovery.

**Fix:** Check which phase is slow. Use narrower `identity_inputs`, avoid
unneeded cache hydration, reduce concurrent DB writers, and hydrate only the
keys you need.

### "Database is huge and slow"

**Diagnosis:** Old runs, ingested data, cache entries, or un-compacted deletes
are accumulating.

**Fix:** Follow [Database Maintenance](db-maintenance.md): snapshot, inspect,
purge with `--dry-run`, optionally prune cache where references are derivable,
then compact.

## See Also

- [Historical Recovery](guides/historical-recovery.md)
- [Container Integration](containers-guide.md#error-handling)
- [DLT Loader](dlt-loader-guide.md#common-errors)
- [Architecture](architecture.md)
- [CLI Reference](cli-reference.md)
