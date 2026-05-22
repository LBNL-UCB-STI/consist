# Historical Recovery

Historical recovery is for rebuilding usable files from prior Consist runs. Use
it when a workflow hit cache but needs bytes on disk, a restart needs selected
outputs in a fresh workspace, or an archive mirror now holds the durable copy.

The short version:

- Use `hydrate_run_outputs(...)` for prior run outputs.
- Use run-level requested input materialization for inputs needed by a callable.
- Use `stage_artifact(...)` / `stage_inputs(...)` only when staging already
  resolved artifacts outside a run lifecycle.
- Use `recovery_roots` when archived bytes should remain discoverable across
  restarts.
- Treat archive roots as byte-location metadata, not new artifact identity.

## Mental Model

Consist separates artifact identity from byte location.

`container_uri` and artifact metadata describe the canonical artifact. A
historical file can move from a workspace to an archive without becoming a new
artifact. Recovery tells Consist where to find or recreate the bytes when the
original path is unavailable.

When Consist hydrates a prior output, it searches in this order:

1. A per-call `source_root=...` override.
2. The historical source derived from the producing run directory, historical
   mount snapshot, or recorded artifact mount root.
3. Ordered `artifact.meta["recovery_roots"]`.
4. DuckDB export fallback for ingested CSV/Parquet artifacts.

The first three sources restore existing files. The DuckDB fallback reconstructs
ingested tabular outputs when cold files are gone.

## Archive Roots And Recovery Roots

An archive root is a directory that contains archived copies of artifact bytes.
Consist records durable archive locations on artifacts as
`artifact.meta["recovery_roots"]`.

Use recovery roots when a workflow copies outputs into long-term storage,
iterations overwrite the same logical workspace path, or future restart/cache
validation should work without a one-off `source_root`.

Use a one-off `source_root=...` for debugging, temporary mirrors, or testing an
archive layout before recording it.

Do not create a second "archived artifact" just because bytes moved. Keep the
original artifact identity and record the additional byte root.

## Finding A Prior Run

If restart code does not already know the exact run ID, use
`tracker.find_matching_run(...)` to select the latest completed run for the
semantic workflow target, then hydrate outputs from that run:

```python
previous = tracker.find_matching_run(
    model="traffic_assignment",
    stage="assignment",
    phase="run",
    status="completed",
    year=2030,
    iteration=2,
    cache_epoch=4,
    run_scope="scenario_2030_base",
)

if previous is None:
    raise RuntimeError("No completed assignment run found for restart recovery.")
```

`run_scope` is supplied by the caller and constrains matches to runs whose ID or
description equals that scope or starts with `f"{run_scope}__"`. Use it when an
archive or workspace contains runs from multiple scenarios or restart attempts.
`find_matching_run(...)` returns `None` for a successful lookup with no match;
database or query failures remain visible.

## Recommended Output Recovery

Use `tracker.hydrate_run_outputs(...)` when you need historical outputs for a
known run:

```python
from pathlib import Path

run_id = "prior_run_id"

hydrated = tracker.hydrate_run_outputs(
    run_id,
    keys=["persons", "households"],
    target_root=tracker.run_dir / "restored_workspace",
    source_root=Path("/archive/outputs_mirror"),  # optional
)

persons = hydrated["persons"]
print(persons.status)
print(persons.path)

if persons.resolvable:
    print(persons.artifact.as_path())
```

Use `target_root` under the tracker `run_dir` or a configured mount root when
possible. Other destinations require `allow_external_paths=True`.

`hydrate_run_outputs(...)` returns keyed results, so each requested output
carries its own status, path, and detached artifact view.

## Compatibility Output Recovery

Use `tracker.materialize_run_outputs(...)` when existing code expects aggregate
summary buckets instead of keyed results:

```python
result = tracker.materialize_run_outputs(
    "prior_run_id",
    target_root=tracker.run_dir / "restored_workspace",
    keys=["persons", "households"],
)

print(result.materialized_from_filesystem)
print(result.materialized_from_db)
print(result.skipped_missing_source)
```

Prefer `hydrate_run_outputs(...)` for new restart or recovery workflows.

## Single Artifact Recovery

Use `tracker.materialize_artifact(...)` when you already have one artifact
object and need its bytes restored into a fresh workspace root:

```python
result = tracker.materialize_artifact(
    artifact,
    target_root=Path("./fresh_workspace"),
)

if result.resolvable:
    print(result.artifact.as_path())
```

For known historical run outputs, prefer `hydrate_run_outputs(...)`; it handles
run-output lookup and returns keyed results. `materialize_artifact(...)` is the
lower-level primitive for artifact-centric recovery outside that flow.

## Recording Archive Locations

If the workflow itself manages archival, record roots once so future hydration
and cache-hit validation can find the files without repeated overrides.

For outputs of the active run:

```python
tracker.archive_current_run_outputs(
    Path("/archive/iteration_004"),
    mode="copy",
)
```

For a prior run:

```python
tracker.archive_run_outputs(
    "prior_run_id",
    Path("/archive/iteration_004"),
    keys=["persons", "households"],
    mode="copy",
)
```

For one artifact:

```python
tracker.archive_artifact(
    artifact,
    Path("/archive/iteration_004"),
    mode="copy",
)
```

Use `mode="copy"` when the workspace file should remain in place. Use
`mode="move"` when the archive copy should become the durable byte source.

If bytes were already copied by another system, verify the archive-side file
and then record the root:

```python
registered = tracker.register_artifact_recovery_copy(
    artifact,
    Path("/archive/iteration_004"),
    verify=True,
)

if registered.status != "registered":
    print(registered.message)
```

For several outputs from a run:

```python
registered = tracker.register_run_output_recovery_copies(
    "prior_run_id",
    Path("/archive/iteration_004"),
    keys=["persons", "households"],
)
```

`register_artifact_recovery_copy(...)` does not copy bytes. It expects the
artifact to already exist at
`recovery_root / <artifact-uri-relative-path>`, blocks symlinks and directory
artifacts, and compares the file bytes to a full SHA-256 before updating
metadata. Pass `content_hash` when the artifact was logged with fast metadata
hashing or otherwise lacks a byte-level `artifact.hash`; without a byte-level
hash, verified registration returns `unverifiable_hash` and leaves metadata
unchanged.

HDF5 containers have an extra recovery policy guard. A parent H5 artifact can
declare `container_recovery_unit="parent_file"` and
`child_recovery_policy="descriptive_only"` through `log_h5_container(...)`.
With that policy, verified recovery-copy registration is allowed on the parent
H5 file but child `h5_table` artifacts return
`blocked_by_container_policy`. For HDF5 containers, a child table artifact is
not independently recoverable unless its parent container policy explicitly
allows child recovery.

```python
from consist import H5ChildSpec

container, children = tracker.log_h5_container(
    "input_data_for_2019_outputs.h5",
    key="usim_input_archive_2019",
    child_selection="include_only",
    child_specs={
        "/households": H5ChildSpec(key="usim_households"),
        "/persons": H5ChildSpec(key="usim_persons"),
    },
    container_recovery_unit="parent_file",
    child_recovery_policy="descriptive_only",
)

registered = tracker.register_run_output_recovery_copies(
    "prior_run_id",
    Path("/archive/iteration_004"),
    keys=["usim_input_archive_2019"],
)
```

Keep `keys=[...]` pointed at the parent H5 artifact for restart recovery. Child
table artifacts remain useful for schema, lineage, diffing, and diagnostics, but
they should not be used as recovery roots unless a future parent policy makes
child recovery explicitly authoritative.

Use the low-level metadata helper only when verification is handled elsewhere
or you intentionally need to override recovery metadata:

```python
tracker.set_artifact_recovery_roots(
    artifact,
    [Path("/archive/iteration_004")],
    append=True,
)
```

## Input Staging

Input staging solves a different problem: a callable or external tool needs a
canonical input artifact at a specific local path.

For normal execution, keep staging attached to the run:

```python
from consist import ExecutionOptions

result = tracker.run(
    fn=run_tool,
    inputs={"config_path": config_artifact},
    outputs=["report"],
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_paths={"config_path": Path("./workspace/config.yaml")},
    ),
)
```

This keeps artifact identity canonical in `inputs={...}` while making an exact
local copy available to path-bound code.

Use low-level staging only when you already have resolved artifacts and are
outside a run lifecycle:

```python
staged = tracker.stage_artifact(
    config_artifact,
    destination=Path("./workspace/config.yaml"),
)

staged_many = tracker.stage_inputs(
    {"config_path": config_artifact},
    destinations_by_key={"config_path": Path("./workspace/config.yaml")},
)
```

`stage_artifact(...)` and `stage_inputs(...)` use the same recovery rules,
including `recovery_roots`, but do not create a new run.

## Which API Should I Use?

| Need | Use |
|---|---|
| Find the latest completed run for a restart/reuse target | `find_matching_run(...)` |
| Restore selected outputs from a prior run into a workspace | `hydrate_run_outputs(...)` |
| Preserve compatibility with code expecting aggregate materialization buckets | `materialize_run_outputs(...)` |
| Restore one already-resolved artifact into a workspace | `materialize_artifact(...)` |
| Copy or move current run outputs into an archive and record the root | `archive_current_run_outputs(...)` |
| Copy or move selected prior run outputs into an archive and record the root | `archive_run_outputs(...)` |
| Copy or move one artifact into an archive and record the root | `archive_artifact(...)` |
| Verify an externally copied artifact file and record the root | `register_artifact_recovery_copy(...)` |
| Verify externally copied run outputs and record the root | `register_run_output_recovery_copies(...)` |
| Record recovery metadata without verifying bytes | `set_artifact_recovery_roots(...)` |
| Stage requested inputs as part of normal execution | `ExecutionOptions(input_materialization="requested", input_paths={...})` |
| Stage one resolved artifact outside a run lifecycle | `stage_artifact(...)` |
| Stage several resolved artifacts outside a run lifecycle | `stage_inputs(...)` |

## Troubleshooting

**Hydration cannot find files:** Confirm the original run directory still exists,
the historical mount snapshot points to a reachable root, or
`artifact.meta["recovery_roots"]` contains the archive root you expect.

**A target path is rejected:** Put `target_root` or staging destinations under
the tracker `run_dir` or a configured mount root, or explicitly allow external
paths where that is appropriate.

**A staged destination already exists:** Consist preserves matching existing
files, but rejects unsafe collisions. Remove the destination or choose a fresh
workspace path if the content is intentionally different.

## See Also

- [Materialization API](../api/materialize.md)
- [Caching & Hydration](../concepts/caching-and-hydration.md)
- [Mounts & Portability](../mounts-and-portability.md)
- [Troubleshooting](../troubleshooting.md)
