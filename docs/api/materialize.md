# Materialization

This page covers filesystem byte recovery and input staging. It is not about
choosing whether tabular artifacts should be ingested into DuckDB, except where
ingested artifacts provide a fallback byte source.

Consist has three related filesystem recovery and staging stories:

- historical output recovery, which starts with `hydrate_run_outputs(...)` or
  `hydrate_run_outputs_to_destinations(...)`
- single-artifact recovery, which starts with `materialize_artifact(...)`
- canonical input staging, which starts with
  `ExecutionOptions(input_materialization="requested", input_paths={...})` on
  `run(...)` and `ScenarioContext.run(...)`

For layout-preserving historical output recovery, start with
`hydrate_run_outputs(...)`. When a consumer needs exact, unrelated paths,
use `hydrate_run_outputs_to_destinations(...)`.

It is the clearest API for restart, archive-mirror recovery, and
cross-workspace reuse because it answers the practical questions in one call:

- Which keys did I ask for?
- Where did each one land?
- Can I use the returned artifact right now?

`materialize_run_outputs(...)` performs the same underlying copy/export work
but returns the older aggregate
[`MaterializationResult`](#consist.core.materialize.MaterializationResult).
Keep it for compatibility or summary-style reporting; use
`hydrate_run_outputs(...)` for new workflows.

### Exact destinations

`hydrate_run_outputs_to_destinations(...)` makes the caller-provided mapping
the complete requested-output set. It supports ordinary artifact files and
directories, including unrelated destinations:

```python
hydrated = tracker.hydrate_run_outputs_to_destinations(
    "prior_run_id",
    destinations_by_key={
        "persons": tracker.run_dir / "tool_inputs" / "persons.csv",
        "skims": tracker.run_dir / "shared" / "skims",
    },
)
```

The caller selects the historical run and is responsible for verifying that an
external tool consumes these staged paths. Results are keyed and can be partial
in warning mode, so inspect each status before invoking that tool. Every
destination must be inside the tracker's run directory or a configured mount
root unless `allow_external_paths=True`. Persisted `OutputSet` hydration is
deferred from this slice.

Use `materialize_artifact(...)` when you already have one artifact object and
need Consist to recover its bytes into a target workspace root. It is the
artifact-centric primitive beside the run-output-centric
`hydrate_run_outputs(...)`.

For input-side workflows, prefer requested input materialization on the run
surface. It uses the same canonical artifact recovery rules, but keeps the
operation attached to normal execution and cache-hit behavior. Reach for
`stage_artifact(...)` and `stage_inputs(...)` only when you need the same
staging behavior outside a run lifecycle.

## Recovery Ordering

When Consist rematerializes a historical output or stages a resolved artifact,
it probes recovery sources in this order:

1. A per-call `source_root=...` override
2. The historical source derived from the producing run directory, historical
   mount snapshot, or recorded artifact mount root
3. Ordered `artifact.meta["recovery_roots"]`
4. DuckDB export fallback for ingested CSV/Parquet artifacts

`container_uri` remains the canonical logical location throughout. Recovery
roots are advisory byte sources, not a second canonical path.

## Archive Workflow

For long-lived workflows that move outputs into an archive or iteration-specific
root, prefer recording recovery roots once instead of passing `source_root=...`
on every restart:

```python
archive_root = Path("/archive/pilates/iteration_004")
run_id = tracker.current_consist.run.id

archive = tracker.archive_current_run_outputs(
    archive_root,
    mode="copy",
)

hydrated = tracker.hydrate_run_outputs(
    run_id,
    keys=["persons"],
    target_root=tracker.run_dir / "restored_workspace",
)

next_inputs = archive.outputs["persons"]
```

`archive_current_run_outputs(...)` and `archive_run_outputs(...)` return an
`ArchivedOutputs` mapping. It still behaves like a read-only `Mapping[str,
Path]` for the archived bytes, while `.outputs` gives you refreshed artifacts
that already carry the new recovery root. Pass those refreshed artifacts
directly into a later `inputs={...}` mapping when you want the next run to
consume the archive.

When an application needs an auditable, no-replacement archive pass for files
only, use `tracker.archive_run_output_files(...)` instead:

```python
report = tracker.archive_run_output_files(
    "prior_run_id",
    Path("/archive/pilates/iteration_004"),
    keys=["persons", "households"],
    preserve_existing=True,
    verify=True,
    append=True,
)

if not report.complete:
    for key, result in report.items():
        if not result.metadata_committed:
            print(key, result.copy_status, result.verification_status, result.message)
```

`ArchivedRunOutputFilesReport` is a read-only mapping of selected output keys
to immutable per-key results. Each result includes the source artifact, known
source and target paths, copy and verification statuses, metadata-commit state,
and a message. The helper supports regular file artifacts only; directories,
output sets, symlinks, and artifacts lacking a full-file hash are reported per
key rather than copied. It never replaces archive bytes. A rerun can retain a
previously verified target and retry only the recovery-root metadata commit.
`report.complete` means every selected key met the requested verification policy
and committed metadata in this call; it is not a durable workflow-state claim.

Use the lower-level helpers when you want to manage archival yourself:

- `tracker.set_artifact_recovery_roots(...)` records one or more archive roots
  without copying bytes or verifying the archive-side files.
- `tracker.register_artifact_recovery_copy(...)` verifies an externally copied
  artifact file before recording the recovery root.
- `tracker.register_run_output_recovery_copies(...)` applies verified
  external-copy adoption to all or selected outputs for a run.
- `tracker.archive_current_run_outputs(...)` archives the outputs of the active
  run without manually extracting the run ID first.
- `tracker.archive_artifact(...)` copies or moves a single artifact into an
  archive root and records that root.
- `tracker.archive_run_outputs(...)` applies the same pattern to all or a
  selected subset of outputs for a run.
- `tracker.archive_run_output_files(...)` copies or conservatively retains
  selected regular files, verifies them when requested, and bulk-registers
  recovery roots while retaining per-key outcomes for retries.

For HDF5 containers, verified recovery-copy adoption respects the parent
container policy. Parent H5 files with
`container_recovery_unit="parent_file"` can register recovery roots; child
`h5_table` artifacts whose parent declares
`child_recovery_policy="descriptive_only"` return
`blocked_by_container_policy`.

## Recommended Workflow

Hydrate only the outputs you need into a fresh workspace root:

```python
from pathlib import Path

hydrated = tracker.hydrate_run_outputs(
    "prior_run_id",
    keys=["persons", "households"],
    target_root=tracker.run_dir / "restored_workspace",
    source_root=Path("/archive/outputs_mirror"),  # optional
)
```

Relative `target_root` values are resolved from the process working directory.
Use `tracker.run_dir / ...` or an absolute mount-backed path when you want the
restore destination to stay inside the tracker's allowed roots.

Inspect the keyed results and reuse the detached artifacts directly:

```python
persons = hydrated["persons"]

print(persons.status)
print(persons.path)

if persons.resolvable:
    path = persons.artifact.as_path()
    print(path)
```

In this flow:

- `hydrated["persons"].status` tells you what happened for that key.
- `hydrated["persons"].path` tells you where Consist expected or created the
  destination.
- `hydrated["persons"].artifact` is a detached artifact view. When
  `resolvable` is `True`, `artifact.as_path()` points at the hydrated
  destination in the new workspace. The detached artifact also preserves
  `artifact.hash`, Consist's canonical artifact fingerprint surface.

## Requested Input Staging

For new workflow code, input-side staging usually belongs on the run surface:

```python
from pathlib import Path
from consist import ExecutionOptions

result = tracker.run(
    fn=run_tool,
    inputs={"config_path": Path("./configs/baseline.yaml")},
    outputs=["report"],
    execution_options=ExecutionOptions(
        input_binding="paths",
        input_materialization="requested",
        input_paths={"config_path": Path("./workspace/config.yaml")},
    ),
)
```

This keeps the canonical input artifact unchanged for identity and lineage
while ensuring the callable sees a real local file at the requested path.

Use the low-level helpers when you already have resolved artifacts and need to
stage them manually:

```python
from pathlib import Path
import consist

staged = consist.stage_inputs(
    {"config_path": artifact},
    destinations_by_key={"config_path": Path("./workspace/config.yaml")},
)
```

## Staging Status Meanings

| Status | Meaning |
|---|---|
| `staged` | Copied bytes to the requested destination |
| `preserved_existing` | Destination already existed with matching content and was reused |
| `missing_source` | No readable source bytes were found for the canonical artifact |
| `failed` | Staging was attempted but failed due to a collision, policy check, or copy error |

## Status Meanings

| Status | Meaning |
|---|---|
| `materialized_from_filesystem` | Copied from historical cold bytes on disk |
| `materialized_from_db` | Exported from DuckDB for an ingested CSV/Parquet artifact |
| `preserved_existing` | Destination already existed and was reused |
| `skipped_unmapped` | No safe historical relative-path mapping was available |
| `missing_source` | Historical bytes were unavailable and no DB fallback applied |
| `failed` | Recovery was attempted but failed due to a collision, policy check, or copy/export error |

## When To Use `materialize_run_outputs(...)`

Use `materialize_run_outputs(...)` only when you intentionally want the older
aggregate summary buckets:

- `materialized_from_filesystem`
- `materialized_from_db`
- `skipped_existing`
- `skipped_unmapped`
- `skipped_missing_source`
- `failed`

If you are writing new restart or recovery logic, prefer
`hydrate_run_outputs(...)`.

## Reference

This page keeps the story focused on the recommended workflow. For full API
signatures and attribute details, use the generated reference below.

::: consist.core.materialize
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - materialize_artifact
        - stage_artifact
        - stage_inputs
        - MaterializedArtifact
        - StagedInput
        - StagedInputsResult
        - ArtifactRecoveryCopyRegistration
        - RunOutputRecoveryCopiesRegistration
        - ArchivedRunOutputFile
        - ArchivedRunOutputFilesReport
        - HydratedRunOutput
        - HydratedRunOutputsResult
        - MaterializationResult
      filters:
        - "!^_"
