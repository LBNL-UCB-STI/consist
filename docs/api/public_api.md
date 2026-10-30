# Public API (v0.1)

This page defines Consist's **public API** for the `0.1.x` series. Items listed as
*Advanced* are still public but are primarily targeted at advanced users and may be
more verbose, lower-level, or easier to misuse.

For tiered guidance:

- [API Essentials](essentials.md) for onboarding and preferred execution patterns
- [API Advanced](advanced.md) for manual lifecycle, decorators, and lower-level APIs

## When to use each run entry point

| Entry point | Use when | Notes |
|---|---|---|
| [`Tracker.run`](tracker.md#consist.core.tracker.Tracker.run) | You want explicit tracker ownership for one cache-aware function | Preferred single-step entry point |
| [`Tracker.exec`](tracker.md#command-execution-exec) | You want to execute a CLI command or binary and record provenance | Returns `CommandExecutionResult` |
| [`Tracker.trace`](tracker.md#consist.core.tracker.Tracker.trace) | You need inline execution with explicit tracker control | Block executes every time |
| [`consist.scenario`](api_helpers.md#consist.api.scenario) | You need multi-step workflows with shared scenario context | Returns [`ScenarioContext`](workflow.md#scenario-context) |
| [`ScenarioContext.run`](workflow.md#consist.core.workflow.ScenarioContext.run) | You are inside a scenario and want cache-aware steps | Step-level equivalent of `Tracker.run`; also accepts `binding=BindingResult(...)` for orchestrator-resolved inputs |
| [`ScenarioContext.map_runs`](workflow.md#consist.core.workflow.ScenarioContext.map_runs) | You are inside a scenario and want one child run per sweep row | Process-backed fan-out for independent parameter sweeps |
| [`ScenarioContext.trace`](workflow.md#consist.core.workflow.ScenarioContext.trace) | You are inside a scenario and need inline step execution | Step-level equivalent of `Tracker.trace` |
| [`consist.exec`](api_helpers.md#consist.api.exec) | You want to execute a CLI command using the active/default tracker | Forwards to `Tracker.exec` |
| [`consist.run`](api_helpers.md#consist.api.run) | You need the legacy context-resolving convenience wrapper | Deprecated compatibility wrapper around `Tracker.run` |
| [`consist.trace`](api_helpers.md#consist.api.trace) | You need the legacy context-resolving tracing wrapper | Deprecated compatibility wrapper around `Tracker.trace` |
| [`consist.start_run`](api_helpers.md#consist.api.start_run) | You need the legacy context-resolving lifecycle wrapper | Deprecated compatibility wrapper around `Tracker.start_run` |

## Relationship: `consist.scenario`, `consist.run`, and `Tracker.run`

- `Tracker.run(...)` is the explicit class method for a single cache-aware step.
- `consist.scenario(...)` creates a scenario context; inside it, call `sc.run(...)`
  for cache-aware steps or `sc.trace(...)` for inline blocks that execute each time.
  If an external planner has already resolved the step bindings, pass
  `binding=BindingResult(...)` to `sc.run(...)` instead of unpacking primitive
  input kwargs. That is the preferred scenario-level envelope for complex or
  externally orchestrated workflows.
- `consist.run(...)` resolves the active/default tracker and then calls `Tracker.run(...)`,
  but it is now a deprecated compatibility wrapper.

## Top-level API policy

Consist manages its top-level package namespace (`import consist`) using PEP 562
lazy loading. Core workflow and tracker entry points are available immediately,
while heavier subpackages (such as DuckDB integration and SQLModel models) are loaded
only when accessed.

Consist keeps top-level `consist.*` helpers when they add one of two things:

- **Ambient run-scoped behavior**: helpers that attach metadata, artifacts, or
  side effects to an already-active run without forcing tracker plumbing
  through nested workflow code. Examples include
  `consist.log_artifact(...)`, `consist.log_artifacts(...)`,
  `consist.log_input(...)`, `consist.log_output(...)`,
  `consist.ingest(...)`, `consist.log_meta(...)`, and
  `consist.capture_outputs(...)`.
- **Pure/context utilities**: helpers that do not create tracker-owned
  execution state, such as `consist.ref(...)`, `consist.refs(...)`,
  `consist.output_path(...)`, and `consist.output_dir(...)`.

Consist organizes domain-specific, zero-weight, and boundary surfaces into dedicated
submodules:

- **`consist.digest`**: Stateless content, table (`ctd1`), state, and array digests
  with zero mandatory third-party imports at module scope.
- **`consist.admission`**: Boundary admission checks, expectation contracts, and
  evidence reporting.
- **`consist.gtfs`**: Transit feed schemas, snapshots, and pure member discovery.

Top-level exports for symbols in these domain submodules are retained as deprecated
aliases for backward compatibility, emitting `FutureWarning` on access, and will be
phased out in a future release. Prefer direct submodule imports (e.g.,
`from consist.digest import digest_file` or `from consist.admission import PriorRunExpectation`).

Consist deprecates top-level helpers when they are only duplicate
tracker-owning execution entry points:

- `consist.run(...)`
- `consist.trace(...)`
- `consist.start_run(...)`

`consist.scenario(...)` remains a deliberate exception. It is still a top-level
workflow entry point, but it carries meaningful API-level behavior beyond
simple forwarding, including the `enabled=False` noop branch and the scenario
ergonomics used throughout examples and multi-step workflows.

## Run/trace identity kwargs (public surface)

Run/trace APIs share these public identity kwargs:

- `adapter=...`
- `identity_inputs=[...]`

This applies to:

- `consist.run(...)` / `consist.trace(...)`
- `Tracker.run(...)` / `Tracker.trace(...)`
- `ScenarioContext.run(...)` / `ScenarioContext.trace(...)`

`config_plan=` and `hash_inputs=` are not accepted on these run/trace surfaces.
Passing them raises `TypeError` (`unexpected keyword argument ...`).

For signature debugging, use `run.identity_summary`.

## Minimal runnable comparison

```python
from pathlib import Path
import consist
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def write_result() -> Path:
    out = consist.output_path("result", ext="txt")
    out.write_text("ok\n")
    return out

# Preferred class API: explicit tracker ownership.
one = tracker.run(fn=write_result, outputs=["result"])

# Scenario API: grouped multi-step workflows.
with tracker.scenario("baseline") as sc:
    step = sc.run(fn=write_result, name="step_result", outputs=["result"])
```

## Stable (intended for external users)

- [`consist.Tracker`](tracker.md)
- [`consist.Run`](run.md)
- [`consist.Artifact`](artifact.md)
- [`consist.ArchivedOutputs`](artifact.md#archived-outputs)
- [`consist.ArchivedRunOutputFilesReport`](materialize.md#consist.core.materialize.ArchivedRunOutputFilesReport)
- [`consist.ArchivedRunOutputFile`](materialize.md#consist.core.materialize.ArchivedRunOutputFile)
- [`consist.HydratedRunOutput`](materialize.md#consist.core.materialize.HydratedRunOutput)
- [`consist.HydratedRunOutputsResult`](materialize.md#consist.core.materialize.HydratedRunOutputsResult)
- [`consist.MaterializedArtifact`](materialize.md#consist.core.materialize.MaterializedArtifact)
- [`consist.ArtifactRecoveryCopyRegistration`](materialize.md#consist.core.materialize.ArtifactRecoveryCopyRegistration)
- [`consist.RunOutputRecoveryCopiesRegistration`](materialize.md#consist.core.materialize.RunOutputRecoveryCopiesRegistration)
- [`consist.MaterializationResult`](materialize.md#consist.core.materialize.MaterializationResult)
- [`consist.StagedInput`](materialize.md#consist.core.materialize.StagedInput)
- [`consist.StagedInputsResult`](materialize.md#consist.core.materialize.StagedInputsResult)
- [`consist.RunSet`](runset.md#consist.runset.RunSet)
- [`consist.AlignedPair`](runset.md#consist.runset.AlignedPair)

### Canonical Content, Table, State, and Array Digests (consist.digest)

Stateless cryptographic hashing with zero mandatory third-party imports at module scope.
See [Content & Tabular Digests](digest.md) for detailed reference and examples.

- [`consist.digest.digest_bytes`](digest.md#consist.digest.digest_bytes)
  (raw 64-character lowercase hexadecimal SHA-256 of in-memory bytes)
- [`consist.digest.file_sha256`](digest.md#consist.digest.file_sha256)
  (raw 64-character lowercase hexadecimal SHA-256 of a regular file via streaming stdlib)
- [`consist.digest.digest_file`](digest.md#consist.digest.digest_file)
  (self-describing `sha256:file:<64 hex>` identity string for admission)
- [`consist.digest.normalize_json`](digest.md#consist.digest.normalize_json)
  (normalize Python/NumPy/Pydantic/Path/set structures into deterministic JSON primitives)
- [`consist.digest.canonical_json_str`](digest.md#consist.digest.canonical_json_str) / [`consist.digest.canonical_json_sha256`](digest.md#consist.digest.canonical_json_sha256)
  (RFC 8785 deterministic JSON formatting and 64-hex SHA-256 digest)
- [`consist.digest.digest_table`](digest.md#consist.digest.digest_table)
  (order-invariant `sha256:ctd1:<64 hex>` canonical table digest across Parquet, Arrow, and SQLite)
- [`consist.digest.normalize_table_scalar`](digest.md#consist.digest.normalize_table_scalar)
  (deterministic scalar value byte normalization for `ctd1` table hashing)
- [`consist.digest.digest_state`](digest.md#consist.digest.digest_state)
  (Merkle state dictionary digest for multi-facet checkpoints and configs)
- [`consist.digest.compute_logical_matrices_sha256`](digest.md#consist.digest.compute_logical_matrices_sha256)
  (compute a canonical `sha256:logical_matrices:<64 hex>` digest from named NumPy arrays)
- [`consist.digest.compute_omx_logical_matrices_sha256`](digest.md#consist.digest.compute_omx_logical_matrices_sha256)
  (compute a canonical `sha256:logical_matrices:<64 hex>` digest from an OMX/HDF5 container)
- [`consist.digest.compute_zarr_logical_matrices_sha256`](digest.md#consist.digest.compute_zarr_logical_matrices_sha256)
  (compute a canonical `sha256:logical_matrices:<64 hex>` digest from a Zarr store)
- [`consist.digest.canonicalize_float32_array`](digest.md) / [`consist.digest.canonicalize_float64_array`](digest.md) / [`consist.digest.canonicalize_float_array`](digest.md)
  (IEEE 754 canonicalization of floating-point arrays normalizing NaNs and `-0.0`)
- [`consist.digest.compute_array_entry_sha256`](digest.md)
  (compute deterministic digest for a single named array)
- [`consist.digest.normalize_logical_matrices_identity`](digest.md)
  (validate and normalize `sha256:logical_matrices:<64 hex>` string)
- [`consist.digest.resolve_canonicalization_policy`](digest.md)
  (resolve named profile string to `ArrayCanonicalizationPolicy`)
- [`consist.digest.ArrayCanonicalizationPolicy`](digest.md#consist.digest.ArrayCanonicalizationPolicy)
  (dataclass specifying numeric and structural canonicalization policy)

### Artifact Admission and Boundary Verification (consist.admission)

Policy-neutral evidence returned before executing model steps. Canonical imports are
from `consist.admission` (top-level `consist` aliases remain available for backward
compatibility with deprecation warnings). See [Artifact Admission](admission.md).

- [`consist.admission.AdmissionReport`](admission.md#consist.core.admission.AdmissionReport)
  (policy-neutral evidence returned by prior-run or declared-digest admission)
- [`consist.admission.AdmissionReference`](admission.md#consist.core.admission.AdmissionReference)
  (caller-proven runtime host/consumer path evidence for admission)
- [`consist.admission.PriorRunExpectation`](admission.md#consist.core.admission.PriorRunExpectation)
  (typed prior-run artifact expectation with optional direction constraint, `identity_scheme`, `logical_profile`, and external `source_tracker_db`)
- [`consist.admission.AdmissionIdentity`](admission.md#consist.core.admission.AdmissionIdentity)
  (validated `sha256:file:...`, `sha256:manifest-v1:...`, or `sha256:logical_matrices:...` identity)
- [`consist.admission.FileIdentity`](admission.md#consist.core.admission.FileIdentity)
  (validated `sha256:file:<64 lowercase hex>` raw-file identity)
- [`consist.admission.DeclaredDigestExpectation`](admission.md#consist.core.admission.DeclaredDigestExpectation)
  (typed locally declared identity with optional `identity_scheme`, `logical_profile`, and opaque source metadata)
- [`consist.admission.MatrixContainerSummary`](admission.md#consist.core.logical_array_identity.MatrixContainerSummary)
  (structural inspection summary for `.omx`/`.h5` and `.zarr` containers)
- [`consist.admission.ArtifactProvenanceSidecar`](admission.md#consist.core.logical_array_identity.ArtifactProvenanceSidecar)
  (standardized `.provenance.json` sidecar schema)
- [`consist.admission.admission_directory_identity`](admission.md#consist.core.logical_array_identity.admission_directory_identity)
  (compute a deterministic `sha256:manifest-v1:<64 hex>` digest for a directory artifact)
- [`consist.admission.inspect_matrix_container`](admission.md#consist.core.logical_array_identity.inspect_matrix_container)
  (inspect structural metadata of an `.omx`/`.h5` or `.zarr` matrix container without loading payloads)
- [`consist.admission.write_artifact_provenance_sidecar`](admission.md#consist.core.logical_array_identity.write_artifact_provenance_sidecar) / [`consist.admission.read_artifact_provenance_sidecar`](admission.md#consist.core.logical_array_identity.read_artifact_provenance_sidecar)
  (write and read standardized `.provenance.json` sidecars)
- [`consist.admission.check_artifact_identity`](admission.md#consist.core.admission.check_artifact_identity)
  (compare one resolved file or container with an exact artifact from a completed run)
- [`consist.admission.check_prior_run_expectation`](admission.md#consist.core.admission.check_prior_run_expectation)
  (compare one resolved file or container against a `PriorRunExpectation`)
- [`consist.admission.check_admission_reference`](admission.md#consist.core.admission.check_admission_reference)
  (compare a runtime-resolved file or container without recreating consumer path logic)
- [`consist.admission.check_admission_reference_prior_run_expectation`](admission.md#consist.core.admission.check_admission_reference_prior_run_expectation)
  (compare a runtime-resolved file or container against a `PriorRunExpectation` while retaining path evidence)
- [`consist.admission.check_expected_identity`](admission.md#consist.core.admission.check_expected_identity)
  (compare one resolved file or container with a declared digest and no Tracker lookup)
- [`consist.admission.check_admission_reference_expected_identity`](admission.md#consist.core.admission.check_admission_reference_expected_identity)
  (compare runtime-resolved declared-digest input while retaining path evidence)

### GTFS Transit Integration (consist.gtfs)

GTFS feed schemas, snapshots, and pure member discovery. Canonical imports are from
`consist.gtfs` (top-level aliases on `consist` remain available for backward compatibility
with deprecation warnings). Note that workflow methods `Tracker.ingest_gtfs(...)`,
`Tracker.canonicalize_gtfs(...)`, and `consist.canonicalize_gtfs(...)` remain on the
tracker and workflow surfaces. See [GTFS Support](../integrations/gtfs.md).

- `consist.gtfs.GtfsCanonicalizationResult` and `consist.gtfs.GtfsFeedSnapshot`
  (GTFS selected-service identity results)
- `consist.gtfs.canonicalize_gtfs_bundle` and `consist.gtfs.discover_gtfs_members`
  (pure GTFS helpers for dependency-light inspection and identity calculation)
- GTFS schema models: `consist.gtfs.GtfsAgency`, `consist.gtfs.GtfsStops`,
  `consist.gtfs.GtfsRoutes`, `consist.gtfs.GtfsTrips`, `consist.gtfs.GtfsStopTimes`,
  `consist.gtfs.GtfsCalendar`, `consist.gtfs.GtfsCalendarDates`,
  `consist.gtfs.GtfsFareAttributes`, `consist.gtfs.GtfsFareRules`,
  `consist.gtfs.GtfsShapes`, `consist.gtfs.GtfsFrequencies`,
  `consist.gtfs.GtfsTransfers`, `consist.gtfs.GtfsPathways`,
  `consist.gtfs.GtfsLevels`, `consist.gtfs.GtfsFeedInfo`

### Execution contracts and options

- `consist.ArtifactSpec` / `consist.OutputArtifactSpec` (rich declared-output
  metadata for `Tracker.run(output_paths=...)`)
- `consist.FilenamePattern`, `consist.IntCapture`, and `consist.EnumCapture`
  (capture-aware output-set declarations for `OutputSet(include=...)`; the
  capture set is intentionally narrow in v1, and future typed captures can be
  added by design if there is demand)
- `consist.CacheOptions`, `consist.OutputPolicyOptions`, `consist.ExecutionOptions`
- `consist.BindingResult` (execution envelope for orchestrator-resolved scenario inputs)
- `consist.StepIdentity` (Scenario-owned resolved name, model, and strict
  callable contract for preflighted advanced bindings)
- `consist.ResolvedBindingBuilder` / `consist.ResolvedBinding`
  (advanced immutable execution contract for a previously verified artifact
  selection; `bind_tracked_artifact(...)` derives the trusted identity and
  locator from a Consist artifact; ordinary workflows should continue to use
  Coupler refs or `BindingResult`)
- `consist.H5ChildSpec` (typed child-artifact customization for HDF5 containers)

### Scenario / workflow helpers

- [`consist.scenario`](api_helpers.md#consist.api.scenario)
- [`consist.single_step_scenario`](api_helpers.md#consist.api.single_step_scenario)
- [`consist.define_step`](api_helpers.md#consist.api.define_step)
- [`consist.use_tracker`](api_helpers.md#consist.api.use_tracker)
- [`consist.output_dir`](api_helpers.md#consist.api.output_dir)
- [`consist.output_path`](api_helpers.md#consist.api.output_path)
- [`Tracker.run`](tracker.md#consist.core.tracker.Tracker.run)
- [`Tracker.trace`](tracker.md#consist.core.tracker.Tracker.trace)
- [`consist.run`](api_helpers.md#consist.api.run) (deprecated compatibility wrapper)
- [`consist.trace`](api_helpers.md#consist.api.trace) (deprecated compatibility wrapper)
- [`consist.start_run`](api_helpers.md#consist.api.start_run) (deprecated compatibility wrapper)
- [`ScenarioContext`](workflow.md#scenario-context) (returned by `consist.scenario(...)`)
  - `run_id`, `config`, `inputs`, `add_input`, `declare_outputs`, `require_outputs`, `collect_by_keys`, `run`, `map_runs`, `trace`
  - `run(..., binding=BindingResult(...))` for resolved orchestrator plans
    or `binding=ResolvedBinding(...)` for verified execution-exact plans
  - `map_runs(...)` for independent process-backed sweep rows
- [`RunContext`](workflow.md#run-context) (injected via `execution_options=ExecutionOptions(inject_context=True)`)
  - `run_dir`, `output_dir`, `output_path`, `inputs`, `load`, `log_artifact`, `log_artifacts`, `log_input`, `log_output`, `log_meta`, `capture_outputs`
- [`Coupler`](workflow.md#coupler) (available at `scenario.coupler`)

Scenario defaults like `name_template` and `cache_epoch` are configured via `consist.scenario(...)` and flow into `ScenarioContext.run(...)`.

### Parallel orchestration models

These models are returned by, or support, `ScenarioContext.map_runs(...)`:

- `consist.BatchResult`: aggregate result for a sweep/batch.
- `consist.RunSpecResult`: per-spec result, including `persisted_run_id` when a
  run record exists.
- `consist.BatchRunSpec`: serializable description of one child run.
- `consist.ExecutionAttempt`: status details for one backend attempt.
- `consist.ExecutionSpec` and `consist.PythonCallableTarget`: lower-level
  execution request objects used by the process backend.

### Utilities and Introspection

- [`StepContract`](step_contracts.md#consist.core.step_contracts.StepContract) (Resolved declarative metadata for a decorated step)
- [`resolve_step_contract`](step_contracts.md#consist.core.step_contracts.resolve_step_contract) (Resolve one decorated step without executing it)
- [`collect_step_contracts`](step_contracts.md#consist.core.step_contracts.collect_step_contracts) (Resolve a sequence of decorated steps with shared context)
- [`collect_step_schema`](../concepts/decorators-and-metadata.md#schema-introspection) (Extract outputs for Coupler schemas)
- [`ArtifactKeyRegistry`](../concepts/decorators-and-metadata.md#artifact-key-registries) (Manage consistent artifact keys)

### Artifact logging and loading

- [`consist.log_artifact`](api_helpers.md#consist.api.log_artifact)
- [`consist.log_artifacts`](api_helpers.md#consist.api.log_artifacts)
- [`consist.log_input`](api_helpers.md#consist.api.log_input)
- [`consist.log_output`](api_helpers.md#consist.api.log_output)
- [`consist.ingest`](api_helpers.md#consist.api.ingest)
- [`consist.log_dataframe`](api_helpers.md#consist.api.log_dataframe)
- [`consist.canonicalize_gtfs`](api_helpers.md#consist.api.canonicalize_gtfs)
- [`consist.load`](api_helpers.md#consist.api.load)
- [`consist.capture_outputs`](api_helpers.md#consist.api.capture_outputs)
- [`consist.get_artifact`](api_helpers.md#consist.api.get_artifact)
- [`consist.hydrate_run_outputs`](api_helpers.md#consist.api.hydrate_run_outputs) (recommended historical output recovery API)
- [`consist.hydrate_run_outputs_to_destinations`](api_helpers.md#consist.api.hydrate_run_outputs_to_destinations) (historical recovery to exact caller-chosen paths)
- [`consist.materialize_run_outputs`](api_helpers.md#consist.api.materialize_run_outputs) (aggregate compatibility wrapper)
- [`consist.materialize_artifact`](api_helpers.md#consist.api.materialize_artifact) (single-artifact recovery)
- [`consist.stage_artifact`](api_helpers.md#consist.api.stage_artifact) (low-level canonical input staging)
- [`consist.stage_inputs`](api_helpers.md#consist.api.stage_inputs) (low-level canonical input staging)
- [`consist.set_artifact_recovery_roots`](api_helpers.md#consist.api.set_artifact_recovery_roots)
- [`consist.archive_artifact`](api_helpers.md#consist.api.archive_artifact)
- [`consist.archive_run_outputs`](api_helpers.md#consist.api.archive_run_outputs)
- [`consist.archive_current_run_outputs`](api_helpers.md#consist.api.archive_current_run_outputs)
- [`consist.archive_run_output_files`](api_helpers.md#consist.api.archive_run_output_files)
- [`consist.register_artifact_facet_parser`](api_helpers.md#consist.api.register_artifact_facet_parser)
- [`consist.cached_output`](api_helpers.md#consist.api.cached_output)
- [`consist.cached_artifacts`](api_helpers.md#consist.api.cached_artifacts)
- [`consist.log_meta`](api_helpers.md#consist.api.log_meta)
- [`consist.current_run`](api_helpers.md#consist.api.current_run)
- [`consist.current_consist`](api_helpers.md#consist.api.current_consist)
- [`Tracker.define_step(...)`](tracker.md#consist.core.tracker.Tracker.define_step) (explicit tracker form)
- [`Tracker.materialize(...)`](tracker.md#consist.core.tracker.Tracker.materialize)

### Querying and views

- [`consist.view`](api_helpers.md#consist.api.view)
- [`consist.register_views`](api_helpers.md#consist.api.register_views)
- [`consist.find_run`](api_helpers.md#consist.api.find_run)
- [`consist.find_runs`](api_helpers.md#consist.api.find_runs)
- [`consist.find_latest_run`](api_helpers.md#consist.api.find_latest_run)
- [`consist.find_matching_run`](api_helpers.md#consist.api.find_matching_run)
- [`consist.find_matching_runs`](api_helpers.md#consist.api.find_matching_runs)
- [`consist.run_set`](api_helpers.md#consist.api.run_set)
- [`Tracker.run_set(...)`](tracker.md#consist.core.tracker.Tracker.run_set)
- [`RunSet`](runset.md#consist.runset.RunSet) and [`AlignedPair`](runset.md#consist.runset.AlignedPair)
- [`Tracker.find_latest_run(...)`](tracker.md#consist.core.tracker.Tracker.find_latest_run)
- [`Tracker.find_matching_run(...)`](tracker.md#consist.core.tracker.Tracker.find_matching_run)
- [`Tracker.find_matching_runs(...)`](tracker.md#consist.core.tracker.Tracker.find_matching_runs)
- [`Tracker.diff_runs(...)`](tracker.md#consist.core.tracker.Tracker.diff_runs)
- [`Tracker.get_child_artifacts(...)`](tracker.md#consist.core.tracker.Tracker.get_child_artifacts) / [`Tracker.get_parent_artifact(...)`](tracker.md#consist.core.tracker.Tracker.get_parent_artifact)
- [`Tracker.get_run_inputs(...)`](tracker.md#consist.core.tracker.Tracker.get_run_inputs) / [`Tracker.get_run_outputs(...)`](tracker.md#consist.core.tracker.Tracker.get_run_outputs)
- [`Tracker.get_run_config(...)`](tracker.md#consist.core.tracker.Tracker.get_run_config)
- [`Tracker.print_lineage(...)`](tracker.md#consist.core.tracker.Tracker.print_lineage)
- [`consist.run_query`](api_helpers.md#consist.api.run_query)
- [`consist.db_session`](api_helpers.md#consist.api.db_session)
- [`consist.pivot_facets`](api_helpers.md#consist.api.pivot_facets)
- Indexing helpers: [`consist.index_by_field`](indexing.md#consist.core.indexing.index_by_field), [`consist.index_by_facet`](indexing.md#consist.core.indexing.index_by_facet), plus [`RunFieldIndex`](indexing.md#consist.core.indexing.RunFieldIndex) / [`FacetIndex`](indexing.md#consist.core.indexing.FacetIndex)
- Views registry: `tracker.views` ([`ViewRegistry`](views.md#view-registry))
- Matrix utilities: [`Tracker.load_matrix(...)`](tracker.md#consist.core.tracker.Tracker.load_matrix), [`MatrixViewFactory`](matrix.md#matrix-views)
- Schema export: [`Tracker.export_schema_sqlmodel(...)`](tracker.md#consist.core.tracker.Tracker.export_schema_sqlmodel)

Run lookup helpers treat `stage` and `phase` as first-class run dimensions, so
you can filter historical runs with `consist.find_runs(...)`,
`consist.find_latest_run(...)`, or `Tracker.find_latest_run(...)` without
treating those values as opaque metadata.
Use `find_matching_run(...)` when restart or reuse code needs the latest run
matching a semantic target with optional `cache_epoch=` and `run_scope=`.

### Tracker methods

This section groups commonly documented public `Tracker` methods. If you're new
to Consist, start with the **Core** and **Logging/Loading** groups and reach for
**Advanced** only as needed.

#### Core lifecycle

- `begin_run`, `start_run`, `run`, `trace`, `scenario`, `end_run`
- `define_step`, `last_run`, `is_cached`, `cached_artifacts`, `cached_output`
- `suspend_cache_options`, `restore_cache_options`, `capture_outputs`, `log_meta`

#### Logging and loading

- `log_artifact`, `log_artifacts`, `log_input`, `log_output`, `log_dataframe`
- `load`, `materialize`, `ingest`

#### Querying and history

- `find_runs`, `run_set`, `find_run`, `find_latest_run`, `find_matching_run`,
  `find_matching_runs`, `get_latest_run_id`
- `find_artifacts`, `get_artifact`, `get_child_artifacts`, `get_parent_artifact`, `get_artifacts_for_run`
- `find_artifacts_by_params`, `get_artifact_kv`, `register_artifact_facet_parser`
- `get_run`, `get_run_config`, `get_run_inputs`, `get_run_outputs`
- `hydrate_run_outputs`, `hydrate_run_outputs_to_destinations`,
  `materialize_run_outputs`, `materialize_artifact`
- `stage_artifact`, `stage_inputs`
- `set_artifact_recovery_roots`, `register_artifact_recovery_copy`,
  `register_run_output_recovery_copies`, `archive_artifact`, `archive_run_outputs`,
  `archive_current_run_outputs`, `archive_run_output_files`
- `get_artifact_lineage`, `print_lineage`, `history`
- `diff_runs`, `get_config_facet`, `get_config_facets`, `get_run_config_kv`
- `get_config_values`, `get_config_value`, `find_runs_by_facet_kv`

#### Views and matrices

- `view`, `create_view`, `load_matrix`, `export_schema_sqlmodel`
- `netcdf_metadata`, `openmatrix_metadata`, `spatial_metadata`

#### Config canonicalization

- `canonicalize_config`, `canonicalize_gtfs`, `prepare_config`,
  `apply_config_plan`, `identity_from_config_plan`

#### Format-specific logging

- `log_h5_container`, `log_h5_table`, `log_netcdf_file`, `log_openmatrix_file`

`log_h5_container(...)` now supports `child_specs={...}` and
`child_selection="all" | "include_only"` so callers can either customize
discovered child artifacts or log only a selected subset of HDF5 dataset paths.
It also accepts parent recovery policy kwargs such as
`container_recovery_unit="parent_file"` and
`child_recovery_policy="descriptive_only"` so downstream recovery tools can
distinguish parent-file recovery from descriptive child-table artifacts.
`log_h5_table(...)` accepts the same child-level semantic metadata surface as
normal artifact logging: `description`, `facet`, `facet_schema_version`, and
`facet_index`.

### Advanced (power-user / lower-level)

These methods are still public, but are more low-level or easier to misuse.

- `engine`, `set_run_subdir_fn`, `run_artifact_dir`, `resolve_uri`
- `run_query`, `get_run_record`, `resolve_historical_path`, `load_input_bundle`
- `get_artifact_by_uri`, `get_run_artifact`, `load_run_output`, `find_matching_run`
- `on_run_start`, `on_run_complete`, `on_run_failed`

## Stable, but optional extras

These APIs are part of the public surface, but require extra dependencies.

- Ingestion helpers: [`consist.ingest`](api_helpers.md#consist.api.ingest) (install with `consist[ingest]`)
- Ibis query bridge: [`consist.ibis_connection`](api_helpers.md#consist.api.ibis_connection), [`consist.ibis_view`](api_helpers.md#consist.api.ibis_view), [`consist.ibis_grouped_view`](api_helpers.md#consist.api.ibis_grouped_view) (install with `consist[ibis]`)
- [`consist.integrations.containers`](../integrations/containers.md) (container execution + caching; requires Docker or Singularity)
- [`consist.integrations.dlt_loader`](../integrations/dlt_loader.md) (low-level ingestion integration; requires `consist[ingest]`)
