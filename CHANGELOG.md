# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

## [0.1.4] - 2026-05-05

### Fixed

- Fix BEAM config reference discovery so multiline and long comma-separated
  scalar values are not probed as filesystem paths, and path-resolution errors
  fail closed during canonicalization.

## [0.1.3] - 2026-05-04

### Added

- Add verified recovery-copy registration APIs for artifacts copied by external
  infrastructure, including `register_artifact_recovery_copy(...)` and
  `register_run_output_recovery_copies(...)`. These helpers verify existing
  archive-side bytes before recording recovery roots
  by @zneedell ([#116](https://github.com/LBNL-UCB-STI/consist/pull/116)).
- Add structured config identity manifests for adapter-backed runs, persisted
  in `run.meta["config_identity_manifest"]` and exposed through
  `Run.identity_summary`
  by @zneedell ([#117](https://github.com/LBNL-UCB-STI/consist/pull/117)).
- Add manifest-first cache-miss diagnostics for adapter-backed config identity,
  including changed, added, and removed config references/files, reference
  status changes, and adapter identity option drift
  by @zneedell ([#117](https://github.com/LBNL-UCB-STI/consist/pull/117)).
- Add BEAM config reference policies and path alias support so scenario-specific
  paths, runtime/output locations, delegated artifact roots, and ignored dormant
  references can be represented explicitly in config identity
  by @zneedell ([#117](https://github.com/LBNL-UCB-STI/consist/pull/117)).

### Changed

- **Breaking for custom config adapters:** `ConfigAdapter.canonicalize(...)`
  implementations must now return `CanonicalizationResult(..., identity=...)`.
  The in-repo ActivitySim and BEAM adapters have been migrated to the structured
  identity contract
  by @zneedell ([#117](https://github.com/LBNL-UCB-STI/consist/pull/117)).
- Reorganize and simplify the documentation surface around a smaller
  reference-first structure, with clearer navigation, expanded API/CLI coverage,
  focused historical recovery guidance, and refreshed landing-page copy
  by @zneedell ([#115](https://github.com/LBNL-UCB-STI/consist/pull/115)).
- Reorder and refresh the example notebooks into a clearer learning path from
  quickstart through BEAM, parameter sweeps, iterative workflows, and
  transportation demand modeling, while updating examples to emphasize current
  preferred APIs such as run-managed outputs, matrix loading, materialization,
  Pydantic config objects, and requested input staging
  by @zneedell ([#119](https://github.com/LBNL-UCB-STI/consist/pull/119)).
- Refresh config adapter documentation, especially ActivitySim and BEAM adapter
  guidance, to match the structured identity, cache-miss, and path-staging
  surfaces
  by @zneedell ([#117](https://github.com/LBNL-UCB-STI/consist/pull/117),
  [#119](https://github.com/LBNL-UCB-STI/consist/pull/119)).
- Upgrade the docs generator dependency to `zensical>=0.0.39`
  by @zneedell ([#119](https://github.com/LBNL-UCB-STI/consist/pull/119)).

### Fixed

- Fix requested input staging validation so
  `ExecutionOptions(input_materialization="requested", input_paths={...})`
  respects the tracker’s configured artifact identity strategy, including
  `hashing_strategy="fast"`
  by @zneedell ([#118](https://github.com/LBNL-UCB-STI/consist/pull/118)).
- Fix docs homepage/version-menu contrast so versioned docs controls remain
  visible against the light header/background
  by @zneedell ([#114](https://github.com/LBNL-UCB-STI/consist/pull/114)).

## [0.1.2] - 2026-04-14

### Added

- Add `hydrate_run_outputs(...)` and `Run.hydrate_outputs(...)` for keyed
  historical output recovery, including per-output status/result reporting
  for restart, archive-mirror recovery, and cross-workspace reuse
  by @zneedell ([#101](https://github.com/LBNL-UCB-STI/consist/pull/101)).
- Add artifact recovery roots and archive helpers so historical outputs can be
  recovered after archival without minting new artifact identities, including
  `archive_artifact(...)`, `archive_run_outputs(...)`, and
  `archive_current_run_outputs(...)`
  by @zneedell ([#102](https://github.com/LBNL-UCB-STI/consist/pull/102)).
- Add first-class input staging for path-bound workflows via
  `ExecutionOptions(input_materialization="requested", input_paths={...})`,
  plus `stage_artifact(...)` and `stage_inputs(...)` helpers
  by @zneedell ([#106](https://github.com/LBNL-UCB-STI/consist/pull/106)).
- Add canonical parent/child artifact relationships, first-class parent/child
  query helpers, and typed HDF5 child selection/customization via
  `H5ChildSpec`
  by @zneedell ([#111](https://github.com/LBNL-UCB-STI/consist/pull/111)).

### Changed

- Clarify `Artifact.hash` as the canonical public artifact fingerprint while
  keeping `content_id` as an internal DB-local dedupe identity, with docs and
  lifecycle tests updated around that contract
  by @zneedell ([#110](https://github.com/LBNL-UCB-STI/consist/pull/110)).
- Tighten the top-level `consist.*` API around the `Tracker` boundary:
  `consist.run(...)`, `consist.trace(...)`, and `consist.start_run(...)` are
  now deprecated compatibility wrappers, while ambient logging/ingestion
  helpers remain supported
  by @zneedell ([#109](https://github.com/LBNL-UCB-STI/consist/pull/109)).
- Refactor tracker and persistence internals into focused service modules and
  tighten typing/error handling across the codebase to make future evolution
  safer without changing the public composition roots
  by @zneedell ([#107](https://github.com/LBNL-UCB-STI/consist/pull/107),
  [#108](https://github.com/LBNL-UCB-STI/consist/pull/108)).

### Fixed

- Fix detached-artifact replay/cache-reuse paths by cloning DB-loaded
  artifacts before applying overrides or attaching `content_id`, avoiding
  SQLAlchemy mutation failures during hydration and reuse
  by @zneedell ([#105](https://github.com/LBNL-UCB-STI/consist/pull/105)).

## [0.1.1] - 2026-04-08

### Added

- Add shared artifact content identity, reducing duplicate persistence work and
  deprecating output-row reuse semantics
  by @zneedell ([#86](https://github.com/LBNL-UCB-STI/consist/pull/86)).
- Add `MaterializationResult` and `materialize_run_outputs(...)` for cache-hit
  hydration and output materialization workflows
  by @zneedell ([#87](https://github.com/LBNL-UCB-STI/consist/pull/87)).
- Add split metadata/data store support, including hot-data store loaders and
  guardrails for single-store compatibility
  by @zneedell ([#88](https://github.com/LBNL-UCB-STI/consist/pull/88)).
- Add the `BindingResult` API for clearer access to resolved bound inputs and
  outputs, with accompanying docs and test coverage
  by @zneedell ([#89](https://github.com/LBNL-UCB-STI/consist/pull/89)).
- Add cache-miss explainer details for config, input, and code mismatches,
  with expanded docstrings and examples
  by @zneedell ([#92](https://github.com/LBNL-UCB-STI/consist/pull/92)).
- Add `facet={...}` filtering to `find_runs(...)` and `find_latest_run(...)`,
  plus scenario-level `step_tags` / `step_facet` defaults for child
  `run(...)` and `trace(...)` calls
  by @zneedell ([#93](https://github.com/LBNL-UCB-STI/consist/pull/93)).

### Changed

- Promote `stage` and `phase` to first-class run dimensions with query support
  in `find_runs(...)` and `find_latest_run(...)`, including the top-level
  `consist.find_latest_run(...)` helper. Legacy JSON metadata mirrors remain
  for compatibility, and DB rebuild paths restore canonical fields from
  snapshot metadata when available.
  by @zneedell ([#91](https://github.com/LBNL-UCB-STI/consist/pull/91)).
- Reduce DB overhead in hot runtime paths by batching repeated artifact
  persistence, parent-link creation, facet/schema writes, and preferring
  signature-first cache lookup with legacy fallback
  by @zneedell ([#94](https://github.com/LBNL-UCB-STI/consist/pull/94)).
- Clean up the docs surface, publish versioned docs with `mike`, and include
  `pyarrow` in the base install for a smoother default environment
  by @zneedell ([#90](https://github.com/LBNL-UCB-STI/consist/pull/90)).
- Expand the development and release-validation environment to include the
  optional dependencies exercised by the non-heavy BEAM, NetCDF, OpenMatrix,
  and Zarr test suites, and refresh the release runbook to sync that
  environment before validation.

### Fixed

- Prevent `consist db purge` from deleting files outside the active workspace
  by @zneedell ([#95](https://github.com/LBNL-UCB-STI/consist/pull/95)).
- Fix run-query ordering in CI and tighten docs/examples lint coverage
  by @zneedell ([#90](https://github.com/LBNL-UCB-STI/consist/pull/90)).
- Fix optional dependency import handling in NetCDF/OpenMatrix ingestion so a
  missing `zarr` install no longer disables `xarray`-backed ingestion paths.
- Make Docker `pull_latest=True` execution resilient to transient registry pull
  failures by falling back to the local image when available.

## [0.1.0] - 2026-03-11

### Added

- Add cache/debug ergonomics including `Run.identity_summary`,
  `RunResult.output_path(...)`, `Artifact.as_path(...)`, `Artifact.as_df(...)`,
  the top-level `runtime_kwargs={...}` alias, and the new domain-tracker guide
  by @zneedell ([#67](https://github.com/LBNL-UCB-STI/consist/pull/67),
  [#71](https://github.com/LBNL-UCB-STI/consist/pull/71)).
- Add `RunSet` for comparing and querying groups of runs from the public API
  by @zneedell ([#80](https://github.com/LBNL-UCB-STI/consist/pull/80)).
- Add end-to-end DB maintenance workflows, including snapshot/export support,
  merge compatibility checks, `consist db rebuild --mode minimal|full`, and
  `consist db purge --prune-cache`
  by @zneedell ([#74](https://github.com/LBNL-UCB-STI/consist/pull/74),
  [#76](https://github.com/LBNL-UCB-STI/consist/pull/76)).
- Add a clearer advanced/reference docs surface, including API pages for
  essentials vs. advanced usage and the execution-style decision tree
  by @zneedell ([#82](https://github.com/LBNL-UCB-STI/consist/pull/82),
  [#85](https://github.com/LBNL-UCB-STI/consist/pull/85)).

### Changed

- Establish a recommended-path docs/examples surface around
  `run(...)` / `trace(...)` / `scenario(...)`, while repositioning manual
  lifecycle and decorator-heavy patterns as advanced guidance
  by @zneedell ([#68](https://github.com/LBNL-UCB-STI/consist/pull/68),
  [#82](https://github.com/LBNL-UCB-STI/consist/pull/82),
  [#84](https://github.com/LBNL-UCB-STI/consist/pull/84),
  [#85](https://github.com/LBNL-UCB-STI/consist/pull/85)).
- Unify run/trace identity public surface around `adapter=` and
  `identity_inputs=` across `Tracker`, `ScenarioContext`, and top-level
  `consist` wrappers, with `trace(...)` routed through shared invocation
  resolution for parity with `run(...)`
  by @zneedell ([#67](https://github.com/LBNL-UCB-STI/consist/pull/67)).
- Remove deprecated `config_plan`/`hash_inputs` kwargs from run/trace/scenario
  and `define_step`, and add adapter-driven config override helpers
  (`run_with_config_overrides`, `get_config_bundle`, and new ActivitySim
  materialization/lookup helpers). Follow-up ergonomics now support
  `run_with_config_overrides(...)` runtime-kwarg auto-injection from
  materialized override roots (ActivitySim + BEAM), with
  `override_runtime_kwargs` customization and explicit runtime kwargs
  precedence by @zneedell
  ([#71](https://github.com/LBNL-UCB-STI/consist/pull/71)).
- Clarify explicit path/input binding and linked-input forms across the README,
  quickstart, tutorials, and notebooks
  by @zneedell ([#72](https://github.com/LBNL-UCB-STI/consist/pull/72),
  [#77](https://github.com/LBNL-UCB-STI/consist/pull/77),
  [#84](https://github.com/LBNL-UCB-STI/consist/pull/84)).
- Improve CLI discovery and interactive shell workflows, including safer
  preview/shell guards and clearer user guidance
  by @zneedell ([#83](https://github.com/LBNL-UCB-STI/consist/pull/83)).

### Fixed

- Restore canonical `unexpected keyword argument` error shape for rejected
  kwargs in run/scenario entrypoints
  by @zneedell ([#84](https://github.com/LBNL-UCB-STI/consist/pull/84)).
- Upgrade run/trace/scenario validation diagnostics to standardized
  `Problem` / `Cause` / `Fix` messages across identity/path/option failures,
  with matching tests and troubleshooting guidance
  by @zneedell ([#69](https://github.com/LBNL-UCB-STI/consist/pull/69)).
- Harden integration and maintenance edge cases across schema export/stub
  flows, adapter handoff, and DB repair/merge operations
  by @zneedell ([#70](https://github.com/LBNL-UCB-STI/consist/pull/70),
  [#75](https://github.com/LBNL-UCB-STI/consist/pull/75),
  [#76](https://github.com/LBNL-UCB-STI/consist/pull/76)).

## [0.1.0-beta.3] - 2026-02-19

### Added

- Functionally Linked Inputs + Ergonomic Migration Helpers
  by @zneedell ([#56](https://github.com/LBNL-UCB-STI/consist/pull/56)).

### Changed

- Manage output paths by @zneedell
  ([#55](https://github.com/LBNL-UCB-STI/consist/pull/55)).
- Consolidate `run(...)` policy kwargs into typed options objects
  by @zneedell ([#54](https://github.com/LBNL-UCB-STI/consist/pull/54)).
- Remove redundant artifact I/O in hashing and batch logging
  by @zneedell ([#63](https://github.com/LBNL-UCB-STI/consist/pull/63)).
- Duplication cleanup by @zneedell
  ([#65](https://github.com/LBNL-UCB-STI/consist/pull/65)).
- Pull out lifecycle, orchestration, and logging features
  by @zneedell ([#66](https://github.com/LBNL-UCB-STI/consist/pull/66)).
- Improve Consist CLI discovery, search/fix behavior, and interactive shell UX
  by @zneedell ([#59](https://github.com/LBNL-UCB-STI/consist/pull/59)).
- Unify run invocation resolution for tracker + scenario paths
  by @zneedell ([#62](https://github.com/LBNL-UCB-STI/consist/pull/62)).
- Docs update by @zneedell
  ([#61](https://github.com/LBNL-UCB-STI/consist/pull/61)).

### Fixed

- Improve DuckDB lock resilience for concurrent runs and expose global retry
  tuning knobs by @zneedell
  ([#57](https://github.com/LBNL-UCB-STI/consist/pull/57)).
- Improve test coverage and CLI reliability (spatial security checks,
  dlt/materialize branches, docs + CI guardrails)
  by @zneedell ([#60](https://github.com/LBNL-UCB-STI/consist/pull/60)).

## [0.1.0-beta.2] - 2026-02-12

### Added

- Add first-class artifact facets + shared facet core by @zneedell
  ([#52](https://github.com/LBNL-UCB-STI/consist/pull/52)).
- Add schema-family grouped hybrid views by @zneedell
  ([#53](https://github.com/LBNL-UCB-STI/consist/pull/53)).

### Changed

- Workflow Ergonomics & Metadata Resolution Refactor by @zneedell
  ([#51](https://github.com/LBNL-UCB-STI/consist/pull/51)).

## [0.1.0-beta.1] - 2026-02-04

### Added

- First public release of Consist, published as a stable beta and stress-tested
  in PILATES workflows.
- Provenance-first caching for scientific workflows (code + config + inputs).
- Multi-step scenarios with coupler-based artifact handoffs.
- SQL-native analysis via DuckDB (runs, artifacts, facets, lineage).
- Optional ingestion + hybrid views for cross-run queries.
- Container integration for external/legacy tools (Docker, Singularity).
- Portable paths and mounts for multi-machine workflows.
- CLI tools for inspection, lineage, and validation.

### Changed

- Focused beta effort on production-grade stability in real-world workflows and
  fixing low-hanging correctness/usability issues.
- Prioritized subtle edge cases, performance traps, and developer experience
  improvements before `0.1.0`.
- Beta API surface may evolve before the first non-beta release.
- Concurrent write workloads are still constrained by DuckDB locking behavior.
- Default cache hits are metadata-only unless hydration/materialization is
  requested.

### Fixed

- Improved robustness around hydration/materialization behavior, mount/path
  handling, and cross-platform test execution.

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/

[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

[Unreleased]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.4...HEAD

[0.1.4]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.3...v0.1.4

[0.1.3]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.2...v0.1.3

[0.1.2]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.1...v0.1.2

[0.1.1]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0...v0.1.1

[0.1.0]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.3...v0.1.0

[0.1.0-beta.3]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.2...v0.1.0-beta.3

[0.1.0-beta.2]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.1...v0.1.0-beta.2

[0.1.0-beta.1]: https://github.com/LBNL-UCB-STI/consist/releases/tag/v0.1.0-beta.1
