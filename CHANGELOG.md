# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

## [Unreleased]

### Added

- Add `consist db rebuild --mode minimal|full` behavior:
  `minimal` restores run/artifact/link baseline and `full` performs best-effort
  facet/schema/index restoration from JSON snapshots where possible
  by @zneedell (PR link TBD).
- Add `consist db purge --prune-cache` for safe, derivation-based pruning of
  unscoped cache rows no longer referenced by surviving run-link rows
  by @zneedell (PR link TBD).

### Changed

- Add explicit merge-time compatibility handling for `global_tables.*` during
  `consist db merge`: `--conflict error` aborts on incompatible schemas while
  `--conflict skip` skips incompatible tables with reporting in JSON and
  non-JSON output by @zneedell (PR link TBD).
- Document and enforce `--prune-cache` assumptions/no-op behavior:
  pruning requires derivable references and assumes `content_hash` equivalence
  across participating run-link/unscoped cache tables
  by @zneedell (PR link TBD).

## [0.1.0] - Unreleased

### Added

- Add `Run.identity_summary` for cache-identity debugging (hash components,
  adapter contribution, identity-input digests, and run identity fields)
  by @zneedell (PR link TBD).
- Add migration coverage tests for adapter handoff, run/trace
  identity kwargs parity, and identity-summary behavior
  by @zneedell (PR link TBD).
- Add one-line output accessors for run/artifact ergonomics:
  `RunResult.output_path(...)`, `Artifact.as_path(...)`, and
  `Artifact.as_df(...)` (with attached-tracker behavior and explicit-tracker
  fallback) by @zneedell (PR link TBD).
- Add top-level `runtime_kwargs={...}` alias on `run(...)`, normalized to
  `ExecutionOptions.runtime_kwargs` with explicit conflict validation
  by @zneedell (PR link TBD).
- Add a new "Building a Domain Tracker" guide and docs navigation entry,
  including wrapper patterns and direct-vs-wrapper guidance
  by @zneedell (PR link TBD).

### Changed

- Establish a recommended-path docs/examples surface around
  `run(...)` / `trace(...)` / `scenario(...)`, while repositioning manual
  lifecycle and decorator-heavy patterns as advanced guidance
  by @zneedell (PR link TBD).
- Unify run/trace identity public surface around `adapter=` and
  `identity_inputs=` across `Tracker`, `ScenarioContext`, and top-level
  `consist` wrappers by @zneedell (PR link TBD).
- Route `trace(...)` through shared invocation resolution for identity and
  options parity with `run(...)` (while preserving trace always-executes
  semantics) by @zneedell (PR link TBD).
- Shift config-adapter run/trace handoff to tracker-orchestrated adapter flow
  (ActivitySim/BEAM/public docs now use `adapter=` handoff)
  by @zneedell (PR link TBD).
- Update user-facing docs for the unified identity model (`identity_inputs`,
  adapter handoff, and `run.identity_summary`)
  by @zneedell (PR link TBD).
- Remove deprecated `config_plan`/`hash_inputs` kwargs from run/trace/scenario
  and `define_step`, and add adapter-driven config override helpers
  (`run_with_config_overrides`, `get_config_bundle`, and new ActivitySim
  materialization/lookup helpers). Follow-up ergonomics now support
  `run_with_config_overrides(...)` runtime-kwarg auto-injection from
  materialized override roots (ActivitySim + BEAM), with
  `override_runtime_kwargs` customization and explicit runtime kwargs
  precedence by @zneedell (PR link TBD).
- Clarify concise and explicit input-linking forms in docs/examples:
  `consist.refs(run_result)` / aliased refs mappings and bare-path vs labeled
  tuple forms for `identity_inputs`
  by @zneedell (PR link TBD).

### Fixed

- Restore canonical `unexpected keyword argument` error shape for rejected
  kwargs in run/scenario entrypoints by @zneedell (PR link TBD).
- Upgrade run/trace/scenario validation diagnostics to standardized
  `Problem` / `Cause` / `Fix` messages across identity/path/option failures,
  with matching tests and troubleshooting guidance
  by @zneedell (PR link TBD).

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

[Unreleased]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.3...HEAD

[0.1.0]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.3...v0.1.0

[0.1.0-beta.3]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.2...v0.1.0-beta.3

[0.1.0-beta.2]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.1...v0.1.0-beta.2

[0.1.0-beta.1]: https://github.com/LBNL-UCB-STI/consist/releases/tag/v0.1.0-beta.1
