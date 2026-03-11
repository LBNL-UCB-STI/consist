# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

## [Unreleased]

## [0.1.0] - Unreleased

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

[Unreleased]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0...HEAD

[0.1.0]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.3...v0.1.0

[0.1.0-beta.3]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.2...v0.1.0-beta.3

[0.1.0-beta.2]: https://github.com/LBNL-UCB-STI/consist/compare/v0.1.0-beta.1...v0.1.0-beta.2

[0.1.0-beta.1]: https://github.com/LBNL-UCB-STI/consist/releases/tag/v0.1.0-beta.1
