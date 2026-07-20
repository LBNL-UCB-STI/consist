---
name: consist
description: Help with brand-new repos/projects being instrumented with Consist from scratch and downstream projects that use Consist for cache-aware scientific workflows. Use when Codex needs to set up or extend Consist in a new application or library, integrate an existing downstream project, choose between consist.run, tracker.run, scenario, and trace, wrap legacy file-writing or container-backed steps, make external files participate in cache identity, inspect or compare runs and artifacts, or debug cache misses, hydration, scenario wiring, or path and mount resolution without digging deeply into the Consist codebase.
---

# Consist

## Overview

Use this skill when a project already depends on Consist or when you are
instrumenting a new repo for the first time. Start from the documented public
APIs and CLI surfaces, not the internals. Read source only when the docs and
public surface do not explain the observed behavior or when a bugfix requires
implementation work inside Consist itself.

## Start Here

- If the project is not instrumented yet, read
  [`references/bootstrap.md`](references/bootstrap.md) first and identify the
  app entrypoint, dependency manager, first cacheable boundary, and where the
  tracker should live.
- Read [`references/instrumentation-patterns.md`](references/instrumentation-patterns.md)
  for workflow integration, API selection, wrapping patterns, and first-run
  instrumentation.
- Read [`references/debugging-patterns.md`](references/debugging-patterns.md)
  when an existing Consist integration behaves unexpectedly.
- Read [`references/analysis-patterns.md`](references/analysis-patterns.md)
  for run inspection, artifact loading, and cross-run comparison.

## Triage The Request

- Brand-new project instrumentation or first-time setup: start with the
  bootstrap reference, then the instrumentation reference.
- Existing workflow integration or feature work: start with the instrumentation
  reference.
- External orchestration that must replay an already-admitted artifact choice:
  read the advanced `ResolvedBindingBuilder` and step-identity guidance in the
  instrumentation reference; keep ordinary `inputs=`, `consist.ref(...)`,
  `consist.refs(...)`, and `BindingResult` as the default path.
- Existing workflow with cache misses, stale outputs, missing files, or weird
  scenario behavior: start with the debugging reference.
- Prior-run input conformance, requested-output hydration failures, or exact
  historical recovery problems: start with the debugging reference and use the
  public admission and recovery APIs before reading source.
- Run inspection, baseline vs policy comparison, or SQL analysis: start with
  the analysis reference.
- Multi-file logical outputs: use `OutputSet` guidance in the instrumentation
  reference. Run comparisons or cohorts are `RunSet` work and belong in the
  analysis reference.

## Default Working Style

- For new repos, identify where the tracker is created and wired before
  choosing the wrapper surface.
- Prefer `consist.run(...)`, `consist.trace(...)`, and `consist.scenario(...)`
  with `use_tracker(...)` in app, script, notebook, and test code.
- Prefer explicit `Tracker` ownership inside reusable libraries, wrappers, and
  framework integrations.
- Prefer `run(...)` for expensive deterministic steps and `trace(...)` for
  diagnostics or always-execute blocks.
- Keep domain code plain; let the Consist wrapper declare inputs, config,
  identity files, and output capture.
- Use the CLI for read-only inspection before writing one-off debug queries,
  especially on remote or headless systems.

## Guardrails

- Do not assume any existing Consist wiring, tracker bootstrap, or scenario
  structure exists in a new repo; verify or create it explicitly before
  choosing the API surface.
- Do not assume scenario header config automatically becomes step identity.
  Repeat identity-relevant config on the step itself.
- Treat `ResolvedBindingBuilder` and `scenario.resolve_step_identity(...)` as
  opt-in orchestration surfaces for execution-exact, already-admitted inputs;
  do not replace ordinary Coupler, `inputs=`, `consist.ref(...)`,
  `consist.refs(...)`, or `BindingResult` wiring with them by default.
- Do not hide true dependencies in `runtime_kwargs`, globals, or undeclared
  filesystem reads.
- Do not debug cached-output problems without checking `cache_hydration`,
  `cache_hydration_failure`, `output_paths`, `validate_cached_outputs`, and the
  artifact URI.
- Treat `cache_hydration_failure="miss"` as an opt-in all-or-miss policy for
  `cache_hydration="outputs-requested"`; it does not choose a recovery source
  or decide downstream restart policy.
- For exact historical recovery, use the caller-selected run and explicit
  destinations. Consist can validate and materialize bytes, but downstream
  code owns selection, staging, mount translation, and proving that a consumer
  actually read the staged path.
- Legacy Zarr and Shapefile artifacts without their immutable manifests fail
  closed for exact archive or hydration; re-log them under the current
  artifact contract instead of weakening validation.
- Do not treat `cache_hydration="inputs-missing"` as stale-input validation by
  default. Existing input destinations are preserved unless
  `validate_materialized_inputs=True` is explicitly enabled.
- Do not confuse `OutputSet` with `RunSet`: an `OutputSet` is one logical
  output made of many files; a `RunSet` is a selected group of runs for
  comparison or analysis.
- Do not turn the skill body into an API dump; load the references as needed.

## References

- [`references/bootstrap.md`](references/bootstrap.md): bootstrap a new repo,
  choose the first tracker surface, and verify the first cached run.
- [`references/instrumentation-patterns.md`](references/instrumentation-patterns.md):
  choose the right execution surface, integrate Consist into downstream code,
  bootstrap first-time instrumentation, wrap legacy or container-backed steps,
  and use advanced resolved bindings or exact historical output recovery only
  when those contracts are required.
- [`references/debugging-patterns.md`](references/debugging-patterns.md):
  debug cache identity, admission, hydration, path resolution, and scenario
  wiring from a downstream project.
- [`references/analysis-patterns.md`](references/analysis-patterns.md):
  inspect runs, load artifacts, query views, and compare baseline vs policy
  runs.
