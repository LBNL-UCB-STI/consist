# Test Suite Guide

This guide explains how Consist tests are organized, how to run the right subset
quickly, and where to add new coverage.

For test quality standards and CI guardrails, see
`/Users/zaneedell/git/consist/tests/TESTING_POLICY.md`.

## Test Organization

### Unit tests (`tests/unit/`)

Fast, isolated tests for core logic and branch behavior.

- `tests/unit/core/`: core modules (`tracker`, `workflow`, `materialize`,
  `spatial_views`, `persistence` error paths, etc.)
- `tests/unit/cli/`: CLI command and shell parsing/guard behavior
- `tests/unit/integrations/`: adapter and ingestion unit branches
- `tests/unit/containers/`: container API/backend helpers

### Integration tests (`tests/integration/`)

Cross-module behavior with database/filesystem interactions.

- `tests/integration/artifacts/`
- `tests/integration/tracker/`
- `tests/integration/workflow/`
- `tests/integration/activitysim/`
- `tests/integration/beam/`
- `tests/integration/containers/`

### End-to-end tests (`tests/e2e/`)

Minimal full workflow sanity checks.

### Stress and benchmarks

- `tests/stress/`: heavy concurrency/scale paths
- `tests/benchmarks/`: comparative performance tests

## Run Commands

Use the project venv Python:
`/Users/zaneedell/git/consist/.venv/bin/python`

```bash
# Fast local sanity
/Users/zaneedell/git/consist/.venv/bin/python -m pytest tests/unit

# Main CI-equivalent scope (non-heavy)
/Users/zaneedell/git/consist/.venv/bin/python -m pytest -m "not heavy" tests/unit tests/integration tests/e2e

# Focused subsystem examples
/Users/zaneedell/git/consist/.venv/bin/python -m pytest tests/unit/cli tests/integration/workflow
/Users/zaneedell/git/consist/.venv/bin/python -m pytest tests/unit/core/test_spatial_views.py tests/integration/artifacts/test_spatial_support.py
/Users/zaneedell/git/consist/.venv/bin/python -m pytest tests/unit/integrations/test_dlt_loader_error_paths.py tests/integration/artifacts/test_dlt_loader.py

# Coverage
/Users/zaneedell/git/consist/.venv/bin/python -m pytest tests/unit tests/integration tests/e2e --cov=src/consist --cov-report=term-missing
```

## Markers

- `heavy`: long-running tests (stress/benchmarks)
- `flaky`: retryable/unstable tests
- `timeout`: explicit timeout expectations

## Where To Add Tests

Use these examples as current entry points:

- Core behavior: `tests/unit/core/test_workflow.py`
- Materialization fallbacks: `tests/unit/core/test_materialize_error_paths.py`
- Persistence error/retry branches: `tests/unit/core/test_persistence_error_paths.py`
- Spatial metadata + security checks: `tests/unit/core/test_spatial_views.py`
- NetCDF view guard/fallback branches: `tests/unit/core/test_netcdf_views_error_paths.py`
- CLI guardrails: `tests/unit/cli/test_cli_search_validate.py`,
  `tests/unit/cli/test_cli_artifacts_lineage_guards.py`,
  `tests/unit/cli/test_cli_preview_shell_guards.py`
- dlt ingestion error/retry/contract branches:
  `tests/unit/integrations/test_dlt_loader_error_paths.py`

## Fixtures

From `tests/conftest.py`:

- `tracker`: isolated tracker with fresh DB schema per test
- `engine`: direct engine handle for DB-backed checks
- `cli_runner`: Typer runner with `get_tracker` patched to shared fixture tracker
- `sample_csv`: helper to create test CSV files
- `write_netcdf`: backend-aware NetCDF writer helper

## Current Priorities

- Continue coverage on `src/consist/core/netcdf_views.py` and
  `src/consist/core/noop.py` (lowest high-value core modules).
- Continue branch coverage in `src/consist/cli.py` (long command/shell tail).
- Keep strengthening negative-path testing for DB retry and ingestion contracts.

## Developer Tips

- Prefer small, deterministic unit tests with monkeypatch/stubs for branch-heavy
  logic.
- Keep integration tests focused on real cross-module behavior, not parser details.
- Run `pytest --collect-only` after creating/splitting files.
- Run Ruff before committing:
  - `/Users/zaneedell/git/consist/.venv/bin/python -m ruff check src tests`
  - `/Users/zaneedell/git/consist/.venv/bin/python -m ruff format --check src tests`
