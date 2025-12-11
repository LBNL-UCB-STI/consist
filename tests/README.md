# Test Suite Guide

This guide helps you run and extend Consist's test suite. This document explains the structure and conventions for tasks such as validating a change, investigating a failure, or adding coverage for a new feature.

---

## Test Organization

The test suite is organized by scope and purpose:

### Unit Tests (`tests/unit/`)

Fast, isolated tests of individual components. These verify correctness without dependencies on databases, filesystems, or external services.

- **`core/`**: Core abstractions (hashing, artifact models, workflow contexts, lineage resolution, path virtualization, Zarr metadata handling)
- **`cli/`**: Command-line interface commands (runs, artifacts, lineage, summary, preview)
- **`containers/`**: Container backend command generation and configuration hashing

### Integration Tests (`tests/integration/`)

Tests that verify how components work together, including database operations, filesystem interactions, and workflow orchestration.

- **`tracker/`**: Tracker lifecycle, caching behavior, resilience, tagging, access modes, hooks, bulk operations, foreign key constraints
- **`workflow/`**: Scenarios, task decorators, manual workflows, helper functions, global API functions
- **`artifacts/`**: Artifact loading, ingestion, view generation (CSV, HDF5, Zarr, matrix formats), data loader integration
- **`containers/`**: Container orchestration and caching workflows

### End-to-End Tests (`tests/e2e/`)

Complete workflow validation from initialization through querying. These tests ensure the entire system works together correctly but are kept minimal to avoid brittleness.

### Stress Tests (`tests/stress/`)

Performance, concurrency, and scale validation. These tests use realistic data volumes and workflow patterns to catch regressions that don't appear in smaller tests.

- **`tracker/`**: Concurrent run creation and artifact logging
- **`containers/`**: Multi-scenario simulation workflows with complex caching and hydration patterns
- **`ingestion/`**: Large-scale view optimization, post-hoc ingestion, schema evolution, typed analysis queries

### Benchmarks (`tests/benchmarks/`)

Comparative performance measurements for querying strategies (cold views vs. hot views, join operations, aggregations).

---

## Running Tests

### Common Patterns

```bash
# Run all tests
pytest

# Run only fast unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run specific subsystem
pytest tests/integration/artifacts

# Skip long-running tests
pytest -m 'not heavy'

# Run only stress tests and benchmarks
pytest tests/stress tests/benchmarks -m heavy

# Show what would run without executing
pytest --collect-only

# Re-run only failed tests from last run
pytest --last-failed

# Generate coverage report
pytest --cov=src/consist --cov-report=term-missing
```

### Test Markers

Tests are marked to enable selective execution:

- **`@pytest.mark.heavy`**: Long-running or large-data tests (stress tests, benchmarks). Skip these during rapid iteration with `-m 'not heavy'`.
- **`@pytest.mark.timeout(N)`**: Tests with explicit time limits, primarily for concurrency validation.

### Recommended Workflows

When working on a specific subsystem, run the relevant subset:

```bash
# Developing artifact loading
pytest tests/unit/core/test_artifacts.py tests/integration/artifacts

# Working on container integration
pytest tests/unit/containers tests/integration/containers -m 'not heavy'

# Validating tracker changes
pytest tests/unit/core/test_tracker.py tests/integration/tracker

# Before committing (fast validation)
pytest tests/unit tests/integration -m 'not heavy'

# Full validation including stress tests
pytest tests/stress tests/benchmarks -m heavy
```

---

## Available Fixtures

Standard fixtures are available across the test suite:

- **`tracker`**: Configured Tracker instance with temporary run directory and database
- **`run_dir`**: Temporary directory for test artifacts
- **`engine`**: DuckDB engine for database-backed tests

Standard pytest fixtures (`tmp_path`, `monkeypatch`, `caplog`) are also available.

---

## Adding New Tests

### Where to Add Coverage

Choose the appropriate test directory based on what you're validating:

**Unit tests** for pure logic that doesn't require external dependencies:
- New hashing strategies → `tests/unit/core/test_identity.py`
- Artifact model validation → `tests/unit/core/test_artifacts.py`
- Path resolution logic → `tests/unit/core/test_path_resolver.py`

**Integration tests** for cross-component workflows:
- Caching behavior → `tests/integration/tracker/test_caching.py`
- Workflow contexts → `tests/integration/workflow/test_scenarios.py`
- Data ingestion → `tests/integration/artifacts/test_ingestion.py`
- Container orchestration → `tests/integration/containers/test_container_integration.py`

**Stress tests** for performance validation:
- Large-scale ingestion → `tests/stress/ingestion/`
- Concurrent operations → `tests/stress/tracker/test_concurrency.py`
- Complex multi-scenario workflows → `tests/stress/containers/`

**End-to-end tests** should remain minimal. Only add these for critical user-facing workflows that aren't adequately covered by integration tests.

### Testing Best Practices

**Use temporary directories**: All file operations should use `tmp_path` or `run_dir` fixtures. Never write to the repository root during tests.

**Mock external dependencies**: Use `pytest.importorskip` for optional dependencies (h5py, Docker). Mock external systems unless you're explicitly testing integration with them.

**Prefer built-in fixtures**: Use `monkeypatch` for patching rather than adding dependencies like `pytest-mock`.

**Mark expensive tests**: Any test that takes more than a few seconds or uses large datasets should be marked with `@pytest.mark.heavy`.

**Verify test discovery**: After reorganizing tests or adding new files, run `pytest --collect-only` to ensure pytest can find everything.

---

## Current Gaps and Placeholders

The following areas have placeholder test files or known coverage gaps:

**Decorator edge cases** (`tests/unit/core/test_decorators.py`):
- `depends_on` with missing files
- `capture_pattern` recursion behavior
- Unsupported return types
- Missing `capture_dir` directories

**Output capture validation** (`tests/integration/artifacts/test_capture_outputs_edges.py`):
- Behavior when capture directory doesn't exist
- Recursive capture patterns
- Cache hydration with captured outputs

**Database resilience** (`tests/integration/tracker/test_persistence_retries.py`):
- Retry logic in DatabaseManager
- Constraint violation handling

**CLI query helpers** (`tests/unit/cli/test_cli.py`):
- Pagination behavior
- Error message formatting

If you encounter failing tests in these areas, consider expanding coverage before debugging.

### Potential Refactorings

Some test files have grown large and could be split for clarity:

- `tests/stress/containers/test_pilates_simulation_containers.py`: Separate cache-focused tests from API validation
- `tests/integration/tracker/test_tracker_hooks_and_bulk_upload.py`: Split hook tests from bulk logging tests

These refactorings are not urgent but would improve navigability.

---

## Troubleshooting

**Tests fail with import errors**: Ensure you've installed the package in development mode (`pip install -e .`) and all optional dependencies are available.

**Stress tests time out**: These tests use realistic data volumes and may be slow on some machines. Consider running them separately or increasing timeout values.

**Database locks in concurrent tests**: Some tests intentionally stress concurrent access. If you see deadlocks, check that you're not running multiple test processes against the same database file.

**Coverage gaps**: Run `pytest --cov=src/consist --cov-report=html` to generate an interactive coverage report showing which lines are untested.