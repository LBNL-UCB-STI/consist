# Testing Policy

This document defines quality expectations for tests in Consist.

## Scope and Test Types

- `unit`: branch-level, deterministic, isolated from external systems
- `integration`: cross-module behavior with DB/filesystem
- `e2e`: minimal workflow smoke checks
- `stress` / `benchmarks`: heavy, non-blocking for rapid iteration

## Required Test Behavior

- Every feature/change must include happy-path and at least one negative-path test.
- Branch-heavy logic should prefer unit tests with monkeypatch/stubs.
- Integration tests should validate realistic interactions, not implementation details.
- Tests must be order-independent and safe for repeated local/CI runs.

## Dependency Strategy

- Optional integrations should use explicit import handling:
  - `pytest.importorskip(...)` for integration tests that truly require extras.
  - Unit tests should mock/monkeypatch optional dependencies where practical.
- Never require production credentials or external services in CI unit/integration jobs.

## Coverage Guardrails

CI enforces overall and module-specific floors for key risk modules.

Current critical modules:

- `src/consist/cli.py`
- `src/consist/core/materialize.py`
- `src/consist/integrations/dlt_loader.py`
- `src/consist/core/spatial_views.py`

Thresholds should be ratcheted up over time, not lowered except with explicit rationale.

## Warnings Policy

- Third-party deprecation noise should be filtered where necessary.
- Project-origin warnings should remain visible and be treated as actionable.
- New tests should avoid introducing warning spam.

## Test Authoring Conventions

- Use descriptive test names (`test_<behavior>_<expected_outcome>`).
- Prefer explicit assertions over broad truthiness checks.
- Keep fixtures minimal and local unless broadly reusable.
- Use `tmp_path` for filesystem tests; never write repo-root artifacts.

## PR Expectations

- PRs that touch behavior should list test commands run.
- For risk-sensitive changes (persistence, ingestion, CLI), include targeted test commands.
- If coverage is reduced in a critical module, explain why and add follow-up plan.

## Local Developer Workflow

Recommended sequence before merge:

1. `ruff check src tests`
2. `ruff format --check src tests`
3. `pytest -m "not heavy" tests/unit tests/integration tests/e2e`
4. Optional focused coverage run for changed modules

Use:
`/Users/zaneedell/git/consist/.venv/bin/python -m ...`
for consistent local behavior.
