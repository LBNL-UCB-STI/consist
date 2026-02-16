# CLI Error Style Guide

This guide defines the error/exit behavior for Consist CLI commands.

## Message Structure

Use short, actionable messages:

1. `Error: <what failed>`
2. `Hint: <how to fix it>` (optional, when recovery is clear)

Examples:

- `Error: run_id required`
- `Error: Database not found at /path/to/provenance.duckdb`
- `Hint: Use --db-path or set CONSIST_DB`

## Exit Codes

- `0`: success
- `1`: runtime/data error (missing DB, missing artifact, failed load)
- `2`: usage/validation error (bad flag value, invalid argument shape)
- `130`: interrupted by user (`Ctrl+C`)

## Rules

- Keep messages specific to the failing input, key, or path.
- Prefer one clear hint over multiple speculative hints.
- Keep non-interactive output parseable; avoid extra noise around errors.
- Do not change successful output formats when adding new error handling.
