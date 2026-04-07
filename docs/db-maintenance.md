# DB Maintenance Guide

Consist includes a `consist db` command group for operating and recovering the
provenance database safely.

Use these commands when you need to inspect health, purge stale runs, export or
merge shards, rebuild from JSON snapshots, or compact database storage.

## Quick Start

```bash
# Health checks
consist db inspect --db-path ./provenance.duckdb
consist db doctor --db-path ./provenance.duckdb

# Snapshot before risky operations
consist db snapshot --out ./snapshots/pre_purge.duckdb --db-path ./provenance.duckdb

# Safe purge flow
consist db purge RUN_ID --dry-run --db-path ./provenance.duckdb
consist db purge RUN_ID --delete-ingested-data --prune-cache --yes --db-path ./provenance.duckdb
```

## Command Coverage

| Command | Purpose | Key options |
|---|---|---|
| `consist db inspect` | Read-only health summary | `--json`, `--db-path` |
| `consist db doctor` | Read-only invariant diagnostics | `--json`, `--db-path` |
| `consist db snapshot` | Copy DB snapshot with sidecar metadata | `--out`, `--no-checkpoint`, `--json`, `--db-path` |
| `consist db rebuild` | Rebuild run/artifact state from JSON snapshots | `--json-dir`, `--mode minimal\|full`, `--dry-run`, `--json`, `--db-path` |
| `consist db compact` | Run `VACUUM` | `--json`, `--db-path` |
| `consist db export` | Export run subtree to shard DB | `--out`, `--no-children`, `--include-data`, `--include-snapshots`, `--dry-run`, `--json`, `--db-path` |
| `consist db merge` | Merge shard DB into canonical DB | `--conflict error\|skip`, `--include-snapshots`, `--dry-run`, `--json`, `--db-path` |
| `consist db purge` | Purge run lineage and optional ingested/global data | `--no-children`, `--dry-run`, `--delete-ingested-data`, `--prune-cache`, `--delete-files`, `--yes`, `--json`, `--db-path` |
| `consist db fix-status` | Correct run status transitions | `--reason`, `--force`, `--json`, `--db-path` |

## Rebuild Modes: `db rebuild --mode minimal|full`

### `minimal`
- Restores baseline provenance tables (`run`, `artifact`, `run_artifact_link`).
- Restores canonical run dimensions such as `stage` and `phase` when they are
  present in the JSON snapshot metadata, while keeping the legacy `run.meta`
  mirror for compatibility.
- Best for core recovery when optional index tables are not required.

### `full`
- Includes `minimal`, plus best-effort restoration of optional metadata/index tables
  from snapshot content where available:
  - `run_config_kv`
  - `artifact_kv`
  - `artifact_schema`
  - `artifact_schema_observation`
- Restoration is compatibility-gated against table schema and intentionally
  best-effort. Missing optional structures are skipped rather than treated as
  fatal rebuild errors.
- This is the supported restore path for historical snapshots; it rebuilds the
  DB from JSON run records rather than depending on a separate
  `Run.from_snapshot()` API.

Example:

```bash
consist db rebuild --json-dir ./consist_runs --mode minimal --db-path ./provenance.duckdb
consist db rebuild --json-dir ./consist_runs --mode full --db-path ./provenance.duckdb
```

## Merge Compatibility: `db merge --conflict error|skip`

Merge validates run collisions and global-table compatibility.

- `--conflict error`:
  - aborts merge on run ID conflicts
  - aborts merge on incompatible `global_tables.*` schemas
- `--conflict skip`:
  - skips conflicting runs
  - skips incompatible global tables and merges remaining compatible data

### Warning surfaces

- Human-readable mode (default):
  - prints warnings for skipped `unscoped_cache` tables
  - prints warnings for skipped incompatible global tables with reasons
- JSON mode (`--json`):
  - includes `unscoped_cache_tables_skipped`
  - includes `incompatible_global_tables_skipped`

Examples:

```bash
consist db merge shard.duckdb --conflict error --json --db-path ./provenance.duckdb
consist db merge shard.duckdb --conflict skip --db-path ./provenance.duckdb
```

## Purge + Cache Pruning: `db purge --prune-cache`

`--prune-cache` is a targeted cleanup mode for `unscoped_cache` tables.

### Behavior contract

- `--prune-cache` only applies when `--delete-ingested-data` is enabled.
- Pruning only attempts tables with derivable references.
- Pruning only removes cache rows that are no longer referenced by surviving
  run-link rows.

### Explicit precondition (important)

`content_hash` values must represent the same identity domain across the
participating derivable `run_link` tables and `unscoped_cache` tables.

If this equivalence does not hold, pruning can be semantically incorrect and
should not be enabled.

### Derivable references

Pruning runs only when all required reference paths are available:

- at least one `run_link` table containing both `run_id` and `content_hash`
- at least one `unscoped_cache` table containing `content_hash`

### Skip / no-op semantics

- If references are not derivable, `--prune-cache` becomes a no-op.
- If `--delete-ingested-data` is not enabled, `--prune-cache` is also a no-op.
- In both cases, normal purge behavior for core provenance tables still applies.

Example:

```bash
consist db purge RUN_ID --delete-ingested-data --prune-cache --yes --db-path ./provenance.duckdb
```

## Terminology

- `run_scoped`: global table includes `consist_run_id`
- `run_link`: global table includes `run_id` (without `consist_run_id`)
- `unscoped_cache`: global table includes neither `consist_run_id` nor `run_id`
- derivable references: enough table/column structure exists to safely infer
  cache row reachability from surviving run-link rows
- skip/no-op: operation intentionally performs no cache-row deletion due to
  safety constraints
