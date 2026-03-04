# CLI Reference

Consist provides command-line tools to inspect, query, and compare runs and artifacts without writing Python code. Use it to answer “what ran, with what inputs, and what changed?” directly from your provenance database.

This is especially useful when you are SSH’d into a remote server or working in a headless environment: you can quickly explore runs, artifacts, and lineage from the shell without starting a Python session. 

In particular, the `consist shell` command creates a persistent `Tracker` object linked to a database and allows you to query run inputs and outputs and artifact metadata, producing nicely formatted tables and summaries in the terminal.

## Database Discovery

The CLI looks for the provenance database in this order:
1. Explicit `--db-path` argument
2. `CONSIST_DB` environment variable
3. `provenance.duckdb` in the current directory
4. Common subdirectories (`./data/`, `./db/`, `./.consist/`)

## Global Options

```bash
consist --help
consist --version
```

---

## Commands

### consist db

Database maintenance and recovery commands.
See the dedicated guide: [DB Maintenance Guide](db-maintenance.md).

```bash
consist db inspect
consist db doctor
consist db snapshot --out snapshot.duckdb
consist db rebuild --json-dir ./consist_runs --mode minimal
consist db compact
consist db export <run_id> --out shard.duckdb
consist db merge shard.duckdb --conflict error
consist db purge <run_id> --dry-run
consist db fix-status <run_id> completed --reason "manual correction"
```

Operational recipes:

```bash
# 1) Health checks (inspect + doctor)
consist db inspect --db-path ./provenance.duckdb
consist db doctor --db-path ./provenance.duckdb

# 2) Safe purge preview + execute (with optional cache pruning)
consist db purge RUN_ID --dry-run --db-path ./provenance.duckdb
consist db purge RUN_ID --delete-ingested-data --prune-cache --yes --db-path ./provenance.duckdb

# 3) Rebuild from JSON snapshots (minimal vs full)
consist db rebuild --json-dir ./consist_runs --mode minimal --db-path ./provenance.duckdb
consist db rebuild --json-dir ./consist_runs --mode full --db-path ./provenance.duckdb

# 4) Merge conflict handling (error|skip)
consist db merge shard.duckdb --conflict error --json --db-path ./provenance.duckdb
consist db merge shard.duckdb --conflict skip --db-path ./provenance.duckdb
```

Notes:
- Merge conflict mode values are `error` and `skip`.
- `--prune-cache` only applies when `--delete-ingested-data` is enabled.

### consist runs

List recent runs with optional filters.

```bash
consist runs                          # Last 10 runs
consist runs --limit 20               # Last 20 runs
consist runs --model travel_demand    # Filter by model name
consist runs --status completed       # Filter by status
consist runs --tag simulation         # Filter by tag
consist runs --json                   # JSON output for scripting
```

### consist show

Display detailed information about a specific run.

```bash
consist show <run_id>
```

Shows run metadata, configuration, status, duration, and any custom metadata fields.

### consist artifacts

Inspect artifacts in two modes:

- **Run mode**: list input/output artifacts for one run.
- **Query mode**: search artifacts by indexed facet predicates.

```bash
consist artifacts <run_id>

# Query by artifact facet params (repeat --param)
consist artifacts --param beam.phys_sim_iteration=2
consist artifacts --param beam.year>=2030 --param beam.year<=2035

# Optional query filters
consist artifacts --param beam.phys_sim_iteration=2 --namespace beam
consist artifacts --param beam.phys_sim_iteration=2 --key-prefix linkstats
consist artifacts --param beam.phys_sim_iteration=2 --family-prefix linkstats_unmodified
consist artifacts --param beam.phys_sim_iteration=2 --limit 500
```

Query-mode options:

- `--param`: facet predicate (`key=value`, `key>=value`, `key<=value`)
- `--namespace`: default namespace when predicates omit one
- `--key-prefix`: prefix filter on artifact key
- `--family-prefix`: prefix filter on indexed `artifact_family` facet
- `--limit`: maximum results (default `100`)

### consist views create

Create a grouped hybrid view from schema identity + facet/run filters.

```bash
consist views create v_linkstats_all \
  --schema-id <schema_hash> \
  --namespace beam \
  --param artifact_family=linkstats_unmodified_phys_sim_iter_parquet \
  --param year=2018 \
  --attach-facet artifact_family \
  --attach-facet year \
  --attach-facet phys_sim_iteration \
  --driver parquet
```

Common options:

- `--mode hybrid|hot_only|cold_only`
- `--if-exists replace|error`
- `--missing-files warn|error|skip_silent`
- `--schema-compatible`
- run filters: `--run-id`, `--parent-run-id`, `--model`, `--status`, `--year`, `--iteration`

### consist lineage

Trace the full provenance chain for an artifact.

```bash
consist lineage <artifact_key_or_id>
```

Displays a tree showing which runs and inputs produced the artifact.

### consist scenarios

List all scenarios and their run counts.

```bash
consist scenarios
consist scenarios --limit 50
```

### consist scenario

Show all runs within a specific scenario.

```bash
consist scenario <scenario_id>
```

### consist search

Search runs by ID, model name, or scenario.

```bash
consist search "baseline"
consist search "travel" --limit 50
```

### consist summary

Display database statistics: total runs, artifacts, date range, and runs per model.

```bash
consist summary
```

### consist preview

Preview tabular artifacts (CSV, Parquet) without loading full data.

```bash
consist preview <artifact_key>
consist preview <artifact_key> --rows 10
consist preview <artifact_key> --trust-db  # Allow mount inference from DB metadata
```

### consist validate

Check that artifacts in the database exist on disk.

```bash
consist validate
consist validate --fix  # Mark missing artifacts
```

### consist shell

Start an interactive session for exploring provenance.

```bash
consist shell
consist shell --trust-db  # Allow mount inference from DB metadata
```

Inside the shell:
```
(consist) runs --limit 5
(consist) show abc123
(consist) artifacts abc123
(consist) scenarios
(consist) exit
```

---

## Scripting with JSON Output

Use `--json` for machine-readable output:

```bash
consist runs --json | jq '.[] | select(.status == "completed")'
```
