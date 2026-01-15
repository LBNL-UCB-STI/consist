# CLI Reference

Consist provides command-line tools to inspect, query, and compare runs and artifacts without writing Python. Use it to answer “what ran, with what inputs, and what changed?” from your provenance database.

## Database Discovery

The CLI looks for the provenance database in this order:
1. Explicit `--db-path` argument
2. `CONSIST_DB` environment variable
3. `provenance.duckdb` in the current directory
4. Common subdirectories (`./data/`, `./db/`, `./.consist/`)

---

## Commands

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

List input and output artifacts for a run.

```bash
consist artifacts <run_id>
```

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
