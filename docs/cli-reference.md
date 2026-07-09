# CLI Reference

Consist provides command-line tools to inspect, query, and compare runs and artifacts without writing Python code. Use it to answer “what ran, with what inputs, and what changed?” directly from your provenance database.

This is especially useful when you are SSH’d into a remote server or working in a headless environment: you can quickly explore runs, artifacts, and lineage from the shell without starting a Python session.

Use the one-shot commands for quick inspection and scripts. Use
`consist shell` when you want a persistent `Tracker` object linked to a
database, cached run and artifact shortcuts, and repeated queries without
typing the same paths every time.

## CLI Quick Tour

The examples below use a tiny generated provenance database. The fixture is
rebuilt during docs generation, so the terminal output is produced by the real
CLI instead of copied by hand.

```bash exec="1" result="text"
python docs/fixtures/cli-demo/create_fixture.py # markdown-exec: hide
```

Start with a database summary to orient yourself.

```console exec="1" source="console" width="100" title="Summarize a provenance database"
$ consist summary --db-path docs/fixtures/cli-demo/.generated/provenance.duckdb
```

`summary` gives you the database shape before you pick individual runs: total
runs, artifact count, date range, and model names.

Then list recent runs to find the run ID you want to inspect.

```console exec="1" source="console" width="100" title="List recent runs"
$ consist runs --db-path docs/fixtures/cli-demo/.generated/provenance.duckdb --limit 3
```

Use the `ID` column as the handle for follow-up commands such as `show`,
`artifacts`, `lineage`, and run-scoped schema inspection.

Inspect one run's artifacts to see what it read and wrote. `demo_summarize`
consumes `cleaned` and writes `summary`, so the output shows both `Input` and
`Output` directions.

```console exec="1" source="console" width="100" title="Inspect run artifacts"
$ consist artifacts demo_summarize --db-path docs/fixtures/cli-demo/.generated/provenance.duckdb --run-dir docs/fixtures/cli-demo/.generated/runs
```

`Access: primary` means the stored artifact URI resolves to an existing file
under the supplied `--run-dir`.

For tabular outputs, preview a few rows without opening Python.

```console exec="1" source="console" width="100" title="Preview a tabular artifact"
$ consist preview cleaned --db-path docs/fixtures/cli-demo/.generated/provenance.duckdb --run-dir docs/fixtures/cli-demo/.generated/runs --rows 5
```

`preview` resolves the `cleaned` CSV under `--run-dir` and prints only the
requested rows.

## Common CLI Tasks

| Task | Start with | Notes |
| --- | --- | --- |
| Find recent runs | `consist runs` | Add `--model`, `--status`, or `--tag` when the database is busy. |
| Inspect one run's outputs | `consist artifacts <run_id>` | Use `--expand-sets` for logical output sets. |
| Preview data | `consist preview <artifact_key>` | Works for tabular artifacts such as CSV and Parquet. |
| Diagnose one input file | `consist admission doctor` | Compare it to one explicit input from a completed baseline run. |
| Script against provenance | `--json` | Supported by common inspection commands for `jq` or Python scripts. |
| Debug moved files | `--run-dir`, `--mount`, `--trust-db` | Keep explicit path settings authoritative; use trusted metadata fallback only when needed. |
| Explore repeatedly | `consist shell` | Caches recent run and artifact shortcuts inside the session. |

## Database Discovery

The CLI looks for the provenance database in this order:

1. Explicit `--db-path` argument
2. `CONSIST_DB` environment variable
3. `provenance.duckdb` in the current directory
4. Common subdirectories (`./data/`, `./db/`, `./.consist/`)

## Artifact Path Resolution

Artifacts may be stored with workspace-relative URIs such as
`workspace://outputs/table.parquet` or relative URIs such as `./outputs/...`.
For commands that inspect, load, validate, or profile artifact files
(`artifacts`, `preview`, `validate`, `schema capture-file`, and shell
`artifacts`/`preview`/`schema_profile`), Consist resolves paths with this
precedence:

1. Explicit `--mount NAME=PATH` values
2. Explicit `--run-dir` plus persisted `archive_mounts[NAME]` entries when
   `--trust-db` is enabled
3. Explicit `--run-dir`, used as the default `workspace://` root
4. Parent directory of `--db-path` for relative `./...` artifact URIs
5. `consist_runs/` next to `--db-path` for relative `./...` artifact URIs
6. Current working directory for relative `./...` artifact URIs
7. Stored execution mount snapshots, `_physical_run_dir`, and ordered
   `artifact.meta["recovery_roots"]` (**only** when `--trust-db` is enabled)

This keeps explicit CLI settings authoritative while still supporting
archived or moved run directories. Use `--trust-db` when you intentionally want
the CLI to fall back to recorded archive mounts, run directories, mount
snapshots, or recovery roots from the database. It never overrides an explicit
`--run-dir` or `--mount`.

Examples:

```bash
# Typical case: db and outputs are colocated (auto-discovery via --db-path parent)
consist preview trip_table --db-path examples/runs/beam_core_demo/beam_core_demo_demo.duckdb

# Archived/moved run root: explicitly point to the new location
consist preview trip_table \
  --db-path archives/beam_core_demo.duckdb \
  --run-dir /data/archive/beam_core_demo

# Promoted/merged DB: make the archive run directory authoritative for workspace:// artifacts
consist shell \
  --db-path /data/archive/provenance.duckdb \
  --run-dir /data/archive/beam_core_demo \
  --trust-db

# Allow fallback to metadata from the DB when no explicit path is available
consist preview trip_table --db-path archives/beam_core_demo.duckdb --trust-db

# Mount-backed artifacts can use explicit mounts or trusted DB mount snapshots.
# Explicit mounts override --run-dir and trusted DB metadata for that scheme.
consist preview trips --db-path archive.duckdb --mount inputs=/data/archive/inputs
consist preview trips --db-path archive.duckdb --mount workspace=/data/archive/beam_core_demo
consist validate --db-path archive.duckdb --trust-db
```

Projects that execute on local scratch and later inspect promoted archives can
persist archive-relative mount roots with `Tracker(archive_mounts=...)`. For
example, if execution logged `beam_input://seattle/lccm-long.csv` with
`archive_mounts={"beam_input": "beam/input"}`, then:

```bash
consist preview lccm_long \
  --db-path /data/archive/provenance.duckdb \
  --run-dir /data/archive/pilates-run \
  --trust-db
```

resolves the artifact under `/data/archive/pilates-run/beam/input/...` without
requiring `--mount beam_input=...`.

## Global Help

```console exec="1" source="console" width="100" title="CLI help output"
$ consist --help
```

Shell completion helpers:

```bash
consist --install-completion   # Install for current shell
consist --show-completion      # Print completion script
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

### consist admission doctor

Diagnose whether one resolved regular file matches exactly one named input on an
explicit, **completed** baseline run. This is a file-admission diagnostic: it
does not change run status, copy files, or apply policy on its own.

```bash
consist admission doctor \
  --db-path ./provenance.duckdb \
  --expected-run BASELINE_RUN_ID \
  --artifact-key gtfs_feed \
  --file ./inputs/gtfs.zip
```

The baseline must have exactly one `input` link with `--artifact-key`. When its
stored identity is an explicitly recorded full-file SHA-256, Consist compares
the supplied `--file` directly to it. Historical hashes and cache fingerprints
without those semantics are deliberately not promoted to admission identities.

For a historical baseline, provide `--expected-file` only when it is a distinct,
immutable copy of the expected bytes:

```bash
consist admission doctor \
  --db-path ./provenance.duckdb \
  --expected-run BASELINE_RUN_ID \
  --artifact-key gtfs_feed \
  --file ./inputs/gtfs.zip \
  --expected-file /archive/baselines/gtfs.zip
```

`--expected-file` must resolve to a different readable regular file than
`--file`. The command never treats the candidate path, a stored artifact URI,
or recovery-root lookup as expected bytes. It reports its verdict and digest
facts first, then supporting evidence. `--output report.json` writes the
canonical JSON report while leaving the human diagnostic on stdout.

By default, all structured outcomes (`verified`, `mismatched`, `unverified`,
and `unreadable`) exit successfully so callers can apply their own policy. Add
`--require-verified` when a script should exit 1 unless the result is
`verified`.

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
- Run mode includes an `Access` column that shows whether each artifact is
  reachable via its primary path or a recorded recovery root.

```bash
consist artifacts <run_id>
consist artifacts <run_id> --expand-sets
consist artifacts <run_id> --run-dir /path/to/archive_run_root
consist artifacts <run_id> --mount workspace=/path/to/archive_run_root
consist artifacts <run_id> --trust-db  # Metadata fallback only

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
- `--run-dir PATH`: explicit base directory for `workspace://` artifacts and
  relative artifact paths
- `--trust-db`: allow database metadata fallback for missing mounts,
  historical run dirs, and recovery roots; explicit `--run-dir` and `--mount`
  values take precedence
- `--mount NAME=PATH`: explicit mount override; repeat for multiple schemes.
  Explicit mounts override `--run-dir` and trusted DB metadata for that scheme.
- `--expand-sets`: show member artifacts for logical output sets. By default,
  run mode shows the parent output-set artifact and hides member/manifest rows.

### consist schema capture-file

Capture file schema metadata for an already-logged artifact so `schema export`
and shell `schema_stub` can generate SQLModel stubs.

```bash
# Capture by artifact key
consist schema capture-file --artifact-key trip_table

# Capture by UUID
consist schema capture-file --artifact-id 00000000-0000-0000-0000-000000000000

# Run-scoped key lookup + explicit path override
consist schema capture-file \
  --artifact-key trip_table \
  --run-id beam_2025_iter2 \
  --path /data/beam/outputs/trip_table.parquet
```

Options:

- `--sample-rows N`: rows to sample during inference (default `1000`)
- `--if-changed`: reuse a prior schema observation when the artifact hash is unchanged; this affects schema capture only, not artifact-row reuse
- `--run-dir PATH`: explicit base directory for `workspace://` artifacts and
  relative artifact paths
- `--trust-db`: allow database metadata fallback for missing mounts,
  historical run dirs, and recovery roots; explicit `--run-dir` and `--mount`
  values take precedence
- `--mount NAME=PATH`: explicit mount override; repeat for multiple schemes.
  For example, `--mount workspace=/data/archive/run` overrides the workspace
  root supplied by `--run-dir`.
- `--db-path`: explicit DB path

`schema capture-file` attaches an observation to an artifact's producing run. For
exogenous/input artifacts with no producing `run_id`, enable automatic run-time
profiling instead, for example `Tracker.run(..., profile_file_schema=True)`.

### consist schema export

Export a captured artifact schema as an editable SQLModel stub. Select exactly
one schema source by schema id, artifact id, or artifact key.

```bash
# Export by schema id to stdout
consist schema export --schema-id <schema_hash>

# Export the schema associated with an artifact key
consist schema export --artifact-key trip_table --class-name TripTable

# Write a concrete model stub to a file
consist schema export \
  --artifact-id 00000000-0000-0000-0000-000000000000 \
  --out models/trip_table_schema.py \
  --concrete
```

Options:

- `--schema-id`: captured artifact schema id/hash to export
- `--artifact-key` / `--table-key`: artifact key whose captured schema should be exported
- `--artifact-id`: artifact UUID whose captured schema should be exported
- `--out PATH`: write the generated stub to a file instead of stdout
- `--class-name NAME`: override the generated SQLModel class name
- `--table-name NAME`: override the generated `__tablename__`
- `--include-system-cols`: include ingestion/system columns such as `consist_*` and `_dlt_*`
- `--stats-comments` / `--no-stats-comments`: include or suppress profile comments
- `--abstract` / `--concrete`: export an abstract import-safe class or a concrete table model
- `--prefer-source file|duckdb`: prefer file or DuckDB schema profiles when no user-provided schema exists
- `--db-path`: explicit DB path

User-provided schema profiles are always preferred when present. Use
`--prefer-source` only to choose between generated file and DuckDB profiles.

### consist schema apply-fks

Apply physical foreign key constraints to the provenance database on a
best-effort basis.

```bash
consist schema apply-fks --db-path ./provenance.duckdb
```

Options:

- `--db-path`: explicit DB path

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

Preview tabular artifacts (CSV, Parquet) without loading full data. For logical
output sets, `preview` shows a set summary and member list instead of trying to
load the parent artifact as one table.

```bash
consist preview <artifact_key>
consist preview <artifact_key> --rows 10
consist preview --hash a1b2c3d4
consist preview <artifact_key> --run-dir /path/to/archive_run_root
consist preview <artifact_key> --mount workspace=/path/to/archive_run_root
consist preview <artifact_key> --trust-db  # Metadata fallback only
```

Hash lookup searches all artifacts by default. In shell workflows, use
`schema_stub --run-id <run_id> --hash <prefix>` when you want to narrow selection
to one run.

### consist validate

Check that artifacts in the database exist on disk.

```bash
consist validate
consist validate --fix  # Mark missing artifacts
consist validate --db-path examples/runs/beam_core_demo/beam_core_demo_demo.duckdb
consist validate --db-path archives/beam_core_demo.duckdb --run-dir /data/archive/beam_core_demo
consist validate --db-path archives/beam_core_demo.duckdb --mount workspace=/data/archive/beam_core_demo
consist validate --db-path archives/beam_core_demo.duckdb --trust-db
```

### consist shell

Start an interactive session for exploring provenance.

```bash
consist shell
consist shell --db-path examples/runs/beam_core_demo/beam_core_demo_demo.duckdb
consist shell --run-dir /data/archive/beam_core_demo
consist shell --mount workspace=/data/archive/beam_core_demo
consist shell --run-dir /data/archive/beam_core_demo --trust-db
```

In shell mode, `--run-dir` becomes the default `workspace://` root for routed
artifact commands. `--trust-db` may add missing mount or recovery metadata, but
it does not replace the shell's explicit `--run-dir` or `--mount` defaults.

Inside the shell:
```text
(consist) runs --limit 5
(consist) show #1
(consist) artifacts #1
(consist) preview @1
(consist) members @1
(consist) manifest @1
(consist) schema_profile @1
(consist) schema capture-file @1
(consist) schema_stub --run-id abc123 --hash a1b2c3d4
(consist) preview --hash a1b2c3d4
(consist) context
(consist) scenarios
(consist) exit
```

Useful shell shortcuts:

- `#<n>` refers to the nth cached run from the last `runs` output.
- `@<n>` refers to the nth cached artifact from the last `artifacts <run_id>` output.
- `schema capture-file @<n>` and `schema capture-file --artifact-ref @<n>` reuse
  cached shell artifact refs and route them to `--artifact-id`.
- `context` prints the active shell defaults for `db_path`, `run_dir`,
  `trust_db`, and mount overrides.
- Shell defaults are applied to routed `preview`, `validate`, and
  `schema capture-file` commands, so you do not need to repeat `--db-path`,
  `--run-dir`, `--trust-db`, or `--mount` inside the shell.
- `preview --hash <prefix>` and `schema_profile --hash <prefix>` search all
  artifacts by hash prefix.
- `schema_stub --hash <prefix> --run-id <run_id>` narrows hash lookup to one run
  when you want deterministic selection.
- `members @<n>` lists child artifacts for an output set. `manifest @<n>`
  previews its JSON manifest artifact.

Tips:

- Run `runs` first to populate `#<n>` run shortcuts.
- Run `artifacts <run_id>` or `artifacts #<n>` first to populate `@<n>` artifact
  shortcuts.

---

## Scripting with JSON Output

Use `--json` for machine-readable output:

```bash
consist runs --json | jq '.[] | select(.status == "completed")'
```
