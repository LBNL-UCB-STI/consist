# Upgrading to Consist 0.5

Consist `0.5.0` introduces a cleaner provenance database layout that separates what a file contains (its **fingerprint**), how a step used it (its **role**), and where valid copies currently live on disk (its **known locations**). Existing `0.4.x` workflows continue to run with minimal or zero code changes after a one-command database upgrade.

## 1. Upgrading Your Provenance Database (`consist migrate`)

When Consist `0.5` opens a `0.4.x` DuckDB file directly, it stops and raises `DatabaseMigrationRequired` rather than modifying your file silently.

### Standard One-Command Upgrade

Run `consist migrate` on your `provenance.duckdb` file:

```bash
consist migrate provenance.duckdb
```

What `consist migrate` does:

1. **Out-of-place build:** Reads your `0.4.x` database read-only and builds a verified `0.5` database in a temporary staging file.
2. **Automatic backup:** Keeps your original `0.4.x` file untouched as `provenance.v1-backup-YYYY-MM-DD.duckdb` in the same directory before swapping the upgraded database into place.
3. **Verification gates:** Verifies run counts, artifact counts, ingested tables, and file fingerprints before completing the cutover.

You can preview what the upgrade will do without writing any files using `--dry-run`:

```bash
consist migrate provenance.duckdb --dry-run
```

### Fast Mode for Large Multi-Gigabyte Archives (`--quick`)

If your `0.4.x` database references terabytes of cold simulation outputs on shared storage and you want to upgrade in seconds without hashing cold files on disk right away, pass `--quick`:

```bash
consist migrate provenance.duckdb --quick
```

In `--quick` mode, `consist migrate` upgrades the database structure, captured table schemas, scenario coupler snapshots, and ingested DuckDB tables immediately without reading cold files from disk. Unverified file entries remain visible in lineage and queries as historical fingerprints until promoted with `consist promote`.

### Opt-In Automatic Upgrade on Startup

For automated pipelines or notebooks where you want `Tracker` to run `consist migrate` automatically the first time it encounters a `0.4.x` database:

```python
from consist import Tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    auto_migrate=True,  # or auto_migrate="quick"
)
```

Or set the environment variable before launching your workflow:

```bash
export CONSIST_AUTO_MIGRATE=1        # full verification
# or
export CONSIST_AUTO_MIGRATE=quick    # fast metadata-only upgrade
```

### Reversibility (`consist migrate undo`)

If you need to switch back to your `0.4.x` database backup at any point:

```bash
consist migrate undo provenance.duckdb
```

---

## 2. How Historical Runs Behave After `--quick` Migration

When you upgrade with `consist migrate --quick` (or when a historical file from `0.4.x` was already deleted from scratch storage before the upgrade):

- **Already-verified artifacts work immediately:** Any artifact that already had a full SHA-256 file fingerprint, a directory manifest, a logical matrix fingerprint, or an ingested DuckDB table in `0.4.x` is ready for cache hits and queries right away.
- **Fast-stat historical artifacts stay queryable but never cause false cache hits:** In `0.4.x`, fast hashing recorded file size and modification time rather than full content bytes. After `--quick` migration, those unverified historical entries remain visible in run history and lineage queries, but Consist marks their fingerprints as historical-only so they never produce an unsafe cache hit.

### Promoting Historical Artifacts Later (`consist promote`)

Whenever you want to promote historical artifacts to full content fingerprints (for example, once an archived directory is mounted), run `consist promote`:

```bash
# Promote a single artifact or all unverified artifacts from a run:
consist promote provenance.duckdb <artifact-id-or-run-id> --mount archive=/path/to/archive
```

When the restored files match the original `0.4.x` file statistics (or explicit `--path` bytes), Consist hashes the files, upgrades their fingerprints in place, and re-enables full cache matching and `ViewFactory` queries for the promoted run.

---

## 3. What's New in the 0.5 Python API (and What Stays Compatible)

### Symmetric `inputs={...}` and `outputs={...}` with `: Path` Annotations

In `0.5`, you can declare both inputs and outputs as `name -> path` dictionaries and annotate your function parameters with `: Path`. Consist injects local `Path` objects for both inputs and outputs and verifies that declared outputs exist when the function returns:

```python
from pathlib import Path
import pandas as pd
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw: Path, cleaned: Path, threshold: float = 0.5) -> None:
    df = pd.read_parquet(raw)
    df[df["value"] > threshold].to_parquet(cleaned, index=False)


result = tracker.run(
    fn=clean_data,
    inputs={"raw": "raw.parquet"},
    outputs={"cleaned": "cleaned.parquet"},
    config={"threshold": 0.5},
)
```

> [!NOTE]
> **Full Backward Compatibility:** Existing functions that return `dict[str, Path]` with `outputs=["cleaned"]` and `ExecutionOptions(input_binding="paths")` continue to work without modification.

### Direct `.path` and `.as_df()` on Every `Artifact`

Every `Artifact` handle returned in `result.outputs[...]` is now an immutable, picklable object with two primary conveniences:

- `art.path`: Resolves a live local `Path` to a valid copy of the file (automatically falling back to archived copies if a scratch file was moved or evicted).
- `art.as_df()`: Loads tabular data (Parquet, CSV, HDF5 table, or ingested DuckDB table) directly into a `pandas.DataFrame`.

```python
cleaned = result.outputs["cleaned"]
print(cleaned.path)
df = cleaned.as_df()
```

### Smarter `Coupler` Path Auto-Detection & Iteration History

`Coupler` now accepts `Path` or `str` values directly in `coupler.set("skims", path)` (automatically observing the file into an `Artifact`) and tracks per-name history across simulation loops:

```python
skims_path = coupler.path("skims")
prior_skims = coupler.previous("skims")
all_iterations = coupler.history("skims")
```

### Removal of `NoopTracker` and `enabled=False`

`NoopTracker`, `NoopScenarioContext`, `NoopCoupler`, `LocalRef`, `NoopArtifact`, and `noop_scenario` have been removed in `0.5.0`. Passing `enabled=False` to `consist.scenario(...)`, `consist.create_tracker(...)`, or top-level logging helpers now raises `ValueError` immediately (`enabled=True` continues to work for backward compatibility, and `consist.create_tracker(...)` is deprecated in favor of instantiating `Tracker(...)` directly).

For lightweight runs without a DuckDB database, instantiate a DB-less `Tracker`:

```python
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path=None)
```

### Analytical Queries Without SQLAlchemy Sessions (`query_df`, `query_rows`, `config_run_rows`)

Consist `0.5.0` removes `duckdb-engine`, `consist.db_session()`, and SQLAlchemy `Executable`-based `consist.run_query(...)`, routing all provenance and analytical SQL through a single native DuckDB connection:

- Replace raw SQL or DataFrame queries with `tracker.query_df(sql, params=...)` (or `consist.query_df(...)`).
- Replace ORM model queries with `tracker.query_rows(sql, params=..., model=MyModel)` (or `consist.query_rows(..., model=MyModel)`), which validates rows into Pydantic or `SQLModel` instances.
- Replace config-table SQLAlchemy joins with `consist.config_run_rows(MyConfigModel, link_table=MyConfigRunLink, where={"key": "sample_rate"}, tracker=tracker)` or `consist.pivot_facets(...)` (which now returns a `ParameterizedSQL(sql, params)` pair ready for `tracker.query_df(q.sql, q.params)`).
- Legacy dynamic ORM view helpers (`consist.view(...)`, `consist.register_views(...)`, `tracker.view(...)`, `tracker.register_views(...)`) now raise `DeprecatedViewsError` with migration guidance; create SQL hybrid views via `tracker.create_view("v_persons", "persons", schema_model=Person)` and query them with `tracker.query_df(...)`, `tracker.query_rows(..., model=Person)`, or `consist.api.ibis_view(tracker, model=Person)`.

```python
from consist import ParameterizedSQL, pivot_facets

df = tracker.query_df(
    "SELECT id, model_name FROM run WHERE status = ?",
    ["completed"],
)

q: ParameterizedSQL = pivot_facets(keys=["sample_rate", "seed"], namespace="sim")
facet_df = tracker.query_df(q.sql, q.params)
```

### Per-Run Recovery Manifests Moved to `consist.store.manifest`

Per-run store reconstruction helpers (`RunManifest`, `export_run_manifest`, `import_run_manifest`, `rebuild_store_from_manifests`) now live under `consist.store.manifest` (keeping `consist.json` as the human-readable run directory snapshot and importing without loading `duckdb` until store reconstruction runs):

```python
from consist.store.manifest import export_run_manifest, rebuild_store_from_manifests
```

---

## 4. Multi-Environment Safety

If you work across multiple Python or Conda environments on the same machine or HPC cluster:

- **Consist `0.4.2+`** checks the database schema version on startup and raises a clear `IncompatibleSchemaError` with upgrade instructions if you accidentally point an older environment at a migrated `0.5` database.
- **Consist `0.4.1` and earlier** will fail fast with a DuckDB `Binder Error` (or `Cannot insert into view "artifact_content"`) because `0.5` installs read-only compatibility views that block legacy writes from corrupting `0.5` provenance records. See [Troubleshooting](troubleshooting.md) if you encounter either message.
