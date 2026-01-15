# Schema Export (SQLModel Stubs)

Consist can capture the observed (post-ingest) schema of tabular artifacts and export that schema as a **static SQLModel class stub** you can commit and edit. This is intended to reduce “retype the table definition” friction while keeping Consist honest about what it can and cannot infer.

## What You Get

- A Python 3.11 file containing one SQLModel class stub (exported as `table=True`).
- Columns with:
  - a best-effort type mapping from DuckDB logical types
  - nullability (`Optional[...] = None` only when nullable)
  - deterministic ordering (prefers `ordinal_position`)
- “Hints” as comments (enums/stats) when available (on by default).
- Messy column names are handled: the generated attribute name is sanitized, but the original DB column name is preserved via `Field(sa_column=Column("original", ...))` when needed.

## Abstract vs Concrete Exports

By default, Consist exports SQLModel classes as **abstract** (`__abstract__ = True`). This keeps the stub **importable immediately**, because SQLAlchemy requires a primary key to map a concrete table/view, and many analysis tables don’t have an obvious primary key.

- **Default (recommended):** abstract export, safe to import, great for view schemas.
- **Concrete export (advanced):** export with `--concrete` (or `abstract=False` in Python) and then add a primary key before importing, otherwise SQLAlchemy will raise an error like “could not assemble any primary key columns”.

## What You Still Do Manually

Consist does not infer semantic intent. You typically edit the stub to add:

- primary keys / foreign keys
- indexes / uniqueness constraints
- relationships
- renames / normalization decisions

## Prerequisites

Schema export uses the schema captured from a DuckDB-ingested table. In practice:

1. You run with a database configured (`db_path=...`).
2. You ingest tabular data into DuckDB (hot data).
3. Consist profiles the ingested table and stores a deduped schema referenced by `artifact.meta["schema_id"]`.

If you already see `schema_id` in an artifact’s `meta`, you’re ready to export.

You can also capture schemas **without ingestion** for CSV/Parquet artifacts by enabling lightweight file profiling at log time (`profile_file_schema=True`, optional `file_schema_sample_rows=`). This writes the same `schema_id` pointer into `artifact.meta`, allowing schema export even if the original file is later missing or moved.

## Exporting from the CLI

Export by artifact UUID (recommended UX for now):

```bash
python -m consist.cli schema export \
  --artifact-id 00000000-0000-0000-0000-000000000000 \
  --out your_pkg/models/persons.py
```

Or export by schema id (hash):

```bash
python -m consist.cli schema export \
  --schema-id <schema-hash> \
  --out your_pkg/models/persons.py
```

Useful flags:

- `--class-name Persons` to override the generated class name
- `--table-name persons` to override `__tablename__`
- `--include-system-cols` to include system/ingestion columns like `consist_*` and `_dlt_*`
- `--no-stats-comments` to omit stats/enum hint comments
- `--concrete` to export a non-abstract mapped class (you must add a primary key)

If `--out` is omitted, the stub is printed to stdout (so it can be piped or redirected).

## Exporting from Python

You can generate the code (and optionally write it) from a `Tracker`:

```python
from pathlib import Path
from consist import Tracker

tracker = Tracker(run_dir=".", db_path="provenance.duckdb")

code = tracker.export_schema_sqlmodel(
    artifact_id="00000000-0000-0000-0000-000000000000",
    out_path=Path("your_pkg/models/persons.py"),
    abstract=True,
)
```

The method returns the generated string regardless of whether you write it.

## Where to Put the Generated File

Place it anywhere importable by your project (on `PYTHONPATH`), for example:

- `your_pkg/models/`
- `your_pkg/schemas/`

If it’s a package directory, include an `__init__.py` so you can `import your_pkg.models.persons`.

Consist does not require a special directory layout; it only needs you to import the class and register it for views.

## Using the Generated Model with Views

Consist’s hybrid views are created from your SQLModel schema. To activate a view:

1) Import the model class.
2) Register it with the `Tracker` (at init time or after).

### Register at Tracker initialization

```python
from consist import Tracker
from your_pkg.models.persons import Persons

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb", schemas=[Persons])
VPersons = tracker.views.Persons
```

### Register later

```python
from your_pkg.models.persons import Persons

VPersons = tracker.view(Persons)  # registers + creates/refreshes the hybrid view
```

### Querying

```python
from sqlmodel import select
import consist

VPersons = tracker.views.Persons
rows = consist.run_query(select(VPersons).limit(5), tracker=tracker)
```

Views automatically include Consist metadata columns (e.g. `consist_run_id`, `consist_year`, `consist_scenario_id`) for filtering and grouping.

## Column Name Rules (Important)

The generated stub aggressively normalizes attribute names to reduce runtime errors:

- invalid characters become `_`
- leading digits are prefixed with `_`
- Python keywords get a trailing `_`
- collisions become `__2`, `__3`, ...

When the attribute name differs from the real column name, the stub uses:

```python
mass_kg: float | None = Field(default=None, sa_column=Column("Mass(kg)", ...))
```

This matters for views and empty-view typing: Consist now uses the **original DB column name** when creating typed empty views, so schemas with renamed attributes still behave correctly.

## Matching Artifacts, Tables, and `__tablename__`

For views to “pick up” your data, the model’s `__tablename__` should match the concept key you are querying:

- for ingested (“hot”) data, this is typically the DuckDB table name used at ingest time
- for file-based (“cold”) data, this is typically the `Artifact.key`

If you want your curated model name to differ from the ingestion key, you can:

- override `--table-name` / `table_name=...` during export, or
- set `__tablename__ = "..."` manually after editing

## System and Ingestion Columns

By default, exported stubs omit system/ingestion columns:

- `consist_*` (Consist provenance columns)
- `_dlt_*` (common ingestion/system columns)

Use `--include-system-cols` if you need a faithful “as-ingested” representation.

## Notes on Wide / Sparse Tables

Very wide tables can cause the stored JSON profile to truncate. Consist still persists per-field rows so schema export continues to work, and emits a warning when truncation occurs.

If your upstream workflow produces sparse wide tables, consider reshaping to a long format before ingestion (or as a dedicated transformation step). Future Consist work may add first-class “pre/post ingestion transforms”, but schema export is designed to work even when the JSON blob is truncated.
