# Data Virtualization & Materialization Strategy

This guide explains when to keep data on disk (virtualize) versus ingesting it into DuckDB (materialize).

---

## Why the Distinction Matters

Simulation workflows produce many artifacts across runs. Comparing results requires querying across runs—but ingesting every output doubles your database size. The cold/hot distinction lets you choose: keep storage efficient for artifacts you rarely query, enable SQL analysis for artifacts you compare frequently.

The tradeoff: ingested data is queryable and recoverable; cold data requires keeping original files but adds no database overhead.

---

## Two Storage Modes

**Cold Data**: Files remain on disk. Provenance metadata is tracked but data is not queryable via SQL until ingested. Load on demand with `consist.load_df(artifact)`. Minimal database size.

**Hot Data**: Ingested into DuckDB. Queryable via SQL, indexed alongside provenance, and recoverable if original files are deleted. Trades disk space for query performance.

---

## When to Ingest

Ingest when you need:

- SQL-native analytics over many runs
- Schema tracking and exportable SQLModel stubs
- Recovery of tabular data if files are missing
- Cross-run comparisons via unified SQL views

Ingest key tabular outputs you compare across runs:

- **Transportation**: Trip tables, skim matrices, mode choice summaries
- **Electric grid**: Aggregated metrics (monthly averages, distribution-level summaries)
- **Urban planning**: Parcel or zoning summaries, development capacity estimates

Ingestion is appropriate for outputs where (1) you need SQL queries across runs, and (2) the storage cost is acceptable—ingestion approximately doubles storage for that artifact. For a 500MB output compared across 100 runs, ingestion adds ~50GB to the database.

---

## When to Stay Cold

Keep data as cold files when:

- Storage duplication is unacceptable (multi-GB rasters, video files, binary blobs)
- Data already lives in an external system
- No cross-run SQL analysis is needed

Cold data lives only on disk; provenance metadata is tracked but data is not queryable via SQL or included in hybrid views until ingested.

---

## Ingesting Artifacts

Install the optional DLT dependency:

```bash
pip install "consist[ingest]"
```

Basic usage:

```python
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

with tracker.start_run("ingest_example", model="demo"):
    artifact = tracker.log_artifact("data/people.csv", key="people", driver="csv")
    tracker.ingest(artifact)
```

Ingest explicit data payloads (dicts, DataFrames, generators):

```python
with tracker.start_run("ingest_inline", model="demo"):
    artifact = tracker.log_artifact("inputs/people.json", key="people")
    tracker.ingest(artifact, data=[{"id": 1, "name": "A"}])
```

---

## Strict Schema Ingestion

Provide a SQLModel schema to validate data:

```python
from sqlmodel import SQLModel, Field

class Person(SQLModel, table=True):
    __tablename__ = "people"
    id: int = Field(primary_key=True)
    name: str

with tracker.start_run("strict_ingest", model="demo"):
    artifact = tracker.log_artifact("data/people.csv", key="people", schema=Person)
    tracker.ingest(artifact, schema=Person)
```

Strict mode rejects incompatible data rather than silently coercing it.

---

## Provenance Columns

Ingested tables include system columns:

| Column | Purpose |
|--------|---------|
| `consist_run_id` | Run that created this row |
| `consist_artifact_id` | Source artifact |
| `consist_year`, `consist_iteration` | Scenario dimensions (optional) |

These enable joins across runs and filtering by scenario or year in hybrid views.

---

## Hybrid Views

Hybrid views are SQL views that union hot data (ingested into DuckDB) with cold data (read from files on disk). Query both as one logical table without requiring all data in the database.

Register a SQLModel schema to activate a view:

```python
from sqlmodel import SQLModel, Field

class Person(SQLModel, table=True):
    __tablename__ = "people"
    person_id: int = Field(primary_key=True)
    age: int

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person],
)

VPerson = tracker.views.Person
```

Once registered, you can query across all runs (both ingested and cold data):

```python
from sqlmodel import select, func
import consist

query = (
    select(VPerson.consist_year, func.avg(VPerson.age).label("avg_age"))
    .group_by(VPerson.consist_year)
)
rows = consist.run_query(query, tracker=tracker)
```

For schema/facet-driven cross-key analysis, use
[Grouped Views](grouped-views.md) to create one view over many artifacts that
share a schema identity instead of a single `concept_key`.

---

## Loading Behavior & Database Fallback

`consist.load(...)` reads from the filesystem. If files are missing and the artifact was ingested, database fallback recovers data:

| Policy | Behavior |
|--------|----------|
| `inputs-only` (default) | Recover inside active run for declared inputs only |
| `always` | Recover whenever artifact is ingested |
| `never` | Disable recovery |

Example:

```python
df = consist.load_df(artifact, tracker=tracker, db_fallback="always")
```

---

## Loader Kwargs

`consist.load(...)` validates loader kwargs and raises `ValueError` for unsupported options.

| Driver | Supported kwargs |
|--------|-----------------|
| `parquet` | `columns` |
| `csv` | `columns`, `delimiter`, `header` (alias: `sep`) |
| `json` | `orient`, `dtype`, `convert_axes`, `convert_dates`, `precise_float`, `date_unit`, `encoding`, `lines`, `compression`, `typ` |
| `h5_table` | `columns`, `where`, `start`, `stop` |

Use `consist.load_relation(...)` to manage DuckDB Relation lifecycle explicitly. Use `consist.load_df(...)` for DataFrame with automatic cleanup.

---

## Schema Export Workflow

For curated schemas with explicit PK/FK constraints: ingest tabular data, run schema export to generate a SQLModel stub, then edit and register the stub for views. See [schema-export.md](../schema-export.md) for details.

---

## HDF5 Native Support Roadmap

Currently `h5_table` uses a staging bridge (`pandas.read_hdf(...)` + `conn.from_df(...)`). Native HDF5 support will replace this with DuckDB's HDF5 extension for direct queries.

**Remaining work**: switch `h5_table` to `hdf5_read(...)` once stable, validate extension version pinning, handle PyTables/object dtype edge cases, update schema capture for native relation path, add performance testing.

**Benefits**: zero-copy streaming reads, lower memory usage, consistent SQL workflows across Parquet/CSV/HDF5.

---

## Decision Summary

| Need | Approach | When |
|------|----------|------|
| Query results across runs in SQL | Ingest | Cross-run comparisons, storage cost acceptable |
| Minimize database size | Stay cold | Large files, no cross-run SQL analysis |
| Query ingested and raw files together | Hybrid views | Mixed—some runs ingested, some not |
| Recover data if files deleted | Ingest | Need resilience to file deletion |

---

## See Also

- **[Caching & Hydration](caching-and-hydration.md)** — How materialization interacts with caching
- **[DLT Loader Guide](../dlt-loader-guide.md)** — Detailed ingestion patterns using DLT
- **[Schema Export](../schema-export.md)** — Generate and manage schemas for ingested data
