# Ingestion & Hybrid Views

Consist supports two storage modes for tabular data:

- **Cold data**: keep files on disk and load from their paths.
- **Hot data**: ingest into DuckDB for fast queries, schema tracking, and optional recovery.

Hybrid views let you query both modes through a single SQLModel view.

---

## When to ingest

Ingest when you want:
- Fast, SQL-native analytics over many runs.
- Schema tracking and exportable SQLModel stubs.
- The ability to recover tabular data if files are missing.

Stay cold when you want:
- Minimal DB size (data stays in files, not duplicated in DuckDB).
- Large binary formats that are not tabular (e.g., rasters, binary blobs).

“Cold” means the data lives only on disk paths; you keep provenance metadata, but you cannot query it in SQL or include it in hybrid views until you ingest it.

---

## Ingesting artifacts

Ingestion uses the optional DLT dependency:

```
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

You can also ingest explicit data payloads (dicts, DataFrames, generators):

```python
with tracker.start_run("ingest_inline", model="demo"):
    artifact = tracker.log_artifact("inputs/people.json", key="people")
    tracker.ingest(artifact, data=[{"id": 1, "name": "A"}])
```

---

## Strict schema ingestion

Provide a SQLModel schema to validate incoming data:

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

## Provenance columns

Ingested tables include Consist system columns such as:
- `consist_run_id`, `consist_year`, `consist_iteration`
- `consist_artifact_id`

These enable joins across runs and allow hybrid views to filter by scenario or year.

---

## Hybrid views (hot + cold)

Hybrid views are SQL views that union:
- **Hot data** ingested into DuckDB tables.
- **Cold data** read directly from files (CSV/Parquet) on disk.

To activate a view, register a SQLModel schema:

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

Once registered, you can query across all runs:

```python
from sqlmodel import select, func
import consist

query = (
    select(VPerson.consist_year, func.avg(VPerson.age).label("avg_age"))
    .group_by(VPerson.consist_year)
)
rows = consist.run_query(query, tracker=tracker)
```

---

## Schema export workflow

If you want a curated schema with explicit PK/FK constraints:

1) Ingest tabular data.
2) Run schema export to generate a SQLModel stub.
3) Edit the stub and register it for views.

See `docs/schema-export.md` for details.

---

## Loading behavior and DB fallback

`consist.load(...)` normally reads from the filesystem. If files are missing and the
artifact was ingested, DB fallback can recover data depending on the policy:

- `db_fallback="inputs-only"` (default): only inside an active run and for declared inputs.
- `db_fallback="always"`: allow recovery whenever the artifact is ingested.
- `db_fallback="never"`: disable recovery.

Example:

```python
df = consist.load(artifact, tracker=tracker, db_fallback="always")
```

---

## Best practices

- Ingest key tabular outputs you will compare across runs (e.g., transportation: trip tables or skim summaries; climate: aggregated metrics like monthly averages; urban planning: parcel or zoning summaries). Rule of thumb: if it is <1GB and you expect to compare it, ingest it.
- Keep raw binary outputs cold, and use mounts for portability.
- Register SQLModel schemas for any concept you want to query via views.
- Use schema export to bootstrap models, then curate them.

---
