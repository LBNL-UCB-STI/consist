# DLT Loader Integration Guide

Consist uses the optional `dlt` integration to ingest tabular data into DuckDB,
attach provenance columns, and make data queryable across runs.

Install the optional ingestion dependencies before using DLT-backed paths:

```bash
pip install "consist[ingest]"
```

## When to Use DLT

Use DLT-backed ingestion when you need one or more of these:

- cross-run SQL queries over tabular outputs
- schema validation with SQLModel classes
- provenance columns such as `consist_run_id` and `consist_year`
- repeatable loading of CSV, Parquet, JSON, or DataFrame data into DuckDB
- later schema export from the observed ingested table

Use direct artifact logging when you only need to preserve a file and do not
need its rows queryable:

```python
with tracker.start_run("write_report", model="demo"):
    tracker.log_artifact("outputs/report.csv", key="report")
```

Decision rule:

```text
Need row-level SQL across runs?
  yes -> use DLT ingestion
  no  -> need schema validation?
          yes -> use DLT ingestion with a SQLModel schema
          no  -> log the file as an artifact
```

## Minimal Schema Ingestion Example

Define a SQLModel schema:

```python
from typing import Optional

from sqlmodel import Field, SQLModel


class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    age: int
    income: Optional[float] = None
    name: str
```

Register the schema with the tracker:

```python
from consist import Tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person],
)
```

Ingest a DataFrame:

```python
import pandas as pd

df = pd.DataFrame(
    {
        "person_id": [1, 2, 3],
        "age": [25, 30, 35],
        "income": [50000.0, 60000.0, None],
        "name": ["Alice", "Bob", "Carol"],
    }
)

with tracker.start_run("ingest_people", model="demo", year=2030):
    tracker.log_dataframe(df, key="persons", schema=Person)
```

Query the hybrid view:

```python
from sqlmodel import Session, func, select

VPerson = tracker.views.Person

with Session(tracker.engine) as session:
    avg_age = session.exec(
        select(func.avg(VPerson.age)).where(VPerson.consist_year == 2030)
    ).one()
```

The ingested rows include Consist provenance columns. You can filter or group by
`consist_run_id`, `consist_scenario_id`, `consist_year`, and related runtime
metadata exposed by the view.

## Ingesting Files

For tabular files, log and ingest with a schema when you want the rows in
DuckDB:

```python
from pathlib import Path

with tracker.start_run("ingest_trips", model="demo"):
    artifact = tracker.log_artifact(
        Path("outputs/trips.parquet"),
        key="trips",
        schema=Trip,
    )
    tracker.ingest(artifact, schema=Trip)
```

For DataFrames, `tracker.log_dataframe(..., schema=...)` is the shorter path and
is usually the simplest ingestion API.

## Schema Expectations

When you pass a schema, keep the input table close to the SQLModel definition:

| Schema feature | Expected input |
| --- | --- |
| Required field | Column exists and values are non-null |
| `Optional[T]` | Column may contain nulls |
| Primary key | Values should be unique, but validate/deduplicate upstream |
| Foreign key | Stored as schema metadata; relationship semantics remain your model's responsibility |
| Extra column | Treat as an input/schema mismatch unless intentionally handled before ingestion |

DLT may coerce compatible types, but explicit conversion is safer:

```python
df = df.astype(
    {
        "person_id": "int64",
        "age": "int64",
        "income": "float64",
    }
)
```

Deduplicate before ingestion when the table has semantic keys:

```python
df = df.drop_duplicates(subset=["person_id"], keep="last")
```

## Common Errors

### `ImportError: No module named 'dlt'`

Install the optional extras:

```bash
pip install "consist[ingest]"
```

### Missing Required Column

The DataFrame or file does not contain a required schema field.

Fix by adding the column before ingestion, renaming the input column to match the
schema, or making the field optional if null/missing values are valid:

```python
class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    school_zone_id: Optional[int] = None
```

### Type Mismatch

The input value cannot be loaded into the schema's target type.

Fix the input dtype explicitly:

```python
df["age"] = df["age"].astype("int64")
```

For mixed string/numeric columns, clean invalid values before ingestion rather
than relying on best-effort coercion.

### Null in a Non-Optional Field

A required field contains missing values.

```python
df = df.dropna(subset=["age"])
```

If missing values are meaningful, change the schema to `Optional[...]`.

### Duplicate Logical Keys

Primary-key declarations document table intent, but you should not depend on
ingestion to clean duplicates:

```python
df = df.drop_duplicates(subset=["trip_id"], keep="last")
```

### View Does Not Show Rows

Check that the registered model's `__tablename__` matches the ingested table or
artifact key. If you generated the schema from an observed artifact, verify the
table name before registering it.

## DLT vs Direct Logging

| Need | Use |
| --- | --- |
| Preserve a file exactly as produced | `log_artifact(...)` |
| Query rows across runs | `log_dataframe(..., schema=...)` or file ingestion |
| Validate column names and types | DLT with SQLModel schema |
| Export a SQLModel stub later | DLT ingestion or file schema profiling |
| Record a non-tabular artifact such as Zarr/NetCDF | Direct artifact logging plus any separate metadata table you create |

## See Also

- [DLT Loader API Reference](integrations/dlt_loader.md) — generated API signatures
- [Data Materialization](concepts/data-materialization.md) — when to ingest vs. keep cold
- [Integrations Overview](integrations/index.md)
- [Schema Export](schema-export.md)
- [dlt Documentation](https://dlthub.com/docs)
