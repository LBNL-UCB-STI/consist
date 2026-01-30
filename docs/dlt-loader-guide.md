# DLT Loader Integration Guide

The DLT (Data Load Tool) integration enables robust, schema-validated ingestion of data into DuckDB with automatic provenance column injection. This guide covers when to use DLT, how to configure schemas, and best practices for data quality.

---

## What is DLT?

`dlt` is an open-source library for extracting and loading data. Consist uses it to:

- **Ingest diverse formats** (Parquet, CSV, JSON, Python objects) into DuckDB
- **Auto-detect and enforce schemas** (schema enforcement when provided)
- **Handle data quality issues** (type mismatches, missing values, duplicates)
- **Inject Consist provenance columns** (`consist_run_id`, `consist_artifact_id`, `consist_year`, etc.)
- **Scale efficiently** with streaming and batching

---

## When to Use DLT vs Direct Logging

### Use DLT for:

- **Large datasets** (100K+ rows) → streaming ingestion with DuckDB backend
  - *Transportation*: 50 runs × 1M persons per run = 50M total household records across all scenarios
  - *Climate*: Multi-year ensemble with 50 model runs, each producing regional summaries that you'll compare statistically
  - *Urban Planning*: Parcel-level zoning changes tracked across 20 scenario runs for cumulative analysis

- **Schema evolution tracking** → understand how data structure changes across runs
  - *Transportation*: New land use categories added in run 15; need to understand impact on mode choice modeling
  - *Climate*: Regional variables refined across ensemble members; track when resolution changed
  - *Urban Planning*: Zoning class definitions evolved across policy iterations; audit the timeline

- **Schema validation** → enforce types, nullability, PKs before loading
  - *Transportation*: Catch mode="car_pool" typos before querying mode choice; enforce person_id uniqueness
  - *Climate*: Ensure temperature is numeric (not string from CSV parsing); reject runs with missing required variables
  - *Urban Planning*: Validate parcel_ids are unique across scenarios; catch mixed data types in zoning codes

- **Complex data** → nested structures, unions, evolving columns
  - *Transportation*: Person-trip links where one trip can have multiple stages; household composition variations
  - *Climate*: Variable-length time series per location; nested metadata for model parameters
  - *Urban Planning*: Multi-level zoning (district → block → parcel) with hierarchical relationships

- **Cross-run analysis** → query data from multiple runs together with `tracker.views`
  - *Transportation*: "Compare average trip distance across all 50 scenarios in one SQL query"
  - *Climate*: "Find 95th percentile precipitation across ensemble members by region"
  - *Urban Planning*: "Count parcels zoned commercial across all scenarios, grouped by district"

**Example:**
```python
import consist
from sqlmodel import select

# DLT ingestion → can query across all runs with SQL
VPerson = tracker.views.Person
rows = consist.run_query(
    select(VPerson.consist_run_id, VPerson.age),
    tracker=tracker,
)
```

### Use Direct Logging for:

- **Small results** (<10K rows) → lightweight, no schema overhead
  - *Transportation*: Single scenario's summary statistics (e.g., 10 lines showing mode split by district)
  - *Climate*: One-off diagnostics (e.g., 50 error logs from model runs, not for re-analysis)
  - *Urban Planning*: Quick impact breakdown (e.g., 3 numbers showing acres zoned before/after one proposal)

- **One-off analyses** → not meant for cross-run queries
  - *Transportation*: Single debug run showing why trip generation failed in zone 42
  - *Climate*: Quick sensitivity test to new parameter value; won't compare with baseline
  - *Urban Planning*: Ad-hoc impact assessment for one proposal that stakeholders asked about once

- **Simple CSVs/Parquets** → no schema validation needed
  - *Transportation*: Raw travel demand model export (you'll validate in downstream step)
  - *Climate*: Raw gridded output from climate model (too large for DuckDB anyway; just store for archival)
  - *Urban Planning*: External zoning shapefile you imported (you don't own, can't enforce schema)

- **External data** → data you don't own or control
  - *Transportation*: Census data, transit schedules, road networks from external sources
  - *Climate*: Observational data, boundary conditions from other modeling teams
  - *Urban Planning*: Parcel maps from county assessor, regulatory texts from local government

**Example:**
```python
# Direct logging → no schema, just store the file
with tracker.start_run("log_result", model="demo"):
    consist.log_artifact(result_path, key="my_result")
```

### Decision Tree

```
Do you want to query this data across multiple runs in SQL?
├─ YES → Use DLT (register a schema)
├─ NO  → Do you trust the data format/types?
│        ├─ YES → Use direct logging (tracker.log_artifact)
│        └─ NO  → Use DLT with a schema
```

---

## Basic DLT Workflow

### Step 1: Define a Schema (SQLModel)

```python
from sqlmodel import SQLModel, Field
from typing import Optional

class Person(SQLModel, table=True):
    """Schema for person records."""
    person_id: int = Field(primary_key=True)
    age: int
    income: Optional[float]
    name: str
```

### Step 2: Create Tracker with Schema

```python
from consist import Tracker
from pathlib import Path

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person],  # Register schema
)
```

### Step 3: Log Data with Schema

```python
import pandas as pd

# Create data
df = pd.DataFrame({
    "person_id": [1, 2, 3],
    "age": [25, 30, 35],
    "income": [50000.0, 60000.0, 70000.0],
    "name": ["Alice", "Bob", "Carol"],
})

# Log with schema (DLT ingestion)
with tracker.start_run("ingest_people", model="demo"):
    tracker.log_dataframe(df, key="persons", schema=Person)
```

### Step 4: Query Across Runs

```python
from sqlmodel import Session, select, func

# Compute an aggregate over a single run_id (via the hybrid view).
VPerson = tracker.views.Person
with Session(tracker.engine) as session:
    avg_age = session.exec(
        # Filter to one Consist run and take an average over the ingested rows.
        select(func.avg(VPerson.age)).where(VPerson.consist_run_id == "run_123")
    ).first()
    print(f"Average age in run_123: {avg_age}")
```

Expected output:

```text
Average age in run_123: 30.0
```

---

## Schema Definition & Validation

### Basic Schema

```python
from sqlmodel import SQLModel, Field
from typing import Optional

class Trip(SQLModel, table=True):
    trip_id: int = Field(primary_key=True)
    person_id: int
    origin: str
    destination: str
    distance_miles: float
    mode: str  # "car", "transit", "bike"
    departure_hour: Optional[int]
```

### Validation: Required vs Optional

```python
class StrictTrip(SQLModel, table=True):
    trip_id: int = Field(primary_key=True, description="Unique trip ID")
    person_id: int = Field(gt=0)  # Must be positive
    mode: str = Field(min_length=1)  # Non-empty
    departure_hour: Optional[int] = Field(ge=0, le=23)  # 0-23 if present
```

### Foreign Keys (Relationships)

```python
from sqlmodel import SQLModel, Field, Relationship

class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    name: str
    # trips: List["Trip"] = Relationship(back_populates="person")

class Trip(SQLModel, table=True):
    trip_id: int = Field(primary_key=True)
    person_id: int = Field(foreign_key="person.person_id")
    distance_miles: float
    # person: Person = Relationship(back_populates="trips")
```

### Type Mapping

| Python Type | DuckDB Type | Notes |
|---|---|---|
| `int` | `INTEGER` | |
| `float` | `DOUBLE` | |
| `str` | `VARCHAR` | |
| `bool` | `BOOLEAN` | |
| `date` | `DATE` | Use `datetime.date` |
| `datetime` | `TIMESTAMP` | Use `datetime.datetime` |
| `Optional[T]` | `T NULL` | Allows NULL |
| `List[T]` | `T[]` | Arrays (advanced) |

---

## Logging Patterns

### Single DataFrame

```python
import consist

df = pd.read_csv("results.csv")
with tracker.start_run("log_results", model="demo"):
    consist.log_dataframe(
        df,
        key="results",
        schema=MySchema,  # Validate against schema
    )
```

### Parquet File

```python
with tracker.start_run("log_parquet", model="demo"):
    tracker.log_artifact(
        Path("results.parquet"),
        key="raw_results",
        schema=MySchema,  # Schema is stored; call tracker.ingest(...) to ingest
    )
```

### CSV File

```python
with tracker.start_run("log_csv", model="demo"):
    tracker.log_artifact(
        Path("results.csv"),
        key="csv_results",
        schema=MySchema,
    )
```

### Zarr / NetCDF (Matrix Data)

```python
# Zarr metadata is ingested as catalog (not raw data)
with tracker.start_run("log_zarr", model="demo"):
    tracker.log_artifact(
        Path("simulation_output.zarr"),
        key="gridded_results",
        driver="zarr",
    )
```

---

## Data Quality & Error Handling

### Schema Enforcement (Fail on Issues)

When you provide a schema, Consist enforces the column set and types. Extra
columns raise a `ValueError`, and type mismatches are surfaced during ingestion.

```python
with tracker.start_run("strict_ingest", model="demo"):
    tracker.log_dataframe(
        df,
        key="strict_results",
        schema=MySchema,
    )
```

If you want best-effort ingestion (no strict schema), omit the schema and let
DLT infer the structure.

### Type Coercion

DLT attempts to coerce types:

```python
df = pd.DataFrame({
    "trip_id": ["1", "2", "3"],      # Strings
    "distance": [1.5, 2.2, 3.1],     # Floats
})

# Ingested as:
# trip_id: [1, 2, 3]  (coerced to int)
# distance: [1.5, 2.2, 3.1]  (floats)
```

To avoid surprises, ensure input DataFrame matches schema types:

```python
df = df.astype({
    "trip_id": "int64",
    "distance": "float64",
})
```

### Handling Missing Data

```python
class Trip(SQLModel, table=True):
    trip_id: int = Field(primary_key=True)
    person_id: int
    departure_hour: Optional[int]  # Can be NULL
    arrival_hour: Optional[int]
```

Missing values in Optional fields → NULL in DB. Missing in required fields → error or default depending on the schema and ingestion behavior.

### Duplicate Handling

Primary keys are not enforced automatically in all cases. If you need
uniqueness, deduplicate before ingestion:

```python
class Household(SQLModel, table=True):
    household_id: int = Field(primary_key=True)
    size: int

# If DataFrame has duplicate household_id:
df = pd.DataFrame({
    "household_id": [1, 2, 1],  # Duplicate!
    "size": [4, 3, 5],
})

```

To handle duplicates, deduplicate before ingestion:

```python
df = df.drop_duplicates(subset=["household_id"], keep="last")
```

---

## Provenance Columns

Consist automatically injects system columns during ingestion:

### Available Columns

| Column | Type | Description |
|---|---|---|
| `consist_run_id` | str | ID of the run that created this data |
| `consist_artifact_id` | str | Artifact ID of the source file |
| `consist_scenario_id` | str | Scenario ID (available in hybrid views) |
| `consist_year` | int | Year (if provided to run context) |
| `consist_iteration` | int | Iteration count (if provided) |

### Example Query

```python
from sqlmodel import Session, select, func

VPerson = tracker.views.Person
with Session(tracker.engine) as session:
    # Count persons per run
    results = session.exec(
        select(
            VPerson.consist_run_id,
            func.count(VPerson.person_id).label("count")
        ).group_by(VPerson.consist_run_id)
    ).all()

    for run_id, count in results:
        print(f"Run {run_id}: {count} persons")
```

### Filtering by Provenance

```python
# Get persons from a specific scenario year
with Session(tracker.engine) as session:
    persons_2030 = session.exec(
        select(VPerson).where(
            VPerson.consist_year == 2030,
            VPerson.consist_scenario_id == "baseline"
        )
    ).all()
```

---

## Advanced Patterns

### Multi-Step Ingestion (Scenario)

```python
import consist
from consist import Tracker, use_tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person, Trip, Household],
)

with use_tracker(tracker):
    with consist.scenario("model_run_2030", year=2030) as sc:

        # Step 1: Load persons
        with sc.trace(name="load_persons"):
            df_persons = load_population_data()
            consist.log_dataframe(
                df_persons,
                key="population",
                schema=Person,
            )

        # Step 2: Simulate trips
        with sc.trace(name="simulate_trips"):
            df_trips = run_trip_simulation(df_persons)
            consist.log_dataframe(
                df_trips,
                key="trips",
                schema=Trip,
            )

        # Step 3: Aggregate
        with sc.trace(name="aggregate"):
            # Query previous step
            with Session(tracker.engine) as session:
                total_trips = session.exec(
                    select(func.count(Trip.trip_id)).where(
                        Trip.consist_scenario_id == "model_run_2030"
                    )
                ).first()
            print(f"Total trips: {total_trips}")
```

All data from steps 1-3 is queryable together:

```python
# Cross-step query
with Session(tracker.engine) as session:
    results = session.exec(
        select(Person, Trip).join(
            Trip, Person.person_id == Trip.person_id
        ).where(Trip.consist_scenario_id == "model_run_2030")
    ).all()
```

### Incremental Ingestion (Batches)

For very large datasets, ingest in batches:

```python
from pathlib import Path

# Split large file into chunks
import pandas as pd

chunk_size = 100000
for i, chunk in enumerate(pd.read_csv("large_file.csv", chunksize=chunk_size)):
    consist.log_dataframe(
        chunk,
        key=f"data_chunk_{i}",
        schema=MySchema,
    )
```

DuckDB automatically unions these into a single table **when the schema/table
name is the same** (for example, when you pass `schema=MySchema` each time):

```python
# Query all chunks together
with Session(tracker.engine) as session:
    count = session.exec(select(func.count(MySchema.id))).first()
```

### Late-Arriving Data

If you ingest data, then later find more data to add:

```python
# Run 1: Initial data
consist.log_dataframe(df1, key="data", schema=MySchema)

# Run 2: Additional data (same key, same schema)
consist.log_dataframe(df2, key="data", schema=MySchema)
```

Both sets are ingested and queryable:

```python
with Session(tracker.engine) as session:
    # Both df1 and df2 are included
    results = session.exec(select(MySchema)).all()
```

---

## Performance Tuning

### Batch Size

DLT loads data in batches. For large files, tune batch size:

```python
# In tracker initialization (future feature):
# tracker = Tracker(..., dlt_batch_size=50000)

# Or split manually:
for chunk in pd.read_csv("file.csv", chunksize=50000):
    consist.log_dataframe(chunk, key="data", schema=MySchema)
```

### Indexing

After ingestion, create indexes for frequently queried columns:

```python
# Manual index (in DuckDB):
with tracker.engine.begin() as conn:
    conn.exec_driver_sql(
        "CREATE INDEX idx_person_run ON global_tables.person(consist_run_id)"
    )
```

### File Format Choice

- **Parquet**: Faster loading, better compression, typed columns → preferred
- **CSV**: Human-readable, larger files, slower parsing → use for interchange

### Deduplication

If your pipeline generates duplicate records, deduplicate before ingestion:

```python
df = df.drop_duplicates(subset=["id"])
```

Consist does not currently deduplicate automatically.

---

## Common Errors

### Schema Mismatch

```
Error: Column 'age' expected int, got str
```

**Fix:** Ensure DataFrame types match schema:
```python
df["age"] = df["age"].astype("int64")
```

### Missing Required Column

```
Error: Column 'person_id' required but missing
```

**Fix:** Add column or make it optional:
```python
df["person_id"] = df.index + 1  # Add column
# OR
class MySchema(SQLModel, table=True):
    person_id: Optional[int]  # Make optional
```

### Primary Key Violation

```
Error: Duplicate primary key value
```

**Fix:** Deduplicate before ingestion:
```python
df = df.drop_duplicates(subset=["id"])
```

### DLT Not Installed

```
ImportError: No module named 'dlt'
```

**Fix:** Install the ingest extras:
```bash
pip install "consist[ingest]"
```

### Null in Non-Optional Field

```
Warning: Null value in non-optional field 'age'
```

**Fix (with schema enforcement):** Ensure no nulls:
```python
df = df.dropna(subset=["age"])

# OR make optional:
class MySchema(SQLModel, table=True):
    age: Optional[int]
```

---

## Comparison: DLT vs Direct Logging

| Feature | DLT | Direct Logging |
|---------|-----|---|
| **Schema validation** | ✅ Yes | ❌ No |
| **Cross-run SQL queries** | ✅ Yes | ❌ No |
| **Type enforcement** | ✅ Yes | ❌ No |
| **Setup overhead** | ⚠️ Moderate | ✅ Minimal |
| **Best for** | Analytics, large data | Simple results |
| **Example use case** | Population, trips table | Single analysis output |

---

## See Also

- [Usage Guide: Ingestion & Hybrid Views](ingestion-and-hybrid-views.md)
- [Schema Export](schema-export.md)
- [Architecture: Data Virtualization](architecture.md#data-virtualization)
- [dlt Documentation](https://dlthub.com/docs)
