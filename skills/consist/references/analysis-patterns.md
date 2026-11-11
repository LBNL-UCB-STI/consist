# Analysis Patterns

## Contents

- Choose the analysis surface
- CLI metadata inspection
- Working with tabular artifacts
- Grouped artifact views
- SQL escape hatches
- Run-to-run comparison
- Loading semantics

## Choose The Analysis Surface

- Use the CLI first for quick read-only inspection:
  `consist show`, `consist artifacts`, `consist preview`, `consist scenario`,
  and `consist summary`.
- Use `consist.api.ibis_view(...)` for analysis-time queries over typed tabular
  artifacts, including filtering to one run or artifact key.
- Use `consist.api.ibis_grouped_view(...)` for grouped views across many compatible
  profiled artifacts selected by schema, facets, and run filters.
- Use `load_df(...)` or `tracker.load_run_output(...)` as a pandas escape hatch
  when you need a concrete local dataframe from one known output.
- Use `RunSet` or `consist.run_set(...)` when the task is run comparison,
  alignment, recency selection, or config diffs.
- Use `tracker.create_view(...)`, `tracker.query_df(...)`, `tracker.query_rows(...)`,
  and `pivot_facets(...)` when you need direct parameterized SQL, Pydantic row
  hydration, or facet-only run summaries.

Terminology: `RunSet` is for selecting and comparing multiple runs.
`OutputSet` is different: it is one logical output artifact made of multiple
files inside a single run. Use the instrumentation reference for `OutputSet`
setup and cache hydration behavior.

## CLI Metadata Inspection

```bash
consist summary --db-path ./provenance.duckdb
consist scenario baseline --db-path ./provenance.duckdb
consist artifacts RUN_ID --db-path ./provenance.duckdb
consist artifacts RUN_ID --expand-sets --db-path ./provenance.duckdb
consist preview persons --db-path ./provenance.duckdb --run-dir ./runs
consist shell --trust-db --db-path ./database/provenance.duckdb
```

Use this when you need to answer “what ran, with what outputs, and where are the
files?” before writing Python.

For logical output sets, use `--expand-sets` to show member and manifest
artifacts. In `consist shell`, use `members <artifact>` to list child artifacts
and `manifest <artifact>` to preview the set manifest.

## Working With Tabular Artifacts

For query work over tabular artifacts, prefer the optional Ibis bridge. It
returns native Ibis table expressions backed by Consist DuckDB views, so the
same code can query cold file-backed artifacts and hot ingested rows without a
Consist-specific dataframe wrapper.

```python
import consist
from consist.api import ibis_view, to_df
from sqlmodel import Field, SQLModel


class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    household_id: int
    age: int
    income: float


people = ibis_view(tracker, model=Person, key="persons")
run = consist.find_run(tracker=tracker, parent_id="baseline", year=2030)
people_for_run = people.filter(people.consist_run_id == run.id)

adults = people_for_run.filter(people_for_run.age >= 18)
summary = (
    adults.group_by("household_id")
    .agg(avg_income=adults.income.mean(), n=adults.count())
    .to_pandas()
)
```

Filter on Consist system columns, such as `consist_run_id`, `consist_year`, and
`consist_scenario_id`, when the question is about one run or scenario. Install
the optional dependency with `consist[ibis]` and configure the tracker with
`db_path=...`; `ibis_view(...)` shares the tracker's `StoreLedger` DuckDB
connection directly.

Use pandas loading when you need a concrete object from one known output rather
than a query expression:

```python
run = consist.find_run(tracker=tracker, parent_id="baseline", year=2030)
df = to_df(tracker.load_run_output(run.id, "persons"))
```

## Grouped Artifact Views

Use `consist.api.ibis_grouped_view(...)` when related tabular artifacts have
different keys or are spread across a sweep, scenario, or repeated model runs.
The grouped view is selected from a profiled seed artifact plus optional schema,
facet, driver, and run filters.

```python
import consist
from consist.api import ibis_grouped_view

seed_run = consist.find_run(tracker=tracker, parent_id="baseline", year=2018)
artifacts = tracker.get_artifacts_for_run(seed_run.id)
seed = artifacts.outputs["linkstats"]

with ibis_grouped_view(
    tracker,
    view_name="v_linkstats_all",
    artifact_id=seed.id,
    namespace="beam",
    params=[
        "artifact_family=linkstats_unmodified_phys_sim_iter_parquet",
        "year=2018",
        "iteration=0",
    ],
    drivers=["parquet"],
    attach_facets=[
        "artifact_family",
        "year",
        "iteration",
        "phys_sim_iteration",
        "beam_sub_iteration",
    ],
    mode="hybrid",
) as linkstats:
    by_iteration = (
        linkstats.group_by("facet_beam_sub_iteration")
        .agg(mean_speed=linkstats.speed.mean(), n=linkstats.count())
        .to_pandas()
    )
```

Grouped Ibis views depend on schema metadata. Prefer
`ArtifactSpec(..., profile_file_schema=True)`, run-level
`profile_file_schema=True`, or `OutputSet(profile_file_schema=True)` during
instrumentation when you expect to query related artifacts together later.

## SQL Escape Hatches

Use DuckDB hybrid views with `tracker.query_df(...)` or `tracker.query_rows(...)`
when you need exact parameterized SQL, typed Pydantic row hydration, or a path
that avoids the optional Ibis dependency.

```python
from pydantic import BaseModel
from sqlmodel import Field, SQLModel
from consist import Tracker


class Person(SQLModel, table=True):
    person_id: int = Field(primary_key=True)
    household_id: int
    number_of_trips: int


class ScenarioTripSummary(BaseModel):
    consist_scenario_id: str
    consist_year: int
    avg_trips: float


tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    schemas=[Person],
)
tracker.create_view("v_persons", "persons", schema_model=Person)

rows = tracker.query_rows(
    """
    SELECT
        consist_scenario_id,
        consist_year,
        AVG(number_of_trips) AS avg_trips
    FROM v_persons
    WHERE consist_scenario_id IN ($s1, $s2)
    GROUP BY consist_scenario_id, consist_year
    """,
    params={"s1": "baseline", "s2": "policy"},
    model=ScenarioTripSummary,
)
```

Use `pivot_facets(...)` when you want a wide, parameter-comparison table from
logged run facets rather than artifact contents.

## Run-To-Run Comparison

Use `RunSet` when the target is the runs themselves rather than one artifact.

```python
from consist import RunSet

baseline = RunSet.from_query(tracker, label="baseline", parent_id="baseline")
policy = RunSet.from_query(tracker, label="policy", parent_id="policy")

aligned = baseline.latest(group_by=["year"]).align(
    policy.latest(group_by=["year"]),
    on="year",
)

summary = aligned.to_frame()
diffs = aligned.config_diffs()
```

Good use cases:

- baseline vs policy comparisons
- sensitivity sweeps aligned by year, seed, or scenario
- selecting the latest successful runs before downstream analysis

## Loading Semantics

- `load_df(...)` prefers the original file when it still exists.
- Use `db_fallback="always"` when the artifact was ingested and the original
  file may have moved or been deleted.
- Inspect `artifact.primary_location` (or `consist.locate(artifact)`) and use
  `tracker.resolve_uri(...)` when the stored path no longer matches the current
  host layout.
- If artifacts display mounted URIs such as `data://...`, `--trust-db` can
  recover stored mount roots when inspecting the same trusted provenance
  database. Use explicit `--mount data=/path/to/root` when inspecting from a
  different machine, archive, or moved data directory.
