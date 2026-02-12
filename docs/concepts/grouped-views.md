# Grouped Views

Grouped views let you create one analysis relation from many artifacts selected by
schema identity and facet/run filters, even when artifact keys vary across runs.

This is useful for iterative workflows where every snapshot has a unique key
(`linkstats_iter_1`, `linkstats_iter_2`, etc.) but the logical table is the same.

## Python API

```python
tracker.create_grouped_view(
    "v_linkstats_all",
    schema_id="a0490d6beb290b489cf08c7fd6b93177095d4a9d7d6d4782d613dcbc94e4199b",
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
    include_system_columns=True,
    mode="hybrid",           # hybrid | hot_only | cold_only
    if_exists="replace",     # replace | error
    missing_files="warn",    # warn | error | skip_silent
)
```

You can also select by a SQLModel class directly (instead of passing a hash):

```python
tracker.create_grouped_view(
    "v_linkstats_all",
    schema=LinkstatsRow,  # SQLModel class
    namespace="beam",
    params=["artifact_family=linkstats_unmodified_phys_sim_iter_parquet"],
)
```

`schema_id` and `schema` are mutually exclusive; provide exactly one.

## CLI

```bash
consist views create v_linkstats_all \
  --schema-id a0490d6beb290b489cf08c7fd6b93177095d4a9d7d6d4782d613dcbc94e4199b \
  --namespace beam \
  --param artifact_family=linkstats_unmodified_phys_sim_iter_parquet \
  --param year=2018 \
  --param iteration=0 \
  --attach-facet artifact_family \
  --attach-facet year \
  --attach-facet iteration \
  --attach-facet phys_sim_iteration \
  --attach-facet beam_sub_iteration \
  --driver parquet
```

## Selection Rules

- `schema_id` is the primary selector.
- Optional facet predicates (`--param` / `params`) are matched against indexed
  `artifact_kv` rows.
- Optional run filters are supported (`run_id`, `parent_run_id`, `model`,
  `status`, `year`, `iteration`).
- `schema_compatible=True` can include subset/superset schema variants by
  field-name compatibility.

## Output Columns

The grouped view returns:

- Source table columns (unioned by name).
- System columns (when `include_system_columns=True`):
  `consist_run_id`, `consist_artifact_id`, `consist_year`,
  `consist_iteration`, `consist_scenario_id`.
- Optional typed facet columns named `facet_<key>`.
