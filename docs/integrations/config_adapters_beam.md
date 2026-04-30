# BEAM Config Adapter

The BEAM config adapter canonicalizes HOCON `.conf` configurations, resolves
includes, and ingests every resolved key/value for fast queries. Paths that
exist on disk are logged as input artifacts; missing paths produce warnings.

Dependencies:
- Requires `pyhocon` for parsing `.conf` files.
- Requires `pandas` only if you use tabular ingestion via `BeamIngestSpec`.

!!! note "Recommended path"
    For workflow execution, prefer `tracker.run(...)`, `tracker.trace(...)`, or
    `consist.scenario(...)` with `adapter=` and `identity_inputs=`. Examples
    using `tracker.begin_run(...)`, `tracker.canonicalize_config(...)`, and
    `tracker.end_run()` are integration-specific advanced lifecycle patterns.

## Usage

!!! note "Integration-specific advanced lifecycle snippet"
    This usage block shows explicit adapter lifecycle control. Prefer the
    explicit tracker/scenario execution path for regular BEAM runs.

```python
from pathlib import Path

from consist.integrations.beam import BeamConfigAdapter

config_root = Path("/path/to/beam/production/sfbay")
adapter = BeamConfigAdapter(
    primary_config=config_root / "sfbay-pilates-base.conf",
    env_overrides={"PWD": str(config_root.parent.parent)},
)

run = tracker.begin_run("beam_baseline", "beam")
tracker.canonicalize_config(adapter, [config_root], ingest=True)
tracker.end_run()
```

## Run/Trace Adapter Handoff (Public Surface)

For `Tracker.run(...)`, `Tracker.trace(...)`, `ScenarioContext.run(...)`,
`ScenarioContext.trace(...)`, and the deprecated `consist.run(...)` /
`consist.trace(...)` wrappers, use `adapter=` and `identity_inputs=`:

```python
import consist
from consist import use_tracker

with use_tracker(tracker):
    consist.run(
        fn=run_beam,
        name="beam",
        adapter=adapter,
        identity_inputs=[("beam_hocon", config_root / "sfbay-pilates-base.conf")],
    )

    with consist.trace(
        "beam_trace",
        adapter=adapter,
        identity_inputs=[("beam_hocon", config_root / "sfbay-pilates-base.conf")],
    ):
        run_beam()
```

`config_plan` is not accepted on run/trace public surfaces. Use `adapter=...`
and `identity_inputs=...`.

## Facets

```python
plan = tracker.prepare_config(
    adapter,
    [config_root],
    facet_spec={
        "keys": [
            "beam.agentsim.simulationName",
            {"key": "beam.physsim.name", "alias": "physsim"},
        ],
    },
    facet_schema_name="beam_config",
)
```

## Tabular Ingestion by Config Key

```python
from sqlmodel import Field, SQLModel

from consist.integrations.beam import BeamIngestSpec


class BeamVehicleTypesCache(SQLModel, table=True):
    __tablename__ = "beam_vehicletypes_cache"
    __table_args__ = {"schema": "global_tables"}

    id: int = Field(primary_key=True)
    value: str
    content_hash: str = Field(index=True)


adapter = BeamConfigAdapter(
    primary_config=config_root / "sfbay-pilates-base.conf",
    ingest_specs=[
        BeamIngestSpec(
            key="beam.agentsim.agents.vehicles.vehicleTypesFilePath",
            table_name="beam_vehicletypes_cache",
            schema=BeamVehicleTypesCache,
        ),
    ],
)
```

Notes:
- Schemas used with `BeamIngestSpec` should include a `content_hash` column for dedupe.
- If your configs use optional env substitutions (e.g., `${?BEAM_OUTPUT}`), set them via `env_overrides` to avoid unresolved keys during canonicalization/materialization.

## Tables

### `beam_config_cache`

Canonicalized config key/value rows, deduplicated by content hash.

| Column | Type | Notes |
|--------|------|-------|
| `content_hash` | str | Primary key: content hash for the config |
| `key` | str | Primary key: dotted config path |
| `value_type` | str | One of `str`, `num`, `bool`, `null`, `json` |
| `value_str` | str \| NULL | String values |
| `value_num` | float \| NULL | Numeric values |
| `value_bool` | bool \| NULL | Boolean values |
| `value_json_str` | str \| NULL | JSON-encoded values |

### `beam_config_ingest_run_link`

Links runs to ingested config hashes for query joins.

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | str | Primary key: Consist run id |
| `table_name` | str | Primary key: cache table name |
| `content_hash` | str | Primary key: config content hash |
| `config_name` | str | Primary key: config file name |

**Query example**: Compare a key across runs

```python
from sqlmodel import Session, select

from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink

with Session(tracker.engine) as session:
    rows = session.exec(
        select(
            BeamConfigIngestRunLink.run_id,
            BeamConfigCache.key,
            BeamConfigCache.value_num,
            BeamConfigCache.value_str,
        )
        .join(
            BeamConfigCache,
            BeamConfigCache.content_hash == BeamConfigIngestRunLink.content_hash,
        )
        .where(BeamConfigIngestRunLink.table_name == "beam_config_cache")
        .where(BeamConfigCache.key == "beam.agentsim.agentSampleSizeAsFractionOfPopulation")
    ).all()
```

## Behavior Notes

- `resolve_substitutions=True` resolves HOCON substitutions; set to False to keep raw expressions.
- `env_overrides` supplies environment variables for optional substitutions (e.g., `${?BEAM_OUTPUT}`).
- `strict=True` raises on missing referenced files; otherwise missing paths are logged as warnings.
- Canonicalization returns a structured `CanonicalConfigIdentity` manifest.
  Path-like config values are recorded as keyed `ConfigReference` entries with
  their dotted BEAM config key, canonical value, status, role, and identity
  policy.
- Reference identity policies are:
  - `content_hash` for resolved file inputs.
  - `path_alias` for aliased or logical input roots such as
    `beam.inputDirectory`.
  - `delegated_to_artifacts` for directories whose content identity is handled
    by logged input artifacts instead of hashing the whole directory in the
    config manifest.
  - `ignored` for explicitly dormant or non-identity references configured via
    `BeamReferencePolicy`.
  - `output_or_runtime_ignored` for output/runtime locations that should not
    make equivalent runs miss cache.

Path aliases can be supplied on the adapter or per call:

```python
from consist.core.config_canonicalization import ConfigAdapterOptions

tracker.canonicalize_config(
    adapter,
    [config_root],
    options=ConfigAdapterOptions(
        path_aliases={"beam_workspace": Path("/local/job123/workspace")}
    ),
)
```

Missing-reference warnings include the config key, status, policy, canonical
value, and raw value. Use `strict=True` to make missing required references fail.

## Materialize Overrides

```python
from consist.integrations.beam import BeamConfigOverrides

overrides = BeamConfigOverrides(
    values={
        "beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.75,
        "beam.agentsim.lastIteration": 5,
    }
)

materialized = adapter.materialize(
    [config_root],
    overrides,
    output_dir=Path("tmp/beam_materialized"),
    identity=tracker.identity,
)
```

If you already built a config plan (e.g., for caching), you can reuse its
`config_dirs` metadata:

```python
plan = tracker.prepare_config(adapter, [config_root])
materialized = adapter.materialize_from_plan(
    plan,
    overrides,
    output_dir=Path("tmp/beam_materialized"),
    identity=tracker.identity,
)
```

## Override Execution with `run_with_config_overrides`

```python
result = tracker.run_with_config_overrides(
    adapter=BeamConfigAdapter(primary_config=Path("overlay.conf")),
    base_config_dirs=[config_root],
    base_primary_config=config_root / "overlay.conf",
    overrides=BeamConfigOverrides(values={"beam.agentsim.lastIteration": 5}),
    output_dir=Path("tmp/beam_override_runs"),
    fn=run_beam,
    name="beam_override_step",
    model="beam",
    identity_inputs=[("manual_context", Path("scenario_flags.txt"))],
)
```

Identity behavior:
- `identity_inputs` are additive and merged with auto resolved-config identity
  by default.
- `resolved_config_identity="auto"` (default) injects the selected resolved
  config root under `identity_label` (default: `"beam_config"`).
- `resolved_config_identity="off"` disables that auto injection and keeps only
  manual `identity_inputs`.

Override runs persist `run.meta["resolved_config_identity"]` with `mode`,
`adapter`, `label`, `path`, and `digest`.

## API Reference

For detailed method signatures, parameters, and return types, see:

- `consist.integrations.beam.BeamConfigAdapter` in the source API.
- `consist.integrations.beam.BeamConfigOverrides` in the source API.
- [`Tracker.canonicalize_config()`](../api/tracker.md#consist.core.tracker.Tracker.canonicalize_config)
