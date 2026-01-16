---
icon: lucide/settings
---

# Config Adapters

Config adapters provide model-specific interfaces to discover, canonicalize, and
ingest complex file-based configurations. They enable tracking of calibration-sensitive
parameters as queryable database tables while preserving full config provenance via artifacts.

## Overview

### When to Use Config Adapters

Use config adapters when your model configuration:

- Lives in **multiple files** across a directory hierarchy (YAML, CSV, HOCON, etc.)
- Has **layered inheritance** or cascade logic (e.g., `inherit_settings: true`)
- Contains **calibration-sensitive parameters** you want to query and compare across runs
- Needs **content hashing** for provenance and cache identity
- Should remain **decoupled** from Consist core (no hard dependency on your model's libraries)

### When to Use In-Memory Configs Instead

Use the standard [config API](../configs.md) if:

- Configuration fits in a Python dict or Pydantic model
- You don't need to track file-based config artifacts
- A simple facet extraction suffices for your use case

### Architecture

Config adapters implement three phases:

```
Config Directory
    ↓
discover()           ← Locate files, compute content hash
    ↓
CanonicalConfig
    ↓
canonicalize()       ← Generate artifact specs + ingest specs
    ↓
CanonicalizationResult (artifacts + ingestables)
    ↓
tracker.canonicalize_config()  ← Log artifacts, ingest to DB
    ↓
Queryable Tables + Full Provenance
```

---

## ActivitySim

### Overview

The ActivitySim config adapter discovers and canonicalizes ActivitySim configuration
directories (with support for YAML inheritance, CSV references, and config bundling).

**Features:**

- **YAML Discovery**: Resolves `settings.yaml` and active model YAMLs via `inherit_settings` and `include_settings`
- **CSV Reference Detection**: Finds coefficients, probabilities, and specification files via registry + heuristics
- **Constants Extraction**: Flattens YAML `CONSTANTS` sections into queryable rows
- **Config Bundling**: Creates tarball archives for full config provenance
- **Config Materialization**: Apply parameter overrides to base bundles for scenario-based runs
- **Strict/Lenient Modes**: Error on file integrity issues or gracefully skip

### Use Cases

**1. Track calibration parameters across scenarios**

Compare how constants and coefficients change between runs:

```python
from consist.integrations.activitysim import ActivitySimConfigAdapter

adapter = ActivitySimConfigAdapter()

# Run baseline scenario
run_a = tracker.begin_run("baseline", "activitysim", cache_mode="overwrite")
tracker.canonicalize_config(adapter, [config_dir], ingest=True)
tracker.end_run()

# Run adjusted scenario
run_b = tracker.begin_run("adjusted", "activitysim", cache_mode="overwrite")
tracker.canonicalize_config(adapter, [config_dir_adjusted], ingest=True)
tracker.end_run()

# Query which runs used a specific sample_rate
rows_by_run = adapter.constants_by_run(
    key="sample_rate",
    collapse="first",
    tracker=tracker,
)

# Use in joins/facet analyses
sample_rate_sq = adapter.constants_query(key="sample_rate").subquery()
```

**2. Precompute config plans for caching + orchestration**

Prepare config artifacts and ingestion specs before a run, then apply them
inside `consist.run`/`consist.trace` (or `Tracker.run`/`Tracker.trace`):

```python
import consist
from consist import use_tracker

adapter = ActivitySimConfigAdapter()
plan = tracker.prepare_config(adapter, [overlay_dir, base_dir])

with use_tracker(tracker):
    consist.run(
        fn=run_activitysim,
        name="activitysim",
        config={"scenario": "baseline"},
        config_plan=plan,
        cache_mode="auto",
    )
```

**3. Apply parameter adjustments for sensitivity testing**

Use the `materialize()` method to apply overrides to a baseline config:

```python
from consist.integrations.activitysim import ConfigOverrides

# Load baseline bundle
baseline_bundle = Path("outputs/base_run/config_bundle_xxxxx.tar.gz")

# Create overrides
overrides = ConfigOverrides(
    constants={
        ("settings.yaml", "sample_rate"): 0.1,
        ("accessibility.yaml", "CONSTANTS.AUTO_TIME"): 60.0,
    },
    coefficients={
        ("tour_mode_coeffs.csv", "car_ASC", ""): -0.5,  # "" for direct coefficients
    }
)

# Materialize new config with overrides
materialized = adapter.materialize(
    baseline_bundle,
    overrides,
    output_dir=Path("temp/adjusted_config"),
    identity=tracker.identity,
)

# Canonicalize the adjusted config
run = tracker.begin_run("sensitivity_test", "activitysim")
tracker.canonicalize_config(adapter, materialized.root_dirs, ingest=True)
tracker.end_run()
```

### Discovery and Canonicalization Workflow

#### Discovery Phase (`adapter.discover()`)

1. Locates `settings.yaml` and loads active `models` list
2. Resolves model YAMLs via suffix stripping and alias mapping
3. Supports YAML `include_settings` for file references
4. Computes content hash of all config directories
5. Returns `CanonicalConfig` with file inventory

**Configuration options:**

```python
adapter = ActivitySimConfigAdapter(
    model_name="activitysim",        # Metadata label
    adapter_version="0.1",           # For run.meta tracking
    allow_heuristic_refs=True,       # Detect *_FILE, *_PATH keys
    bundle_configs=True,             # Create tarball archive
    bundle_cache_dir=None,           # Default: <run_dir>/config_bundles
)

canonical = adapter.discover(
    root_dirs=[config_dir],
    identity=tracker.identity,
    strict=False,  # Warn on missing settings.yaml
)
```

#### Canonicalization Phase (`adapter.canonicalize()`)

1. Loads effective settings (after merging inheritance chain)
2. Logs active YAMLs and referenced CSVs as input artifacts
3. Extracts `CONSTANTS` + allowlisted settings → ingestion spec
4. Classifies CSVs as coefficients or probabilities → ingestion specs
5. Creates config bundle tarball (cached by content hash)
6. Returns specs for artifact logging and ingest

**Key behaviors:**

- **Constant Attribution**: Constants attributed to effective YAML after inheritance
- **CSV Classification**: Files ending in `_coefficients.csv`, `_coeffs.csv`, `_coefficients_template.csv`, or `_probs.csv`
- **CSV Format Support**: Both direct (`value` column) and template (segment columns) coefficient formats
- **Gzip Support**: Transparently handles `.csv.gz` files
- **Lenient Mode** (default): Missing referenced CSVs logged as warnings; ingestion proceeds
- **Strict Mode**: Raises on missing files or malformed CSVs

### Queryable Schemas

ActivitySim config tables are deduplicated by content hash. To map rows back to
individual runs, join against `activitysim_config_ingest_run_link` on
`content_hash` and `table_name`.

#### `activitysim_constants_cache`

Flattened YAML constants and allowlisted settings, deduplicated by content hash.

| Column | Type | Notes |
|--------|------|-------|
| `content_hash` | str | Primary key: content hash for the source file |
| `file_name` | str | Primary key: source YAML file name |
| `key` | str | Primary key: dot-notation constant key (e.g., `CONSTANTS.AUTO_TIME`, `sample_rate`) |
| `value_type` | str | Type tag: `null`, `bool`, `num`, `str`, `json` |
| `value_str` | str \| NULL | String value when `value_type == "str"` |
| `value_num` | float \| NULL | Numeric value when `value_type == "num"` |
| `value_bool` | bool \| NULL | Boolean value when `value_type == "bool"` |
| `value_json` | JSON \| NULL | Complex value when `value_type == "json"` |

**Query example**: Find runs with specific constant values

```python
from sqlmodel import Session, select
from consist.models.activitysim import ActivitySimConstantsCache

with Session(tracker.engine) as session:
    # Find runs where AUTO_TIME > 50
    rows = session.exec(
        select(
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimConstantsCache.value_num,
        )
        .join(
            ActivitySimConstantsCache,
            ActivitySimConstantsCache.content_hash
            == ActivitySimConfigIngestRunLink.content_hash,
        )
        .where(ActivitySimConfigIngestRunLink.table_name == "activitysim_constants_cache")
        .where(ActivitySimConstantsCache.key == "CONSTANTS.AUTO_TIME")
        .where(ActivitySimConstantsCache.value_num > 50)
    ).all()

    for run_id, value_num in rows:
        print(f"{run_id}: AUTO_TIME = {value_num}")
```

#### `activitysim_coefficients_cache`

Parsed coefficient rows from CSV files, deduplicated by content hash.

| Column | Type | Notes |
|--------|------|-------|
| `content_hash` | str | Primary key: content hash for the source file |
| `file_name` | str | Primary key: source CSV file name |
| `coefficient_name` | str | Primary key: coefficient identifier |
| `segment` | str | Primary key: segment name (template) or "" (direct) |
| `source_type` | str | "direct" (value column) or "template" (segment columns) |
| `value_raw` | str | Raw CSV cell value |
| `value_num` | float \| NULL | Parsed numeric value |
| `constrain` | str \| NULL | Constraint flag from CSV (if present) |
| `is_constrained` | bool \| NULL | Parsed constraint boolean |

**Query example**: Compare coefficients across runs

```python
from sqlmodel import Session, select
from consist.models.activitysim import ActivitySimCoefficientsCache

with Session(tracker.engine) as session:
    # Find how a specific coefficient changed
    rows = session.exec(
        select(
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimCoefficientsCache.coefficient_name,
            ActivitySimCoefficientsCache.segment,
            ActivitySimCoefficientsCache.value_raw,
        )
        .join(
            ActivitySimCoefficientsCache,
            ActivitySimCoefficientsCache.content_hash
            == ActivitySimConfigIngestRunLink.content_hash,
        )
        .where(
            ActivitySimConfigIngestRunLink.table_name
            == "activitysim_coefficients_cache"
        )
        .where(ActivitySimCoefficientsCache.coefficient_name == "car_ASC")
    ).all()

    for run_id, coef_name, segment, value_raw in rows:
        print(f"{run_id}: {coef_name}[{segment}] = {value_raw}")
```

#### `activitysim_probabilities_cache`

Parsed probability table rows (dims and numeric probabilities separated), deduplicated by content hash.

| Column | Type | Notes |
|--------|------|-------|
| `content_hash` | str | Primary key: content hash for the source file |
| `file_name` | str | Primary key: source CSV file name |
| `row_index` | int | Primary key: row index in source file |
| `dims` | JSON | Non-numeric dimension values (e.g., `{"mode": "drive", "income": "high"}`) |
| `probs` | JSON | Numeric probability/weight values (e.g., `{"prob_0": 0.3, "prob_1": 0.7}`) |

**Query example**: Inspect probability tables

```python
from sqlmodel import Session, select
from consist.models.activitysim import ActivitySimProbabilitiesCache

with Session(tracker.engine) as session:
    # Find probability rows for a specific file
    rows = session.exec(
        select(
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimProbabilitiesCache.row_index,
            ActivitySimProbabilitiesCache.dims,
            ActivitySimProbabilitiesCache.probs,
        )
        .join(
            ActivitySimProbabilitiesCache,
            ActivitySimProbabilitiesCache.content_hash
            == ActivitySimConfigIngestRunLink.content_hash,
        )
        .where(
            ActivitySimConfigIngestRunLink.table_name
            == "activitysim_probabilities_cache"
        )
        .where(ActivitySimProbabilitiesCache.file_name == "atwork_probs.csv")
    ).all()
```

---

## BEAM

### Overview

The BEAM config adapter canonicalizes HOCON `.conf` configurations, resolves
includes, and ingests every resolved key/value for fast queries. Paths that
exist on disk are logged as input artifacts; missing paths produce warnings.

Dependencies:
- Requires `pyhocon` for parsing `.conf` files.
- Requires `pandas` only if you use tabular ingestion via `BeamIngestSpec`.

### Usage

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

### Facets

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

### Tabular Ingestion by Config Key

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

### Tables

#### `beam_config_cache`

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

#### `beam_config_ingest_run_link`

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

### Behavior Notes

- `resolve_substitutions=True` resolves HOCON substitutions; set to False to keep raw expressions.
- `env_overrides` supplies environment variables for optional substitutions (e.g., `${?BEAM_OUTPUT}`).
- `strict=True` raises on missing referenced files; otherwise missing paths are logged as warnings.

### Materialize Overrides

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

### API Reference

For detailed method signatures, parameters, and return types, see:

- [`ActivitySimConfigAdapter`](../../api/integrations/activitysim.md#consist.integrations.activitysim.ActivitySimConfigAdapter)
- [`ConfigOverrides`](../../api/integrations/activitysim.md#consist.integrations.activitysim.ConfigOverrides)
- [`Tracker.canonicalize_config()`](../../api/tracker.md#consist.core.tracker.Tracker.canonicalize_config)

#### `activitysim_probabilities_entries_cache`

Row-level probability entries (one row per key/value), deduplicated by content hash.

#### `activitysim_probabilities_meta_entries_cache`

Row-level probability metadata entries (e.g., depart ranges), deduplicated by content hash.

#### `activitysim_coefficients_template_refs_cache`

Template coefficient reference rows, deduplicated by content hash.

#### `activitysim_config_ingest_run_link`

Run-to-ingest link table for mapping deduped rows to runs.

### Configuration Best Practices

**1. Organize config directories with inheritance**

```
configs/
  base/
    settings.yaml      # inherit_settings: true
    accessibility.yaml
    coefficients.csv
  overlay/
    settings.yaml      # inherits from base
    local_overrides.yaml
```

Call with overlay first (takes precedence):

```python
tracker.canonicalize_config(adapter, [overlay_dir, base_dir])
```

**2. Use strict mode for validation**

In development/testing, catch config problems early:

```python
tracker.canonicalize_config(adapter, config_dirs, strict=True)
```

**3. Leverage config bundling for reconstruction**

The bundle tarball preserves full config state, enabling:
- Auditing exact config used for a run
- Reproducing runs via `materialize()`
- Sharing configs across machines

Bundles are cached by content hash, so repeated canonicalizations are efficient.

**4. Query constants before running**

Use `activitysim_constants_cache` + `activitysim_config_ingest_run_link` queries to validate assumptions:

```python
from sqlmodel import Session, select
from consist.models.activitysim import ActivitySimConstantsCache, ActivitySimConfigIngestRunLink

# Verify sample_rate is set to expected value
with Session(tracker.engine) as session:
    result = session.exec(
        select(ActivitySimConstantsCache.value_num)
        .join(
            ActivitySimConfigIngestRunLink,
            ActivitySimConfigIngestRunLink.content_hash
            == ActivitySimConstantsCache.content_hash,
        )
        .where(ActivitySimConfigIngestRunLink.run_id == run.id)
        .where(ActivitySimConfigIngestRunLink.table_name == "activitysim_constants_cache")
        .where(ActivitySimConstantsCache.key == "sample_rate")
    ).first()
    value_num = result[0] if result else None
    assert value_num == 0.25, f"Expected sample_rate=0.25, got {value_num}"
```

### Extensibility

Config adapters follow a Protocol interface, making it straightforward to add support
for other models. Future adapters might include:

- **BEAM (HOCON)**: Layered configuration discovery + calibration-sensitive key extraction
- **Custom Models**: Any model with file-based config that needs parameterization tracking

If you'd like to add an adapter for your own model, consult the
[ActivitySim adapter source](https://github.com/LBNL-UCB-STI/consist/tree/main/src/consist/integrations/activitysim)
as a reference implementation.

---

## See Also

- [Configuration, Identity, and Facets](../configs.md) — In-memory config and facets
- [Ingestion and Hybrid Views](../ingestion-and-hybrid-views.md) — DLT-based data ingestion
- [Caching and Hydration](../caching-and-hydration.md) — Run signature and cache identity
