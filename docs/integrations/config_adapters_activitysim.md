# ActivitySim Config Adapter

The ActivitySim config adapter discovers and canonicalizes ActivitySim configuration
directories (with support for YAML inheritance, CSV references, and config bundling).

!!! note "Recommended path"
    For workflow execution, prefer `tracker.run(...)`, `tracker.trace(...)`, or
    `consist.scenario(...)` with `adapter=` and `identity_inputs=`. Lifecycle
    snippets using `tracker.begin_run(...)`, `tracker.canonicalize_config(...)`,
    and `tracker.end_run()` are integration-specific advanced patterns for
    manual orchestration.

## Overview

**Features:**

- **YAML Discovery**: Resolves `settings.yaml` and active model YAMLs via `inherit_settings` and `include_settings`
- **CSV Reference Detection**: Finds coefficients, probabilities, and specification files via registry + heuristics
- **Constants Extraction**: Flattens YAML `CONSTANTS` sections into queryable rows
- **Config Bundling**: Creates tarball archives for full config provenance
- **Config Materialization**: Apply parameter overrides to base bundles for scenario-based runs
- **Strict/Lenient Modes**: Error on file integrity issues or gracefully skip

## Use Cases

**1. Track calibration parameters across scenarios**

Compare how constants and coefficients change between runs:

!!! note "Integration-specific advanced lifecycle snippet"
    This example uses explicit lifecycle APIs for adapter-centric ingestion and
    diagnostics. Use the explicit tracker/scenario execution path for day-to-day
    workflow execution.

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

**2. Use adapter handoff on run/trace surfaces**

For run/trace APIs, pass `adapter=...` and `identity_inputs=...` directly:

```python
from consist.integrations.activitysim import ActivitySimConfigAdapter

import consist
from consist import CacheOptions, use_tracker

adapter = ActivitySimConfigAdapter()

with use_tracker(tracker):
    consist.run(
        fn=run_activitysim,
        name="activitysim",
        config={"scenario": "baseline"},
        adapter=adapter,
        identity_inputs=[("asim_config", overlay_dir)],
        cache_options=CacheOptions(cache_mode="auto"),
    )

    with consist.trace(
        name="activitysim_trace",
        adapter=adapter,
        identity_inputs=[("asim_config", overlay_dir)],
    ):
        run_activitysim()
```

`config_plan` is not accepted on run/trace public surfaces. Use `adapter=...`
and `identity_inputs=...`.

You can still precompute plans for validation and orchestration workflows:

```python
from consist.core.config_canonicalization import ConfigAdapterOptions

options = ConfigAdapterOptions(strict=True, bundle=False, ingest=False)
plan = tracker.prepare_config(
    adapter,
    [overlay_dir, base_dir],
    options=options,
    validate_only=True,
)
if plan.diagnostics and not plan.diagnostics.ok:
    raise ValueError("Config validation failed.")
```

**3. Apply parameter adjustments for sensitivity testing**

Use the `materialize()` method to apply overrides to a baseline config:

!!! note "Integration-specific advanced lifecycle snippet"
    The final `begin_run`/`canonicalize_config`/`end_run` block below is advanced
    adapter orchestration. Keep standard execution on the explicit
    tracker/scenario execution path.

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

For historical runs, you can skip manual bundle lookup:

```python
materialized = adapter.materialize_from_run(
    tracker=tracker,
    run_id="baseline_run_id",
    overrides=ConfigOverrides(constants={("settings.yaml", "sample_rate"): 0.1}),
    output_dir=Path("temp/materialized"),
)

# Choose a deterministic preferred root (optionally enforce a required file).
root_dir = adapter.select_root_dir(materialized, required_file="settings.yaml")
```

For one-off coefficient reads, use either config directories or a historical run:

```python
coef = adapter.get_coefficient_value(
    run_id="baseline_run_id",
    tracker=tracker,
    file_name="accessibility_coefficients.csv",
    coefficient_name="time",
)
```

For end-to-end override execution with no prior run, use config roots directly:

```python
result = tracker.run_with_config_overrides(
    adapter=adapter,
    base_config_dirs=[overlay_dir, base_dir],
    base_primary_config=Path("settings.yaml"),  # optional hint
    overrides=ConfigOverrides(
        coefficients={("accessibility_coefficients.csv", "time", ""): 2.3}
    ),
    output_dir=Path("temp/materialized"),
    fn=run_model_step,
    name="activitysim_calibration_step",
    model="activitysim",
    config={"iteration": 2},
)
```

`run_with_config_overrides(...)` supports additive manual identity inputs:

```python
result = tracker.run_with_config_overrides(
    adapter=adapter,
    base_config_dirs=[overlay_dir, base_dir],
    overrides=ConfigOverrides(),
    output_dir=Path("temp/materialized"),
    fn=run_model_step,
    name="activitysim_calibration_step",
    identity_inputs=[("manual_context", Path("calibration_notes.yaml"))],
)
```

By default, the adapter also auto-adds the selected resolved config root to
identity hashing (`resolved_config_identity="auto"`). To disable this escape
hatch, pass `resolved_config_identity="off"` and only your manual
`identity_inputs` are used.

Each override run stores `run.meta["resolved_config_identity"]` with:
`mode`, `adapter`, `label`, `path`, and `digest`.

You can still use a historical run as the base selector:

```python
result = tracker.run_with_config_overrides(
    adapter=adapter,
    base_run_id="baseline_run_id",
    overrides=ConfigOverrides(
        coefficients={("accessibility_coefficients.csv", "time", ""): 2.3}
    ),
    output_dir=Path("temp/materialized"),
    fn=run_model_step,
    name="activitysim_calibration_step",
    model="activitysim",
    config={"iteration": 2},
)
```

## Discovery and Canonicalization Workflow

### Discovery Phase (`adapter.discover()`)

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

### Canonicalization Phase (`adapter.canonicalize()`)

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

## Queryable Schemas

ActivitySim config tables are deduplicated by content hash. To map rows back to
individual runs, join against `activitysim_config_ingest_run_link` on
`content_hash` and `table_name`.

### `activitysim_constants_cache`

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

### `activitysim_coefficients_cache`

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

### `activitysim_probabilities_cache`

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

#### Additional Tables

- `activitysim_probabilities_entries_cache`
- `activitysim_probabilities_meta_entries_cache`
- `activitysim_coefficients_template_refs_cache`
- `activitysim_config_ingest_run_link`

## Configuration Best Practices

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

## API Reference

For detailed method signatures, parameters, and return types, see:

- `consist.integrations.activitysim.ActivitySimConfigAdapter` in the source API.
- `consist.integrations.activitysim.ConfigOverrides` in the source API.
- [`Tracker.canonicalize_config()`](../api/tracker.md#consist.core.tracker.Tracker.canonicalize_config)
