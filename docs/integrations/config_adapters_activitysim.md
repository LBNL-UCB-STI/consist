# ActivitySim Config Adapter

ActivitySim projects accumulate config complexity fast: `settings.yaml` pulling
in model YAMLs via `inherit_settings`, coefficient CSVs referenced by name,
CONSTANTS scattered across files. When you're running calibration sweeps or
comparing scenarios, you need to know exactly which config files and parameter
values went into each run — and catch immediately if something changed.

The ActivitySim adapter handles all of this automatically. Point it at your
config directory and it will:

- Walk the full inheritance chain and hash every relevant file
- Extract CONSTANTS, settings, and coefficients into queryable SQL tables
- Record a config bundle (tarball) so any run can be exactly reproduced
- Generate cache-invalidating signatures that change when any config file changes

The result: Consist can tell you *which specific files or keys changed* when a
run misses cache, and you can query `AUTO_TIME` or `car_ASC` across every run
you've ever recorded.

---

## Setup

```python
from pathlib import Path
from consist import Tracker
from consist.integrations.activitysim import ActivitySimConfigAdapter

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")
adapter = ActivitySimConfigAdapter()
```

---

## How config composition works

ActivitySim projects typically layer configs in two ways, and Consist handles
both:

**1. Directory layering (`root_dirs` order)**

When you pass multiple directories to the adapter, the first directory wins on
any file that exists in more than one. This mirrors how ActivitySim itself
resolves configs:

```python
# overlay_dir/settings.yaml takes precedence over base_dir/settings.yaml
identity_inputs=[("asim_config", overlay_dir), ("asim_base", base_dir)]
```

If `overlay_dir/settings.yaml` contains `inherit_settings: true`, the adapter
deep-merges all `settings.yaml` files it finds across `root_dirs` (base first,
overlay on top), which is how ActivitySim's own inheritance chain works.

One important subtlety: the adapter hashes **the full contents of every
`root_dir`**, not just the files that win after merging. If you change a value
in `base_dir/settings.yaml` that is overridden by `overlay_dir/settings.yaml`,
ActivitySim would never see that change — but Consist will still invalidate the
cache. This is conservative by design: Consist doesn't simulate ActivitySim's
merge logic to determine what "actually" changed. If you want a base-file edit
to be a no-op for caching purposes, make the edit in the overlay instead, or
accept the cache miss.

**2. Programmatic overrides (`ConfigOverrides`)**

`ConfigOverrides` is a separate, code-level layer applied on top of your config
directories. Use it when you want to sweep a parameter value without editing
files:

```python
from consist.integrations.activitysim import ConfigOverrides

overrides = ConfigOverrides(
    constants={
        ("settings.yaml", "sample_rate"): 0.1,          # (file, key): value
        ("accessibility.yaml", "CONSTANTS.AUTO_TIME"): 60.0,
    },
    coefficients={
        ("tour_mode_coeffs.csv", "car_ASC", ""): -0.5,  # (file, coef, segment): value
    },
)
```

The `""` segment means the coefficient file uses a direct `value` column (not
template-style segment columns). `ConfigOverrides` are hashed into the run
signature, so two runs with different overrides produce different cache entries
even if the underlying config files are identical.

Use `tracker.run_with_config_overrides(...)` to apply these — it handles
materializing the modified config to a temp directory and feeding it to the
adapter automatically. See [Sensitivity testing](#sensitivity-testing-apply-parameter-overrides)
below.

---

## Common workflows

### Run a model with full config tracking

For day-to-day runs, pass the adapter directly to `tracker.run(...)`:

```python
from consist import CacheOptions

result = tracker.run(
    fn=run_activitysim,
    name="activitysim",
    config={"scenario": "baseline"},
    adapter=adapter,
    identity_inputs=[("asim_config", config_dir)],
    cache_options=CacheOptions(cache_mode="auto"),
)
```

Consist hashes every config file in `config_dir`, records them as artifacts,
and ingests extracted parameters to DuckDB. On the next run with identical
config, it returns a cache hit without re-executing.

For `trace` blocks (always-execute steps you still want to track):

```python
with tracker.trace(
    name="postprocess",
    adapter=adapter,
    identity_inputs=[("asim_config", config_dir)],
) as t:
    postprocess()
    t.log_output(Path("./outputs/summary.csv"), key="summary")
```

---

### Query parameters across runs

After a few runs, the ingested tables let you ask questions that would otherwise
require grepping config directories or reading old notes:

```python
from sqlmodel import Session, select
from consist.models.activitysim import (
    ActivitySimConstantsCache,
    ActivitySimConfigIngestRunLink,
)

with Session(tracker.engine) as session:
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
    ).all()

for run_id, value in rows:
    print(f"{run_id}: AUTO_TIME = {value}")
```

Or use the adapter's helper to get a constant grouped by run:

```python
rows_by_run = adapter.constants_by_run(
    key="sample_rate",
    collapse="first",
    tracker=tracker,
)
```

---

### Sensitivity testing: apply parameter overrides

To run a sweep over coefficient values without manually editing files, use
`tracker.run_with_config_overrides(...)`. It takes your base config, applies
the overrides, runs the model, and records everything with full provenance:

```python
from consist.integrations.activitysim import ConfigOverrides

result = tracker.run_with_config_overrides(
    adapter=adapter,
    base_config_dirs=[overlay_dir, base_dir],
    overrides=ConfigOverrides(
        constants={
            ("settings.yaml", "sample_rate"): 0.1,
        },
        coefficients={
            ("tour_mode_coeffs.csv", "car_ASC", ""): -0.5,
        },
    ),
    output_dir=Path("temp/adjusted_config"),
    fn=run_activitysim,
    name="activitysim_sensitivity",
    model="activitysim",
    config={"iteration": 1},
)
```

To base the sweep on a previously recorded run rather than config directories:

```python
result = tracker.run_with_config_overrides(
    adapter=adapter,
    base_run_id="baseline_run_id",
    overrides=ConfigOverrides(
        coefficients={("accessibility_coefficients.csv", "time", ""): 2.3}
    ),
    output_dir=Path("temp/adjusted"),
    fn=run_activitysim,
    name="activitysim_sensitivity",
    model="activitysim",
    config={"iteration": 2},
)
```

Each override run automatically records which resolved config root was used
(`run.meta["resolved_config_identity"]`), so you can trace any result back to
the exact parameter values that produced it.

---

### Read a single coefficient from a historical run

```python
coef = adapter.get_coefficient_value(
    run_id="baseline_run_id",
    tracker=tracker,
    file_name="accessibility_coefficients.csv",
    coefficient_name="time",
)
```

---

### Validate config before running

Catch missing files or broken inheritance chains before a long run starts:

```python
from consist.core.config_canonicalization import ConfigAdapterOptions

plan = tracker.prepare_config(
    adapter,
    [overlay_dir, base_dir],
    options=ConfigAdapterOptions(strict=True, bundle=False, ingest=False),
    validate_only=True,
)
if plan.diagnostics and not plan.diagnostics.ok:
    raise ValueError("Config validation failed.")
```

---

## What gets discovered and extracted

### Discovery

The adapter locates and hashes these files:

1. `settings.yaml` — resolves the active `models` list
2. Model YAMLs — resolved via suffix stripping and `include_settings` references
3. Referenced CSVs — found via registry keys (`*_FILE`, `*_PATH`) and heuristics

**Constructor options:**

```python
adapter = ActivitySimConfigAdapter(
    model_name="activitysim",     # label in run metadata
    adapter_version="0.1",        # version tag for run.meta
    allow_heuristic_refs=True,    # detect *_FILE, *_PATH keys automatically
    bundle_configs=True,          # create a tarball of the full config
    bundle_cache_dir=None,        # defaults to <run_dir>/config_bundles
)
```

**Layered config directories** (overlay takes precedence):

```python
tracker.run(
    ...,
    identity_inputs=[("asim_config", overlay_dir), ("asim_base", base_dir)],
)
```

### Extracted tables

| Table | Contents |
|---|---|
| `activitysim_constants_cache` | Flattened YAML CONSTANTS and allowlisted settings |
| `activitysim_coefficients_cache` | Parsed rows from coefficient CSVs |
| `activitysim_probabilities_cache` | Parsed probability table rows |
| `activitysim_config_ingest_run_link` | Join table: maps content hashes to run IDs |

Tables are deduplicated by content hash, so identical config files across runs
produce one row, not duplicates. To query by run, join against
`activitysim_config_ingest_run_link`.

#### `activitysim_constants_cache` schema

| Column | Type | Notes |
|---|---|---|
| `content_hash` | str | Source file identity |
| `file_name` | str | Source YAML file |
| `key` | str | Dot-notation key, e.g. `CONSTANTS.AUTO_TIME`, `sample_rate` |
| `value_type` | str | `null`, `bool`, `num`, `str`, or `json` |
| `value_num` | float \| NULL | Set when `value_type == "num"` |
| `value_str` | str \| NULL | Set when `value_type == "str"` |
| `value_bool` | bool \| NULL | Set when `value_type == "bool"` |
| `value_json` | JSON \| NULL | Set when `value_type == "json"` |

#### `activitysim_coefficients_cache` schema

| Column | Type | Notes |
|---|---|---|
| `content_hash` | str | Source file identity |
| `file_name` | str | Source CSV file |
| `coefficient_name` | str | Coefficient identifier |
| `segment` | str | Segment name (template format) or `""` (direct) |
| `source_type` | str | `"direct"` or `"template"` |
| `value_raw` | str | Raw CSV cell value |
| `value_num` | float \| NULL | Parsed numeric value |
| `constrain` | str \| NULL | Constraint flag from CSV |
| `is_constrained` | bool \| NULL | Parsed constraint boolean |

---

## API reference

- `consist.integrations.activitysim.ActivitySimConfigAdapter`
- `consist.integrations.activitysim.ConfigOverrides`
- [`Tracker.run_with_config_overrides()`](../api/tracker.md)
- [`Tracker.prepare_config()`](../api/tracker.md#consist.core.tracker.Tracker.prepare_config)
- [`Tracker.canonicalize_config()`](../api/tracker.md#consist.core.tracker.Tracker.canonicalize_config)

## See Also

- [Example 03: Demand Modeling](../examples.md) — end-to-end transportation simulation workflow
- [Config Adapters Overview](config_adapters.md)
- [Config, Facets, and Identity Inputs](../concepts/config-management.md)
