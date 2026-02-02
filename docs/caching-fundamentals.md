# How Caching Works

Consist uses intelligent caching to skip redundant computation. This page explains the core mechanism.

## The Basic Idea

Consist computes a fingerprint (signature) from three components:

1. **Your function's code** – Git commit hash + tracked local Python modifications
2. **Configuration** – The `config` dict you pass to `consist.run()`
3. **Input files** – Content or metadata hashes of files in the `inputs` dict (depends on `hashing_strategy`)

If you run the same function with the same code, config, and inputs, the signature is identical. When Consist sees an identical signature, it returns the cached result from a previous run instead of re-executing.

## Example

```python
import consist
from consist import Tracker, use_tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

with use_tracker(tracker):
    # First run
    result1 = consist.run(
        fn=clean_data,
        inputs={"raw_df": "data.csv"},      # Hash of data.csv
        config={"threshold": 0.5},          # Hash of this config
        outputs=["cleaned"],
    )
# Signature: SHA256("clean_data_v1" + "threshold:0.5" + "data.csv_hash")
# Output: executed, returned new result


    # Second run with identical inputs/config
    result2 = consist.run(
        fn=clean_data,
        inputs={"raw_df": "data.csv"},      # Same hash
        config={"threshold": 0.5},          # Same hash
        outputs=["cleaned"],
    )
# Signature: same as above
# Output: cache hit! Result returned instantly without execution


    # Third run with different config
    result3 = consist.run(
        fn=clean_data,
        inputs={"raw_df": "data.csv"},      # Same hash
        config={"threshold": 0.8},          # Different hash!
        outputs=["cleaned"],
    )
# Signature: different (config changed)
# Output: cache miss, function executes, new run recorded
```

## What Changes Break Cache Hits?

| What Changed | Cache Hit? | Why |
|---|---|---|
| Input file content | ❌ No | File hash changes → signature changes |
| Config value | ❌ No | Config hash changes → signature changes |
| Function code | ❌ No | Code hash changes → signature changes |
| `runtime_kwargs` | ✅ Yes | runtime_kwargs are NOT hashed; don't affect signature |
| Output file names | ✅ Yes | Output names don't affect signature |
| Comments in code | Depends | Committed comment changes affect the code hash; uncommitted changes mark the repo dirty and break cache. |

## What Does Cache Return?

When there's a cache hit, Consist returns:

- **Artifact metadata** – Information about what run created the output, with what config
- **File paths** – Where the output was stored
- **Optionally, file bytes** – Depend on your cache hydration policy (see [Caching & Hydration](concepts/caching-and-hydration.md) for advanced options)

Important: A cache hit returns **metadata about the result**, not necessarily a copy of the files.

## Common Misconception

**"Cache hit means the output files are copied to my new run directory."**

Not necessarily. A cache hit returns artifact metadata (provenance). Whether file bytes are copied depends on your cache hydration policy. By default:
- File paths are preserved (you can access the original file)
- Bytes are not copied (saves disk space)

See [Caching & Hydration](concepts/caching-and-hydration.md) if you need to force file copying.

## When Caching Saves Time

Caching is most valuable in workflows with many runs and expensive computation. Here are realistic scenarios from scientific domains:

**Example 1: Land-Use Model Sensitivity Analysis**

Transportation planners use activity-based models to evaluate how pricing policies affect commute patterns. A sensitivity sweep tests 40 parameter combinations (toll levels: 0–\$10, parking costs: \$2-\$15, transit pass subsidies: 0–50%).

- Each ActivitySim run: 30 minutes (generating synthetic population tours)
- Without caching: 40 runs × 30 min = 1200 minutes (20 hours)
- With caching: Base population synthesis (30 min, once) + 39 parameter tweaks with cache hits (5 min each, only trip mode choice re-run) = 30 + (39 × 5) = 225 minutes (3.75 hours)
- **Time saved: 81% reduction in modeling time**

**Example 2: ActivitySim Calibration Iteration**

Mode choice coefficients need iterative calibration against observed transit ridership. A modeler:
1. Runs population synthesis (45 minutes, computationally heavy)
2. Generates tours (20 minutes)
3. Runs mode choice with initial coefficients (10 minutes)

After reviewing results, the coefficients are adjusted slightly and the model reruns.

- Without caching: Repeat all 3 steps = 75 minutes per iteration × 5 iterations = 375 minutes total
- With caching: Step 1–2 are cache hits (data unchanged), only step 3 re-executes = 45 + 20 (cached) + (10 × 5 iterations) = 115 minutes
- **Time saved: 69% reduction; frees analyst time for interpretation**

**Example 3: Grid Dispatch Multi-Scenario Ensemble**

Grid planners evaluate resource adequacy under uncertainty. A baseline scenario and 8 future scenarios (4 load growth levels × 2 renewable penetration targets) all share the same network preprocessing pipeline.

- Baseline preprocessing (load shape development, transmission topology, renewable capacity factors): 3 hours
- Each scenario-specific dispatch simulation: 20 minutes
- Without caching: 9 × (3 hours + 20 min) = 29.85 hours
- With caching: Preprocessing once (3 hours), then 8 scenario runs hit cache on preprocessing = 3 hours + (8 × 20 min) = 5.67 hours
- **Time saved: 81% reduction; enables rapid scenario exploration for integrated resource planning**

## Next Steps

- See [Caching & Hydration](concepts/caching-and-hydration.md) for advanced policies (when/how to copy files, handling large datasets)
- See [Configuration & Facets](configs.md) to learn when to use `config` vs `facet`
- See [Usage Guide](usage-guide.md) for multi-step workflow patterns
