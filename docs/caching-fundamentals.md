# How Caching Works

Consist uses intelligent caching to skip redundant computation. This page explains the core mechanism.

## The Basic Idea

Consist computes a fingerprint (signature) from three components:

1. **Your function's code** – Git commit hash + local modifications
2. **Configuration** – The `config` dict you pass to `consist.run()`
3. **Input files** – SHA256 hashes of files in the `inputs` dict

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
| Comments in code | ❌ No | Code still hashes to same value if not committed |

## What Does Cache Return?

When there's a cache hit, Consist returns:

- **Artifact metadata** – Information about what run created the output, with what config
- **File paths** – Where the output was stored
- **Optionally, file bytes** – Depend on your cache hydration policy (see [Caching & Hydration](caching-and-hydration.md) for advanced options)

Important: A cache hit returns **metadata about the result**, not necessarily a copy of the files.

## Common Misconception

**"Cache hit means the output files are copied to my new run directory."**

Not necessarily. A cache hit returns artifact metadata (provenance). Whether file bytes are copied depends on your cache hydration policy. By default:
- File paths are preserved (you can access the original file)
- Bytes are not copied (saves disk space)

See [Caching & Hydration](caching-and-hydration.md) if you need to force file copying.

## When Caching Saves Time

Caching is most valuable in workflows with many runs and expensive computation:

**Example 1: Parameter sweep**
- 100 parameter combinations to test
- Each run is 10 minutes
- Without caching: 1000 minutes total
- If 80 are cache hits: 200 minutes + 200 cached = 400 minutes total (60% savings)

**Example 2: Iterative development**
- You're developing a 5-step pipeline
- Step 3 takes 1 hour
- You fix a bug in step 3's code
- Without caching: re-run steps 1-2 (wasted 2 hours)
- With caching: steps 1-2 are hits, only step 3 re-executes (20 minutes)

**Example 3: Multi-scenario analysis**
- You have a shared upstream model (2 hours)
- Run with 20 different downstream configs
- First scenario: 2 hours upstream + 5 minutes downstream
- Scenarios 2-20: cache hits on upstream (5 minutes each)
- Total: 2.5 hours instead of 42 hours

## Next Steps

- See [Caching & Hydration](caching-and-hydration.md) for advanced policies (when/how to copy files, handling large datasets)
- See [Configuration & Facets](configs.md) to learn when to use `config` vs `facet`
- See [Usage Guide](usage-guide.md) for multi-step workflow patterns
