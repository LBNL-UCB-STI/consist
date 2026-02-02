# Frequently Asked Questions (FAQ)

## Caching & Performance

### Why is my cache not hitting?
Cache misses occur when the **Signature** changes. Check if:
- You have uncommitted code changes (your Git repo is "dirty").
- The input file content has changed (even a tiny metadata change can trigger a re-hash).
- You are passing a non-deterministic config (e.g., a dict with a timestamp or a random seed).
- Your function code changed.

Use `CONSIST_CACHE_DEBUG=1` to log detailed signature components and identify exactly what changed.

### Can I use Consist without Git?
Yes. If Git is not found, Consist falls back to a static `unknown_code_version`. However, this means code changes won't automatically invalidate your cache. We recommend using Git for full reproducibility.

### Does Consist work with massive files?
Yes. Consist tracks files as **Artifacts**. It stores the path and hash but doesn't necessarily copy the bytes into the database unless you call `tracker.ingest()`. For multi-GB files, we recommend keeping them as "Cold Data" (on disk) and using `cache_hydration="metadata"` (the default).

---

## Data & Storage

### Where are my results stored?
Results are stored in the `run_dir` you provided when initializing the `Tracker`. Each run gets its own subdirectory containing a `consist.json` snapshot and any output files you logged.

### Can I share my provenance database with a colleague?
Yes. If you share your DuckDB file and your `run_dir`, your colleague can query your results. If you use **Mounts**, they can resolve the same portable URIs to their local filesystem paths. See [Mounts & Portability](mounts-and-portability.md).

### How do I reclaim disk space?
You can safely delete intermediate output files if they have been **ingested** into DuckDB. Consist's **Ghost Mode** allows you to recover the data from the database or re-run the pipeline to regenerate the files.

---

## Integration

### Does Consist support R or Java?
Consist is a Python library, but you can track *any* external tool using the **Container Integration** or by wrapping a subprocess call in a Python function. As long as you can define the inputs and capture the outputs, Consist can track it.

### How does this compare to DVC or MLflow?
- **DVC**: Focuses on large file versioning and pipeline orchestration. Consist focuses on **fine-grained provenance** and **SQL-native analytics** via DuckDB.
- **MLflow**: Focuses on experiment tracking and model deployment. Consist focuses on **scientific simulation lineage** and **data handoffs** between complex model steps.

Consist is designed specifically for "heavy" scientific workflows where data integrity and SQL queryability are paramount.
