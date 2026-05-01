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
The short version: pick based on what your team actually does day-to-day.

- **Consist** fits teams running large simulation codebases — travel demand models, power grid simulations, weather and climate models, etc. — where a single workflow runs for hours, produces many intermediate files, gets re-run across parameter variations, and needs to be auditable years later. It's strongest when you want to ask "which config produced this output?" or "what changed between this run and last week's?" using SQL.
- **DVC** is built around versioning large binary files alongside Git and orchestrating pipelines that produce them. If your primary need is "treat this 10GB dataset like a Git-tracked file," DVC is a closer fit.
- **MLflow** is built around model training experiments — its UI and abstractions center on metrics, hyperparameters, and model artifacts for ML workflows. If you're tracking accuracy across training runs, MLflow's experiment-tracking surface is more idiomatic.

These tools aren't mutually exclusive — Consist focuses on the lineage and caching layer for simulation work, and stays out of model registries or large-file Git replacement.

---

## What you get

### What does Consist record for each run?
At minimum, each tracked run captures:

- **Status and timing**: started_at, ended_at, duration, success/failure
- **Code version**: Git commit SHA (and dirty state) or a callable-source hash
- **Config snapshot**: the full config dict that affected the cache signature
- **Input artifacts**: paths and content hashes for every file the run consumed
- **Output artifacts**: paths, content hashes, and format metadata for everything the run produced
- **Lineage**: which prior runs produced the inputs to this run
- **Scenario / facet metadata**: scenario id, stage, year, iteration, tags, and any custom facets you logged

All of this is stored in two synchronized places — a per-run `consist.json` snapshot in `run_dir`, and a queryable DuckDB database — so you can both inspect a single run on disk and run SQL across the full history.
