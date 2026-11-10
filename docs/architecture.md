# How Consist Works

Consist tracks three things about every file your workflow touches:

1. **What is it called?** Its **name** inside a step — for example, `"raw_trips"` or `"cleaned"`.
2. **What does it contain?** Its **fingerprint** — a hash of the file's contents.
3. **Where is it?** Its **location** — a path on disk or a table in DuckDB where those contents can be read right now.

Consist keeps these three answers separate on purpose:

- **Renaming or moving a file** changes its location, not its fingerprint.
- **Re-running a step that produces identical bytes** creates a new run, not a new fingerprint.
- **Swapping two inputs** changes which name points to which fingerprint, even though the files themselves did not change.

An **artifact** is the handle you get back from Consist that ties all three together:

| Question | Term | In code |
| :--- | :--- | :--- |
| What is it called in this step? | **Name** | `art.key` |
| What does it contain? | **Fingerprint** | `art.identity`, `art.digest()` |
| Where can I read it right now? | **Location** | `art.path`, `art.location` |

---

<a id="how-caching-works"></a>

## A run is a recipe

When you call `tracker.run(...)` or `scenario.run(...)`, Consist builds a **signature** from the step's recipe before running your function:

```mermaid
flowchart LR
    Code["Code"] --> Sig(["Signature"])
    Config["Config"] --> Sig
    Inputs["Inputs (name + fingerprint)"] --> Sig
    Outputs["Declared outputs (name + format)"] --> Sig
    Sig --> Result["Outputs"]
```

The recipe includes:

- **Code** — the identity of the function or container being executed (plus any declared extra file dependencies).
- **Config** — the canonical JSON representation of your `config` dictionary or Pydantic model.
- **Inputs** — each input's **name** and **fingerprint** (plus any sub-table or array selector).
- **Declared outputs** — when outputs are declared upfront (such as `outputs={"cleaned": "cleaned.parquet"}`), their **names** and **file names/formats** (never resolved workspace paths).

Notice what is **not** in the signature: **locations**. Consist cares *what* an input contains and *which parameter* it is passed to, not which folder or machine it currently sits in. Likewise, declared output filenames capture the requested format (`"cleaned.parquet"` vs. `"cleaned.csv"`) without baking your machine's `run_dir` path into the signature.

---

## When is a result reused?

Before executing a step, Consist checks whether an earlier completed run with the same **signature** already exists and whether its output files are still available at a valid **location**.

| If you change... | Reused? | Why |
| :--- | :--- | :--- |
| the contents of an input | No | Its fingerprint changed |
| a config value | No | The config is part of the recipe |
| your function's code | No | The code is part of the recipe |
| which parameter an input goes to | No | Input names are part of the recipe |
| a declared output's name or file format (`cleaned.parquet` -> `cleaned.csv`) | No | Declared output names and formats are part of the recipe |
| where an input or run directory lives (moved, archived, copied) | **Yes** | Locations aren't part of the recipe |
| an upstream step, which then produced identical output | **Yes** | Downstream steps see the same fingerprint |
| `runtime_kwargs` | **Yes** | Treated as runtime tuning, not part of the recipe |

> [!NOTE]
> **Containers and host volume paths:** When you run a container step and map host directories through `tracker.mounts`, Consist records portable mount prefixes (`inputs://...`, `workspace://...`) so moving a mounted directory across machines still reuses cached runs. Unmounted raw host volume paths remain host-specific; see the [Container Integration Guide](containers-guide.md) for details.

### Cache modes

You can control how Consist uses the cache on any run via `cache_mode`:

| Mode | Behavior |
| :--- | :--- |
| `reuse` (default) | Reuse a matching prior run if its outputs can still be read; otherwise execute the step. |
| `overwrite` | Always execute the step and record the new outputs as the latest result. |
| `readonly` | Reuse matching cached runs, or execute without writing new records to the database. |

---

## What you get on a cache hit

Whether a step ran fresh or hit the cache, `tracker.run(...)` returns a `RunResult` containing output **artifacts**:

```python
result = tracker.run(
    fn=clean_passengers,
    inputs={"raw": "data/passengers.csv"},
    outputs={"cleaned": "cleaned.parquet"},
    config={"min_age": 18},
)

if result.cache_hit:
    print("Reused cached result!")

cleaned = result.outputs["cleaned"]
print(cleaned.path)      # Current readable Path on disk
df = cleaned.as_df()     # Load directly into a DataFrame
```

Always read files through `artifact.path` or `artifact.as_df()` rather than hardcoding paths into your own `run_dir` subfolder. On a cache hit, `cleaned.path` automatically resolves to wherever those bytes live right now — whether in the original run folder, a shared archive, or a staged local copy.

### Ghost Mode

Because Consist separates an artifact's **fingerprint** from its **location**, you can delete bulky intermediate files to save disk space without losing provenance ("Ghost Mode"):

- Downstream steps whose final outputs are still on disk continue to hit the cache, even if intermediate files in the middle of the pipeline were deleted.
- If an intermediate artifact was ingested into DuckDB, `artifact.as_df()` can still load it directly from the database after the file on disk is gone.
- If a missing intermediate file is needed again for a new downstream branch, Consist knows which upstream run produced that fingerprint and re-executes only the steps required to regenerate it.

---

## Where files live

By default, each step writes its outputs into a subfolder of `run_dir`. To keep workflows portable across laptops, servers, and HPC clusters, you can configure named `mounts` on your `Tracker`:

```python
tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/shared/data/inputs",
        "archive": "/mnt/nfs/project_archive",
    },
)
```

When a file lives inside a configured mount, Consist records its location relative to that mount (for example, `inputs://land_use.csv`). When you move to another machine and point `"inputs"` at that machine's folder, Consist resolves the location automatically. See [Mounts & Portability](mounts-and-portability.md) for the complete guide.

---

## What gets recorded

Consist records every run in two places:

1. **`consist.json` (per-run snapshot):** Written atomically inside each run directory when the step finishes. It is a self-contained, human-readable snapshot of that run's recipe, signature, input/output names, fingerprints, and initial locations at the moment the run completed.
2. **`provenance.duckdb` (analytics and location index):** Indexes all runs, fingerprints, and current locations in DuckDB for fast lineage queries, cache lookups, and cross-run comparisons. Post-run location updates — such as archiving files to cold storage, evicting scratch directories, or promoting migrated runs — are tracked in `provenance.duckdb`. If the database is ever lost, run history and fingerprints can be reconstructed from your `consist.json` snapshots and surviving files on disk.

<a id="data-virtualization"></a>

### Querying across runs

Because `provenance.duckdb` indexes every run and artifact, you can query outputs across dozens of scenarios or simulation years at once. Consist's **hybrid views** combine rows ingested into DuckDB with raw Parquet/CSV/Zarr files on disk in a single SQL view, tagging rows with `consist_run_id`, `consist_year`, and `consist_scenario_id`. See [Data Storage and Ingestion](concepts/data-materialization.md) and the [CLI Reference](cli-reference.md) for examples.

---

## Upgraded from 0.4?

If you upgraded an existing project from Consist 0.4 using `consist migrate <db_path>`, Consist verifies existing files on disk and computes their v2 fingerprints.

Any historical run whose files were already deleted, overwritten, or skipped with `consist migrate <db_path> --quick` is preserved as **history-only**:

- It remains **fully visible** in lineage graphs, run queries, and hybrid views.
- It **never produces a false cache hit**, because its original file contents could not be verified on disk during migration.
- If you later restore the original file from a backup or archive, you can upgrade that artifact to full cache-eligible status at any time:

```bash
consist promote ./provenance.duckdb <artifact-id-or-historical-fingerprint> --path /path/to/restored_file.parquet
```

---

## Going deeper

- [Caching & Hydration](concepts/caching-and-hydration.md) — cache modes, materialization options, and Ghost Mode recovery patterns.
- [Mounts & Portability](mounts-and-portability.md) — configuring portable mounts and relocating run directories across machines.
- [Container Integration Guide](containers-guide.md) — running Docker and Apptainer steps with portable volume mounts.
- [Internals](internals.md) — contributor reference covering the DuckDB hub-and-spokes store schema, digest schemes, location witnesses, and migration architecture.
