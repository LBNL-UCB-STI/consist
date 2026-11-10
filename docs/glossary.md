# Glossary

Short definitions for terms used across Consist docs. For the mental model, see
[How Consist Works](architecture.md) and [Core Concepts](concepts/overview.md).

## Six Core Terms

| Term | What it means | In code |
| :--- | :--- | :--- |
| **Name** | The label a file has within one step | `art.key`, the keys of `inputs` / `outputs` dicts |
| **Fingerprint** | The hash of a file's contents | `art.identity`, `art.digest()` |
| **Location** | A place where the contents can be read right now | `art.path`, `art.location` |
| **Artifact** | The handle that ties a name, fingerprint, and locations together | `Artifact` |
| **Run** | One execution of a step, cached or not | `Run`, the result of `tracker.run(...)` |
| **Signature** | The recipe's identity, used to find cache hits | `run.signature` |

---

## All Terms

| Term | Meaning |
| :--- | :--- |
| **Artifact** | The handle that ties a file's **name** (`art.key`), **fingerprint** (`art.identity`), and readable **locations** (`art.path`, `art.location`) together. |
| **Cache hit** | A run lookup result where Consist finds a prior completed run with the same signature and readable outputs, returning those artifacts instead of executing again. |
| **Cache miss** | A run lookup result where no matching completed run with readable outputs exists, so Consist executes the step and records a new run. |
| **Canonical hashing** | Deterministic hashing of config, inputs, and identity metadata so equivalent values produce the same signature. |
| **Config** | Run parameters included in the signature through `config={...}`. Changing config invalidates the cache for that run. |
| **Config identity** | The configuration component (full config dict or adapter-derived structure) that contributes to a run signature. |
| **Coupler** | The scenario helper that maps named outputs from one step into named inputs for later steps. |
| **Derivable references** | Maintenance condition where Consist can infer safe table relationships from stored keys such as `run_id` and `content_hash`. |
| **DLT** | Optional data loading integration used by Consist to ingest typed tabular data into DuckDB with provenance columns. |
| **DuckDB ingestion** | Loading artifact contents into DuckDB for SQL-native analysis and recovery. |
| **Facet** | Queryable run metadata, such as `year`, `scenario`, `stage`, or `phase`, used to find and compare runs without changing the signature unless requested. |
| **Filesystem materialization** | Ensuring bytes exist at a requested filesystem destination, either by copying historical files or exporting ingested data. |
| **Fingerprint** | The content hash of a file, directory tree, or table (`art.identity`, `art.digest()`). Independent of where the file is stored or what it is named. |
| **Ghost mode** | Deleting bulky intermediate files on disk while preserving their fingerprints, provenance, and ingested data in DuckDB so downstream steps remain cached and missing files can be regenerated on demand. |
| **Hybrid view** | A SQL view that combines hot ingested data in DuckDB with cold file-backed data on disk so runs can be queried together. |
| **Hydration** | Restoring usable artifact handles and locations from prior runs without necessarily copying bytes. |
| **Lineage** | The dependency chain from an output artifact back through the runs, inputs, configs, and upstream artifacts that produced it. |
| **Location** | A filesystem path (`art.path`) or DuckDB table (`art.location`) where an artifact's contents can be read right now. |
| **Mount** | A named root directory (for example `mounts={"inputs": "/shared/data"}`) that lets Consist record portable locations like `inputs://land_use.csv` across machines. |
| **Name** | The label an input or output has inside a step (`art.key`, or the dictionary key passed to `inputs={...}` / `outputs={...}`). |
| **Provenance** | The complete record of what produced a result: code, config, input names and fingerprints, declared outputs, timestamps, and status. |
| **Run** | One execution of a step, cached or not (`Run`, returned via `tracker.run(...)`). |
| **Run-link table** | A global table containing `run_id` but not `consist_run_id`; used for run-associated metadata and safe maintenance derivation. |
| **Run-scoped table** | A global table containing `consist_run_id`; rows are directly attributable to a specific run. |
| **Scenario** | A parent run that groups related child runs for a multi-step workflow, iteration, or scenario variant. |
| **Signature** | The recipe's deterministic identity built from code, config, input names and fingerprints, and declared output names and formats (`run.signature`). Used for cache lookup. |
| **Staging** | Copying or linking an artifact to a requested local path for a path-bound callable or external tool. |
| **Trace** | An always-execute tracked block that records provenance without cache-skipping the block. |
| **Unscoped cache table** | A global table with neither `run_id` nor `consist_run_id`; preserved by default unless maintenance can prove safe pruning. |
| **View** | A registered SQLModel-backed query surface for analyzing runs or artifact data. |
| **Virtualization** | Querying file-backed and/or ingested artifacts through DuckDB without eagerly loading every file into memory. |

See also: [How Consist Works](architecture.md), [Caching & Hydration](concepts/caching-and-hydration.md),
[Data Storage and Ingestion](concepts/data-materialization.md), and
[Historical Recovery](guides/historical-recovery.md).
