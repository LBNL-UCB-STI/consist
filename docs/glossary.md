# Glossary

Short definitions for terms used across Consist docs. For the mental model, see
[Core Concepts](concepts/overview.md).

| Term | Meaning |
|---|---|
| Artifact | A tracked file or file-like output with path, format, fingerprint, direction, and producing or consuming run metadata. |
| Cache hit | A run lookup result where Consist finds a prior completed run with the same cache signature and returns its output artifact metadata instead of executing again. |
| Cache miss | A run lookup result where no matching completed run exists, so Consist executes the step and records a new run. |
| Canonical hashing | Deterministic hashing of config, inputs, and identity metadata so equivalent values produce the same cache signature. |
| Config | Run parameters included in cache identity through `config={...}`. Changing identity config invalidates the cache for that run. |
| Coupler | The scenario helper that maps named outputs from one step into named inputs for later steps. |
| DLT | Optional data loading integration used by Consist to ingest typed tabular data into DuckDB with provenance columns. |
| Derivable references | Maintenance condition where Consist can infer safe table relationships from stored keys such as `run_id` and `content_hash`. |
| Facet | Queryable run metadata, such as `year`, `scenario`, `stage`, or `phase`, used to find and compare runs. |
| Ghost mode | Recovery mode where provenance or ingested data remains queryable even when the original cold file is missing. |
| Hydration | Restoring artifact metadata and usable artifact objects from prior runs without necessarily copying bytes. |
| Identity config | The full configuration or adapter-derived identity payload that contributes to a run signature. |
| Ingestion | Loading artifact contents into DuckDB for SQL-native analysis and recovery. |
| Lineage | The dependency chain from an output artifact back through the runs, inputs, configs, and upstream artifacts that produced it. |
| Materialization | Ensuring bytes exist at a requested destination, either by copying historical files or exporting ingested data. |
| Provenance | Evidence for what produced a result: code, config, inputs, environment metadata, outputs, and status. |
| Recovery roots | Ordered archive or mirror roots stored on artifact metadata so future hydration can find bytes after the original workspace path moves. |
| Run | A single tracked execution, cache hit, or trace block with inputs, config, outputs, status, timing, and metadata. |
| Run-link table | A global table containing `run_id` but not `consist_run_id`; used for run-associated metadata and safe maintenance derivation. |
| Run-scoped table | A global table containing `consist_run_id`; rows are directly attributable to a specific run. |
| Scenario | A parent run that groups related child runs for a multi-step workflow, iteration, or scenario variant. |
| Signature | The deterministic cache key built from code identity, config identity, inputs, and selected execution metadata. |
| Staging | Copying a canonical artifact to a requested local path for a path-bound callable or external tool. |
| Trace | An always-execute tracked block that records provenance without cache-skipping the block. |
| Unscoped cache table | A global table with neither `run_id` nor `consist_run_id`; preserved by default unless maintenance can prove safe pruning. |
| View | A registered SQLModel-backed query surface for analyzing runs or artifact data. |
| Virtualization | Querying file-backed and/or ingested artifacts through DuckDB without eagerly loading every file into memory. |
| Hybrid view | A SQL view that combines hot ingested data with cold file-backed data so runs can be queried together. |

See also: [Caching & Hydration](concepts/caching-and-hydration.md),
[Data Materialization](concepts/data-materialization.md), and
[Historical Recovery](guides/historical-recovery.md).
