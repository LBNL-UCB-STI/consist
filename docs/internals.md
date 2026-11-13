# Internals

This page documents Consist's internal persistence schema, digest schemes, signature construction, location freshness verification, and migration pipeline. It is intended for Consist contributors and power users inspecting `provenance.duckdb` directly.

For the user-facing mental model, see [How Consist Works](architecture.md).

---

## Core Data Model

In user-facing code, an `Artifact` ties together three concepts: its **name** (`art.key`), its **fingerprint** (`art.digest`), and its **location** (`art.path` / `art.uri`). Inside `provenance.duckdb`, Consist records runs, artifacts, content identities, run-artifact bindings, and location witnesses via SQLModel + DuckDB (`consist.core.persistence.DatabaseManager`).

### Mapping User-Facing Terms to Store Tables

| User-facing concept | Public v0.5 API | Internal store representation |
| :--- | :--- | :--- |
| **Fingerprint** | `art.digest` (`Digest(scheme, hex)`) | `artifact_content(id, content_hash, driver, ...)` and `artifact.hash` / `artifact.content_id` |
| **Name** | `art.key`, `inputs`/`outputs` keys | `run_artifact_link(run_id, artifact_id, direction)` and `artifact.key` |
| **Location** | `art.path`, `art.uri` | `artifact.container_uri` + `artifact_location(artifact_id, uri, witness_json, ...)` |
| **Artifact** | `Artifact` | `artifact(id, key, driver, container_uri, hash, content_id, run_id, parent_artifact_id, ...)` |
| **Run** | `Run` | `run(id, model_name, status, stage, phase, git_hash, config_hash, input_hash, signature, ...)` |

---

## Execution Modes (`Tracker`)

Consist supports three execution modes depending on `db_path` and `run_dir`:

1. **Full Provenance Mode (`db_path=<path>, run_dir=<path>`)**: Dual-writes run and artifact records to `provenance.duckdb` and `consist.json`, computes content digests according to `hashing_strategy`, and enables full caching and SQL querying.
2. **JSON-Only Mode (`db_path=None, run_dir=<path>`)**: Computes full digests and writes `consist.json` snapshots inside `run_dir` without opening DuckDB. Useful for DB-less distributed workers whose provenance can later be reconstructed into a central DuckDB store via `tracker.rebuild_from_json()`.
3. **Ephemeral Mode (`db_path=None, run_dir=None`)**: Creates no directories, opens no DuckDB database, writes no `consist.json`, and uses lightweight `FileStat` fingerprints with sub-millisecond overhead.

---

## Signature Composition

Consist computes a single unified content-addressed signature across `tracker.run()`, `scenario.run()`, and `trace()`:

```text
signature = SHA256(code_identity || config_identity || bound_inputs || declared_outputs)
```

1. **Code Identity (`code_hash`)**:
   - `repo_git` (default): Git HEAD commit SHA plus dirty working-tree state.
   - `callable_module`: SHA-256 of the Python module file defining the unwrapped callable, plus any declared `extra_deps`.
   - `callable_source`: SHA-256 of the unwrapped callable's source code, plus any declared `extra_deps`.
   - Container steps record the container image digest, command, environment digest, backend, working directory, and virtualized volume mounts.
2. **Config Identity (`config_hash`)**:
   - Canonical JSON serialization with sorted keys, normalized numeric types, deterministic Pydantic model serialization, and adapter-provided config facets/identity inputs.
3. **Role-Aware Input Bindings (`input_hash`)**:
   - Ordered by binding role/key. Each input binding contributes its parameter name, its verified digest, and any sub-artifact selector (such as an HDF5 table path or Zarr variable key).
   - Resolved filesystem paths are never hashed into `input_hash`.
4. **Declared Output Specifications**:
   - When outputs are declared upfront via `outputs={...}` (e.g., `outputs={"cleaned": "cleaned.parquet"}`), the declared output names and filenames/extensions are included in `config_hash` without baking resolved host workspace directories (`run_dir`) into the signature.

---

## Location Witnesses & Freshness Verification

To avoid re-hashing gigabytes of unchanged data on every step while still catching out-of-band file modifications, `consist.location` provides typed location witnesses:

- **`FileStat`**: Records `(size, mtime_ns, inode)` for a single file.
- **`DirTreeStat`**: Records directory-level and member-level stat summaries for directory trees and Zarr stores.

Before executing a step with pre-existing declared output targets, Consist snapshots pre-step stats/digests and raises `StaleOutputError` if a pre-existing output file, directory/Zarr store, or HDF5 member was left untouched on disk.

---

## Dual-Write Persistence & `consist migrate` Architecture

### Dual-Write Order

Every run in Full Provenance Mode maintains two persistence records:

1. **`consist.json` (Per-Run Snapshot)**: Written atomically inside `run_dir` for human-readable inspection and JSON-based recovery (`rebuild_from_json()`).
2. **`provenance.duckdb` (`DatabaseManager`)**: Commits the run, artifact, link, and location records inside an explicit DuckDB transaction.

### `consist migrate` Pipeline

`consist migrate <db_path>` upgrades a Consist `0.4.x` database to `0.5` out-of-place:

1. **Out-of-place staging**: Reads the `0.4.x` database read-only and builds a verified `0.5` staging database.
2. **Verification & `--trust-v04-hashes`**: Verifies surviving files on disk (or trusts existing v0.4 SHA-256 hashes when `--trust-v04-hashes` is passed), marking missing/unverified historical entries so they never produce unsafe cache hits.
3. **Atomic backup & swap**: Saves the original database to `<db_path>.v04.bak` and swaps the verified staging database into place, supporting `consist migrate undo <db_path>` for reversibility.
