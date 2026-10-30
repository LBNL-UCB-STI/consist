# Content & Tabular Digests (consist.digest)

`consist.digest` is Consist's zero-weight cryptographic hashing library. It
provides deterministic, format-neutral hashing for bytes, files, canonical JSON,
order-invariant tables (`ctd1`), multi-facet state dictionaries, and
floating-point array/matrix containers.

## Two-Layer Identity Architecture

Consist separates identity into two layers:

| Layer | Module / Surface | Scope | Runtime Dependencies |
|---|---|---|---|
| **Layer 1: Content Digests** | `consist.digest` | Deterministic hashing of bytes, files, canonical JSON, `ctd1` order-invariant tables, state dictionaries, and logical array/matrix containers. | Python standard library only at module scope (`hashlib`, `json`, `sqlite3`, `struct`). Heavy array/table libraries (`pyarrow`, `numpy`, `h5py`, `zarr`) load lazily when invoked. |
| **Layer 2: Run & Boundary Provenance** | `IdentityManager`, `consist.admission` | Merkle-DAG run signatures (Code + Config + Inputs), cache keys, and boundary admission contracts (`PriorRunExpectation`, `DeclaredDigestExpectation`). | DuckDB, SQLModel, SQLAlchemy, and active Tracker or boundary evaluation context. |

### Design Separation

- **Layer 1 (`consist.digest`)** computes data fingerprints without database state, migrations, or tracker instances. Module import loads only Python standard library modules. This keeps digests usable in worker scripts, data validation hooks, CI quality gates, and container wrappers without tracker overhead.
- **Layer 2 (`IdentityManager` and `consist.admission`)** tracks execution lineage across runs. `IdentityManager` hashes the combination of code state, run configs, and input artifacts to generate cache keys. At pipeline boundaries, `consist.admission` checks physical inputs against expected prior-run outputs or declared fingerprints before step execution.

---

## Capabilities & Usage

### 1. Byte and File Hashing

Compute SHA-256 digests for in-memory bytes or filesystem files:

```python
from pathlib import Path
from consist.digest import digest_bytes, file_sha256, digest_file

# Raw bytes SHA-256 (64 lowercase hex)
raw_hex = digest_bytes(b"hello world\n")

# File SHA-256 using streaming stdlib hashlib.file_digest (fast & memory-efficient)
file_hex = file_sha256("data/sample.csv")

# Self-describing file identity for Consist admission ("sha256:file:<64 hex>")
identity = digest_file("data/sample.csv")
print(identity)
# => "sha256:file:a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
```

### 2. Canonical JSON Serialization & Hashing

Standard `json.dumps` produces non-deterministic outputs depending on key insertion
order and formatting options. `canonical_json_str` and `canonical_json_sha256` provide
deterministic, RFC 8785-inspired JSON serialization:

```python
from consist.digest import canonical_json_str, canonical_json_sha256

data_a = {"beta": [1, 2, 3], "alpha": "test", "nested": {"b": 2, "a": 1}}
data_b = {"alpha": "test", "nested": {"a": 1, "b": 2}, "beta": [1, 2, 3]}

# Lexicographically sorted keys, compact separators, normalized types
assert canonical_json_str(data_a) == canonical_json_str(data_b)

# Deterministic 64-character SHA-256 digest
digest = canonical_json_sha256(data_a)
```

### 3. Order-Invariant Tabular Digests (`ctd1`)

Data pipelines often produce tables where row ordering varies across parallel partitions,
or where data is stored in different formats (such as Parquet, PyArrow Table, or SQLite).
`digest_table` implements the Canonical Table Digest specification (`ctd1`):

- **Order-invariance**: Row order differences do not affect the digest.
- **Cross-format stability**: A table in Parquet, PyArrow, or SQLite yields the identical
  `ctd1` digest given equivalent logical schemas and data.
- **Scalar normalization**: Values are normalized deterministically via
  `normalize_table_scalar`, converting strings (NFC Unicode), booleans, integers,
  and floating-point numbers into canonical bit representations.

```python
import pyarrow as pa
from consist.digest import digest_table

table_1 = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
# Reversed row order
table_2 = pa.table({"id": [3, 2, 1], "value": ["c", "b", "a"]})

digest_1 = digest_table(table_1)
digest_2 = digest_table(table_2)

assert digest_1 == digest_2
print(digest_1)
# => "sha256:ctd1:..."
```

### 4. Hierarchical State Hashing

`digest_state` calculates a deterministic Merkle digest across multi-facet state
dictionaries, configuration bundles, or structured metadata checkpoints:

```python
from consist.digest import digest_state

state = {
    "parameters": {"learning_rate": 0.01, "epochs": 100},
    "features": ["age", "income", "zone"],
    "version": 1,
}

state_digest = digest_state(state)
```

### 5. Logical Array & Matrix Canonicalization

Transportation and simulation models frequently interchange matrix containers (such as
OMX, HDF5, or Zarr stores) across Python and JVM/C++ pipelines. Slight differences in
file layout, compression chunks, or floating-point IEEE 754 representations (e.g. `NaN`
payload bits or `-0.0` vs `+0.0`) can cause bitwise file hash mismatches even when the
underlying matrices are mathematically equivalent.

`consist.digest` provides array canonicalization policies and format-neutral
`sha256:logical_matrices:<64 hex>` computation:

- **Profiles**:
  - `"preserve_canonical"` (default): Preserves native numeric dtypes (`float64`,
    `float32`, integers) in canonical little-endian form while normalizing non-signaling
    NaNs and `-0.0`. In OMX files, preserves `/lookup` zone mappings.
  - `"float32_matrices_v1"`: Opt-in profile casting float arrays under `/data` to
    canonical little-endian `float32` (`<f4`) with normalized NaNs (`0x7FC00000`) and
    `+0.0` (`0x00000000`), matching JVM/Scala matrix writers.

```python
from consist.digest import (
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
)

# Compute logical identity of an OMX container
omx_digest = compute_omx_logical_matrices_sha256(
    "models/skims.omx",
    policy="preserve_canonical",
)

# Compute logical identity of an equivalent Zarr store
zarr_digest = compute_zarr_logical_matrices_sha256(
    "models/skims.zarr",
    policy="preserve_canonical",
)

# Cross-format equality verification
if omx_digest == zarr_digest:
    print("OMX and Zarr contain logically identical matrices!")
```

---

## API Reference

### File & Byte Hashing

::: consist.digest.digest_bytes
    options:
      show_root_heading: true

::: consist.digest.file_sha256
    options:
      show_root_heading: true

::: consist.digest.digest_file
    options:
      show_root_heading: true

### Canonical JSON

::: consist.digest.normalize_json
    options:
      show_root_heading: true

::: consist.digest.canonical_json_str
    options:
      show_root_heading: true

::: consist.digest.canonical_json_sha256
    options:
      show_root_heading: true

### Tabular & State Hashing

::: consist.digest.normalize_table_scalar
    options:
      show_root_heading: true

::: consist.digest.digest_table
    options:
      show_root_heading: true

::: consist.digest.digest_state
    options:
      show_root_heading: true

### Logical Array & Matrix Hashing

::: consist.digest.compute_logical_matrices_sha256
    options:
      show_root_heading: true

::: consist.digest.compute_omx_logical_matrices_sha256
    options:
      show_root_heading: true

::: consist.digest.compute_zarr_logical_matrices_sha256
    options:
      show_root_heading: true

::: consist.digest.ArrayCanonicalizationPolicy
    options:
      show_root_heading: true
