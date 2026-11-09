"""
Consist unified cryptographic digest primitives.

Provides zero-weight single-file digests (``file_sha256``, ``digest_file``),
canonical JSON serialization and digests (``canonical_json_str``,
``canonical_json_sha256``), canonical tabular content digests (``digest_table``)
under the ``ctd1`` specification, Merkle state digests (``digest_state``), and
pure array/matrix canonicalization and SHA-256 digests.

This is a leaf module in Consist's dependency graph:
- Top-level imports are restricted strictly to the Python standard library.
- Heavy dependencies (PyArrow, NumPy, h5py, zarr, xarray) are imported lazily
  inside the specific functions that require them.
- Importing ``consist.digest`` never loads SQLModel, SQLAlchemy, DuckDB, or
  the Consist Tracker/ORM infrastructure.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import closing
import dataclasses
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import importlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import struct
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Protocol,
    cast,
    runtime_checkable,
)
import unicodedata

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

# --- Protocol Versions & Constants ---

ARRAY_BINARY_PROTOCOL_VERSION: str = "consist.array_binary.v1"

_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_PRIMARY_LOOKUP_KEYS = (
    "zone_number",
    "TAZ",
    "zone_id",
    "original_zone_ids",
    "otaz",
    "origin",
)
_DEFAULT_CHUNK_ROWS = 512

_DELIMITER_FIELD = b"\x1f"
_DELIMITER_RECORD = b"\x1e"
_SENTINEL_NULL = b"\x00"

LogicalProfileName = Literal["preserve_canonical", "float32_matrices_v1"]


# --- Byte & File Digests ---


def digest_bytes(data: bytes) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path | str) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of a file using stdlib hashlib.file_digest."""
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def digest_file(path: Path | str) -> str:
    """Return self-describing file identity 'sha256:file:<64-hex>'.

    Parameters
    ----------
    path : Path or str
        Path to a regular file.

    Returns
    -------
    str
        Self-describing identity string.

    Raises
    ------
    ValueError
        If ``path`` is missing or is not a regular file.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise ValueError(f"digest_file only supports regular files: {file_path}")
    return f"sha256:file:{file_sha256(file_path)}"


# --- Canonical JSON Serialization & Hashing ---


@runtime_checkable
class _ModelDumpable(Protocol):
    def model_dump(self, *, mode: str = "json") -> Any: ...


@runtime_checkable
class _LegacyModelDictable(Protocol):
    def dict(self) -> Any: ...

    def json(self) -> Any: ...


def _clean_for_canonical_json(
    obj: Any,
    exclude_keys: set[str] | None = None,
    _visited: set[int] | None = None,
) -> Any:
    """Recursively clean a structure for deterministic canonical JSON serialization."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, Enum):
        return _clean_for_canonical_json(obj.value, exclude_keys, _visited)

    # 1. Handle Pydantic v2 (model_dump) and v1 (dict + json) models
    if not isinstance(obj, type):
        if isinstance(obj, _ModelDumpable):
            return _clean_for_canonical_json(
                obj.model_dump(mode="json"), exclude_keys, _visited
            )
        if not isinstance(obj, Mapping) and isinstance(obj, _LegacyModelDictable):
            return _clean_for_canonical_json(obj.dict(), exclude_keys, _visited)
        if dataclasses.is_dataclass(obj):
            return _clean_for_canonical_json(
                dataclasses.asdict(obj), exclude_keys, _visited
            )

    # 2. Check for NumPy ndarray or scalar if numpy is loaded
    np_mod = sys.modules.get("numpy")
    if np_mod is not None:
        ndarray_cls = np_mod.__dict__.get("ndarray")
        if isinstance(ndarray_cls, type) and isinstance(obj, ndarray_cls):
            return _clean_for_canonical_json(obj.tolist(), exclude_keys, _visited)
        generic_cls = np_mod.__dict__.get("generic")
        if isinstance(generic_cls, type) and isinstance(obj, generic_cls):
            return _clean_for_canonical_json(obj.item(), exclude_keys, _visited)

    # 3. Mappings (dict, MappingProxyType, etc.)
    if isinstance(obj, Mapping):
        visited = set() if _visited is None else _visited
        obj_id = id(obj)
        if obj_id in visited:
            raise ValueError("Circular reference detected in JSON canonicalization")
        next_visited = visited | {obj_id}
        excluded = exclude_keys if exclude_keys is not None else set()
        return {
            k: _clean_for_canonical_json(v, exclude_keys, next_visited)
            for k, v in obj.items()
            if k not in excluded and str(k) not in excluded
        }

    # 4. Lists and tuples
    if isinstance(obj, (list, tuple)):
        visited = set() if _visited is None else _visited
        obj_id = id(obj)
        if obj_id in visited:
            raise ValueError("Circular reference detected in JSON canonicalization")
        next_visited = visited | {obj_id}
        return [_clean_for_canonical_json(x, exclude_keys, next_visited) for x in obj]

    # 5. Sets and frozensets (preserve natural ordering if sortable, else tokenize)
    if isinstance(obj, (set, frozenset)):
        visited = set() if _visited is None else _visited
        obj_id = id(obj)
        if obj_id in visited:
            raise ValueError("Circular reference detected in JSON canonicalization")
        next_visited = visited | {obj_id}
        cleaned_members = [
            _clean_for_canonical_json(x, exclude_keys, next_visited) for x in obj
        ]
        try:
            return sorted(cleaned_members)
        except TypeError:
            tokenized = [
                (
                    json.dumps(
                        m,
                        sort_keys=True,
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                    m,
                )
                for m in cleaned_members
            ]
            tokenized.sort(key=lambda item: item[0])
            return [m for _, m in tokenized]

    return obj


def normalize_json(payload: Any, *, exclude_keys: set[str] | None = None) -> Any:
    """Normalize a Python structure into deterministic JSON-compatible primitives.

    Normalizes dictionaries/mappings by sorting keys and optionally excluding keys,
    converts sets into deterministically sorted lists, Paths into strings,
    Pydantic models into dumped mappings, and NumPy arrays/scalars into native
    Python lists and primitives.
    """
    return _clean_for_canonical_json(payload, exclude_keys)


def canonical_json_str(payload: Any, *, exclude_keys: set[str] | None = None) -> str:
    """Serialize a Python structure into a deterministic, canonical JSON string.

    Normalizes dictionaries/mappings by sorting keys, sets into sorted lists,
    Paths into strings, and NumPy arrays/scalars into standard Python primitives.
    Uses compact separators `(',', ':')` and `ensure_ascii=True`.
    """
    cleaned = normalize_json(payload, exclude_keys=exclude_keys)
    return json.dumps(
        cleaned,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def canonical_json_sha256(payload: Any, *, exclude_keys: set[str] | None = None) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of canonical_json_str(payload)."""
    return digest_bytes(
        canonical_json_str(payload, exclude_keys=exclude_keys).encode("utf-8")
    )


# --- Canonical Tabular Digest (ctd1) & State Digest (core_state/v1) ---


def normalize_table_scalar(val: Any) -> bytes:
    """Normalize a scalar value to canonical UTF-8 bytes for tabular digesting."""
    if val is None:
        return _SENTINEL_NULL

    if isinstance(val, bool):
        return b"true" if val else b"false"

    if isinstance(val, int):
        return str(val).encode("utf-8")

    if isinstance(val, float):
        if math.isnan(val):
            return b"nan"
        if val == 0.0:
            return b"0.0"
        return str(val).encode("utf-8")

    if isinstance(val, str):
        return unicodedata.normalize("NFC", val).encode("utf-8")

    if isinstance(val, bytes):
        return val

    # Fallback to string representation normalized to NFC
    return unicodedata.normalize("NFC", str(val)).encode("utf-8")


def _quote_sql_ident(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(
            "SQLite identifier must be a non-empty string without NUL bytes"
        )
    return '"' + name.replace('"', '""') + '"'


def _has_nontrivial_pandas_index(df: Any) -> bool:
    """Return True when a pandas DataFrame carries a named, MultiIndex, or non-default index."""
    idx = df.index
    names = list(idx.names)
    if len(names) > 1:
        return True
    if names and names[0] is not None:
        return True
    # Unnamed single-level index: check if it is the default 0..N-1 positional index
    if idx.__class__.__name__ == "RangeIndex":
        return not (int(idx.start) == 0 and int(idx.step) == 1)
    if idx.dtype.kind in ("i", "u"):
        n = len(df)
        if len(idx) == n and all(int(v) == i for i, v in enumerate(idx.tolist())):
            return False
    return len(idx) > 0


def _dataframe_to_arrow_table_with_index(df: Any, pa_mod: Any) -> Any:
    """Convert a pandas DataFrame to a PyArrow Table, preserving non-trivial indexes as leading columns."""
    if len(set(df.columns)) != len(df.columns):
        raise ValueError("Duplicate column names are not supported in digest_table")
    if not _has_nontrivial_pandas_index(df):
        return pa_mod.Table.from_pandas(df, preserve_index=False)

    idx_names = [
        str(name) if name is not None else f"__index_level_{i}__"
        for i, name in enumerate(df.index.names)
    ]
    overlap = set(idx_names) & set(str(c) for c in df.columns)
    if overlap:
        raise ValueError(
            f"Index level name(s) {sorted(overlap)} collide with DataFrame columns"
        )
    df_copy = df.copy()
    df_copy.index.names = idx_names
    df_reset = df_copy.reset_index()
    return pa_mod.Table.from_pandas(df_reset, preserve_index=False)


def _materialize_arrow_pandas_index(
    table: Any,
    pa_mod: Any,
    columns: Sequence[str] | None,
) -> tuple[Any, Sequence[str] | None]:
    """Ensure non-trivial pandas indexes in a PyArrow Table are materialized as leading columns."""
    meta = table.schema.pandas_metadata
    if not meta:
        return table, columns
    raw_idx_cols = meta.get("index_columns", [])
    if not raw_idx_cols:
        return table, columns

    has_nontrivial = False
    for entry in raw_idx_cols:
        if isinstance(entry, str):
            has_nontrivial = True
            break
        if isinstance(entry, Mapping) and entry.get("kind") == "range":
            if (
                entry.get("name") is not None
                or int(entry.get("start", 0)) != 0
                or int(entry.get("step", 1)) != 1
            ):
                has_nontrivial = True
                break

    if not has_nontrivial:
        return table, columns

    idx_col_names: list[str] = []
    for i, entry in enumerate(raw_idx_cols):
        if isinstance(entry, str):
            if entry in table.column_names:
                idx_col_names.append(entry)
        elif isinstance(entry, Mapping) and entry.get("kind") == "range":
            col_name = (
                str(entry["name"])
                if entry.get("name") is not None
                else f"__index_level_{i}__"
            )
            start = int(entry.get("start", 0))
            stop = int(entry.get("stop", 0))
            step = int(entry.get("step", 1))
            if col_name not in table.column_names:
                arr = pa_mod.array(range(start, stop, step), type=pa_mod.int64())
                table = table.append_column(col_name, arr)
            idx_col_names.append(col_name)

    if columns is None and idx_col_names:
        idx_set = set(idx_col_names)
        non_idx_cols = [c for c in table.column_names if c not in idx_set]
        columns = [*idx_col_names, *non_idx_cols]
    return table, columns


def _read_hdf5_dataframe_for_digest(
    source_path: Path,
    h5_key: str | None = None,
) -> Any:
    """Read an HDF5 fixed or table store member into a pandas DataFrame."""
    pd_mod = importlib.import_module("pandas")
    if h5_key is not None:
        return pd_mod.read_hdf(source_path, key=h5_key)

    with pd_mod.HDFStore(str(source_path), mode="r") as store:
        keys = store.keys()
        if not keys:
            raise ValueError(f"HDF5 store contains no tables: {source_path}")
        if len(keys) > 1:
            raise ValueError(
                f"HDF5 store {source_path} contains multiple keys {keys}; "
                "specify h5_key to select a table."
            )
        return store.get(keys[0])


def digest_table(
    source: Any,
    columns: Sequence[str] | None = None,
    key: Sequence[str] | None = None,
    *,
    key_columns: Sequence[str] | None = None,
    profile: str = "ctd1",
    sqlite_table: str | None = None,
    h5_key: str | None = None,
    member: str | None = None,
) -> str:
    """Compute an order-invariant canonical SHA-256 tabular digest.

    Parameters
    ----------
    source : pyarrow.Table, pyarrow.RecordBatch, pandas.DataFrame, DuckDBPyRelation, Path, or str
        Tabular data source to digest. If a Path/str is given, reads from Parquet,
        CSV, HDF5 (``format="fixed"`` or ``format="table"``), or SQLite (if
        ``sqlite_table`` is provided).
    columns : Sequence[str], optional
        Declared column projection in strict order. If omitted, uses source column order
        (with non-trivial index columns placed first).
    key : Sequence[str], optional
        Sort key columns defining row ordering (``ASC NULLS FIRST``). If omitted,
        sorts across all projected columns.
    key_columns : Sequence[str], optional
        Alias for ``key`` (for synergy-schema compatibility).
    profile : str, default "ctd1"
        Tabular digest profile identifier. When "ctd1", returns ``"ctd1:sha256:<64-hex>"``.
        When another profile string is provided (e.g. "core_facet_table/v1" or
        "view_table/v1"), returns ``"sha256:<64-hex>"``.
    sqlite_table : str, optional
        Table name when ``source`` points to a SQLite database.
    h5_key : str, optional
        HDF5 key/path when ``source`` points to an HDF5 file.
    member : str, optional
        Alias for ``h5_key``.

    Returns
    -------
    str
        Prefixed hexadecimal SHA-256 digest string.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not profile:
        raise ValueError("profile must be a non-empty string")

    if columns is not None and len(set(columns)) != len(columns):
        raise ValueError("Duplicate column names are not supported in digest_table")

    resolved_keys = key if key is not None else key_columns
    effective_h5_key = h5_key if h5_key is not None else member

    if isinstance(source, (str, Path)):
        raw_str = str(source)
        source_path = Path(source)
        if not source_path.exists() and effective_h5_key is None and "::" in raw_str:
            path_part, key_part = raw_str.split("::", 1)
            source_path = Path(path_part)
            effective_h5_key = key_part

        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        if sqlite_table is not None:
            quoted_table = _quote_sql_ident(sqlite_table)
            sqlite_uri = f"{source_path.resolve().as_uri()}?mode=ro"
            with closing(sqlite3.connect(sqlite_uri, uri=True)) as con:
                cur = con.cursor()
                cur.execute("PRAGMA trusted_schema = OFF")
                cur.execute("PRAGMA query_only = ON")
                exists = cur.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                    (sqlite_table,),
                ).fetchone()
                if exists is None:
                    raise ValueError(f"SQLite table not found: {sqlite_table!r}")
                actual_cols = [
                    row[1]
                    for row in cur.execute(
                        f"PRAGMA table_info({quoted_table})"
                    ).fetchall()
                ]
                if len(set(actual_cols)) != len(actual_cols):
                    raise ValueError(
                        "Duplicate column names are not supported in digest_table"
                    )
                if columns is not None:
                    cols_to_query = list(columns)
                    actual_col_set = set(actual_cols)
                    missing_cols = [c for c in cols_to_query if c not in actual_col_set]
                    if missing_cols:
                        raise ValueError(
                            f"Columns not found in SQLite table {sqlite_table!r}: {missing_cols}"
                        )
                else:
                    cols_to_query = actual_cols
                if cols_to_query:
                    cols_sql = ", ".join(_quote_sql_ident(c) for c in cols_to_query)
                    rows = cur.execute(
                        f"SELECT {cols_sql} FROM {quoted_table}"
                    ).fetchall()
                    col_data = {
                        col: [row[i] for row in rows]
                        for i, col in enumerate(cols_to_query)
                    }
                    table = pa.Table.from_pydict(col_data)
                else:
                    table = pa.table({})
            if columns is None:
                columns = cols_to_query
        elif effective_h5_key is not None or source_path.suffix.lower() in (
            ".h5",
            ".hdf5",
            ".hdf",
        ):
            df_h5 = _read_hdf5_dataframe_for_digest(
                source_path, h5_key=effective_h5_key
            )
            table = _dataframe_to_arrow_table_with_index(df_h5, pa)
        elif source_path.suffix.lower() == ".csv":
            import pyarrow.csv as pa_csv

            table = pa_csv.read_csv(source_path)
        else:
            table = pq.read_table(source_path)
            table, columns = _materialize_arrow_pandas_index(table, pa, columns)
    elif isinstance(source, pa.Table):
        table, columns = _materialize_arrow_pandas_index(source, pa, columns)
    elif isinstance(source, pa.RecordBatch):
        table, columns = _materialize_arrow_pandas_index(
            pa.Table.from_batches([source]), pa, columns
        )
    elif isinstance(source, pa.RecordBatchReader):
        table, columns = _materialize_arrow_pandas_index(source.read_all(), pa, columns)
    else:
        # Check DataFrame or DuckDB relation via duck typing
        pandas_mod = sys.modules.get("pandas")
        if (
            pandas_mod is not None
            and isinstance(source, pandas_mod.__dict__.get("DataFrame", ()))
        ) or source.__class__.__name__ == "DataFrame":
            table = _dataframe_to_arrow_table_with_index(source, pa)
        else:
            duckdb_mod = sys.modules.get("duckdb")
            if duckdb_mod is not None:
                rel_cls = duckdb_mod.__dict__.get("DuckDBPyRelation")
                if isinstance(rel_cls, type) and isinstance(source, rel_cls):
                    arrow_res = source.arrow()
                    table = (
                        arrow_res.read_all()
                        if isinstance(arrow_res, pa.RecordBatchReader)
                        else arrow_res
                    )
                else:
                    raise TypeError(
                        f"Unsupported tabular data source type: {type(source)}"
                    )
            else:
                raise TypeError(f"Unsupported tabular data source type: {type(source)}")

    if len(set(table.column_names)) != len(table.column_names):
        raise ValueError("Duplicate column names are not supported in digest_table")

    if columns is None:
        columns = list(table.column_names)

    if resolved_keys is not None:
        for k in resolved_keys:
            if k not in columns:
                raise ValueError(
                    f"Key column '{k}' must be in declared columns: {columns}"
                )

    table = table.select(list(columns))

    sort_cols = list(resolved_keys) if resolved_keys is not None else list(columns)
    for col_name in sort_cols:
        field_def = table.schema.field(col_name)
        if pa.types.is_dictionary(field_def.type):
            col_idx = table.schema.get_field_index(col_name)
            table = table.set_column(
                col_idx,
                col_name,
                table.column(col_name).cast(field_def.type.value_type),
            )

    if sort_cols:
        sort_keys = [(col, "ascending") for col in sort_cols]
        table = table.sort_by(sort_keys, null_placement="at_start")

    hasher = hashlib.sha256()

    header_parts = [profile.encode("utf-8")] + [col.encode("utf-8") for col in columns]
    hasher.update(_DELIMITER_FIELD.join(header_parts) + _DELIMITER_RECORD)

    for batch in table.to_batches():
        batch_dict = batch.to_pydict()
        num_rows = batch.num_rows
        for row_idx in range(num_rows):
            row_bytes = [
                normalize_table_scalar(batch_dict[col][row_idx]) for col in columns
            ]
            hasher.update(_DELIMITER_FIELD.join(row_bytes) + _DELIMITER_RECORD)

    digest_hex = hasher.hexdigest()
    if profile == "ctd1":
        return f"ctd1:sha256:{digest_hex}"
    return f"sha256:{digest_hex}"


def digest_state(
    entries: Mapping[str, str] | Sequence[Mapping[str, Any]],
    *,
    profile: str = "core_state/v1",
) -> str:
    """Compute canonical StateDigest over sorted (facet, digest) pairs.

    Parameters
    ----------
    entries : Mapping[str, str] or Sequence[Mapping[str, Any]]
        Facet-to-digest mapping or sequence of dicts with ``"facet"`` and ``"digest"`` keys.
    profile : str, default "core_state/v1"
        State digest profile identifier.

    Returns
    -------
    str
        Hexadecimal SHA-256 digest string prefixed with ``"sha256:"``.
    """
    if profile != "core_state/v1":
        raise ValueError(f"Unsupported state digest profile: {profile}")

    pairs: list[tuple[str, str]] = []
    if isinstance(entries, Mapping):
        pairs = sorted((str(k), str(v)) for k, v in entries.items())
    else:
        for entry in entries:
            facet = entry.get("facet") if "facet" in entry else entry.get("role")
            digest = entry.get("digest")
            if facet is None or digest is None:
                raise ValueError(
                    "State entry must contain 'facet' (or 'role') and 'digest' keys"
                )
            pairs.append((str(facet), str(digest)))
        pairs.sort()

    hasher = hashlib.sha256()
    hasher.update(profile.encode("utf-8") + _DELIMITER_RECORD)
    for facet, digest in pairs:
        hasher.update(
            facet.encode("utf-8")
            + _DELIMITER_FIELD
            + digest.encode("utf-8")
            + _DELIMITER_RECORD
        )

    return f"sha256:{hasher.hexdigest()}"


# --- Pure Logical Array & Matrix Canonicalization / Digests ---


def open_h5py_read_only(path: str | Path, *, h5py_module: Any = None) -> Any:
    """Open an HDF5 file in read-only mode with Lustre-safe locking defaults.

    When ``HDF5_USE_FILE_LOCKING`` is not set in the environment, passes
    ``locking=False`` to ``h5py.File(..., "r")`` so concurrent read-only opens
    on network filesystems (such as Lustre without ``flock``) do not fail or
    block. Respects ``HDF5_USE_FILE_LOCKING`` when explicitly set by the user,
    and falls back cleanly if an older ``h5py`` version or test fake does not
    accept the ``locking`` keyword argument.
    """
    h5py = h5py_module if h5py_module is not None else importlib.import_module("h5py")
    if "HDF5_USE_FILE_LOCKING" not in os.environ:
        try:
            return h5py.File(path, "r", locking=False)
        except TypeError:
            return h5py.File(path, "r")
        except OSError as exc:
            if "file locking flag values don't match" in str(exc).lower():
                return h5py.File(path, "r")
            raise
    return h5py.File(path, "r")


@dataclass(frozen=True, slots=True)
class ArrayCanonicalizationPolicy:
    """Controls how logical arrays and coordinates are normalized before hashing.

    Attributes
    ----------
    profile : {"preserve_canonical", "float32_matrices_v1"}
        Named profile identifier recorded in provenance metadata.
    dtype_policy : {"preserve", "float32"}
        ``"preserve"`` keeps each array's native numeric/string width in
        little-endian format (``<f8``, ``<f4``, ``<i8``, ``<i4``, ``<u1``, etc.).
        ``"float32"`` casts data matrices to ``<f4``.
    canonicalize_float_bits : bool
        When True, normalizes ``-0.0`` to ``+0.0`` and all ``NaN`` bit patterns
        (quiet or signaling) to the IEEE-754 canonical quiet ``NaN`` for the
        active float width.
    lookup_policy : {"all_actual", "single_int32_zone_number"}
        ``"all_actual"`` hashes every lookup/coordinate array in sorted key
        order under ``"lookup/<actual_name>"`` with its preserved dtype.
        ``"single_int32_zone_number"`` resolves the single primary zone lookup,
        coerces it to ``<i4``, and hashes it as ``"lookup/zone_number"`` for
        byte-for-byte parity with BEAM's Scala ``OmxSkimWriter``.
    h5_data_group : str
        HDF5 group path containing data matrices (default ``"/data"``).
    h5_lookup_group : str
        HDF5 group path containing lookup arrays (default ``"/lookup"``).
    allow_root_h5_datasets : bool
        When True and ``h5_data_group`` is absent, falls back to top-level
        HDF5 datasets outside ``h5_lookup_group``.
    unstack_dim : str or None
        Optional dimension name (e.g., ``"time_period"``) along which 3D+ Zarr
        variables should be unstacked into 2D slices named
        ``"<var><unstack_sep><coord>"``.
    unstack_sep : str
        Separator used when unstacking ``unstack_dim`` slices (default ``"__"``).
    include_na_attribute : bool
        When True, includes an array's ``NA`` fill-value attribute (if present)
        in the per-array framing header.
    """

    profile: LogicalProfileName = "preserve_canonical"
    dtype_policy: Literal["preserve", "float32"] = "preserve"
    canonicalize_float_bits: bool = True
    lookup_policy: Literal["all_actual", "single_int32_zone_number"] = "all_actual"
    h5_data_group: str = "/data"
    h5_lookup_group: str = "/lookup"
    allow_root_h5_datasets: bool = True
    unstack_dim: str | None = None
    unstack_sep: str = "__"
    include_na_attribute: bool = False


PRESERVE_CANONICAL_POLICY = ArrayCanonicalizationPolicy(
    profile="preserve_canonical",
    dtype_policy="preserve",
    canonicalize_float_bits=True,
    lookup_policy="all_actual",
    h5_data_group="/data",
    h5_lookup_group="/lookup",
    allow_root_h5_datasets=True,
    unstack_dim=None,
    unstack_sep="__",
    include_na_attribute=False,
)

FLOAT32_MATRICES_V1_POLICY = ArrayCanonicalizationPolicy(
    profile="float32_matrices_v1",
    dtype_policy="float32",
    canonicalize_float_bits=True,
    lookup_policy="single_int32_zone_number",
    h5_data_group="/data",
    h5_lookup_group="/lookup",
    allow_root_h5_datasets=False,
    unstack_dim="time_period",
    unstack_sep="__",
    include_na_attribute=False,
)


def resolve_canonicalization_policy(
    policy: ArrayCanonicalizationPolicy | LogicalProfileName | None = None,
) -> ArrayCanonicalizationPolicy:
    """
    Resolve a profile name or policy instance to an ``ArrayCanonicalizationPolicy``.

    Parameters
    ----------
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"} or None, optional
        Either a named canonicalization profile or an explicit
        ``ArrayCanonicalizationPolicy`` instance. Defaults to
        ``"preserve_canonical"`` when ``None``.

    Returns
    -------
    ArrayCanonicalizationPolicy
        Resolved immutable canonicalization policy.

    Raises
    ------
    ValueError
        If ``policy`` is not a recognized profile name or
        ``ArrayCanonicalizationPolicy`` instance.
    """
    if policy is None or policy == "preserve_canonical":
        return PRESERVE_CANONICAL_POLICY
    if policy == "float32_matrices_v1":
        return FLOAT32_MATRICES_V1_POLICY
    if isinstance(policy, ArrayCanonicalizationPolicy):
        return policy
    raise ValueError(
        f"Unsupported logical array canonicalization policy: {policy!r}. "
        "Expected 'preserve_canonical', 'float32_matrices_v1', or an ArrayCanonicalizationPolicy."
    )


def normalize_logical_matrices_identity(raw_val: str) -> str:
    """
    Normalize a 64-hex digest or ``sha256:logical_matrices:<64hex>`` URI.

    Parameters
    ----------
    raw_val : str
        Either a bare 64-character hexadecimal SHA-256 digest or a prefixed
        ``sha256:logical_matrices:<64hex>`` string.

    Returns
    -------
    str
        Canonical lowercase ``sha256:logical_matrices:<64 lowercase hex>`` URI.

    Raises
    ------
    ValueError
        If ``raw_val`` is not a string, uses an incompatible ``sha256:``
        scheme prefix, or does not contain a valid 64-character hex digest.
    """
    if not isinstance(raw_val, str):
        raise ValueError("logical matrices identity must be a string")
    val = raw_val.strip()
    if val.startswith("sha256:logical_matrices:v1:"):
        digest = val.removeprefix("sha256:logical_matrices:v1:")
    elif val.startswith("sha256:logical_matrices:"):
        digest = val.removeprefix("sha256:logical_matrices:")
    elif val.startswith("sha256:"):
        raise ValueError(
            f"logical matrices identity must use scheme 'sha256:logical_matrices:<64hex>', got {raw_val!r}"
        )
    else:
        digest = val
    if not _HEX64_RE.fullmatch(digest):
        raise ValueError(
            f"logical matrices identity requires a 64-character hexadecimal SHA-256 digest, got {raw_val!r}"
        )
    return f"sha256:logical_matrices:{digest.lower()}"


def canonicalize_float32_array(arr: np.ndarray) -> bytes:
    """
    Return little-endian float32 bytes with canonical IEEE-754 bit patterns.

    Parameters
    ----------
    arr : numpy.ndarray
        Input array-like object to convert to ``<f4`` and canonicalize.

    Returns
    -------
    bytes
        C-contiguous row-major little-endian ``<f4`` byte representation with
        ``-0.0f`` normalized to ``+0.0f`` (``0x00000000``) and all ``NaN``
        payloads normalized to canonical quiet ``NaN`` (``0x7fc00000``).

    Notes
    -----
    Matches the floating-point canonicalization contract of BEAM's Scala
    ``OmxSkimWriter`` / ``SkimsProvenanceMetadata``.
    """
    import numpy as np

    f32 = np.ascontiguousarray(arr, dtype="<f4")
    u32 = f32.view("<u4")
    needs_copy = bool(np.any(f32 == 0.0) or np.any(np.isnan(f32)))
    if needs_copy:
        u32 = u32.copy()
        u32[f32 == 0.0] = 0
        u32[np.isnan(f32)] = 0x7FC00000
    return u32.tobytes()


def canonicalize_float64_array(arr: np.ndarray) -> bytes:
    """
    Return little-endian float64 bytes with canonical IEEE-754 bit patterns.

    Parameters
    ----------
    arr : numpy.ndarray
        Input array-like object to convert to ``<f8`` and canonicalize.

    Returns
    -------
    bytes
        C-contiguous row-major little-endian ``<f8`` byte representation with
        ``-0.0`` normalized to ``+0.0`` (``0x0000000000000000``) and all
        ``NaN`` payloads normalized to canonical quiet ``NaN``
        (``0x7ff8000000000000``).
    """
    import numpy as np

    f64 = np.ascontiguousarray(arr, dtype="<f8")
    u64 = f64.view("<u8")
    needs_copy = bool(np.any(f64 == 0.0) or np.any(np.isnan(f64)))
    if needs_copy:
        u64 = u64.copy()
        u64[f64 == 0.0] = 0
        u64[np.isnan(f64)] = 0x7FF8000000000000
    return u64.tobytes()


def canonicalize_float16_array(arr: np.ndarray) -> bytes:
    """
    Return little-endian float16 bytes with canonical IEEE-754 bit patterns.

    Parameters
    ----------
    arr : numpy.ndarray
        Input array-like object to convert to ``<f2`` and canonicalize.

    Returns
    -------
    bytes
        C-contiguous row-major little-endian ``<f2`` byte representation with
        ``-0.0`` normalized to ``+0.0`` (``0x0000``) and all ``NaN`` payloads
        normalized to canonical quiet ``NaN`` (``0x7e00``).
    """
    import numpy as np

    f16 = np.ascontiguousarray(arr, dtype="<f2")
    u16 = f16.view("<u2")
    needs_copy = bool(np.any(f16 == 0.0) or np.any(np.isnan(f16)))
    if needs_copy:
        u16 = u16.copy()
        u16[f16 == 0.0] = 0
        u16[np.isnan(f16)] = 0x7E00
    return u16.tobytes()


def canonicalize_float_array(
    arr: np.ndarray,
    *,
    target_dtype: Literal["preserve", "float32", "float64"] = "preserve",
) -> np.ndarray:
    """
    Return a C-contiguous little-endian float array with canonicalized ``-0.0`` and ``NaN`` bits.

    Parameters
    ----------
    arr : numpy.ndarray
        Input floating-point (or numeric, when ``target_dtype`` is explicit)
        array to canonicalize.
    target_dtype : {"preserve", "float32", "float64"}, default "preserve"
        Target floating-point width. ``"preserve"`` retains the input float
        width (``float16``, ``float32``, or ``float64``) while normalizing
        endianness and IEEE-754 bit patterns.

    Returns
    -------
    numpy.ndarray
        Canonicalized C-contiguous little-endian NumPy array with the same
        shape as ``arr``.

    Raises
    ------
    ValueError
        If ``target_dtype="preserve"`` and ``arr`` does not have a supported
        floating-point dtype.
    """
    import numpy as np

    raw = np.asarray(arr)
    if target_dtype == "float32" or (
        target_dtype == "preserve" and raw.dtype.kind == "f" and raw.dtype.itemsize == 4
    ):
        raw_bytes = canonicalize_float32_array(raw)
        return np.frombuffer(raw_bytes, dtype="<f4").reshape(raw.shape)
    if target_dtype == "float64" or (
        target_dtype == "preserve" and raw.dtype.kind == "f" and raw.dtype.itemsize == 8
    ):
        raw_bytes = canonicalize_float64_array(raw)
        return np.frombuffer(raw_bytes, dtype="<f8").reshape(raw.shape)
    if target_dtype == "preserve" and raw.dtype.kind == "f" and raw.dtype.itemsize == 2:
        raw_bytes = canonicalize_float16_array(raw)
        return np.frombuffer(raw_bytes, dtype="<f2").reshape(raw.shape)
    raise ValueError(
        f"canonicalize_float_array requires a floating-point array or explicit float target_dtype, got {raw.dtype}."
    )


def _resolve_preserved_dtype_str(dtype: np.dtype[Any]) -> str:
    """Map a NumPy dtype to its canonical little-endian descriptor string."""
    kind = dtype.kind
    size = dtype.itemsize
    if kind == "f" and size in (2, 4, 8):
        return f"<f{size}"
    if kind == "i" and size in (1, 2, 4, 8):
        return f"<i{size}"
    if kind == "u" and size in (1, 2, 4, 8):
        return f"<u{size}"
    if kind == "b":
        return "|b1"
    if kind == "S":
        return f"|S{size}"
    if kind == "U":
        return f"<U{size // 4}"
    raise ValueError(
        f"Unsupported array dtype for lossless canonicalization: {dtype!r}"
    )


def _coerce_object_or_string_array(arr: np.ndarray) -> tuple[str, np.ndarray]:
    """Convert a string/bytes object array to a deterministic fixed-width byte array, or raise ValueError."""
    import numpy as np

    flat = arr.ravel()
    encoded_items: list[bytes] = []
    for item in flat:
        if isinstance(item, bytes):
            encoded_items.append(item)
        elif isinstance(item, str):
            encoded_items.append(item.encode("utf-8"))
        elif isinstance(item, (int, np.integer)) and not isinstance(item, bool):
            raise ValueError(
                "Object array contains integers; convert to an explicit integer dtype before canonicalization."
            )
        else:
            raise ValueError(
                f"Object array contains unsupported element of type {type(item).__name__} for canonical hashing."
            )
    max_len = max((len(b) for b in encoded_items), default=1)
    max_len = max(1, max_len)
    target_dtype = f"|S{max_len}"
    out = np.array(encoded_items, dtype=target_dtype).reshape(arr.shape)
    return target_dtype, np.ascontiguousarray(out)


def _canonicalize_chunk_bytes(
    arr: np.ndarray,
    *,
    canonical_dtype_str: str,
    canonicalize_float_bits: bool,
) -> bytes:
    """Convert an array or slice into canonical C-contiguous little-endian bytes."""
    import numpy as np

    if canonical_dtype_str == "<f4":
        if canonicalize_float_bits:
            return canonicalize_float32_array(arr)
        return np.ascontiguousarray(arr, dtype="<f4").tobytes()
    if canonical_dtype_str == "<f8":
        if canonicalize_float_bits:
            return canonicalize_float64_array(arr)
        return np.ascontiguousarray(arr, dtype="<f8").tobytes()
    if canonical_dtype_str == "<f2":
        if canonicalize_float_bits:
            return canonicalize_float16_array(arr)
        return np.ascontiguousarray(arr, dtype="<f2").tobytes()
    return np.ascontiguousarray(arr, dtype=canonical_dtype_str).tobytes()


def _resolve_array_canonical_dtype(
    arr_or_ds: Any,
    *,
    dtype_policy: Literal["preserve", "float32"],
) -> tuple[str, np.ndarray | None]:
    """Return ``(canonical_dtype_str, materialized_array_if_needed)``."""
    import numpy as np

    dtype = np.dtype(arr_or_ds.dtype)
    if dtype_policy == "float32":
        if dtype.kind not in ("f", "i", "u"):
            raise ValueError(
                f"Cannot coerce array of dtype {dtype!r} to float32 under dtype_policy='float32'."
            )
        return "<f4", None

    if dtype.kind == "O":
        materialized = np.asarray(arr_or_ds[...])
        dtype_str, converted = _coerce_object_or_string_array(materialized)
        return dtype_str, converted

    return _resolve_preserved_dtype_str(dtype), None


def _update_string(hasher: Any, value: str) -> None:
    """Update hasher with a 4-byte little-endian length-prefixed UTF-8 string."""
    raw = value.encode("utf-8")
    hasher.update(struct.pack("<I", len(raw)))
    hasher.update(raw)


def _format_shape(shape: Sequence[int]) -> str:
    """Format an array shape tuple deterministically.

    1D shapes use ``(N,)`` (matching BEAM's ``lookup/zone_number`` format), and
    2D+ shapes use ``(d0,d1,...)`` without spaces (matching BEAM's ``(rows,cols)``).
    """
    dims = tuple(int(d) for d in shape)
    if len(dims) == 1:
        return f"({dims[0]},)"
    return f"({','.join(str(d) for d in dims)})"


def _stream_dataset_into_hasher(
    hasher: Any,
    name: str,
    dataset: Any,
    *,
    dtype_policy: Literal["preserve", "float32"],
    canonicalize_float_bits: bool,
    include_na_attribute: bool = False,
    na_value: Any = None,
    chunk_rows: int = _DEFAULT_CHUNK_ROWS,
) -> None:
    """Write framing header and stream canonicalized array chunks into ``hasher``."""
    import numpy as np

    canonical_dtype_str, pre_materialized = _resolve_array_canonical_dtype(
        dataset,
        dtype_policy=dtype_policy,
    )
    target = pre_materialized if pre_materialized is not None else dataset
    shape = tuple(int(d) for d in target.shape)

    _update_string(hasher, name)
    _update_string(hasher, canonical_dtype_str)
    _update_string(hasher, _format_shape(shape))

    if include_na_attribute:
        resolved_na = na_value
        if resolved_na is None and hasattr(dataset, "attrs") and "NA" in dataset.attrs:
            resolved_na = _decode_attr_value(dataset.attrs["NA"])
        if resolved_na is not None:
            _update_string(hasher, f"NA={resolved_na}")

    if isinstance(target, np.ndarray) or len(shape) == 0:
        arr = np.asarray(target)
        hasher.update(
            _canonicalize_chunk_bytes(
                arr,
                canonical_dtype_str=canonical_dtype_str,
                canonicalize_float_bits=canonicalize_float_bits,
            )
        )
        return

    total_rows = shape[0]
    if total_rows == 0:
        return

    step = max(1, int(chunk_rows))
    for start in range(0, total_rows, step):
        end = min(total_rows, start + step)
        chunk = np.asarray(target[start:end])
        hasher.update(
            _canonicalize_chunk_bytes(
                chunk,
                canonical_dtype_str=canonical_dtype_str,
                canonicalize_float_bits=canonicalize_float_bits,
            )
        )


def compute_array_entry_sha256(
    name: str,
    arr: np.ndarray,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    na_value: Any = None,
) -> str:
    """
    Compute the standalone canonical SHA-256 hex digest for a single named array.

    Parameters
    ----------
    name : str
        Logical array name written into the framing header (e.g., ``"DIST__AM"``
        or ``"lookup/zone_number"``).
    arr : numpy.ndarray
        Array-like object to canonicalize and hash.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy controlling dtype preservation/coercion,
        floating-point bit normalization, and ``NA`` attribute framing.
    na_value : Any, optional
        Optional fill-value metadata to include when
        ``policy.include_na_attribute`` is ``True``.

    Returns
    -------
    str
        64-character lowercase hexadecimal SHA-256 digest of the framed array entry.

    Raises
    ------
    ValueError
        If ``policy`` is invalid or ``arr`` has an unsupported dtype under the
        active policy.
    """
    import numpy as np

    resolved_policy = resolve_canonicalization_policy(policy)
    hasher = hashlib.sha256()
    _stream_dataset_into_hasher(
        hasher,
        name,
        np.asarray(arr),
        dtype_policy=resolved_policy.dtype_policy,
        canonicalize_float_bits=resolved_policy.canonicalize_float_bits,
        include_na_attribute=resolved_policy.include_na_attribute,
        na_value=na_value,
    )
    return hasher.hexdigest()


def _coerce_int32_zone_lookup(raw_lookup: Any) -> np.ndarray:
    """Coerce a 1D lookup array to little-endian ``<i4`` or raise ``ValueError`` (fail closed)."""
    import numpy as np

    arr = np.asarray(raw_lookup)
    if arr.ndim != 1:
        raise ValueError(
            f"Zone lookup array must be 1-dimensional, got shape {arr.shape}."
        )
    kind = arr.dtype.kind
    if kind in ("i", "u"):
        if arr.size > 0:
            iinfo = np.iinfo(np.int32)
            min_val = int(np.min(arr))
            max_val = int(np.max(arr))
            if min_val < iinfo.min or max_val > iinfo.max:
                raise ValueError(
                    f"Zone lookup integer values [{min_val}, {max_val}] overflow int32."
                )
        return np.ascontiguousarray(arr, dtype="<i4")

    if kind == "f":
        if not np.all(np.isfinite(arr)):
            raise ValueError("Zone lookup array contains NaN or infinite float values.")
        rounded = np.floor(arr)
        if not np.array_equal(arr, rounded):
            raise ValueError(
                "Zone lookup float array contains non-integer fractional values."
            )
        iinfo = np.iinfo(np.int32)
        if arr.size > 0 and (
            float(np.min(arr)) < iinfo.min or float(np.max(arr)) > iinfo.max
        ):
            raise ValueError("Zone lookup float values overflow int32.")
        return np.ascontiguousarray(arr, dtype="<i4")

    if kind in ("S", "U", "O"):
        parsed: list[int] = []
        for item in arr.tolist():
            if isinstance(item, (bytes, bytearray)):
                text = item.decode("utf-8", errors="strict").strip()
            elif isinstance(item, str):
                text = item.strip()
            elif isinstance(item, (int, np.integer)) and not isinstance(item, bool):
                parsed.append(int(item))
                continue
            else:
                raise ValueError(
                    f"Zone lookup contains non-integer element {item!r} ({type(item).__name__})."
                )
            if not text or not (
                text.isdigit() or (text[0] in "+-" and text[1:].isdigit())
            ):
                raise ValueError(
                    f"Zone lookup string element {item!r} cannot be converted to int32."
                )
            parsed.append(int(text))
        iinfo = np.iinfo(np.int32)
        for val in parsed:
            if val < iinfo.min or val > iinfo.max:
                raise ValueError(f"Zone lookup value {val} overflows int32.")
        return np.ascontiguousarray(np.array(parsed, dtype="<i4"))

    raise ValueError(f"Unsupported zone lookup dtype {arr.dtype!r} for int32 coercion.")


def _iter_sorted_matrices(
    matrices: Mapping[str, np.ndarray] | Iterable[tuple[str, np.ndarray]],
) -> Iterator[tuple[str, np.ndarray]]:
    """Yield ``(name, array)`` in strict lexicographical UTF-8 key order."""
    import numpy as np

    if isinstance(matrices, Mapping):
        mapping_matrices = cast(Mapping[str, np.ndarray], matrices)
        for key in sorted(mapping_matrices.keys()):
            yield str(key), np.asarray(mapping_matrices[key])
        return

    if isinstance(matrices, Sequence):
        seq_items = cast(Sequence[tuple[str, np.ndarray]], matrices)
        sorted_items = sorted(seq_items, key=lambda pair: str(pair[0]))
        for key, arr in sorted_items:
            yield str(key), np.asarray(arr)
        return

    prev_key: str | None = None
    for key, arr in matrices:
        key_str = str(key)
        if prev_key is not None and key_str < prev_key:
            raise ValueError(
                f"Streaming matrices iterator yielded unsorted key {key_str!r} after {prev_key!r}; "
                "yield matrices in sorted key order or pass a Mapping/Sequence."
            )
        prev_key = key_str
        yield key_str, np.asarray(arr)


def compute_logical_matrices_sha256(
    matrices: Mapping[str, np.ndarray] | Iterable[tuple[str, np.ndarray]],
    *,
    zone_ids: Sequence[int] | np.ndarray | None = None,
    lookups: Mapping[str, np.ndarray | Sequence[Any]] | None = None,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
) -> str:
    """
    Compute the canonical logical SHA-256 hex digest over lookup and data arrays.

    Parameters
    ----------
    matrices : Mapping[str, numpy.ndarray] or Iterable[tuple[str, numpy.ndarray]]
        Data matrices keyed by matrix name. Mapping or Sequence inputs are
        sorted lexicographically by UTF-8 key; streaming iterators must yield
        ``(name, array)`` pairs in sorted key order.
    zone_ids : Sequence[int] or numpy.ndarray or None, optional
        Optional 1D zone identifier array. Under ``"preserve_canonical"``,
        included as ``"lookup/zone_number"`` when not already present in
        ``lookups``. Under ``"float32_matrices_v1"``, coerced to ``<i4`` and
        hashed as ``"lookup/zone_number"`` (defaulting to ``1..N`` when absent).
    lookups : Mapping[str, numpy.ndarray or Sequence[Any]] or None, optional
        Optional mapping of lookup/coordinate arrays. Under
        ``"preserve_canonical"``, every lookup array is hashed in sorted key
        order under ``"lookup/<key>"`` with its native dtype preserved.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy or named profile.

    Returns
    -------
    str
        64-character lowercase hexadecimal SHA-256 digest.

    Raises
    ------
    ValueError
        If ``matrices`` is empty, a streaming iterator yields unsorted keys, or
        any lookup/data array cannot be canonicalized under ``policy``.

    Notes
    -----
    Each entry is framed with 4-byte little-endian length-prefixed UTF-8 strings
    for ``name``, ``dtype``, and ``shape``, followed by C-contiguous
    little-endian array bytes.
    """
    import numpy as np

    resolved_policy = resolve_canonicalization_policy(policy)

    hasher = hashlib.sha256()
    matrix_iter = _iter_sorted_matrices(matrices)
    first_matrix = next(matrix_iter, None)
    if first_matrix is None:
        raise ValueError("Cannot compute logical matrices SHA-256 over empty matrices.")

    first_name, first_arr = first_matrix
    default_zone_count = int(first_arr.shape[0]) if first_arr.ndim >= 1 else 0

    if resolved_policy.lookup_policy == "single_int32_zone_number":
        raw_zone_lookup: Any = zone_ids
        if raw_zone_lookup is None and lookups:
            for pkey in _PRIMARY_LOOKUP_KEYS:
                if pkey in lookups:
                    raw_zone_lookup = lookups[pkey]
                    break
            if raw_zone_lookup is None:
                first_lkey = sorted(lookups.keys())[0]
                raw_zone_lookup = lookups[first_lkey]
        if raw_zone_lookup is None:
            zone_arr = np.arange(1, default_zone_count + 1, dtype="<i4")
        else:
            zone_arr = _coerce_int32_zone_lookup(raw_zone_lookup)
        _stream_dataset_into_hasher(
            hasher,
            "lookup/zone_number",
            zone_arr,
            dtype_policy="preserve",
            canonicalize_float_bits=False,
        )
    else:
        effective_lookups: dict[str, np.ndarray] = {}
        if lookups:
            for lkey, lval in lookups.items():
                clean_key = str(lkey).removeprefix("lookup/").lstrip("/")
                effective_lookups[clean_key] = np.asarray(lval)
        if zone_ids is not None and "zone_number" not in effective_lookups:
            effective_lookups["zone_number"] = np.asarray(zone_ids)

        for lkey in sorted(effective_lookups.keys()):
            larr = effective_lookups[lkey]
            _stream_dataset_into_hasher(
                hasher,
                f"lookup/{lkey}",
                larr,
                dtype_policy="preserve",
                canonicalize_float_bits=resolved_policy.canonicalize_float_bits,
            )

    _stream_dataset_into_hasher(
        hasher,
        first_name,
        first_arr,
        dtype_policy=resolved_policy.dtype_policy,
        canonicalize_float_bits=resolved_policy.canonicalize_float_bits,
        include_na_attribute=resolved_policy.include_na_attribute,
    )
    for matrix_name, matrix_arr in matrix_iter:
        _stream_dataset_into_hasher(
            hasher,
            matrix_name,
            matrix_arr,
            dtype_policy=resolved_policy.dtype_policy,
            canonicalize_float_bits=resolved_policy.canonicalize_float_bits,
            include_na_attribute=resolved_policy.include_na_attribute,
        )

    return hasher.hexdigest()


def _resolve_h5_group(h5f: Any, group_path: str) -> Any | None:
    """Resolve an HDF5 group case-insensitively (e.g. ``/data`` or ``/DATA``)."""
    clean = group_path.strip("/")
    if not clean:
        return h5f
    if clean in h5f:
        return h5f[clean]
    upper = clean.upper()
    if upper in h5f:
        return h5f[upper]
    lower = clean.lower()
    if lower in h5f:
        return h5f[lower]
    return None


def _load_omx_sidecar_mapping_zone_ids(omx_path: Path | None) -> np.ndarray | None:
    """Read ``<omx_path>.mapping`` JSON if present and return ordered zone IDs."""
    import numpy as np

    if omx_path is None:
        return None
    mapping_file = Path(f"{omx_path}.mapping")
    if not mapping_file.is_file():
        return None
    try:
        mapping_data = json.loads(mapping_file.read_text(encoding="utf-8"))
        if isinstance(mapping_data, dict) and mapping_data:
            sorted_pairs = sorted(mapping_data.items(), key=lambda kv: int(str(kv[1])))
            return np.array([int(str(k)) for k, _ in sorted_pairs], dtype="<i4")
    except Exception as exc:
        logger.debug("Unable to parse OMX mapping file %s: %s", mapping_file, exc)
    return None


def compute_omx_logical_matrices_sha256(
    omx_path_or_handle: str | Path | Any,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    chunk_rows: int = _DEFAULT_CHUNK_ROWS,
) -> str:
    """
    Compute the canonical logical matrices SHA-256 hex digest for an OMX/HDF5 file.

    Parameters
    ----------
    omx_path_or_handle : str or pathlib.Path or h5py.File
        Filesystem path to an OpenMatrix (``.omx``) or HDF5 (``.h5``/``.hdf5``)
        file, or an already-open ``h5py.File``-like object.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy controlling lookup resolution, dtype handling,
        and HDF5 group paths.
    chunk_rows : int, default 512
        Number of leading-dimension rows streamed per chunk to bound peak memory
        when hashing multi-gigabyte containers.

    Returns
    -------
    str
        64-character lowercase hexadecimal SHA-256 digest.

    Raises
    ------
    ValueError
        If no data group or matrices are found, or if an array cannot be
        canonicalized under ``policy``.

    Notes
    -----
    Streams one dataset and row chunk at a time in sorted UTF-8 key order so
    multi-gigabyte OMX files are hashed in constant memory regardless of HDF5
    chunk layout, compression filter, or write timestamp.
    """
    resolved_policy = resolve_canonicalization_policy(policy)
    if isinstance(omx_path_or_handle, (str, Path)):
        omx_path = Path(omx_path_or_handle)
        with open_h5py_read_only(omx_path) as h5f:
            return _compute_omx_logical_matrices_sha256_from_h5(
                h5f,
                omx_path=omx_path,
                policy=resolved_policy,
                chunk_rows=chunk_rows,
            )
    return _compute_omx_logical_matrices_sha256_from_h5(
        omx_path_or_handle,
        omx_path=None,
        policy=resolved_policy,
        chunk_rows=chunk_rows,
    )


def _compute_omx_logical_matrices_sha256_from_h5(
    h5f: Any,
    *,
    omx_path: Path | None,
    policy: ArrayCanonicalizationPolicy,
    chunk_rows: int,
) -> str:
    import numpy as np

    data_group = _resolve_h5_group(h5f, policy.h5_data_group)
    lookup_group = _resolve_h5_group(h5f, policy.h5_lookup_group)
    lookup_group_name = policy.h5_lookup_group.strip("/").lower()

    data_container: Any = None
    matrix_names: list[str] = []
    if data_group is not None and hasattr(data_group, "keys"):
        data_container = data_group
        matrix_names = sorted(
            str(k) for k in data_group.keys() if hasattr(data_group[k], "shape")
        )
    elif policy.allow_root_h5_datasets and hasattr(h5f, "keys"):
        data_container = h5f
        matrix_names = sorted(
            str(k)
            for k in h5f.keys()
            if str(k).lower() != lookup_group_name and hasattr(h5f[k], "shape")
        )
    else:
        raise ValueError("OMX file does not contain a /data group.")

    if not matrix_names:
        raise ValueError("OMX file /data group contains no matrices.")

    first_ds = data_container[matrix_names[0]]
    zone_count = int(first_ds.shape[0]) if len(first_ds.shape) >= 1 else 0

    hasher = hashlib.sha256()

    if policy.lookup_policy == "single_int32_zone_number":
        raw_lookup: Any = None
        if lookup_group is not None and hasattr(lookup_group, "keys"):
            for lkey in _PRIMARY_LOOKUP_KEYS:
                if lkey in lookup_group:
                    raw_lookup = lookup_group[lkey][...]
                    break
            if raw_lookup is None:
                sorted_lkeys = sorted(str(k) for k in lookup_group.keys())
                if sorted_lkeys:
                    raw_lookup = lookup_group[sorted_lkeys[0]][...]

        if (
            raw_lookup is None
            and hasattr(h5f, "attrs")
            and "original_zone_ids" in h5f.attrs
        ):
            raw_zones = h5f.attrs["original_zone_ids"]
            if hasattr(raw_zones, "tolist"):
                raw_lookup = np.asarray(raw_zones.tolist())
            elif isinstance(raw_zones, (list, tuple)):
                raw_lookup = np.asarray(raw_zones)

        if raw_lookup is None:
            raw_lookup = _load_omx_sidecar_mapping_zone_ids(omx_path)

        if raw_lookup is None:
            zone_vector = np.arange(1, zone_count + 1, dtype="<i4")
        else:
            zone_vector = _coerce_int32_zone_lookup(raw_lookup)

        _stream_dataset_into_hasher(
            hasher,
            "lookup/zone_number",
            zone_vector,
            dtype_policy="preserve",
            canonicalize_float_bits=False,
            chunk_rows=chunk_rows,
        )
    else:
        has_lookup_entries = False
        if lookup_group is not None and hasattr(lookup_group, "keys"):
            sorted_lkeys = sorted(
                str(k) for k in lookup_group.keys() if hasattr(lookup_group[k], "shape")
            )
            for lkey in sorted_lkeys:
                has_lookup_entries = True
                lookup_ds = lookup_group[lkey]
                _stream_dataset_into_hasher(
                    hasher,
                    f"lookup/{lkey}",
                    lookup_ds,
                    dtype_policy="preserve",
                    canonicalize_float_bits=policy.canonicalize_float_bits,
                    chunk_rows=chunk_rows,
                )

        if (
            not has_lookup_entries
            and hasattr(h5f, "attrs")
            and "original_zone_ids" in h5f.attrs
        ):
            raw_zones = h5f.attrs["original_zone_ids"]
            zone_arr = np.asarray(
                raw_zones.tolist() if hasattr(raw_zones, "tolist") else raw_zones
            )
            _stream_dataset_into_hasher(
                hasher,
                "lookup/zone_number",
                zone_arr,
                dtype_policy="preserve",
                canonicalize_float_bits=policy.canonicalize_float_bits,
                chunk_rows=chunk_rows,
            )

    for name in matrix_names:
        ds = data_container[name]
        _stream_dataset_into_hasher(
            hasher,
            name,
            ds,
            dtype_policy=policy.dtype_policy,
            canonicalize_float_bits=policy.canonicalize_float_bits,
            include_na_attribute=policy.include_na_attribute,
            chunk_rows=chunk_rows,
        )

    return hasher.hexdigest()


def compute_zarr_logical_matrices_sha256(
    zarr_path: str | Path,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    chunk_rows: int = _DEFAULT_CHUNK_ROWS,
) -> str:
    """
    Compute the canonical logical matrices SHA-256 hex digest for a Zarr store.

    Parameters
    ----------
    zarr_path : str or pathlib.Path
        Filesystem path to a Zarr directory store.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy controlling coordinate inclusion, dtype
        handling, and optional 3D dimension unstacking (``policy.unstack_dim``).
    chunk_rows : int, default 512
        Number of leading-dimension rows streamed per chunk to bound peak memory.

    Returns
    -------
    str
        64-character lowercase hexadecimal SHA-256 digest.

    Raises
    ------
    ValueError
        If ``zarr_path`` is not a directory, contains no data variables, or
        cannot be canonicalized under ``policy``.

    Notes
    -----
    Supports both 2D/N-D Zarr variables and 3D+ variables unstacked along
    ``policy.unstack_dim`` (for example ``unstack_dim="time_period"`` so a 3D
    Zarr ``DIST(time_period, otaz, dtaz)`` produces the exact same digest as an
    OMX file with ``/data/DIST__AM``, ``/data/DIST__PM``, etc.).
    """
    resolved_policy = resolve_canonicalization_policy(policy)
    store_path = Path(zarr_path)
    if not store_path.is_dir():
        raise ValueError(f"Zarr store path is not a directory: {store_path}")

    xr = importlib.import_module("xarray")
    with xr.open_zarr(store_path) as ds:
        return _compute_zarr_logical_matrices_sha256_from_xarray(
            ds,
            policy=resolved_policy,
            chunk_rows=chunk_rows,
        )


def _compute_zarr_logical_matrices_sha256_from_xarray(
    ds: Any,
    *,
    policy: ArrayCanonicalizationPolicy,
    chunk_rows: int,
) -> str:
    import numpy as np

    data_var_names = sorted(str(k) for k in ds.data_vars.keys())
    if not data_var_names:
        raise ValueError("Zarr dataset contains no data variables.")

    unstack_dim = policy.unstack_dim
    virtual_entries: list[tuple[str, Any]] = []
    for var_name in data_var_names:
        da = ds[var_name]
        if unstack_dim is not None and unstack_dim in da.dims:
            coord_values = da.coords[unstack_dim].values.tolist()
            for idx, coord_val in enumerate(coord_values):
                coord_label = _decode_attr_value(coord_val)
                slice_key = f"{var_name}{policy.unstack_sep}{coord_label}"
                virtual_entries.append((slice_key, da.isel({unstack_dim: idx})))
        else:
            virtual_entries.append((var_name, da))

    virtual_entries.sort(key=lambda item: item[0])
    first_da = virtual_entries[0][1]
    zone_count = int(first_da.shape[0]) if len(first_da.shape) >= 1 else 0

    hasher = hashlib.sha256()
    attrs = dict(ds.attrs) if hasattr(ds, "attrs") else {}

    if policy.lookup_policy == "single_int32_zone_number":
        raw_lookup: Any = None
        for pkey in _PRIMARY_LOOKUP_KEYS:
            if pkey in ds.coords and pkey != unstack_dim:
                raw_lookup = ds.coords[pkey].values
                break
        if raw_lookup is None and "original_zone_ids" in attrs:
            raw_zones = attrs["original_zone_ids"]
            raw_lookup = np.asarray(
                raw_zones.tolist() if hasattr(raw_zones, "tolist") else raw_zones
            )
        if raw_lookup is None:
            for ckey in sorted(str(k) for k in ds.coords.keys()):
                if ckey != unstack_dim:
                    raw_lookup = ds.coords[ckey].values
                    break

        if raw_lookup is None:
            zone_vector = np.arange(1, zone_count + 1, dtype="<i4")
        else:
            zone_vector = _coerce_int32_zone_lookup(raw_lookup)

        _stream_dataset_into_hasher(
            hasher,
            "lookup/zone_number",
            zone_vector,
            dtype_policy="preserve",
            canonicalize_float_bits=False,
            chunk_rows=chunk_rows,
        )
    else:
        coord_keys = sorted(
            str(k)
            for k in ds.coords.keys()
            if str(k) != unstack_dim and ds.coords[k].ndim >= 1
        )
        # If the dataset uses symmetric 2D origin/destination dimension coords
        # (e.g. otaz/dtaz or origin/destination) and also carries original_zone_ids
        # or zone_number metadata, check whether a canonical zone_number lookup
        # is represented.
        if (
            set(coord_keys) in ({"otaz", "dtaz"}, {"origin", "destination"})
            and "zone_number" not in coord_keys
            and "original_zone_ids" in attrs
        ):
            raw_zones = attrs["original_zone_ids"]
            zone_arr = np.asarray(
                raw_zones.tolist() if hasattr(raw_zones, "tolist") else raw_zones
            )
            _stream_dataset_into_hasher(
                hasher,
                "lookup/zone_number",
                zone_arr,
                dtype_policy="preserve",
                canonicalize_float_bits=policy.canonicalize_float_bits,
                chunk_rows=chunk_rows,
            )
        else:
            for ckey in coord_keys:
                coord_arr = np.asarray(ds.coords[ckey].values)
                _stream_dataset_into_hasher(
                    hasher,
                    f"lookup/{ckey}",
                    coord_arr,
                    dtype_policy="preserve",
                    canonicalize_float_bits=policy.canonicalize_float_bits,
                    chunk_rows=chunk_rows,
                )

    for entry_name, da in virtual_entries:
        na_val = (
            _decode_attr_value(da.attrs["NA"])
            if hasattr(da, "attrs") and "NA" in da.attrs
            else None
        )
        _stream_dataset_into_hasher(
            hasher,
            entry_name,
            da.variable,
            dtype_policy=policy.dtype_policy,
            canonicalize_float_bits=policy.canonicalize_float_bits,
            include_na_attribute=policy.include_na_attribute,
            na_value=na_val,
            chunk_rows=chunk_rows,
        )

    return hasher.hexdigest()


def _decode_attr_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    np = sys.modules.get("numpy")
    if np is not None and isinstance(val, np.generic):
        item_val = val.item()
        if isinstance(item_val, (bytes, bytearray)):
            return item_val.decode("utf-8", errors="replace")
        return item_val
    if isinstance(val, (str, int, float, bool)):
        return val
    return str(val)


@dataclass(frozen=True, slots=True)
class MatrixContainerSummary:
    """
    Domain-agnostic structural and cryptographic summary of an array container.

    Attributes
    ----------
    artifact_path : str
        Filesystem path of the inspected container or file.
    artifact_format : {"omx", "h5", "zarr", "directory", "file"}
        Detected container format.
    container_identity : str
        Byte-level container identity (``sha256:file:<hex>`` for files or
        ``sha256:manifest-v1:<hex>`` for Zarr/directories).
    size_bytes : int
        Total byte size of the file or non-symlink files in the directory.
    logical_identity : str or None
        Canonical logical array identity (``sha256:logical_matrices:<hex>``)
        when embedded in container attributes or computed on demand.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"}
        Canonicalization profile used or recorded for ``logical_identity``.
    shape : tuple[int, ...] or None
        Shape of the first discovered data variable/dataset, if any.
    coord_keys : tuple[str, ...]
        Sorted lookup or coordinate names discovered in the container.
    variable_count : int
        Total number of discovered data matrices/variables.
    variable_names : tuple[str, ...]
        Sorted names of discovered data matrices/variables.
    root_attributes : Mapping[str, Any]
        Decoded container-level root attributes.
    facets : Mapping[str, Any]
        Optional caller-extracted facet mapping derived from ``variable_names``.
    """

    artifact_path: str
    artifact_format: Literal["omx", "h5", "zarr", "directory", "file"]
    container_identity: str
    size_bytes: int
    logical_identity: str | None
    logical_profile: LogicalProfileName
    shape: tuple[int, ...] | None
    coord_keys: tuple[str, ...]
    variable_count: int
    variable_names: tuple[str, ...]
    root_attributes: Mapping[str, Any] = field(default_factory=dict)
    facets: Mapping[str, Any] = field(default_factory=dict)


# --- v2 Content Identity Hub, SchemeRegistry, Identity & Comparison Engine ---

_LOWER_HEX_RE = re.compile(r"^[0-9a-f]+$")
_POLICY_SCHEME_RE = re.compile(r"^sha256:manifest-v1\+p:[0-9a-f]{64}$")


class Kind(Enum):
    """Classification of what equality on a digest scheme guarantees."""

    BYTES = "bytes"
    CONTAINER = "container"
    LOGICAL = "logical"
    FILTERED = "filtered"
    SEMANTIC = "semantic"
    HISTORICAL = "historical"


class Agreement(Enum):
    """Outcome of comparing two identities under an expectation contract."""

    MATCH = "match"
    MISMATCH = "mismatch"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SchemeSpec:
    """Metadata specification for a registered digest scheme."""

    name: str
    kind: Kind
    applies_to: frozenset[str] = frozenset(
        {"file", "tree", "bundle", "table", "matrix_set", "member"}
    )
    cost_rank: int = 10
    hex_len: int = 64
    aliases: tuple[str, ...] = ()
    content_pure: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("SchemeSpec.name must be a non-empty string")
        object.__setattr__(self, "applies_to", frozenset(self.applies_to))
        object.__setattr__(self, "aliases", tuple(self.aliases))


_FILE_DRIVERS: frozenset[str] = frozenset(
    {
        "file",
        "parquet",
        "csv",
        "tsv",
        "json",
        "geojson",
        "h5",
        "hdf5",
        "omx",
        "netcdf",
        "nc",
        "feather",
        "arrow",
        "sqlite",
        "txt",
        "bin",
    }
)

_BUILTIN_SCHEME_SPECS: tuple[SchemeSpec, ...] = (
    SchemeSpec(
        name="blake3:file",
        kind=Kind.BYTES,
        applies_to=_FILE_DRIVERS,
        cost_rank=5,
    ),
    SchemeSpec(
        name="sha256:file",
        kind=Kind.BYTES,
        applies_to=_FILE_DRIVERS,
        cost_rank=10,
    ),
    SchemeSpec(
        name="blosc2:header:v1",
        kind=Kind.CONTAINER,
        applies_to=frozenset({"blosc2", "zarr", "file", "tree"}),
        cost_rank=15,
    ),
    SchemeSpec(
        name="sha256:manifest-v1",
        kind=Kind.CONTAINER,
        applies_to=frozenset(
            {"tree", "bundle", "directory", "zarr", "shapefile", "gtfs"}
        ),
        cost_rank=20,
    ),
    SchemeSpec(
        name="sha256:legacy-dir-v0",
        kind=Kind.CONTAINER,
        applies_to=frozenset({"tree", "directory"}),
        cost_rank=25,
        content_pure=False,
    ),
    SchemeSpec(
        name="ctd1:sha256",
        kind=Kind.LOGICAL,
        applies_to=frozenset(
            {
                "table",
                "file",
                "member",
                "parquet",
                "csv",
                "tsv",
                "h5_table",
                "hdf5",
                "h5",
                "duckdb",
                "sqlite",
                "feather",
                "arrow",
                "dataframe",
            }
        ),
        cost_rank=30,
    ),
    SchemeSpec(
        name="sha256:logical_matrix:v1",
        kind=Kind.LOGICAL,
        applies_to=frozenset(
            {"member", "matrix", "omx", "h5", "hdf5", "zarr", "npy", "npz"}
        ),
        cost_rank=35,
        aliases=("sha256:logical_matrix",),
    ),
    SchemeSpec(
        name="sha256:logical_matrices:v1",
        kind=Kind.LOGICAL,
        applies_to=frozenset(
            {
                "file",
                "tree",
                "matrix_set",
                "member",
                "omx",
                "h5",
                "hdf5",
                "zarr",
                "netcdf",
                "nc",
                "npz",
            }
        ),
        cost_rank=40,
        aliases=("sha256:logical_matrices",),
    ),
    SchemeSpec(
        name="core_state/v1:sha256",
        kind=Kind.LOGICAL,
        applies_to=frozenset({"tree", "table"}),
        cost_rank=45,
    ),
    SchemeSpec(
        name="legacy-v1:unverified",
        kind=Kind.HISTORICAL,
        applies_to=frozenset({"historical"}),
        cost_rank=999,
        content_pure=False,
    ),
)


class SchemeRegistry:
    """Registry of digest schemes."""

    _by_name: dict[str, SchemeSpec] = {}
    _canonical_specs: dict[str, SchemeSpec] = {}

    @classmethod
    def register(cls, spec: SchemeSpec) -> SchemeSpec:
        """Register a `SchemeSpec` by its canonical name and any aliases."""
        if not isinstance(spec, SchemeSpec):
            raise TypeError(f"Expected SchemeSpec, got {type(spec).__name__}")
        cls._canonical_specs[spec.name] = spec
        for alias in (spec.name, *spec.aliases):
            cls._by_name[alias] = spec
        return spec

    @classmethod
    def get(cls, scheme: str) -> SchemeSpec:
        """Return the `SchemeSpec` for `scheme`, or raise `ValueError` if unregistered."""
        if not isinstance(scheme, str) or not scheme:
            raise ValueError(f"Invalid digest scheme {scheme!r}")
        if _POLICY_SCHEME_RE.match(scheme):
            return SchemeSpec(
                name=scheme,
                kind=Kind.FILTERED,
                applies_to=frozenset({"tree", "bundle", "directory"}),
                cost_rank=22,
                hex_len=64,
                content_pure=True,
            )
        try:
            return cls._by_name[scheme]
        except KeyError:
            raise ValueError(f"Unregistered digest scheme {scheme!r}") from None

    @classmethod
    def is_historical(cls, scheme_or_digest: str | Digest | SchemeSpec) -> bool:
        """Return True iff the given scheme, `Digest`, or `SchemeSpec` has `Kind.HISTORICAL`."""
        if isinstance(scheme_or_digest, (Digest, SchemeSpec)):
            return scheme_or_digest.kind is Kind.HISTORICAL
        return cls.get(scheme_or_digest).kind is Kind.HISTORICAL

    @classmethod
    def all_specs(cls) -> tuple[SchemeSpec, ...]:
        """Return all registered canonical `SchemeSpec` entries."""
        return tuple(cls._canonical_specs.values())


for _spec in _BUILTIN_SCHEME_SPECS:
    SchemeRegistry.register(_spec)


@dataclass(frozen=True, slots=True)
class Digest:
    """Scheme-qualified cryptographic digest."""

    scheme: str
    hex: str

    def __post_init__(self) -> None:
        spec = SchemeRegistry.get(self.scheme)
        if not isinstance(self.hex, str) or not _LOWER_HEX_RE.fullmatch(self.hex):
            raise ValueError(
                f"Digest.hex must be a non-empty lowercase hex string, got {self.hex!r}"
            )
        if spec.hex_len > 0 and len(self.hex) != spec.hex_len:
            raise ValueError(
                f"Expected {spec.hex_len}-char lowercase hex for {spec.name}, got {self.hex!r}"
            )
        object.__setattr__(self, "scheme", spec.name)

    @property
    def value(self) -> str:
        """Alias for `hex`."""
        return self.hex

    @property
    def kind(self) -> Kind:
        """Return the `Kind` of this digest's scheme."""
        return SchemeRegistry.get(self.scheme).kind

    @classmethod
    def parse(cls, value: str) -> Digest:
        """Parse `<scheme>:<hex>` using `rpartition(':')`."""
        if not isinstance(value, str):
            raise ValueError(f"Expected digest string, got {value!r}")
        scheme, sep, hex_val = value.strip().rpartition(":")
        if not sep or not scheme or not hex_val or scheme == "sha256":
            raise ValueError(f"Unqualified or malformed digest {value!r}")
        return cls(scheme=scheme, hex=hex_val)

    def __str__(self) -> str:
        return f"{self.scheme}:{self.hex}"


class IdentityConflict(ValueError):
    """Raised when two claims for the same scheme contradict."""


@dataclass(frozen=True, slots=True)
class Identity:
    """Anchor-keyed content identity (never None). Equality and hash depend ONLY on `anchor`."""

    anchor: Digest
    claims: tuple[Digest, ...] = field(default=(), compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.anchor, Digest):
            raise ValueError(
                f"Identity.anchor must be a Digest, got {type(self.anchor).__name__}"
            )
        seen_by_scheme: dict[str, Digest] = {self.anchor.scheme: self.anchor}
        for claim in self.claims:
            if not isinstance(claim, Digest):
                raise ValueError(
                    f"Identity.claims elements must be Digest instances, got {type(claim).__name__}"
                )
            if self.anchor.kind is Kind.HISTORICAL or claim.kind is Kind.HISTORICAL:
                raise ValueError(
                    "Kind.HISTORICAL digests cannot participate in Identity.claims"
                )
            existing = seen_by_scheme.get(claim.scheme)
            if existing is not None:
                if existing.hex != claim.hex:
                    raise IdentityConflict(
                        f"Conflicting digests for scheme {claim.scheme!r}: "
                        f"{existing.hex} != {claim.hex}"
                    )
                continue
            seen_by_scheme[claim.scheme] = claim

        object.__setattr__(self, "claims", tuple(seen_by_scheme.values())[1:])

    @property
    def all_digests(self) -> tuple[Digest, ...]:
        """Return `(anchor, *claims)` in order without duplicates."""
        return (self.anchor, *self.claims)

    @property
    def verifiable(self) -> bool:
        """Return True iff `anchor` is not `Kind.HISTORICAL`."""
        return self.anchor.kind is not Kind.HISTORICAL

    def by_scheme(self) -> dict[str, Digest]:
        """Return a mapping of `scheme -> Digest` across `all_digests`."""
        return {d.scheme: d for d in self.all_digests}

    def get(self, scheme: str) -> Digest | None:
        """Return the `Digest` for `scheme` if present on this `Identity`."""
        canonical = SchemeRegistry.get(scheme).name
        return self.by_scheme().get(canonical)

    def with_claim(self, d: Digest) -> Identity:
        """Return a new `Identity` with `d` appended to `claims` (or `self` if already present)."""
        nxt = Identity(anchor=self.anchor, claims=(*self.claims, d))
        return self if nxt.claims == self.claims else nxt

    @staticmethod
    def refines(old: Identity, new: Identity) -> bool:
        """Return True iff `new` has the same anchor as `old` and preserves all of `old`'s digests."""
        if not isinstance(old, Identity) or not isinstance(new, Identity):
            raise TypeError("Identity.refines requires two Identity instances")
        if old.anchor != new.anchor:
            return False
        new_by_scheme = new.by_scheme()
        return all(
            new_by_scheme.get(scheme) == digest
            for scheme, digest in old.by_scheme().items()
        )


def refines(a: Identity | SchemeSpec, b: Identity | SchemeSpec) -> bool:
    """Return True if `b` refines `a` (for `Identity`) or scheme `a` refines scheme `b` (for `SchemeSpec`)."""
    if isinstance(a, Identity) and isinstance(b, Identity):
        return Identity.refines(a, b)
    if isinstance(a, SchemeSpec) and isinstance(b, SchemeSpec):
        if Kind.HISTORICAL in (a.kind, b.kind):
            return False
        if a.name == b.name:
            return True
        return (
            a.kind in (Kind.BYTES, Kind.CONTAINER)
            and a.content_pure
            and b.content_pure
            and bool(a.applies_to & b.applies_to)
        )
    raise TypeError(
        "refines() requires two Identity instances or two SchemeSpec instances"
    )


@dataclass(frozen=True, slots=True)
class Expectation:
    """Comparison contract specifying accepted digest kinds and optional pinned schemes."""

    accepts_kinds: frozenset[Kind] = frozenset(
        {Kind.BYTES, Kind.CONTAINER, Kind.LOGICAL, Kind.SEMANTIC, Kind.FILTERED}
    )
    pinned_schemes: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        kinds = frozenset(self.accepts_kinds)
        if any(not isinstance(k, Kind) for k in kinds) or Kind.HISTORICAL in kinds:
            raise ValueError(
                "Expectation accepts_kinds must be non-HISTORICAL Kind values"
            )
        pinned = frozenset(
            SchemeRegistry.get(s).name for s in (self.pinned_schemes or ())
        )
        if any(SchemeRegistry.is_historical(s) for s in pinned):
            raise ValueError("Expectation cannot pin a Kind.HISTORICAL digest scheme")
        object.__setattr__(self, "accepts_kinds", kinds)
        object.__setattr__(self, "pinned_schemes", pinned)


@dataclass(frozen=True, slots=True)
class Contract:
    """Built-in `Expectation` presets and per-(step, role) staging/verification contract."""

    EXACT_BYTES: ClassVar[Expectation] = Expectation(
        accepts_kinds=frozenset({Kind.BYTES, Kind.CONTAINER}),
    )
    LOGICAL_DATA: ClassVar[Expectation] = Expectation(
        accepts_kinds=frozenset({Kind.LOGICAL}),
    )
    SCHEMA_ONLY: ClassVar[Expectation] = Expectation(
        accepts_kinds=frozenset({Kind.SEMANTIC}),
    )
    ANY_SHARED: ClassVar[Expectation] = Expectation(
        accepts_kinds=frozenset(
            {Kind.BYTES, Kind.CONTAINER, Kind.LOGICAL, Kind.SEMANTIC, Kind.FILTERED}
        ),
    )

    equivalence: str = "sha256:file"
    mutates: bool = False
    allow_extra_files: bool = False
    witness: str = "structural"

    def __post_init__(self) -> None:
        spec = SchemeRegistry.get(self.equivalence)
        if spec.kind is Kind.HISTORICAL:
            raise ValueError(
                "Kind.HISTORICAL cannot be used as a Contract equivalence scheme"
            )
        object.__setattr__(self, "equivalence", spec.name)


@dataclass(frozen=True, slots=True)
class Verdict:
    """Result of comparing two `Identity` objects under an `Expectation` contract."""

    agreement: Agreement
    matched_scheme: str | None = None
    needed: tuple[str, ...] = ()
    agree_reachable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.needed, tuple):
            object.__setattr__(self, "needed", tuple(self.needed))


def compare(
    a: Identity,
    b: Identity,
    contract: Expectation = Contract.LOGICAL_DATA,
    *,
    driver_a: str | None = None,
    driver_b: str | None = None,
) -> Verdict:
    """Compare two `Identity` objects under `contract` and optional driver constraints."""
    if a.anchor.kind is Kind.HISTORICAL or b.anchor.kind is Kind.HISTORICAL:
        return Verdict(Agreement.UNKNOWN, None, (), False)

    map_a = a.by_scheme()
    map_b = b.by_scheme()

    shared_schemes = [s for s in map_a if s in map_b]
    agree_schemes = [s for s in shared_schemes if map_a[s].hex == map_b[s].hex]
    differ_schemes = [s for s in shared_schemes if map_a[s].hex != map_b[s].hex]
    for s_agree in agree_schemes:
        for s_diff in differ_schemes:
            if refines(SchemeRegistry.get(s_agree), SchemeRegistry.get(s_diff)):
                raise IdentityConflict(
                    f"Scheme {s_agree!r} agrees but refined scheme {s_diff!r} differs"
                )

    norm_driver_a = driver_a.strip().lower() if isinstance(driver_a, str) else None
    norm_driver_b = driver_b.strip().lower() if isinstance(driver_b, str) else None

    all_scheme_names = {
        *(spec.name for spec in SchemeRegistry.all_specs()),
        *map_a,
        *map_b,
        *contract.pinned_schemes,
    }
    candidates = sorted(
        (
            spec
            for s in all_scheme_names
            if (spec := SchemeRegistry.get(s)).kind in contract.accepts_kinds
            and (not contract.pinned_schemes or spec.name in contract.pinned_schemes)
            and (norm_driver_a is None or norm_driver_a in spec.applies_to)
            and (norm_driver_b is None or norm_driver_b in spec.applies_to)
        ),
        key=lambda s: (s.cost_rank, s.name),
    )

    first_mismatch: str | None = None
    for spec in candidates:
        if spec.name in map_a and spec.name in map_b:
            if map_a[spec.name].hex == map_b[spec.name].hex:
                return Verdict(Agreement.MATCH, spec.name, (), True)
            if first_mismatch is None:
                first_mismatch = spec.name

    if first_mismatch is not None:
        return Verdict(Agreement.MISMATCH, first_mismatch, (), True)

    needed = tuple(
        spec.name
        for spec in candidates
        if spec.name not in map_a or spec.name not in map_b
    )
    return Verdict(Agreement.UNKNOWN, None, needed, bool(candidates))


# --- v2 Digest Helpers for `sha256:manifest-v1`, `ctd1:sha256`, and Logical Matrices ---


def _reject_manifest_symlink_components(path: Path, *, label: str) -> None:
    candidate = Path(path).expanduser().absolute()
    for p in reversed((candidate, *candidate.parents)):
        if p.is_symlink():
            raise ValueError(
                f"directory artifact {label} cannot traverse a symlink: {p}"
            )


def build_manifest_v1(root: Path | str) -> dict[str, Any]:
    """Build a deterministic v1-compatible directory manifest (`version=1`, `entries`, `tree_hash`).

    Produces byte-for-byte identical canonical JSON and `tree_hash` to
    `consist.core.directory_artifacts.build_directory_manifest`.
    """
    root_path = Path(root).expanduser().absolute()
    _reject_manifest_symlink_components(root_path, label="root")
    if not root_path.is_dir():
        raise ValueError(f"directory artifact root must be a directory: {root_path}")

    entries: list[dict[str, Any]] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as scan:
            children = sorted(scan, key=lambda entry: entry.name)
        for child in children:
            child_path = Path(child.path)
            relative_path = child_path.relative_to(root_path).as_posix()
            if child.is_symlink():
                raise ValueError(
                    f"directory artifact cannot contain a symlink: {child_path}"
                )
            if child.is_dir(follow_symlinks=False):
                entries.append({"kind": "directory", "path": relative_path})
                visit(child_path)
                continue
            if child.is_file(follow_symlinks=False):
                entries.append(
                    {
                        "kind": "file",
                        "path": relative_path,
                        "sha256": file_sha256(child_path),
                        "size": child.stat(follow_symlinks=False).st_size,
                    }
                )
                continue
            raise ValueError(
                "directory artifact cannot contain a non-regular filesystem entry: "
                f"{child_path}"
            )

    visit(root_path)
    entries.sort(key=lambda entry: (entry["path"], entry["kind"]))
    identity_payload = {"version": 1, "entries": entries}
    return {**identity_payload, "tree_hash": canonical_json_sha256(identity_payload)}


def compute_manifest_v1_digest(root: Path | str) -> Digest:
    """Compute `Digest('sha256:manifest-v1', tree_hash)` for a directory tree."""
    manifest = build_manifest_v1(root)
    return Digest("sha256:manifest-v1", manifest["tree_hash"])


def compute_ctd1_digest(
    source: Any,
    columns: Sequence[str] | None = None,
    key: Sequence[str] | None = None,
    *,
    key_columns: Sequence[str] | None = None,
    sqlite_table: str | None = None,
    h5_key: str | None = None,
    member: str | None = None,
) -> Digest:
    """Compute a `Digest('ctd1:sha256', <hex>)` over a tabular data source."""
    raw = digest_table(
        source,
        columns=columns,
        key=key,
        key_columns=key_columns,
        profile="ctd1",
        sqlite_table=sqlite_table,
        h5_key=h5_key,
        member=member,
    )
    return Digest.parse(raw)


def compute_logical_matrix_digest(
    name: str,
    arr: np.ndarray,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    na_value: Any = None,
) -> Digest:
    """Compute `Digest('sha256:logical_matrix:v1', <hex>)` for a single named matrix."""
    hex_val = compute_array_entry_sha256(name, arr, policy=policy, na_value=na_value)
    return Digest("sha256:logical_matrix:v1", hex_val)


def compute_logical_matrices_digest(
    matrices: Mapping[str, np.ndarray] | Sequence[tuple[str, np.ndarray]],
    *,
    zone_ids: Sequence[int] | np.ndarray | None = None,
    lookups: Mapping[str, np.ndarray | Sequence[Any]] | None = None,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
) -> Digest:
    """Compute `Digest('sha256:logical_matrices:v1', <hex>)` for a mapping/sequence of matrices."""
    hex_val = compute_logical_matrices_sha256(
        matrices,
        zone_ids=zone_ids,
        lookups=lookups,
        policy=policy,
    )
    return Digest("sha256:logical_matrices:v1", hex_val)


__all__ = [
    # Constants & Protocols
    "ARRAY_BINARY_PROTOCOL_VERSION",
    "FLOAT32_MATRICES_V1_POLICY",
    "PRESERVE_CANONICAL_POLICY",
    "LogicalProfileName",
    # v2 Content Identity Hub & Comparison Engine
    "Kind",
    "SchemeSpec",
    "SchemeRegistry",
    "Digest",
    "IdentityConflict",
    "Identity",
    "Expectation",
    "Contract",
    "Agreement",
    "Verdict",
    "refines",
    "compare",
    "build_manifest_v1",
    "compute_manifest_v1_digest",
    "compute_ctd1_digest",
    "compute_logical_matrix_digest",
    "compute_logical_matrices_digest",
    # Dataclasses
    "ArrayCanonicalizationPolicy",
    "MatrixContainerSummary",
    # File & Byte Digests
    "digest_bytes",
    "file_sha256",
    "digest_file",
    # Canonical JSON
    "normalize_json",
    "canonical_json_str",
    "canonical_json_sha256",
    # Canonical Tabular & State Digests
    "normalize_table_scalar",
    "digest_table",
    "digest_state",
    # Array & Matrix Functions
    "open_h5py_read_only",
    "canonicalize_float32_array",
    "canonicalize_float64_array",
    "canonicalize_float16_array",
    "canonicalize_float_array",
    "compute_array_entry_sha256",
    "compute_logical_matrices_sha256",
    "compute_omx_logical_matrices_sha256",
    "compute_zarr_logical_matrices_sha256",
    "normalize_logical_matrices_identity",
    "resolve_canonicalization_policy",
]
