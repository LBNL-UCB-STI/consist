"""
Consist unified cryptographic digest primitives and scheme driver registry.

This module is a strict Layer-1 leaf:
- Zero imports from ``consist.models.*`` or ``consist.core.*``.
- Heavy dependencies (PyArrow, NumPy, h5py, xarray) are imported lazily inside
  the specific functions that require them.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import closing
import dataclasses
from dataclasses import dataclass
from enum import Enum
import hashlib
import importlib
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import struct
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast
from typing_extensions import Buffer, Protocol, runtime_checkable
import unicodedata

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

ARRAY_BINARY_PROTOCOL_VERSION: str = "consist.array_binary.v1"

_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_LOWER_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_SCHEME_RE = re.compile(r"^[A-Za-z0-9_./+-]+(?::[A-Za-z0-9_./+-]+)*$")
_MANIFEST_VERSION = 1
_SHAPEFILE_REQUIRED_SUFFIXES = frozenset({".shp", ".shx", ".dbf"})
SHAPEFILE_SIDECAR_SUFFIXES: frozenset[str] = frozenset(
    {
        ".shp",
        ".shx",
        ".dbf",
        ".prj",
        ".cpg",
        ".qpj",
        ".sbn",
        ".sbx",
        ".fbn",
        ".fbx",
        ".ain",
        ".aih",
        ".ixs",
        ".mxs",
        ".atx",
        ".shp.xml",
    }
)


def is_shapefile_sidecar_filename(filename: str, stem: str) -> bool:
    """Return True if ``filename`` is a known shapefile sidecar matching ``stem`` (case-insensitive)."""
    if not stem:
        return False
    lower_name = filename.lower()
    lower_stem = stem.lower()
    if not lower_name.startswith(f"{lower_stem}."):
        return False
    return lower_name[len(lower_stem) :] in SHAPEFILE_SIDECAR_SUFFIXES


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
_T = TypeVar("_T")


class ConcurrentMutationError(ValueError):
    """Raised when a file changes on disk while its descriptor is being hashed."""


@dataclass(frozen=True, slots=True)
class Digest:
    """Self-describing immutable cryptographic digest ``<scheme>:<64-lowercase-hex>``."""

    scheme: str
    hex: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scheme, str)
            or not self.scheme
            or not _SCHEME_RE.fullmatch(self.scheme)
        ):
            raise ValueError(f"Invalid digest scheme: {self.scheme!r}")
        if not isinstance(self.hex, str) or not _LOWER_HEX64_RE.fullmatch(self.hex):
            raise ValueError(
                f"Digest hex must be 64 lowercase hexadecimal characters, got {self.hex!r}"
            )

    def __str__(self) -> str:
        return f"{self.scheme}:{self.hex}"

    @classmethod
    def parse(cls, raw: str | "Digest") -> "Digest":
        """Parse ``<scheme>:<hex>`` preserving colons inside ``scheme``."""
        if isinstance(raw, cls):
            return raw
        if not isinstance(raw, str):
            raise ValueError(
                f"Expected digest string or Digest, got {type(raw).__name__}"
            )
        scheme, sep, hex_val = raw.rpartition(":")
        if not sep or not scheme or not _LOWER_HEX64_RE.fullmatch(hex_val):
            raise ValueError(f"Malformed digest string: {raw!r}")
        if scheme == "sha256:manifest-v1":
            scheme = "dir_manifest_v1:sha256"
        return cls(scheme=scheme, hex=hex_val)


# --- Structural Protocols for Optional Runtime Objects ---


@runtime_checkable
class _H5ArrayLike(Protocol):
    @property
    def dtype(self) -> Any: ...

    @property
    def shape(self) -> tuple[int, ...]: ...

    def __getitem__(self, key: Any) -> Any: ...


@runtime_checkable
class _H5GroupLike(Protocol):
    def keys(self) -> Iterable[str]: ...

    def __contains__(self, key: str) -> bool: ...

    def __getitem__(self, key: str) -> Any: ...


def _read_attrs_mapping(obj: Any) -> Any | None:
    try:
        return obj.attrs
    except AttributeError:
        return None


# --- Descriptor-Pinned File Hashing ---


def _stat_signature(st: os.stat_result) -> tuple[int, int, int, int]:
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def _run_fd_pinned(path: Path | str, fn: Callable[[int], _T]) -> _T:
    """Execute ``fn(fd)`` on an open file descriptor and verify no mid-read race occurred."""
    file_path = Path(path)
    stat_pre_open = os.stat(file_path)
    fd = os.open(file_path, os.O_RDONLY)
    try:
        fstat_before = os.fstat(fd)
        if _stat_signature(stat_pre_open) != _stat_signature(fstat_before):
            raise ConcurrentMutationError(
                f"Concurrent file replacement detected while opening {file_path}"
            )
        result = fn(fd)
        fstat_after = os.fstat(fd)
        try:
            stat_post_read = os.stat(file_path)
        except OSError as exc:
            raise ConcurrentMutationError(
                f"Concurrent file removal detected while hashing {file_path}"
            ) from exc
    finally:
        os.close(fd)

    if _stat_signature(fstat_before) != _stat_signature(fstat_after) or _stat_signature(
        fstat_before
    ) != _stat_signature(stat_post_read):
        raise ConcurrentMutationError(
            f"Concurrent mid-read mutation detected while hashing {file_path}"
        )
    return result


def digest_bytes(data: bytes) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path | str) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of a regular file pinned by descriptor."""

    def _hash_fd(fd: int) -> str:
        with io.FileIO(fd, mode="rb", closefd=False) as raw_io:
            return hashlib.file_digest(raw_io, "sha256").hexdigest()

    return _run_fd_pinned(path, _hash_fd)


def digest_file(path: Path | str) -> str:
    """Return self-describing file identity ``sha256:file:<64-hex>``."""
    file_path = Path(path)
    if not file_path.is_file():
        raise ValueError(f"digest_file only supports regular files: {file_path}")
    return f"sha256:file:{file_sha256(file_path)}"


# --- Canonical JSON Serialization & Hashing ---


def _clean_for_canonical_json(
    obj: Any,
    exclude_keys: set[str] | None = None,
    _visited: set[int] | None = None,
) -> Any:
    """Recursively clean a structure for deterministic canonical JSON serialization."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, Digest):
        return str(obj)

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, Enum):
        return _clean_for_canonical_json(obj.value, exclude_keys, _visited)

    if not isinstance(obj, type):
        try:
            dump_fn = obj.model_dump
        except AttributeError:
            dump_fn = None
        if callable(dump_fn):
            return _clean_for_canonical_json(
                dump_fn(mode="json"), exclude_keys, _visited
            )
        if not isinstance(obj, Mapping):
            try:
                dict_fn = obj.dict
                json_fn = obj.json
            except AttributeError:
                dict_fn = None
                json_fn = None
            if callable(dict_fn) and callable(json_fn):
                return _clean_for_canonical_json(dict_fn(), exclude_keys, _visited)
        if dataclasses.is_dataclass(obj):
            return _clean_for_canonical_json(
                dataclasses.asdict(obj), exclude_keys, _visited
            )

    if "numpy" in sys.modules:
        import numpy as np

        if isinstance(obj, np.ndarray):
            return _clean_for_canonical_json(obj.tolist(), exclude_keys, _visited)
        if isinstance(obj, np.generic):
            return _clean_for_canonical_json(obj.item(), exclude_keys, _visited)

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

    if isinstance(obj, (list, tuple)):
        visited = set() if _visited is None else _visited
        obj_id = id(obj)
        if obj_id in visited:
            raise ValueError("Circular reference detected in JSON canonicalization")
        next_visited = visited | {obj_id}
        return [_clean_for_canonical_json(x, exclude_keys, next_visited) for x in obj]

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
    """Normalize a Python structure into deterministic JSON-compatible primitives."""
    return _clean_for_canonical_json(payload, exclude_keys)


def canonical_json_str(payload: Any, *, exclude_keys: set[str] | None = None) -> str:
    """Serialize a Python structure into a deterministic, canonical JSON string."""
    cleaned = normalize_json(payload, exclude_keys=exclude_keys)
    return json.dumps(
        cleaned,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def canonical_json_sha256(payload: Any, *, exclude_keys: set[str] | None = None) -> str:
    """Return 64-char lowercase hexadecimal SHA-256 of ``canonical_json_str(payload)``."""
    return digest_bytes(
        canonical_json_str(payload, exclude_keys=exclude_keys).encode("utf-8")
    )


# --- Deterministic Directory & Shapefile Bundle Manifests (v1) ---


def _normalize_manifest_relative_path(path: str | Path) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        raise ValueError(f"unsafe directory artifact member path: {path}")
    normalized = candidate.as_posix()
    if not normalized or normalized == ".":
        raise ValueError(f"unsafe directory artifact member path: {path}")
    if any(part in {"", ".", ".."} for part in candidate.parts):
        raise ValueError(f"unsafe directory artifact member path: {path}")
    return normalized


def _resolve_manifest_root(path: Path | str, *, label: str) -> Path:
    raw_path = Path(path).expanduser()
    if raw_path.is_symlink():
        raise ValueError(
            f"directory artifact {label} contains a symlink component: {raw_path}"
        )
    canonical = raw_path.parent.resolve() / raw_path.name
    if canonical.is_symlink():
        raise ValueError(
            f"directory artifact {label} contains a symlink component: {canonical}"
        )
    return canonical


def build_manifest_v1(root: Path | str) -> dict[str, Any]:
    """Build a deterministic v1 manifest for a regular-file directory tree."""
    root_path = _resolve_manifest_root(root, label="root")
    if not root_path.is_dir():
        raise ValueError(f"directory artifact root must be a directory: {root_path}")

    entries: list[dict[str, Any]] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as scan:
            children = sorted(scan, key=lambda entry: entry.name)
        for child in children:
            child_path = Path(child.path)
            relative_path = _normalize_manifest_relative_path(
                child_path.relative_to(root_path)
            )
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
    identity = {"version": _MANIFEST_VERSION, "entries": entries}
    return {**identity, "tree_hash": canonical_json_sha256(identity)}


def build_shapefile_manifest_v1(shapefile: Path | str) -> dict[str, Any]:
    """Build a deterministic v1 manifest for a ``.shp`` file and all same-stem sidecars."""
    raw_shapefile = Path(shapefile).expanduser()
    if raw_shapefile.is_symlink():
        raise ValueError(f"shapefile bundle cannot contain a symlink: {raw_shapefile}")
    root = raw_shapefile.parent.resolve()
    shapefile_name = raw_shapefile.name
    shapefile_path = root / shapefile_name
    if shapefile_path.is_symlink():
        raise ValueError(f"shapefile bundle cannot contain a symlink: {shapefile_path}")
    if Path(shapefile_name).name != shapefile_name:
        raise ValueError(f"unsafe shapefile bundle entry name: {shapefile_name}")
    if shapefile_path.suffix.lower() != ".shp":
        raise ValueError(f"shapefile bundle entry must end in .shp: {shapefile_name}")
    if not root.is_dir():
        raise ValueError(f"shapefile bundle root must be a directory: {root}")

    stem = shapefile_path.stem
    members: list[dict[str, Any]] = []
    required_counts = {suffix: 0 for suffix in _SHAPEFILE_REQUIRED_SUFFIXES}
    with os.scandir(root) as scan:
        candidates = sorted(
            (
                entry
                for entry in scan
                if is_shapefile_sidecar_filename(entry.name, stem)
            ),
            key=lambda entry: entry.name,
        )
    for candidate in candidates:
        candidate_path = Path(candidate.path)
        if candidate.is_symlink():
            raise ValueError(
                f"shapefile bundle cannot contain a symlink: {candidate_path}"
            )
        if not candidate.is_file(follow_symlinks=False):
            raise ValueError(
                "shapefile bundle cannot contain a non-regular filesystem entry: "
                f"{candidate_path}"
            )
        suffix = candidate_path.suffix.lower()
        if suffix in required_counts:
            required_counts[suffix] += 1
        members.append(
            {
                "kind": "file",
                "path": candidate.name,
                "sha256": file_sha256(candidate_path),
                "size": candidate.stat(follow_symlinks=False).st_size,
            }
        )
    missing = sorted(suffix for suffix, count in required_counts.items() if count == 0)
    duplicates = sorted(
        suffix for suffix, count in required_counts.items() if count > 1
    )
    if missing:
        raise ValueError(
            "shapefile bundle is missing required member(s): " + ", ".join(missing)
        )
    if duplicates:
        raise ValueError(
            "shapefile bundle has duplicate required sidecar(s): "
            + ", ".join(duplicates)
        )
    identity = {"version": _MANIFEST_VERSION, "entries": members}
    return {**identity, "tree_hash": canonical_json_sha256(identity)}


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

    return unicodedata.normalize("NFC", str(val)).encode("utf-8")


def _quote_sql_ident(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(
            "SQLite identifier must be a non-empty string without NUL bytes"
        )
    return '"' + name.replace('"', '""') + '"'


def digest_table(
    source: Any,
    columns: Sequence[str] | None = None,
    key: Sequence[str] | None = None,
    *,
    key_columns: Sequence[str] | None = None,
    profile: str = "ctd1",
    sqlite_table: str | None = None,
) -> str:
    """Compute an order-invariant canonical SHA-256 tabular digest."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not profile:
        raise ValueError("profile must be a non-empty string")

    if columns is not None and len(set(columns)) != len(columns):
        raise ValueError("Duplicate column names are not supported in digest_table")

    resolved_keys = key if key is not None else key_columns

    if isinstance(source, (str, Path)):
        source_path = Path(source)
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
        elif source_path.suffix.lower() == ".csv":
            import pyarrow.csv as pa_csv

            table = pa_csv.read_csv(source_path)
        else:
            table = pq.read_table(
                source_path,
                columns=list(columns) if columns is not None else None,
            )
    elif isinstance(source, pa.Table):
        table = source
    elif isinstance(source, pa.RecordBatch):
        table = pa.Table.from_batches([source])
    elif isinstance(source, pa.RecordBatchReader):
        table = source.read_all()
    else:
        is_df = False
        if "pandas" in sys.modules:
            import pandas as pd

            is_df = isinstance(source, pd.DataFrame)
        elif source.__class__.__name__ == "DataFrame":
            is_df = True

        if is_df:
            if len(set(source.columns)) != len(source.columns):
                raise ValueError(
                    "Duplicate column names are not supported in digest_table"
                )
            table = pa.Table.from_pandas(source, preserve_index=False)
        elif "duckdb" in sys.modules:
            import duckdb

            if isinstance(source, duckdb.DuckDBPyRelation):
                arrow_res = source.arrow()
                table = (
                    arrow_res.read_all()
                    if isinstance(arrow_res, pa.RecordBatchReader)
                    else arrow_res
                )
            else:
                raise TypeError(f"Unsupported tabular data source type: {type(source)}")
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

    for batch in table.to_batches(max_chunksize=16_384):
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


def compute_ctd1_digest(
    source: Any,
    columns: Sequence[str] | None = None,
    key: Sequence[str] | None = None,
    *,
    key_columns: Sequence[str] | None = None,
    sqlite_table: str | None = None,
) -> Digest:
    """Compute ``Digest('ctd1:sha256', <hex>)`` over a tabular source."""
    return Digest.parse(
        digest_table(
            source,
            columns=columns,
            key=key,
            key_columns=key_columns,
            profile="ctd1",
            sqlite_table=sqlite_table,
        )
    )


def digest_state(
    entries: Mapping[str, str] | Sequence[Mapping[str, Any]],
    *,
    profile: str = "core_state/v1",
) -> str:
    """Compute canonical StateDigest over sorted ``(facet, digest)`` pairs."""
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
    """Open an HDF5 file in read-only mode with Lustre-safe locking defaults."""
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
    """Controls how logical arrays and coordinates are normalized before hashing."""

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
    policy: ArrayCanonicalizationPolicy | LogicalProfileName | str | None = None,
) -> ArrayCanonicalizationPolicy:
    """Resolve a profile name or policy instance to an ``ArrayCanonicalizationPolicy``."""
    if policy is None or policy in ("preserve_canonical", "preserve_canonical_v1"):
        return PRESERVE_CANONICAL_POLICY
    if policy == "float32_matrices_v1":
        return FLOAT32_MATRICES_V1_POLICY
    if isinstance(policy, ArrayCanonicalizationPolicy):
        return policy
    raise ValueError(
        f"Unsupported logical array canonicalization policy: {policy!r}. "
        "Expected 'preserve_canonical', 'float32_matrices_v1', or an ArrayCanonicalizationPolicy."
    )


def logical_matrices_scheme_for_policy(
    policy: ArrayCanonicalizationPolicy | LogicalProfileName | None = None,
) -> str:
    """Return the distinct ``Digest.scheme`` for the active logical matrix policy."""
    resolved = resolve_canonicalization_policy(policy)
    if resolved.profile == "float32_matrices_v1" or resolved.dtype_policy == "float32":
        return "logical_matrices_f32_v1:sha256"
    return "logical_matrices_canonical_v1:sha256"


def normalize_logical_matrices_identity(
    raw_val: str,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
) -> str:
    """Normalize a bare 64-hex or logical-matrices digest string into its canonical scheme."""
    if not isinstance(raw_val, str):
        raise ValueError("logical matrices identity must be a string")
    val = raw_val.strip()
    target_scheme = logical_matrices_scheme_for_policy(policy)
    if val.startswith(
        (
            "logical_matrices_canonical_v1:sha256:",
            "logical_matrices_f32_v1:sha256:",
        )
    ):
        return str(Digest.parse(val))
    if val.startswith("sha256:logical_matrices:"):
        digest = val.removeprefix("sha256:logical_matrices:")
    elif val.startswith("sha256:"):
        raise ValueError(
            "logical matrices identity must use a logical_matrices scheme, "
            f"got {raw_val!r}"
        )
    else:
        digest = val
    if not _HEX64_RE.fullmatch(digest):
        raise ValueError(
            f"logical matrices identity requires a 64-character hexadecimal SHA-256 digest, got {raw_val!r}"
        )
    return str(Digest(target_scheme, digest.lower()))


def canonicalize_float32_array(arr: np.ndarray) -> bytes:
    """Return little-endian float32 bytes with canonical IEEE-754 bit patterns."""
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
    """Return little-endian float64 bytes with canonical IEEE-754 bit patterns."""
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
    """Return little-endian float16 bytes with canonical IEEE-754 bit patterns."""
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
    """Return a C-contiguous little-endian float array with canonicalized ``-0.0`` and ``NaN`` bits."""
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
    raw = value.encode("utf-8")
    hasher.update(struct.pack("<I", len(raw)))
    hasher.update(raw)


def _format_shape(shape: Sequence[int]) -> str:
    dims = tuple(int(d) for d in shape)
    if len(dims) == 1:
        return f"({dims[0]},)"
    return f"({','.join(str(d) for d in dims)})"


def _extract_to_list(val: Any) -> Any:
    try:
        tolist_fn = val.tolist
    except AttributeError:
        return val
    return tolist_fn() if callable(tolist_fn) else val


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
        ds_attrs = _read_attrs_mapping(dataset)
        if resolved_na is None and ds_attrs is not None and "NA" in ds_attrs:
            resolved_na = _decode_attr_value(ds_attrs["NA"])
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
    """Compute the standalone canonical SHA-256 hex digest for a single named array."""
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
    """Compute the canonical logical SHA-256 hex digest over lookup and data arrays."""
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
    """Compute the canonical logical matrices SHA-256 hex digest for an OMX/HDF5 file."""
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
    if data_group is not None and isinstance(data_group, _H5GroupLike):
        data_container = data_group
        matrix_names = sorted(
            str(k) for k in data_group.keys() if isinstance(data_group[k], _H5ArrayLike)
        )
    elif policy.allow_root_h5_datasets and isinstance(h5f, _H5GroupLike):
        data_container = h5f
        matrix_names = sorted(
            str(k)
            for k in h5f.keys()
            if str(k).lower() != lookup_group_name and isinstance(h5f[k], _H5ArrayLike)
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
        if lookup_group is not None and isinstance(lookup_group, _H5GroupLike):
            for lkey in _PRIMARY_LOOKUP_KEYS:
                if lkey in lookup_group:
                    raw_lookup = lookup_group[lkey][...]
                    break
            if raw_lookup is None:
                sorted_lkeys = sorted(str(k) for k in lookup_group.keys())
                if sorted_lkeys:
                    raw_lookup = lookup_group[sorted_lkeys[0]][...]

        h5_attrs = _read_attrs_mapping(h5f)
        if (
            raw_lookup is None
            and h5_attrs is not None
            and "original_zone_ids" in h5_attrs
        ):
            raw_zones = _extract_to_list(h5_attrs["original_zone_ids"])
            if isinstance(raw_zones, (list, tuple)):
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
        if lookup_group is not None and isinstance(lookup_group, _H5GroupLike):
            sorted_lkeys = sorted(
                str(k)
                for k in lookup_group.keys()
                if isinstance(lookup_group[k], _H5ArrayLike)
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

        h5_attrs = _read_attrs_mapping(h5f)
        if (
            not has_lookup_entries
            and h5_attrs is not None
            and "original_zone_ids" in h5_attrs
        ):
            raw_zones = _extract_to_list(h5_attrs["original_zone_ids"])
            zone_arr = np.asarray(raw_zones)
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
    """Compute the canonical logical matrices SHA-256 hex digest for a Zarr store."""
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
    raw_ds_attrs = _read_attrs_mapping(ds)
    attrs = dict(raw_ds_attrs) if raw_ds_attrs is not None else {}

    if policy.lookup_policy == "single_int32_zone_number":
        raw_lookup: Any = None
        for pkey in _PRIMARY_LOOKUP_KEYS:
            if pkey in ds.coords and pkey != unstack_dim:
                raw_lookup = ds.coords[pkey].values
                break
        if raw_lookup is None and "original_zone_ids" in attrs:
            raw_lookup = np.asarray(_extract_to_list(attrs["original_zone_ids"]))
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
        if (
            set(coord_keys) in ({"otaz", "dtaz"}, {"origin", "destination"})
            and "zone_number" not in coord_keys
            and "original_zone_ids" in attrs
        ):
            zone_arr = np.asarray(_extract_to_list(attrs["original_zone_ids"]))
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
        da_attrs = _read_attrs_mapping(da)
        na_val = (
            _decode_attr_value(da_attrs["NA"])
            if da_attrs is not None and "NA" in da_attrs
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
    if "numpy" in sys.modules:
        import numpy as np

        if isinstance(val, np.ndarray):
            return [_decode_attr_value(x) for x in val.tolist()]
        if isinstance(val, np.generic):
            item_val = val.item()
            if isinstance(item_val, (bytes, bytearray)):
                return item_val.decode("utf-8", errors="replace")
            return item_val
    if isinstance(val, (list, tuple)):
        return [_decode_attr_value(x) for x in val]
    if isinstance(val, (str, int, float, bool)):
        return val
    return str(val)


# --- HDF5 Dataset (`h5_dataset_v1:sha256`), PyTables (`h5_fixed_v1:sha256`), & OutputSet (`output_set_v1:sha256`) ---

_H5_STREAM_TARGET_BYTES = 8 * 1024 * 1024


def _hash_h5_attrs(hasher: Any, node: Any) -> None:
    node_attrs = _read_attrs_mapping(node)
    if node_attrs is None:
        _update_string(hasher, "attrs:0")
        return
    attr_keys = sorted(str(k) for k in node_attrs.keys())
    _update_string(hasher, f"attrs:{len(attr_keys)}")
    for attr_key in attr_keys:
        _update_string(hasher, attr_key)
        decoded = _decode_attr_value(node_attrs[attr_key])
        _update_string(hasher, canonical_json_str(decoded))


def _update_h5_array_chunk(hasher: Any, chunk: Any) -> None:
    import numpy as np

    arr = np.asarray(chunk)
    if arr.dtype.hasobject:
        flat = arr.ravel().tolist()
        _update_string(hasher, f"obj_len:{len(flat)}")
        for item in flat:
            if item is None:
                hasher.update(b"\x00")
            elif isinstance(item, (bytes, bytearray)):
                hasher.update(b"\x01")
                hasher.update(struct.pack("<I", len(item)))
                hasher.update(bytes(item))
            elif isinstance(item, str):
                raw = item.encode("utf-8", errors="surrogatepass")
                hasher.update(b"\x02")
                hasher.update(struct.pack("<I", len(raw)))
                hasher.update(raw)
            elif isinstance(item, np.ndarray):
                hasher.update(b"\x04")
                _update_string(hasher, str(item.dtype))
                _update_string(hasher, _format_shape(tuple(int(d) for d in item.shape)))
                if item.dtype.hasobject:
                    _update_h5_array_chunk(hasher, item)
                else:
                    contig_item = np.ascontiguousarray(item)
                    view = memoryview(cast(Buffer, contig_item)).cast("B")
                    hasher.update(struct.pack("<Q", len(view)))
                    hasher.update(view)
            else:
                raw = canonical_json_str(_decode_attr_value(item)).encode("utf-8")
                hasher.update(b"\x03")
                hasher.update(struct.pack("<I", len(raw)))
                hasher.update(raw)
        return
    hasher.update(memoryview(cast(Buffer, np.ascontiguousarray(arr))).cast("B"))


def _hash_h5_dataset_node(
    hasher: Any,
    dataset: Any,
    *,
    chunk_rows: int | None = None,
) -> None:
    import numpy as np

    dtype = np.dtype(dataset.dtype)
    shape = tuple(int(d) for d in dataset.shape)
    _update_string(hasher, str(dtype))
    _update_string(hasher, _format_shape(shape))
    _hash_h5_attrs(hasher, dataset)

    if len(shape) == 0:
        _update_h5_array_chunk(hasher, dataset[()])
        return

    total_rows = shape[0]
    if total_rows == 0:
        return

    rows_per_chunk = chunk_rows
    if rows_per_chunk is None:
        trailing_elems = math.prod(shape[1:]) if len(shape) > 1 else 1
        row_bytes = max(1, int(trailing_elems) * max(1, int(dtype.itemsize)))
        budget_rows = max(1, _H5_STREAM_TARGET_BYTES // row_bytes)
        chunks = dataset.chunks if isinstance(dataset.chunks, tuple) else None
        if chunks and chunks[0]:
            rows_per_chunk = min(int(chunks[0]), budget_rows)
        else:
            rows_per_chunk = min(1024, total_rows, budget_rows)
    step = max(1, int(rows_per_chunk))
    for start in range(0, total_rows, step):
        end = min(total_rows, start + step)
        _update_h5_array_chunk(hasher, dataset[start:end])


def _resolve_h5_member_node(h5f: Any, member: str | None) -> tuple[str, Any]:
    if member is not None and member.strip():
        clean = "/" + member.strip().lstrip("/")
        if clean == "/":
            return ("/", h5f)
        rel = clean.lstrip("/")
        if rel not in h5f:
            raise ValueError(f"HDF5 member path {clean!r} not found in container.")
        return (clean, h5f[rel])

    top_keys = sorted(str(k) for k in h5f.keys())
    if len(top_keys) == 1:
        only_key = top_keys[0]
        return (f"/{only_key}", h5f[only_key])
    raise ValueError(
        "HDF5 member path is required when container does not have a single root member."
    )


def compute_h5_dataset_digest(
    path_or_dataset: Path | str | Any,
    *,
    member: str | None = None,
    chunk_rows: int | None = None,
) -> Digest:
    """Compute ``Digest('h5_dataset_v1:sha256', <hex>)`` over a raw HDF5 dataset."""
    h5py = importlib.import_module("h5py")
    hasher = hashlib.sha256()
    _update_string(hasher, "h5_dataset_v1")

    if isinstance(path_or_dataset, (str, Path)):
        with open_h5py_read_only(path_or_dataset, h5py_module=h5py) as h5f:
            _, node = _resolve_h5_member_node(h5f, member)
            if not isinstance(node, h5py.Dataset):
                raise ValueError(
                    f"Expected HDF5 Dataset for h5_dataset_v1:sha256 at member={member!r}, "
                    f"got {type(node).__name__}"
                )
            _hash_h5_dataset_node(hasher, node, chunk_rows=chunk_rows)
    else:
        if not isinstance(path_or_dataset, _H5ArrayLike):
            raise ValueError(
                f"Expected HDF5 Dataset-like object for h5_dataset_v1:sha256, got {type(path_or_dataset).__name__}"
            )
        _hash_h5_dataset_node(hasher, path_or_dataset, chunk_rows=chunk_rows)

    return Digest("h5_dataset_v1:sha256", hasher.hexdigest())


def compute_h5_fixed_digest(
    path: Path | str,
    *,
    member: str | None = None,
    chunk_rows: int | None = None,
) -> Digest:
    """
    Compute ``Digest('h5_fixed_v1:sha256', <hex>)`` over a PyTables HDF5 table/group.

    Hashes **all** child groups, axes, block items, block values, and PyTables
    table datasets in deterministic sorted path order so edits to string columns,
    float columns, index axes, or attributes never collide.
    """
    h5py = importlib.import_module("h5py")
    hasher = hashlib.sha256()
    _update_string(hasher, "h5_fixed_v1")

    with open_h5py_read_only(path, h5py_module=h5py) as h5f:
        resolved_member, root_node = _resolve_h5_member_node(h5f, member)
        _update_string(hasher, resolved_member)
        if isinstance(root_node, h5py.Dataset):
            _update_string(hasher, "dataset_root")
            _hash_h5_dataset_node(hasher, root_node, chunk_rows=chunk_rows)
            return Digest("h5_fixed_v1:sha256", hasher.hexdigest())

        if not isinstance(root_node, h5py.Group):
            raise ValueError(
                f"Unsupported HDF5 node at {resolved_member!r}: {type(root_node).__name__}"
            )

        def _visit_group(group: Any, rel_prefix: str) -> None:
            _update_string(hasher, f"group:{rel_prefix}")
            _hash_h5_attrs(hasher, group)
            child_names = sorted(str(k) for k in group.keys())
            _update_string(hasher, f"children:{len(child_names)}")
            for child_name in child_names:
                child_rel = f"{rel_prefix}/{child_name}" if rel_prefix else child_name
                child = group[child_name]
                if isinstance(child, h5py.Group):
                    _visit_group(child, child_rel)
                elif isinstance(child, h5py.Dataset):
                    _update_string(hasher, f"dataset:{child_rel}")
                    _hash_h5_dataset_node(hasher, child, chunk_rows=chunk_rows)
                else:
                    raise ValueError(
                        f"Unsupported HDF5 child {child_rel!r} of type {type(child).__name__}"
                    )

        _visit_group(root_node, "")

    return Digest("h5_fixed_v1:sha256", hasher.hexdigest())


def compute_output_set_digest(
    members: Mapping[str, str | Digest] | Iterable[tuple[str, str | Digest]],
) -> Digest:
    """Compute ``Digest('output_set_v1:sha256', <hex>)`` from sorted ``(member_key, str(digest))`` pairs."""
    items = members.items() if isinstance(members, Mapping) else members
    normalized_pairs: list[tuple[str, str]] = []
    seen_keys: set[str] = set()
    for raw_key, raw_digest in items:
        key_str = str(raw_key)
        if not key_str:
            raise ValueError("OutputSet member key must be non-empty.")
        if key_str in seen_keys:
            raise ValueError(f"Duplicate OutputSet member key: {key_str!r}")
        seen_keys.add(key_str)
        parsed = Digest.parse(raw_digest)
        normalized_pairs.append((key_str, str(parsed)))
    normalized_pairs.sort(key=lambda pair: pair[0])
    payload = {
        "version": 1,
        "members": [{"key": k, "digest": d} for k, d in normalized_pairs],
    }
    return Digest("output_set_v1:sha256", canonical_json_sha256(payload))


def _compute_stat_file_digest(path: Path) -> Digest:
    st = path.stat()
    meta_str = f"{st.st_size}_{st.st_mtime_ns}"
    return Digest("stat_file_v1:sha256", digest_bytes(meta_str.encode("utf-8")))


def _compute_stat_dir_digest(path: Path) -> Digest:
    if not path.is_dir():
        raise ValueError(f"Directory required for stat_dir_v1:sha256: {path}")
    hasher = hashlib.sha256()
    for member in sorted(path.rglob("*")):
        if not member.is_file():
            continue
        st = member.stat()
        rel_path = member.relative_to(path).as_posix()
        hasher.update(f"{rel_path}:{st.st_size}:{st.st_mtime_ns}|".encode("utf-8"))
    return Digest("stat_dir_v1:sha256", hasher.hexdigest())


# --- Unified Scheme Driver Registry ---

DigestDriverFn = Callable[
    [Path | None, str | None, Mapping[str, Any] | None],
    Digest,
]


def _driver_sha256_file(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    del member, options
    if path is None or not path.is_file():
        raise ValueError(f"sha256:file requires an existing regular file, got {path!r}")
    return Digest("sha256:file", file_sha256(path))


def _driver_ctd1_sha256(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    opts = options or {}
    source = opts.get("source", path)
    if source is None:
        raise ValueError("ctd1:sha256 requires a path or tabular source.")
    return compute_ctd1_digest(
        source,
        columns=opts.get("columns"),
        key=opts.get("key"),
        key_columns=opts.get("key_columns"),
        sqlite_table=opts.get("sqlite_table", member),
    )


def _driver_dir_manifest_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    del member
    if path is None:
        raise ValueError("dir_manifest_v1:sha256 requires a filesystem path.")
    opts = options or {}
    if opts.get("shapefile_bundle") is True or path.suffix.lower() == ".shp":
        manifest = build_shapefile_manifest_v1(path)
    else:
        manifest = build_manifest_v1(path)
    return Digest("dir_manifest_v1:sha256", manifest["tree_hash"])


def _driver_h5_fixed_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    if path is None:
        raise ValueError("h5_fixed_v1:sha256 requires an HDF5 file path.")
    opts = options or {}
    return compute_h5_fixed_digest(
        path,
        member=member,
        chunk_rows=opts.get("chunk_rows"),
    )


def _driver_h5_dataset_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    opts = options or {}
    dataset_obj = opts.get("dataset")
    if dataset_obj is not None:
        return compute_h5_dataset_digest(
            dataset_obj,
            member=member,
            chunk_rows=opts.get("chunk_rows"),
        )
    if path is None:
        raise ValueError("h5_dataset_v1:sha256 requires an HDF5 file path or dataset.")
    return compute_h5_dataset_digest(
        path,
        member=member,
        chunk_rows=opts.get("chunk_rows"),
    )


def _compute_logical_matrices_container_hex(
    path: Path,
    *,
    member: str | None,
    policy: ArrayCanonicalizationPolicy,
    chunk_rows: int,
    options: Mapping[str, Any],
) -> str:
    suffix = path.suffix.lower()
    if suffix == ".b2nd":
        blosc2 = importlib.import_module("blosc2")
        arr: Any = blosc2.open(str(path), mode="r")
        matrix_name = str(options.get("matrix_name") or member or path.stem)
        hasher = hashlib.sha256()
        if policy.lookup_policy == "single_int32_zone_number":
            import numpy as np

            shape = tuple(int(d) for d in arr.shape)
            zone_count = shape[0] if len(shape) >= 1 else 0
            zone_vec = np.arange(1, zone_count + 1, dtype="<i4")
            _stream_dataset_into_hasher(
                hasher,
                "lookup/zone_number",
                zone_vec,
                dtype_policy="preserve",
                canonicalize_float_bits=False,
                chunk_rows=chunk_rows,
            )
        _stream_dataset_into_hasher(
            hasher,
            matrix_name,
            arr[:],
            dtype_policy=policy.dtype_policy,
            canonicalize_float_bits=policy.canonicalize_float_bits,
            include_na_attribute=policy.include_na_attribute,
            chunk_rows=chunk_rows,
        )
        return hasher.hexdigest()
    if path.is_dir():
        return compute_zarr_logical_matrices_sha256(
            path, policy=policy, chunk_rows=chunk_rows
        )
    if suffix in {".nc", ".nc4", ".cdf", ".netcdf"}:
        xr = importlib.import_module("xarray")
        with xr.open_dataset(path) as ds:
            return _compute_zarr_logical_matrices_sha256_from_xarray(
                ds,
                policy=policy,
                chunk_rows=chunk_rows,
            )
    return compute_omx_logical_matrices_sha256(
        path, policy=policy, chunk_rows=chunk_rows
    )


def _driver_logical_matrices_canonical_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    if path is None:
        raise ValueError(
            "logical_matrices_canonical_v1:sha256 requires a container path."
        )
    opts = options or {}
    chunk_rows = int(opts.get("chunk_rows", _DEFAULT_CHUNK_ROWS))
    policy_opt = opts.get("policy")
    policy = (
        resolve_canonicalization_policy(policy_opt)
        if policy_opt is not None
        else PRESERVE_CANONICAL_POLICY
    )
    hex_val = _compute_logical_matrices_container_hex(
        path,
        member=member,
        policy=policy,
        chunk_rows=chunk_rows,
        options=opts,
    )
    return Digest("logical_matrices_canonical_v1:sha256", hex_val)


def _driver_logical_matrices_f32_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    if path is None:
        raise ValueError("logical_matrices_f32_v1:sha256 requires a container path.")
    opts = options or {}
    chunk_rows = int(opts.get("chunk_rows", _DEFAULT_CHUNK_ROWS))
    policy_opt = opts.get("policy")
    policy = (
        resolve_canonicalization_policy(policy_opt)
        if policy_opt is not None
        else FLOAT32_MATRICES_V1_POLICY
    )
    hex_val = _compute_logical_matrices_container_hex(
        path,
        member=member,
        policy=policy,
        chunk_rows=chunk_rows,
        options=opts,
    )
    return Digest("logical_matrices_f32_v1:sha256", hex_val)


def _parse_output_set_manifest_members(
    payload: Mapping[str, Any],
) -> list[tuple[str, str | Digest]]:
    raw_members = payload.get("members")
    if not isinstance(raw_members, Sequence) or isinstance(raw_members, (str, bytes)):
        raise ValueError("OutputSet manifest must contain a 'members' sequence.")
    pairs: list[tuple[str, str | Digest]] = []
    for entry in raw_members:
        if not isinstance(entry, Mapping):
            raise ValueError("OutputSet manifest member entry must be a mapping.")
        m_key = entry.get("key") or entry.get("relative_path")
        if not isinstance(m_key, str) or not m_key:
            raise ValueError("OutputSet manifest member entry is missing 'key'.")
        m_digest = entry.get("digest") or entry.get("content_digest")
        if isinstance(m_digest, str) and m_digest:
            pairs.append((m_key, Digest.parse(m_digest)))
            continue
        c_hash = entry.get("content_hash")
        if isinstance(c_hash, str) and c_hash:
            pairs.append((m_key, Digest("sha256:file", c_hash)))
            continue
        raise ValueError(f"OutputSet manifest member {m_key!r} is missing 'digest'.")
    return pairs


def _driver_output_set_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    del member
    opts = options or {}
    members = opts.get("members")
    if members is not None:
        return compute_output_set_digest(members)
    manifest_opt = opts.get("manifest")
    if isinstance(manifest_opt, Mapping):
        return compute_output_set_digest(
            _parse_output_set_manifest_members(manifest_opt)
        )
    member_paths = opts.get("member_paths")
    if path is not None and path.is_dir() and isinstance(member_paths, Mapping):
        computed_pairs: list[tuple[str, Digest]] = []
        for m_key, rel_path in member_paths.items():
            child_path = path / str(rel_path)
            if not child_path.is_file():
                raise FileNotFoundError(
                    f"Missing OutputSet member on disk: {child_path}"
                )
            computed_pairs.append(
                (str(m_key), Digest("sha256:file", file_sha256(child_path)))
            )
        return compute_output_set_digest(computed_pairs)
    if path is not None and path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, Mapping):
            return compute_output_set_digest(
                _parse_output_set_manifest_members(payload)
            )
    raise ValueError(
        "output_set_v1:sha256 requires options={'members': ...}, options={'manifest': ...}, directory + options={'member_paths': ...}, or an OutputSet manifest path."
    )


def _driver_stat_file_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    del member, options
    if path is None or not path.is_file():
        raise ValueError(f"stat_file_v1:sha256 requires a regular file, got {path!r}")
    return _compute_stat_file_digest(path)


def _driver_stat_dir_v1(
    path: Path | None,
    member: str | None,
    options: Mapping[str, Any] | None,
) -> Digest:
    del member, options
    if path is None or not path.is_dir():
        raise ValueError(f"stat_dir_v1:sha256 requires a directory, got {path!r}")
    return _compute_stat_dir_digest(path)


DIGEST_DRIVERS: Mapping[str, DigestDriverFn] = MappingProxyType(
    {
        "sha256:file": _driver_sha256_file,
        "ctd1:sha256": _driver_ctd1_sha256,
        "dir_manifest_v1:sha256": _driver_dir_manifest_v1,
        "h5_fixed_v1:sha256": _driver_h5_fixed_v1,
        "h5_dataset_v1:sha256": _driver_h5_dataset_v1,
        "logical_matrices_canonical_v1:sha256": _driver_logical_matrices_canonical_v1,
        "logical_matrices_f32_v1:sha256": _driver_logical_matrices_f32_v1,
        "output_set_v1:sha256": _driver_output_set_v1,
        "stat_file_v1:sha256": _driver_stat_file_v1,
        "stat_dir_v1:sha256": _driver_stat_dir_v1,
    }
)


def resolve_default_scheme(
    path: Path | None,
    *,
    driver: str | None = None,
    member: str | None = None,
    options: Mapping[str, Any] | None = None,
) -> str:
    """Resolve the canonical ``Digest.scheme`` for ``(path, driver, member, options)``."""
    opts = options or {}
    explicit_scheme = opts.get("scheme")
    if isinstance(explicit_scheme, str) and explicit_scheme:
        if explicit_scheme == "sha256:manifest-v1":
            return "dir_manifest_v1:sha256"
        return explicit_scheme

    if driver is not None and driver in DIGEST_DRIVERS:
        return driver
    if driver == "logical_matrices_canonical":
        return "logical_matrices_canonical_v1:sha256"
    if driver == "logical_matrices_f32":
        return "logical_matrices_f32_v1:sha256"
    if driver == "ctd1":
        return "ctd1:sha256"

    if (
        driver in {"output_set", "artifact_set"}
        or opts.get("members") is not None
        or opts.get("manifest") is not None
    ):
        return "output_set_v1:sha256"

    if opts.get("dataset") is not None:
        return "h5_dataset_v1:sha256"

    hashing_strategy = opts.get("hashing_strategy", "full")

    if driver == "shapefile" or (path is not None and path.suffix.lower() == ".shp"):
        return "dir_manifest_v1:sha256"

    if driver in {"zarr", "directory"} or (path is not None and path.is_dir()):
        if hashing_strategy == "fast" and not opts.get("directory_artifact"):
            return "stat_dir_v1:sha256"
        return "dir_manifest_v1:sha256"

    if driver in {"h5_table", "h5", "hdf5"} and member is not None:
        if path is not None and path.is_file():
            h5py = importlib.import_module("h5py")
            with open_h5py_read_only(path, h5py_module=h5py) as h5f:
                _, node = _resolve_h5_member_node(h5f, member)
                if isinstance(node, h5py.Group) or (
                    isinstance(node, h5py.Dataset)
                    and (
                        node.dtype.names is not None
                        or node.attrs.get("CLASS") in {b"TABLE", "TABLE"}
                    )
                ):
                    return "h5_fixed_v1:sha256"
                return "h5_dataset_v1:sha256"
        return "h5_fixed_v1:sha256" if driver == "h5_table" else "h5_dataset_v1:sha256"

    if driver == "h5_table":
        return "h5_fixed_v1:sha256"

    if hashing_strategy == "fast":
        return "stat_file_v1:sha256"

    return "sha256:file"


def compute_digest(
    path: Path | str | None,
    *,
    driver: str | None = None,
    member: str | None = None,
    options: Mapping[str, Any] | None = None,
) -> Digest:
    """Compute a ``Digest`` through the single unified ``DIGEST_DRIVERS`` registry."""
    resolved_path = Path(path) if path is not None else None
    scheme = resolve_default_scheme(
        resolved_path,
        driver=driver,
        member=member,
        options=options,
    )
    driver_fn = DIGEST_DRIVERS.get(scheme)
    if driver_fn is None:
        raise ValueError(f"Unsupported digest scheme: {scheme!r}")
    return driver_fn(resolved_path, member, options)


def verify_digest(
    path: Path | str | None,
    expected: Digest | str,
    *,
    driver: str | None = None,
    member: str | None = None,
    options: Mapping[str, Any] | None = None,
) -> bool:
    """Verify ``expected`` against ``path`` using the exact scheme registered in ``DIGEST_DRIVERS``."""
    parsed_expected = Digest.parse(expected)
    if parsed_expected.scheme not in DIGEST_DRIVERS:
        raise ValueError(
            f"Cannot verify unregistered digest scheme: {parsed_expected.scheme!r}"
        )
    merged_options = dict(options) if options is not None else {}
    merged_options["scheme"] = parsed_expected.scheme
    if driver == "shapefile":
        merged_options.setdefault("shapefile_bundle", True)
    actual = compute_digest(
        path,
        driver=driver,
        member=member,
        options=merged_options,
    )
    return actual == parsed_expected


__all__ = [
    "ARRAY_BINARY_PROTOCOL_VERSION",
    "DIGEST_DRIVERS",
    "FLOAT32_MATRICES_V1_POLICY",
    "PRESERVE_CANONICAL_POLICY",
    "ArrayCanonicalizationPolicy",
    "ConcurrentMutationError",
    "Digest",
    "LogicalProfileName",
    "build_manifest_v1",
    "build_shapefile_manifest_v1",
    "canonical_json_sha256",
    "canonical_json_str",
    "canonicalize_float16_array",
    "canonicalize_float32_array",
    "canonicalize_float64_array",
    "canonicalize_float_array",
    "compute_array_entry_sha256",
    "compute_ctd1_digest",
    "compute_digest",
    "compute_h5_dataset_digest",
    "compute_h5_fixed_digest",
    "compute_logical_matrices_sha256",
    "compute_omx_logical_matrices_sha256",
    "compute_output_set_digest",
    "compute_zarr_logical_matrices_sha256",
    "digest_bytes",
    "digest_file",
    "digest_state",
    "digest_table",
    "file_sha256",
    "logical_matrices_scheme_for_policy",
    "normalize_json",
    "normalize_logical_matrices_identity",
    "normalize_table_scalar",
    "open_h5py_read_only",
    "resolve_canonicalization_policy",
    "resolve_default_scheme",
    "verify_digest",
]
