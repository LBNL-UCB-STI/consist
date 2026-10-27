"""
Logical array canonicalization, streaming SHA-256 identity, and container inspection.

Separates the binary framing protocol from the canonicalization policy
(``ArrayCanonicalizationPolicy``) so general OpenMatrix (OMX), ActivitySim,
HDF5, and Zarr workflows get lossless native-dtype preservation by default
(``profile="preserve_canonical"``), while pipelines requiring cross-language
float32 parity with BEAM's Scala ``OmxSkimWriter`` can opt into
``profile="float32_matrices_v1"``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import importlib
import json
import logging
import os
from pathlib import Path
import re
import struct
from typing import Any, Literal, cast

import numpy as np

from consist.core.directory_artifacts import build_directory_manifest

logger = logging.getLogger(__name__)

LogicalProfileName = Literal["preserve_canonical", "float32_matrices_v1"]

ARTIFACT_PROVENANCE_SCHEMA_VERSION = "consist.artifact_provenance.v1"

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
    if val.startswith("sha256:logical_matrices:"):
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


def admission_directory_identity(path: str | Path) -> str:
    """
    Compute Consist's canonical directory admission identity.

    Parameters
    ----------
    path : str or pathlib.Path
        Directory path whose deterministic tree manifest should be hashed.

    Returns
    -------
    str
        Self-describing directory manifest identity in the form
        ``sha256:manifest-v1:<64 lowercase hex>``.

    Raises
    ------
    ValueError
        If ``path`` is not a readable directory or contains symlinks rejected by
        Consist's directory manifest builder.
    OSError
        If any member file in ``path`` cannot be read.
    """
    manifest = build_directory_manifest(Path(path))
    return f"sha256:manifest-v1:{manifest['tree_hash']}"


def _decode_attr_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, np.generic):
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


@dataclass(frozen=True, slots=True)
class ArtifactProvenanceSidecar:
    """
    Portable ``.provenance.json`` sidecar representation.

    Attributes
    ----------
    schema_version : str
        Sidecar schema version identifier (``"consist.artifact_provenance.v1"``).
    artifact_name : str
        Basename of the described artifact.
    artifact_format : str
        Detected artifact format (``"omx"``, ``"h5"``, ``"zarr"``, ``"directory"``,
        or ``"file"``).
    container_identity : str
        Container-level admission identity (``sha256:file:<hex>`` or
        ``sha256:manifest-v1:<hex>``).
    size_bytes : int
        Total byte size of the artifact.
    logical_identity : str or None
        Canonical logical array identity (``sha256:logical_matrices:<hex>``),
        if available.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"}
        Canonicalization profile associated with ``logical_identity``.
    shape : tuple[int, ...] or None
        Shape of the primary matrix/variable, if available.
    coord_keys : tuple[str, ...]
        Sorted lookup or coordinate names.
    variable_count : int
        Number of data matrices/variables in the container.
    variable_names : tuple[str, ...]
        Sorted names of data matrices/variables.
    producer_run_id : str or None, optional
        Run ID of the producer run, if supplied.
    producer_meta : Mapping[str, Any]
        Additional producer metadata recorded in the sidecar.
    root_attributes : Mapping[str, Any]
        Decoded root attributes from the container.
    facets : Mapping[str, Any]
        Extracted facet dictionary from ``key_facet_parser``, if supplied.
    """

    schema_version: str
    artifact_name: str
    artifact_format: str
    container_identity: str
    size_bytes: int
    logical_identity: str | None
    logical_profile: LogicalProfileName
    shape: tuple[int, ...] | None
    coord_keys: tuple[str, ...]
    variable_count: int
    variable_names: tuple[str, ...]
    producer_run_id: str | None = None
    producer_meta: Mapping[str, Any] = field(default_factory=dict)
    root_attributes: Mapping[str, Any] = field(default_factory=dict)
    facets: Mapping[str, Any] = field(default_factory=dict)


def inspect_matrix_container(
    path: str | Path,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    compute_logical_hash: bool = False,
    key_facet_parser: Callable[[str], Mapping[str, Any]] | None = None,
) -> MatrixContainerSummary:
    """
    Inspect an OMX, HDF5, or Zarr container without domain-specific coupling.

    Parameters
    ----------
    path : str or pathlib.Path
        Filesystem path to the file or directory container to inspect.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy used when locating HDF5 groups or computing a
        logical array hash.
    compute_logical_hash : bool, default False
        When ``True``, streams the container contents to compute
        ``logical_identity`` even if not already embedded in root attributes.
        For Zarr stores without an embedded ``logical_identity``, the logical
        hash is computed automatically when data variables are present.
    key_facet_parser : Callable[[str], Mapping[str, Any]] or None, optional
        Optional callback invoked on each discovered variable name to populate
        aggregated facet lists in ``MatrixContainerSummary.facets``.

    Returns
    -------
    MatrixContainerSummary
        Immutable structural and cryptographic summary of the container.

    Raises
    ------
    FileNotFoundError
        If ``path`` does not exist.
    """
    from consist.core.admission import admission_file_identity

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Artifact path does not exist: {p}")

    resolved_policy = resolve_canonicalization_policy(policy)
    root_attrs: dict[str, Any] = {}
    variable_names: list[str] = []
    coord_keys: list[str] = []
    shape: tuple[int, ...] | None = None
    logical_identity: str | None = None

    if p.is_dir():
        is_zarr = (
            p.suffix.lower() == ".zarr"
            or (p / ".zgroup").is_file()
            or (p / ".zarray").is_file()
            or (p / "zarr.json").is_file()
        )
        artifact_format: Literal["omx", "h5", "zarr", "directory", "file"] = (
            "zarr" if is_zarr else "directory"
        )
        container_identity = admission_directory_identity(p)
        size_bytes = sum(
            f.stat().st_size for f in p.rglob("*") if f.is_file() and not f.is_symlink()
        )
        if is_zarr:
            try:
                xr = importlib.import_module("xarray")
                with xr.open_zarr(p) as ds:
                    root_attrs = {
                        str(k): _decode_attr_value(v) for k, v in ds.attrs.items()
                    }
                    variable_names = sorted(str(k) for k in ds.data_vars.keys())
                    coord_keys = sorted(str(k) for k in ds.coords.keys())
                    if variable_names:
                        shape = tuple(int(d) for d in ds[variable_names[0]].shape)
            except Exception as exc:
                logger.debug("Unable to inspect Zarr store %s: %s", p, exc)
            if "logical_identity" in root_attrs:
                logical_identity = normalize_logical_matrices_identity(
                    str(root_attrs["logical_identity"])
                )
            if compute_logical_hash or (logical_identity is None and variable_names):
                try:
                    digest = compute_zarr_logical_matrices_sha256(
                        p, policy=resolved_policy
                    )
                    logical_identity = f"sha256:logical_matrices:{digest}"
                except Exception as exc:
                    logger.debug(
                        "Unable to compute Zarr logical hash for %s: %s", p, exc
                    )
    else:
        suffix = p.suffix.lower()
        if suffix == ".omx":
            artifact_format = "omx"
        elif suffix in {".h5", ".hdf5"}:
            artifact_format = "h5"
        else:
            artifact_format = "file"
        container_identity = admission_file_identity(p)
        size_bytes = p.stat().st_size

        if artifact_format in {"omx", "h5"}:
            try:
                with open_h5py_read_only(p) as h5f:
                    root_attrs = {
                        str(k): _decode_attr_value(v) for k, v in h5f.attrs.items()
                    }
                    data_group = _resolve_h5_group(h5f, resolved_policy.h5_data_group)
                    lookup_group = _resolve_h5_group(
                        h5f, resolved_policy.h5_lookup_group
                    )
                    if data_group is not None and hasattr(data_group, "keys"):
                        variable_names = sorted(
                            str(k)
                            for k in data_group.keys()
                            if hasattr(data_group[k], "shape")
                        )
                        if variable_names:
                            shape = tuple(
                                int(d) for d in data_group[variable_names[0]].shape
                            )
                    if lookup_group is not None and hasattr(lookup_group, "keys"):
                        coord_keys = sorted(
                            str(k)
                            for k in lookup_group.keys()
                            if hasattr(lookup_group[k], "shape")
                        )
            except Exception as exc:
                logger.debug("Unable to inspect HDF5/OMX file %s: %s", p, exc)

            if "logical_identity" in root_attrs:
                logical_identity = normalize_logical_matrices_identity(
                    str(root_attrs["logical_identity"])
                )
            if compute_logical_hash:
                try:
                    digest = compute_omx_logical_matrices_sha256(
                        p, policy=resolved_policy
                    )
                    logical_identity = f"sha256:logical_matrices:{digest}"
                except Exception as exc:
                    logger.debug(
                        "Unable to compute OMX logical hash for %s: %s", p, exc
                    )

    facets: dict[str, Any] = {}
    if key_facet_parser is not None and variable_names:
        parsed_facets: dict[str, set[Any]] = {}
        for vname in variable_names:
            item_facets = key_facet_parser(vname)
            for fkey, fval in item_facets.items():
                if fval is not None:
                    parsed_facets.setdefault(fkey, set()).add(fval)
        facets = {fkey: sorted(vals) for fkey, vals in sorted(parsed_facets.items())}

    return MatrixContainerSummary(
        artifact_path=str(p),
        artifact_format=artifact_format,
        container_identity=container_identity,
        size_bytes=size_bytes,
        logical_identity=logical_identity,
        logical_profile=resolved_policy.profile,
        shape=shape,
        coord_keys=tuple(coord_keys),
        variable_count=len(variable_names),
        variable_names=tuple(variable_names),
        root_attributes=root_attrs,
        facets=facets,
    )


def artifact_provenance_sidecar_path(artifact_path: str | Path) -> Path:
    """
    Return the co-located ``<artifact>.provenance.json`` path.

    Parameters
    ----------
    artifact_path : str or pathlib.Path
        Path to the target file or directory artifact.

    Returns
    -------
    pathlib.Path
        Sibling path named ``<artifact_path.name>.provenance.json``.
    """
    p = Path(artifact_path)
    return p.parent / f"{p.name}.provenance.json"


def write_artifact_provenance_sidecar(
    artifact_path: str | Path,
    *,
    policy: ArrayCanonicalizationPolicy | LogicalProfileName = "preserve_canonical",
    compute_logical_hash: bool = False,
    producer_run_id: str | None = None,
    producer_meta: Mapping[str, Any] | None = None,
    key_facet_parser: Callable[[str], Mapping[str, Any]] | None = None,
    extra_metadata: Mapping[str, Any] | None = None,
) -> tuple[Path, dict[str, Any]]:
    """
    Inspect ``artifact_path`` and atomically write ``<artifact_path>.provenance.json``.

    Parameters
    ----------
    artifact_path : str or pathlib.Path
        File or directory container to inspect.
    policy : ArrayCanonicalizationPolicy or {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization policy used when inspecting ``artifact_path`` and
        computing its logical hash.
    compute_logical_hash : bool, default False
        Whether to compute the streaming logical array digest if not already
        embedded in root attributes.
    producer_run_id : str or None, optional
        Optional producer run identifier recorded under ``producer.run_id``.
    producer_meta : Mapping[str, Any] or None, optional
        Optional additional metadata merged into the ``producer`` object.
    key_facet_parser : Callable[[str], Mapping[str, Any]] or None, optional
        Optional parser extracting facet key-values from each matrix/variable name.
    extra_metadata : Mapping[str, Any] or None, optional
        Optional extra fields merged into ``container_metadata``.

    Returns
    -------
    tuple[pathlib.Path, dict[str, Any]]
        Tuple of ``(sidecar_path, payload_dict)`` written to disk.

    Raises
    ------
    FileNotFoundError
        If ``artifact_path`` does not exist.
    OSError
        If the sidecar file cannot be written.
    """
    p = Path(artifact_path)
    summary = inspect_matrix_container(
        p,
        policy=policy,
        compute_logical_hash=compute_logical_hash,
        key_facet_parser=key_facet_parser,
    )
    metadata: dict[str, Any] = {
        "logical_identity": summary.logical_identity,
        "logical_profile": summary.logical_profile,
        "shape": list(summary.shape) if summary.shape is not None else None,
        "coord_keys": list(summary.coord_keys),
        "variable_count": summary.variable_count,
        "variable_names": list(summary.variable_names),
        "root_attributes": dict(summary.root_attributes),
        "facets": dict(summary.facets),
    }
    if extra_metadata:
        metadata.update(dict(extra_metadata))

    payload: dict[str, Any] = {
        "schema_version": ARTIFACT_PROVENANCE_SCHEMA_VERSION,
        "artifact_name": p.name,
        "artifact_format": summary.artifact_format,
        "identity": summary.container_identity,
        "logical_identity": summary.logical_identity,
        "logical_profile": summary.logical_profile,
        "size_bytes": summary.size_bytes,
        "producer": {
            "run_id": producer_run_id,
            **(dict(producer_meta) if producer_meta else {}),
        },
        "container_metadata": metadata,
    }

    sidecar_path = artifact_provenance_sidecar_path(p)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = sidecar_path.parent / f".tmp_{sidecar_path.name}.{os.getpid()}"
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(sidecar_path)
    return sidecar_path, payload


def read_artifact_provenance_sidecar(sidecar_path: str | Path) -> dict[str, Any]:
    """
    Read and validate a Consist ``.provenance.json`` sidecar.

    Parameters
    ----------
    sidecar_path : str or pathlib.Path
        Filesystem path to the ``.provenance.json`` file.

    Returns
    -------
    dict[str, Any]
        Validated sidecar payload dictionary with normalized
        ``logical_identity`` and ``logical_profile`` fields.

    Raises
    ------
    FileNotFoundError
        If ``sidecar_path`` does not exist or is not a regular file.
    ValueError
        If the JSON payload is not an object, does not match
        ``ARTIFACT_PROVENANCE_SCHEMA_VERSION``, or contains an invalid
        ``logical_identity`` digest.
    """
    p = Path(sidecar_path)
    if not p.is_file():
        raise FileNotFoundError(f"Artifact provenance sidecar does not exist: {p}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Artifact provenance sidecar must be a JSON object: {p}")
    schema_version = raw.get("schema_version")
    if schema_version != ARTIFACT_PROVENANCE_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported provenance sidecar schema_version {schema_version!r} in {p}; "
            f"expected {ARTIFACT_PROVENANCE_SCHEMA_VERSION!r}."
        )
    normalized = dict(raw)
    container_meta = raw.get("container_metadata")
    raw_logical = raw.get("logical_identity") or (
        container_meta.get("logical_identity")
        if isinstance(container_meta, Mapping)
        else None
    )
    normalized["logical_identity"] = (
        normalize_logical_matrices_identity(str(raw_logical))
        if raw_logical is not None
        else None
    )
    profile = raw.get("logical_profile") or (
        container_meta.get("logical_profile")
        if isinstance(container_meta, Mapping)
        else None
    )
    normalized["logical_profile"] = (
        cast(LogicalProfileName, profile)
        if profile in ("preserve_canonical", "float32_matrices_v1")
        else "preserve_canonical"
    )
    return normalized
