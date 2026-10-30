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

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import importlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Literal, cast

from consist.core.directory_artifacts import build_directory_manifest
from consist.digest import (
    ARRAY_BINARY_PROTOCOL_VERSION,
    FLOAT32_MATRICES_V1_POLICY,
    PRESERVE_CANONICAL_POLICY,
    ArrayCanonicalizationPolicy,
    LogicalProfileName,
    MatrixContainerSummary,
    _decode_attr_value,
    _resolve_h5_group,
    canonicalize_float16_array,
    canonicalize_float32_array,
    canonicalize_float64_array,
    canonicalize_float_array,
    compute_array_entry_sha256,
    compute_logical_matrices_sha256,
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
    normalize_logical_matrices_identity,
    open_h5py_read_only,
    resolve_canonicalization_policy,
)

logger = logging.getLogger(__name__)

ARTIFACT_PROVENANCE_SCHEMA_VERSION = "consist.artifact_provenance.v1"

__all__ = [
    "ARTIFACT_PROVENANCE_SCHEMA_VERSION",
    "ARRAY_BINARY_PROTOCOL_VERSION",
    "FLOAT32_MATRICES_V1_POLICY",
    "PRESERVE_CANONICAL_POLICY",
    "ArrayCanonicalizationPolicy",
    "ArtifactProvenanceSidecar",
    "LogicalProfileName",
    "MatrixContainerSummary",
    "admission_directory_identity",
    "artifact_provenance_sidecar_path",
    "canonicalize_float16_array",
    "canonicalize_float32_array",
    "canonicalize_float64_array",
    "canonicalize_float_array",
    "compute_array_entry_sha256",
    "compute_logical_matrices_sha256",
    "compute_omx_logical_matrices_sha256",
    "compute_zarr_logical_matrices_sha256",
    "inspect_matrix_container",
    "normalize_logical_matrices_identity",
    "open_h5py_read_only",
    "read_artifact_provenance_sidecar",
    "resolve_canonicalization_policy",
    "write_artifact_provenance_sidecar",
]


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
