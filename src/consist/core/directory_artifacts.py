"""Identity and verification helpers for immutable directory artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


_HASH_CHUNK_SIZE = 8 * 1024 * 1024
_MANIFEST_VERSION = 1
_SHAPEFILE_REQUIRED_SUFFIXES = frozenset({".shp", ".shx", ".dbf"})


def build_directory_manifest(root: Path) -> dict[str, Any]:
    """Build a deterministic manifest for a regular-file directory tree.

    The root itself is implicit. Every descendant directory, including empty
    ones, and every regular file is represented by a normalized relative path.
    Symlinks and non-regular filesystem entries are rejected.
    """
    root_path = Path(root).expanduser().absolute()
    _reject_symlink_components(root_path, label="root")
    if not root_path.is_dir():
        raise ValueError(f"directory artifact root must be a directory: {root_path}")

    entries: list[dict[str, Any]] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as scan:
            children = sorted(scan, key=lambda entry: entry.name)
        for child in children:
            child_path = Path(child.path)
            relative_path = _normalize_relative_path(child_path.relative_to(root_path))
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
                        "sha256": _compute_file_sha256(child_path),
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
    return {**identity, "tree_hash": _manifest_hash(identity)}


def build_shapefile_bundle_manifest(shapefile: Path) -> dict[str, Any]:
    """Build an immutable manifest for one Shapefile and all its sidecars.

    A Shapefile is a same-stem bundle rooted at the ``.shp`` entry. Every
    regular sibling whose filename starts with ``<stem>.`` belongs to the
    bundle, including optional projection, encoding, and spatial-index files.
    The required ``.shp``, ``.shx``, and ``.dbf`` members must each occur once.
    """
    shapefile_path = Path(shapefile).expanduser().absolute()
    root = shapefile_path.parent
    _reject_symlink_components(root, label="shapefile bundle root")
    entries = _shapefile_bundle_entries(root, shapefile_path.name)
    identity = {"version": _MANIFEST_VERSION, "entries": entries}
    return {**identity, "tree_hash": _manifest_hash(identity)}


def validate_shapefile_bundle_root(
    root: Path,
    shapefile_name: str,
    manifest: Mapping[str, Any],
    *,
    require_clean_root: bool = False,
) -> None:
    """Raise unless a bundle root exactly matches a persisted Shapefile manifest."""
    normalized_manifest = validate_directory_manifest(manifest)
    root_path = Path(root).expanduser().absolute()
    _reject_symlink_components(root_path, label="shapefile bundle root")
    expected_entries = _validate_shapefile_bundle_manifest(
        shapefile_name,
        normalized_manifest,
    )
    actual_entries = _shapefile_bundle_entries(root_path, shapefile_name)
    _validate_exact_file_entries(
        expected_entries, actual_entries, label="shapefile bundle"
    )
    if require_clean_root:
        expected_names = {entry["path"] for entry in expected_entries}
        with os.scandir(root_path) as scan:
            extra_names = sorted(
                entry.name for entry in scan if entry.name not in expected_names
            )
        if extra_names:
            raise ValueError(
                "shapefile bundle contains unexpected member(s): "
                + ", ".join(extra_names)
            )


def materialize_shapefile_bundle(
    source_root: Path,
    destination_root: Path,
    shapefile_name: str,
    manifest: Mapping[str, Any],
    *,
    preserve_existing: bool,
) -> bool:
    """Atomically copy a verified Shapefile bundle into a clean bundle root.

    Returns ``True`` when a staged bundle is published and ``False`` when an
    exact pre-existing bundle is retained.
    """
    normalized_manifest = validate_directory_manifest(manifest)
    source_root_path = Path(source_root)
    destination_root_path = Path(destination_root)
    _reject_symlink_components(source_root_path, label="shapefile bundle source")
    if validate_shapefile_bundle_destination(
        destination_root_path,
        shapefile_name,
        normalized_manifest,
        preserve_existing=preserve_existing,
    ):
        return False
    validate_shapefile_bundle_root(
        source_root_path,
        shapefile_name,
        normalized_manifest,
    )

    destination_root_path.parent.mkdir(parents=True, exist_ok=True)
    staging_parent = Path(
        tempfile.mkdtemp(
            dir=str(destination_root_path.parent),
            prefix=f".consist-shapefile-{destination_root_path.name}-",
        )
    )
    staging = staging_parent / "payload"
    try:
        staging.mkdir()
        for entry in normalized_manifest["entries"]:
            shutil.copy2(source_root_path / entry["path"], staging / entry["path"])
        validate_shapefile_bundle_root(
            staging,
            shapefile_name,
            normalized_manifest,
            require_clean_root=True,
        )
        validate_shapefile_bundle_root(
            source_root_path,
            shapefile_name,
            normalized_manifest,
        )
        if destination_root_path.is_symlink():
            raise ValueError(
                "shapefile bundle destination cannot be a symlink: "
                f"{destination_root_path}"
            )
        if destination_root_path.exists():
            raise FileExistsError(
                f"shapefile bundle destination already exists: {destination_root_path}"
            )
        os.rename(staging, destination_root_path)
        return True
    finally:
        shutil.rmtree(staging_parent, ignore_errors=True)


def validate_shapefile_bundle_destination(
    destination_root: Path,
    shapefile_name: str,
    manifest: Mapping[str, Any],
    *,
    preserve_existing: bool,
) -> bool:
    """Validate a requested Shapefile bundle root before source bytes are needed."""
    destination_root_path = Path(destination_root)
    _reject_symlink_components(
        destination_root_path.parent, label="shapefile bundle destination"
    )
    if destination_root_path.is_symlink():
        raise ValueError(
            f"shapefile bundle destination cannot be a symlink: {destination_root_path}"
        )
    if not destination_root_path.exists():
        return False
    if not destination_root_path.is_dir():
        raise ValueError(
            f"shapefile bundle destination type mismatch: {destination_root_path}"
        )
    if not preserve_existing:
        raise ValueError(
            "shapefile bundle hydration requires a clean destination when "
            "preserve_existing=False"
        )
    validate_shapefile_bundle_root(
        destination_root_path,
        shapefile_name,
        manifest,
        require_clean_root=True,
    )
    return True


def validate_directory_tree(root: Path, manifest: Mapping[str, Any]) -> None:
    """Raise when ``root`` is not an exact match for ``manifest``."""
    expected_entries = _validate_manifest(manifest)
    actual = build_directory_manifest(root)
    actual_entries = actual["entries"]
    expected_by_path = {entry["path"]: entry for entry in expected_entries}
    actual_by_path = {entry["path"]: entry for entry in actual_entries}

    missing = sorted(set(expected_by_path) - set(actual_by_path))
    if missing:
        raise ValueError(
            "directory artifact is missing expected member(s): " + ", ".join(missing)
        )
    extra = sorted(set(actual_by_path) - set(expected_by_path))
    if extra:
        raise ValueError(
            "directory artifact contains unexpected member(s): " + ", ".join(extra)
        )
    for path in sorted(expected_by_path):
        expected = expected_by_path[path]
        actual_entry = actual_by_path[path]
        if expected["kind"] != actual_entry["kind"]:
            raise ValueError(f"directory artifact member type mismatch: {path}")
        if expected["kind"] == "file":
            if expected["sha256"] != actual_entry["sha256"]:
                raise ValueError(f"directory artifact file hash mismatch: {path}")
            if expected["size"] != actual_entry["size"]:
                raise ValueError(f"directory artifact file size mismatch: {path}")

    expected_identity = {"version": _MANIFEST_VERSION, "entries": expected_entries}
    if manifest["tree_hash"] != _manifest_hash(expected_identity):
        raise ValueError("directory artifact manifest tree hash is invalid")


def validate_directory_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize a persisted directory-artifact manifest."""
    entries = _validate_manifest(manifest)
    identity = {"version": _MANIFEST_VERSION, "entries": entries}
    if manifest.get("tree_hash") != _manifest_hash(identity):
        raise ValueError("directory artifact manifest tree hash is invalid")
    return {**identity, "tree_hash": manifest["tree_hash"]}


def materialize_directory_tree(
    source: Path,
    destination: Path,
    manifest: Mapping[str, Any],
    *,
    preserve_existing: bool,
) -> bool:
    """Copy a verified directory tree into an exact destination atomically.

    Returns ``True`` when a new destination is published and ``False`` when an
    existing, already-verified destination is preserved.
    """
    normalized_manifest = validate_directory_manifest(manifest)
    source_path = Path(source)
    destination_path = Path(destination)
    _reject_symlink_components(source_path, label="source")
    if validate_directory_destination(
        destination_path,
        normalized_manifest,
        preserve_existing=preserve_existing,
    ):
        return False
    validate_directory_tree(source_path, normalized_manifest)

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(
            dir=str(destination_path.parent),
            prefix=f".consist-directory-{destination_path.name}-",
        )
    )
    staging = staging_root / "payload"
    try:
        staging.mkdir()
        for entry in normalized_manifest["entries"]:
            target = staging / entry["path"]
            if entry["kind"] == "directory":
                target.mkdir()
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path / entry["path"], target)
        validate_directory_tree(source_path, normalized_manifest)
        validate_directory_tree(staging, normalized_manifest)
        if destination_path.is_symlink():
            raise ValueError(
                f"directory artifact destination cannot be a symlink: {destination_path}"
            )
        if destination_path.exists():
            raise FileExistsError(
                f"directory artifact destination already exists: {destination_path}"
            )
        os.rename(staging, destination_path)
        return True
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def validate_directory_destination(
    destination: Path,
    manifest: Mapping[str, Any],
    *,
    preserve_existing: bool,
) -> bool:
    """Validate an exact directory destination before source bytes are needed.

    Returns ``True`` when an existing destination exactly matches the manifest
    and may be retained. Returns ``False`` when the destination does not yet
    exist. Unsafe or conflicting destinations raise ``ValueError``.
    """
    normalized_manifest = validate_directory_manifest(manifest)
    destination_path = Path(destination)
    _reject_symlink_components(destination_path.parent, label="destination")
    if destination_path.is_symlink():
        raise ValueError(
            f"directory artifact destination cannot be a symlink: {destination_path}"
        )
    if not destination_path.exists():
        return False
    if not destination_path.is_dir():
        raise ValueError(
            f"directory artifact destination type mismatch: {destination_path}"
        )
    if not preserve_existing:
        raise ValueError(
            "directory artifact hydration requires a clean destination when "
            "preserve_existing=False"
        )
    validate_directory_tree(destination_path, normalized_manifest)
    return True


def _shapefile_bundle_entries(root: Path, shapefile_name: str) -> list[dict[str, Any]]:
    """Return sorted manifest entries for all same-stem regular-file members."""
    if Path(shapefile_name).name != shapefile_name:
        raise ValueError(f"unsafe shapefile bundle entry name: {shapefile_name}")
    shapefile_path = root / shapefile_name
    if shapefile_path.suffix.lower() != ".shp":
        raise ValueError(f"shapefile bundle entry must end in .shp: {shapefile_name}")
    if not root.is_dir():
        raise ValueError(f"shapefile bundle root must be a directory: {root}")

    stem_prefix = f"{shapefile_path.stem}."
    members: list[dict[str, Any]] = []
    required_counts = {suffix: 0 for suffix in _SHAPEFILE_REQUIRED_SUFFIXES}
    with os.scandir(root) as scan:
        candidates = sorted(
            (entry for entry in scan if entry.name.startswith(stem_prefix)),
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
                "sha256": _compute_file_sha256(candidate_path),
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
    return members


def _validate_shapefile_bundle_manifest(
    shapefile_name: str,
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return manifest entries after enforcing the flat Shapefile bundle shape."""
    if Path(shapefile_name).name != shapefile_name:
        raise ValueError(f"unsafe shapefile bundle entry name: {shapefile_name}")
    shapefile_path = Path(shapefile_name)
    if shapefile_path.suffix.lower() != ".shp":
        raise ValueError(f"shapefile bundle entry must end in .shp: {shapefile_name}")
    entries = _validate_manifest(manifest)
    expected_prefix = f"{shapefile_path.stem}."
    required_counts = {suffix: 0 for suffix in _SHAPEFILE_REQUIRED_SUFFIXES}
    for entry in entries:
        path = entry["path"]
        if (
            entry["kind"] != "file"
            or "/" in path
            or not path.startswith(expected_prefix)
        ):
            raise ValueError("shapefile bundle manifest has invalid member layout")
        suffix = Path(path).suffix.lower()
        if suffix in required_counts:
            required_counts[suffix] += 1
    missing = sorted(suffix for suffix, count in required_counts.items() if count == 0)
    duplicates = sorted(
        suffix for suffix, count in required_counts.items() if count > 1
    )
    if missing or duplicates:
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if duplicates:
            detail.append("duplicate " + ", ".join(duplicates))
        raise ValueError("shapefile bundle manifest has " + "; ".join(detail))
    return entries


def _validate_exact_file_entries(
    expected_entries: Sequence[Mapping[str, Any]],
    actual_entries: Sequence[Mapping[str, Any]],
    *,
    label: str,
) -> None:
    """Raise unless two flat regular-file manifests contain the same bytes."""
    expected_by_path = {entry["path"]: entry for entry in expected_entries}
    actual_by_path = {entry["path"]: entry for entry in actual_entries}
    missing = sorted(set(expected_by_path) - set(actual_by_path))
    if missing:
        raise ValueError(
            f"{label} is missing expected member(s): " + ", ".join(missing)
        )
    extra = sorted(set(actual_by_path) - set(expected_by_path))
    if extra:
        raise ValueError(f"{label} contains unexpected member(s): " + ", ".join(extra))
    for path in sorted(expected_by_path):
        expected = expected_by_path[path]
        actual = actual_by_path[path]
        if expected["sha256"] != actual["sha256"]:
            raise ValueError(f"{label} file hash mismatch: {path}")
        if expected["size"] != actual["size"]:
            raise ValueError(f"{label} file size mismatch: {path}")


def _validate_manifest(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    if manifest.get("version") != _MANIFEST_VERSION:
        raise ValueError("unsupported directory artifact manifest version")
    tree_hash = manifest.get("tree_hash")
    if not isinstance(tree_hash, str) or not tree_hash:
        raise ValueError("directory artifact manifest is missing tree_hash")
    raw_entries = manifest.get("entries")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str):
        raise ValueError("directory artifact manifest entries must be a sequence")

    entries: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping):
            raise ValueError("directory artifact manifest entry must be a mapping")
        kind = raw_entry.get("kind")
        path = raw_entry.get("path")
        if kind not in {"directory", "file"} or not isinstance(path, str):
            raise ValueError("directory artifact manifest entry is invalid")
        normalized_path = _normalize_relative_path(path)
        if normalized_path in seen_paths:
            raise ValueError(
                f"directory artifact manifest has duplicate member: {normalized_path}"
            )
        seen_paths.add(normalized_path)
        entry: dict[str, Any] = {"kind": kind, "path": normalized_path}
        if kind == "file":
            sha256 = raw_entry.get("sha256")
            size = raw_entry.get("size")
            if not isinstance(sha256, str) or not sha256:
                raise ValueError(
                    f"directory artifact manifest file is missing sha256: {normalized_path}"
                )
            if not isinstance(size, int) or size < 0:
                raise ValueError(
                    f"directory artifact manifest file has invalid size: {normalized_path}"
                )
            entry.update(sha256=sha256, size=size)
        entries.append(entry)
    normalized_entries = sorted(
        entries, key=lambda entry: (entry["path"], entry["kind"])
    )
    entries_by_path = {entry["path"]: entry for entry in normalized_entries}
    for entry in normalized_entries:
        parent = Path(entry["path"]).parent
        while parent != Path("."):
            parent_entry = entries_by_path.get(parent.as_posix())
            if parent_entry is None:
                raise ValueError(
                    "directory artifact manifest is missing parent directory: "
                    f"{parent.as_posix()}"
                )
            if parent_entry["kind"] != "directory":
                raise ValueError(
                    "directory artifact manifest has conflicting parent/member paths: "
                    f"{parent.as_posix()} and {entry['path']}"
                )
            parent = parent.parent
    return normalized_entries


def _normalize_relative_path(path: str | Path) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        raise ValueError(f"unsafe directory artifact member path: {path}")
    normalized = candidate.as_posix()
    if not normalized or normalized == ".":
        raise ValueError(f"unsafe directory artifact member path: {path}")
    if any(part in {"", ".", ".."} for part in candidate.parts):
        raise ValueError(f"unsafe directory artifact member path: {path}")
    return normalized


def _reject_symlink_components(path: Path, *, label: str) -> None:
    candidate = Path(path)
    for component in (candidate, *candidate.parents):
        if component.is_symlink():
            raise ValueError(
                f"directory artifact {label} contains a symlink component: {component}"
            )


def _compute_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_hash(manifest: Mapping[str, Any]) -> str:
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
