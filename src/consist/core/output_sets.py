from __future__ import annotations

import fnmatch
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Protocol, Sequence, cast

from sqlmodel import SQLModel

from consist.models.artifact import Artifact
from consist.types import FacetLike, OutputSet


_OUTPUT_SET_MANIFEST_VERSION = 1
_FILE_HASH_CHUNK_SIZE = 8 * 1024 * 1024
_SAFE_KEY_RE = re.compile(r"[^A-Za-z0-9_]+")


class OutputSetTracker(Protocol):
    db: Any

    def log_artifact(
        self,
        path: Any,
        key: str | None = None,
        direction: str = "output",
        schema: type[SQLModel] | None = None,
        strict_schema: bool = True,
        driver: str | None = None,
        table_path: str | None = None,
        array_path: str | None = None,
        content_hash: str | None = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        parent_artifact_id: uuid.UUID | None = None,
        profile_file_schema: bool | Literal["if_changed"] | None = None,
        file_schema_sample_rows: int | None = None,
        facet: FacetLike | None = None,
        facet_schema_version: str | int | None = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> Artifact: ...


@dataclass(frozen=True, slots=True)
class OutputSetMember:
    """A discovered file that belongs to one output set.

    ``relative_path`` is the stable identity used in manifests and cache
    hydration. Absolute paths can move between run directories, but relative
    paths under the set root should remain stable.
    """

    path: Path
    relative_path: str
    size_bytes: int
    content_hash: str


def discover_output_set_members(output_set: OutputSet) -> list[OutputSetMember]:
    """Discover output-set member files in deterministic relative-path order.

    The output set's ``root`` must already exist and must be a directory. The
    ``include`` and ``exclude`` patterns are matched against paths relative to
    that root, not against absolute filesystem paths.
    """
    root = Path(output_set.root).expanduser()
    if not root.exists():
        raise ValueError(f"Output set root does not exist: {root}")
    if not root.is_dir():
        raise ValueError(f"Output set root must be a directory: {root}")

    include_patterns = _normalize_patterns(output_set.include)
    exclude_patterns = _normalize_patterns(output_set.exclude)
    candidates = root.rglob("*") if output_set.recursive else root.glob("*")
    members: list[OutputSetMember] = []
    for path in candidates:
        if path.is_symlink():
            raise ValueError(f"Output set member cannot be a symlink: {path}")
        if not path.is_file():
            continue
        relative_path = _normalize_output_set_relative_path(path.relative_to(root))
        if not _matches_any(relative_path, include_patterns):
            continue
        if exclude_patterns and _matches_any(relative_path, exclude_patterns):
            continue
        members.append(
            OutputSetMember(
                path=path,
                relative_path=relative_path,
                size_bytes=path.stat().st_size,
                content_hash=_compute_file_sha256(path),
            )
        )
    return sorted(members, key=lambda member: member.relative_path)


def resolve_output_set_expected_members(
    *,
    key: str,
    output_set: OutputSet,
    members: Sequence[OutputSetMember],
    config: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Validate optional completeness checks for an output-set declaration.

    ``expected_count`` and ``expected_members`` are optional. When present, they
    are treated as requirements for both cache misses and cache hits. This keeps
    a stale cached set from satisfying a newer declaration that expects more
    members.
    """
    if output_set.validate in {"hashes", "schema"}:
        raise ValueError(
            f"OutputSet validate={output_set.validate!r} is not implemented yet."
        )
    if output_set.validate not in {"exists", "manifest"}:
        raise ValueError(
            "OutputSet validate must be one of: 'exists', 'manifest', "
            "'hashes', 'schema'"
        )

    config_mapping = config or {}
    expected_count = _resolve_expected_count(output_set, config_mapping)
    expected_members = _resolve_expected_member_names(output_set, config_mapping)
    discovered_names = {member.relative_path for member in members}

    if expected_members is not None:
        missing = sorted(set(expected_members) - discovered_names)
        if missing:
            raise ValueError(
                f"Output set {key!r} missing expected members: {', '.join(missing)}"
            )
    if expected_count is not None and len(members) != expected_count:
        raise ValueError(
            f"Output set {key!r} expected {expected_count} members, "
            f"discovered {len(members)}."
        )
    if output_set.validate == "exists" and not members:
        raise ValueError(f"Output set {key!r} did not discover any members.")

    return {
        "count": expected_count,
        "members": list(expected_members) if expected_members is not None else None,
    }


def build_output_set_manifest(
    *,
    key: str,
    output_set: OutputSet,
    members: Sequence[OutputSetMember],
    config: Mapping[str, Any] | None,
    logged_members: Sequence[Artifact],
) -> dict[str, Any]:
    """Build the persisted JSON manifest for an output set.

    The manifest is the detailed source of truth for member paths, hashes, and
    member facets. Parent artifact metadata stays compact so callers can inspect
    the logical artifact without duplicating the full member list there.
    """
    expected = resolve_output_set_expected_members(
        key=key,
        output_set=output_set,
        members=members,
        config=config,
    )
    artifacts_by_relative_path = {
        str((artifact.meta or {}).get("output_set_relative_path")): artifact
        for artifact in logged_members
    }
    manifest_members: list[dict[str, Any]] = []
    for member in members:
        facets = _member_facets(
            key=key,
            output_set=output_set,
            member=member,
            config=config,
        )
        artifact = artifacts_by_relative_path.get(member.relative_path)
        manifest_members.append(
            {
                "key": _member_key(key, member.relative_path),
                "artifact_id": str(artifact.id) if artifact is not None else None,
                "relative_path": member.relative_path,
                "uri": (
                    artifact.container_uri if artifact is not None else str(member.path)
                ),
                "driver": (
                    artifact.driver
                    if artifact is not None
                    else _infer_driver(member.path)
                ),
                "size_bytes": member.size_bytes,
                "content_hash": member.content_hash,
                "facets": facets,
            }
        )

    manifest: dict[str, Any] = {
        "manifest_version": _OUTPUT_SET_MANIFEST_VERSION,
        "output_set_key": key,
        "kind": output_set.kind,
        "root_uri": str(output_set.root),
        "include": _normalize_patterns(output_set.include),
        "exclude": _normalize_patterns(output_set.exclude),
        "recursive": output_set.recursive,
        "schema": _schema_manifest(output_set.schema),
        "expected": {k: v for k, v in expected.items() if v is not None},
        "members": manifest_members,
        "totals": {
            "file_count": len(members),
            "byte_size": sum(member.size_bytes for member in members),
        },
    }
    if manifest["schema"] is None:
        manifest.pop("schema")
    if not manifest["expected"]:
        manifest.pop("expected")
    return manifest


def register_output_sets(
    *,
    tracker: OutputSetTracker,
    output_sets: Mapping[str, OutputSet] | None,
    config: Mapping[str, Any] | None,
    output_base_dir: Path,
    profile_file_schema: bool | Literal["if_changed"] | None = None,
) -> dict[str, Artifact]:
    """Log output-set parent artifacts, member artifacts, and manifests.

    Each declared set produces:

    - one parent artifact with ``driver="artifact_set"``;
    - one child artifact for each discovered member file, linked through
      ``parent_artifact_id``;
    - one JSON manifest artifact for detailed member metadata.

    The returned mapping contains only logical parent artifacts, matching the
    public ``RunResult.outputs`` behavior.
    """
    if not output_sets:
        return {}

    registered: dict[str, Artifact] = {}
    for key, output_set in output_sets.items():
        resolved_output_set = _resolve_output_set_root(output_set, output_base_dir)
        members = discover_output_set_members(resolved_output_set)
        resolve_output_set_expected_members(
            key=key,
            output_set=resolved_output_set,
            members=members,
            config=config,
        )
        parent_manifest = build_output_set_manifest(
            key=key,
            output_set=resolved_output_set,
            members=members,
            config=config,
            logged_members=[],
        )
        parent = tracker.log_artifact(
            Path(resolved_output_set.root),
            key=key,
            direction="output",
            schema=resolved_output_set.schema,
            strict_schema=False,
            driver="artifact_set",
            content_hash=_manifest_identity_hash(parent_manifest),
            force_hash_override=True,
            facet=resolved_output_set.facet,
            facet_index=True,
            **_parent_metadata(
                key=key,
                output_set=resolved_output_set,
                members=members,
                manifest_artifact_id=None,
            ),
        )

        logged_members: list[Artifact] = []
        member_profile_file_schema = (
            profile_file_schema
            if resolved_output_set.profile_file_schema is None
            else resolved_output_set.profile_file_schema
        )
        for member in members:
            facets = _member_facets(
                key=key,
                output_set=resolved_output_set,
                member=member,
                config=config,
            )
            logged_members.append(
                tracker.log_artifact(
                    member.path,
                    key=_member_key(key, member.relative_path),
                    direction="output",
                    schema=resolved_output_set.schema,
                    strict_schema=False,
                    parent_artifact_id=parent.id,
                    profile_file_schema=member_profile_file_schema,
                    file_schema_sample_rows=resolved_output_set.file_schema_sample_rows,
                    facet=facets,
                    facet_index=True,
                    output_set_key=key,
                    output_set_member=True,
                    output_set_relative_path=member.relative_path,
                )
            )

        manifest = build_output_set_manifest(
            key=key,
            output_set=resolved_output_set,
            members=members,
            config=config,
            logged_members=logged_members,
        )
        manifest_path = output_base_dir / f"{key}.output_set_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        manifest_artifact = tracker.log_artifact(
            manifest_path,
            key=f"{key}_manifest",
            direction="output",
            driver="json",
            output_set_key=key,
            output_set_manifest=True,
        )
        updates = _parent_metadata(
            key=key,
            output_set=resolved_output_set,
            members=members,
            manifest_artifact_id=str(manifest_artifact.id),
        )
        if tracker.db is not None:
            tracker.db.update_artifact_meta(parent, updates, raise_on_error=True)
        else:
            parent.meta.update(updates)
        registered[key] = parent
    return registered


def output_set_child_destinations(
    *,
    parent: Artifact,
    children: Sequence[Artifact],
    destination_root: Path,
) -> dict[str, Path]:
    """Expand a requested output-set destination root into child file paths.

    Cache hydration receives a logical destination for the parent set root. This
    helper converts that into concrete destinations for each child artifact by
    appending the stored member ``relative_path``.
    """
    if parent.driver != "artifact_set":
        return {}
    destinations: dict[str, Path] = {}
    for child in children:
        child_meta = child.meta or {}
        if child_meta.get("output_set_member") is not True:
            continue
        relative_path = child_meta.get("output_set_relative_path")
        if not isinstance(relative_path, str) or not relative_path:
            continue
        safe_relative_path = _normalize_output_set_relative_path(relative_path)
        destinations[child.key] = destination_root / Path(safe_relative_path)
    return destinations


def build_output_set_child_destinations(
    *,
    outputs: Sequence[Artifact],
    output_set_roots: Mapping[str, Path],
) -> dict[str, Path]:
    """Derive requested member destinations from cached output-set parents."""
    outputs_by_key = {artifact.key: artifact for artifact in outputs}
    destinations: dict[str, Path] = {}
    for set_key, root in output_set_roots.items():
        parent = outputs_by_key.get(set_key)
        if parent is None:
            continue
        children = [
            artifact for artifact in outputs if artifact.parent_artifact_id == parent.id
        ]
        destinations.update(
            output_set_child_destinations(
                parent=parent,
                children=children,
                destination_root=Path(root),
            )
        )
    return destinations


def validate_cached_output_sets(
    *,
    outputs: Mapping[str, Artifact],
    output_sets: Mapping[str, OutputSet] | None,
    config: Mapping[str, Any] | None,
) -> None:
    """Validate cached output-set child membership against current declarations.

    A cache hit reuses previously logged artifacts, so validation cannot inspect
    the newly declared output root on disk. Instead, it checks the cached child
    artifacts linked to the parent and compares their stored relative paths
    against the current ``expected_*`` options.
    """
    if not output_sets:
        return

    for key, output_set in output_sets.items():
        parent = outputs.get(key)
        if parent is None:
            continue
        members: list[OutputSetMember] = []
        for artifact in outputs.values():
            if artifact.parent_artifact_id != parent.id:
                continue
            artifact_meta = artifact.meta or {}
            if artifact_meta.get("output_set_member") is not True:
                continue
            relative_path = artifact_meta.get("output_set_relative_path")
            if not isinstance(relative_path, str) or not relative_path:
                continue
            safe_relative_path = _normalize_output_set_relative_path(relative_path)
            members.append(
                OutputSetMember(
                    path=Path(safe_relative_path),
                    relative_path=safe_relative_path,
                    size_bytes=0,
                    content_hash=artifact.hash or "",
                )
            )
        resolve_output_set_expected_members(
            key=key,
            output_set=output_set,
            members=members,
            config=config,
        )


def _normalize_patterns(patterns: str | Sequence[str] | None) -> list[str]:
    if patterns is None:
        return []
    if isinstance(patterns, str):
        return [patterns]
    return [str(pattern) for pattern in patterns]


def _matches_any(relative_path: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(relative_path, pattern) for pattern in patterns)


def _normalize_output_set_relative_path(path: str | Path) -> str:
    relative_path = Path(path)
    if relative_path.is_absolute():
        raise ValueError(f"unsafe output-set member path: {path}")
    normalized = relative_path.as_posix()
    if not normalized or normalized == ".":
        raise ValueError(f"unsafe output-set member path: {path}")
    if any(part in {"", ".", ".."} for part in Path(normalized).parts):
        raise ValueError(f"unsafe output-set member path: {path}")
    return normalized


def _compute_file_sha256(path: Path) -> str:
    sha256 = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(_FILE_HASH_CHUNK_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def _resolve_expected_count(
    output_set: OutputSet, config: Mapping[str, Any]
) -> int | None:
    expected_count = output_set.expected_count
    if expected_count is None:
        return None
    if callable(expected_count):
        count_resolver = cast(Callable[[Mapping[str, Any]], int], expected_count)
        return int(count_resolver(config))
    return int(expected_count)


def _resolve_expected_member_names(
    output_set: OutputSet, config: Mapping[str, Any]
) -> list[str] | None:
    expected_members = output_set.expected_members
    if expected_members is None:
        return None
    if callable(expected_members):
        member_resolver = cast(
            Callable[[Mapping[str, Any]], Sequence[str]], expected_members
        )
        expected_members = member_resolver(config)
    return sorted(Path(str(member)).as_posix() for member in expected_members)


def _member_facets(
    *,
    key: str,
    output_set: OutputSet,
    member: OutputSetMember,
    config: Mapping[str, Any] | None,
) -> dict[str, Any]:
    facets: dict[str, Any] = {"output_set_key": key}
    if output_set.member_facets is not None:
        facets.update(
            dict(
                output_set.member_facets(
                    member.path,
                    member.relative_path,
                    config or {},
                )
            )
        )
    return facets


def _schema_manifest(schema: type[SQLModel] | None) -> dict[str, str] | None:
    if schema is None:
        return None
    return {"name": schema.__name__}


def _parent_metadata(
    *,
    key: str,
    output_set: OutputSet,
    members: Sequence[OutputSetMember],
    manifest_artifact_id: str | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "artifact_set": True,
        "output_set_key": key,
        "output_set_kind": output_set.kind,
        "member_count": len(members),
        "total_size_bytes": sum(member.size_bytes for member in members),
    }
    if manifest_artifact_id is not None:
        metadata["manifest_artifact_id"] = manifest_artifact_id
    if output_set.schema is not None:
        metadata["schema_name"] = output_set.schema.__name__
    return metadata


def _manifest_identity_hash(manifest: Mapping[str, Any]) -> str:
    identity_manifest = json.loads(json.dumps(manifest, sort_keys=True))
    for member in identity_manifest.get("members", []):
        member.pop("artifact_id", None)
        member.pop("uri", None)
    payload = json.dumps(identity_manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _member_key(output_set_key: str, relative_path: str) -> str:
    suffix = _SAFE_KEY_RE.sub("_", relative_path).strip("_")
    return f"{output_set_key}__{suffix}"


def _infer_driver(path: Path) -> str:
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "other"


def _resolve_output_set_root(output_set: OutputSet, output_base_dir: Path) -> OutputSet:
    root = Path(output_set.root)
    if not root.is_absolute():
        root = output_base_dir / root
    return replace(output_set, root=root)
