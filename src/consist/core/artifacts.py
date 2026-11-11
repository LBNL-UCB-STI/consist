from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
import re
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Literal,
    Optional,
    Protocol,
    Type,
    TypeAlias,
    Union,
    runtime_checkable,
)
import uuid

from sqlmodel import SQLModel

from consist.artifact import Artifact
from consist.core.cache_output_logging import _demote_cache_hit
from consist.core.container_policy import (
    ChildRecoveryPolicy,
    ContainerRecoveryUnit,
    apply_container_policy_meta,
)
from consist.core.drivers import ARRAY_DRIVERS, TABLE_DRIVERS
from consist.core.logical_array_identity import open_h5py_read_only
from consist.core.validation import validate_artifact_key
from consist.digest import (
    Digest,
    Identity,
    build_manifest_v1,
    canonical_json_str,
)
from consist.engine import virtualize_path as _engine_virtualize_path
from consist.location import (
    DirTreeStat,
    FileStat,
    Freshness,
    Location,
    MemberOf,
    Observation,
    ObservationBasis,
    Witness,
)
from consist.meta import EMPTY_META, FrozenMeta, merge_artifact_output_meta
from consist.types import (
    ArtifactRef,
    FacetLike,
    H5ChildSelectionMode,
    H5ChildSpec,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ContainerPolicyMetaValue: TypeAlias = (
    ContainerRecoveryUnit | ChildRecoveryPolicy | Mapping[str, object]
)

_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")


@runtime_checkable
class _HasDtypeAndShape(Protocol):
    dtype: Any
    shape: Any


@runtime_checkable
class _HasKeys(Protocol):
    def keys(self) -> Any: ...
    def __getitem__(self, key: Any) -> Any: ...


@runtime_checkable
class _HasLedger(Protocol):
    @property
    def ledger(self) -> Any: ...


@runtime_checkable
class _HasMounts(Protocol):
    mounts: Any


@runtime_checkable
class _HasRunDir(Protocol):
    run_dir: Any


@runtime_checkable
class _HasArchiveMounts(Protocol):
    archive_mounts: Any


@dataclass(slots=True)
class _ContentHashState:
    """
    Mutable per-call state for lazy content-hash memoization.

    This keeps helper functions explicit and debugger-friendly without relying on
    `nonlocal` closure assignment.
    """

    effective_hash: Optional[str]
    computed_hash: Optional[str] = None
    attempted_compute: bool = False
    caller_supplied_override_applied: bool = False


def _normalize_64hex(raw: Optional[str]) -> str:
    if not isinstance(raw, str) or not raw.strip():
        return "0" * 64
    cleaned = raw.strip()
    if _HEX64_RE.fullmatch(cleaned):
        return cleaned.lower()
    tail = cleaned.rsplit(":", 1)[-1]
    if _HEX64_RE.fullmatch(tail):
        return tail.lower()
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()


def _matches_artifact_hash(artifact: Artifact, candidate: Optional[str]) -> bool:
    if not isinstance(candidate, str) or not candidate:
        return False
    if artifact.hash == candidate:
        return True
    cand_tail = candidate.rsplit(":", 1)[-1].lower()
    return artifact.identity.anchor.hex == cand_tail


def _capture_witness(
    path: Optional[Path],
    *,
    member: Optional[str],
) -> Optional[Witness]:
    if path is None:
        return None
    now_ns = time.time_ns()
    try:
        if not path.exists():
            return None
        if path.is_dir():
            return DirTreeStat.capture(path, observed_at_ns=now_ns)
        if member is not None:
            return MemberOf.capture(path, member=member, observed_at_ns=now_ns)
        return FileStat.capture(path, observed_at_ns=now_ns)
    except OSError:
        return None


def _record_manifest_if_writable(
    tracker: Any,
    anchor: Digest,
    manifest: Mapping[str, Any],
) -> None:
    if not isinstance(tracker, _HasLedger):
        return
    ledger = tracker.ledger
    if ledger is None or ledger.read_only:
        return
    try:
        raw_entries = manifest.get("entries", [])
        entries = list(raw_entries) if isinstance(raw_entries, Sequence) else []
        body_payload = {
            "version": manifest.get("version", 1),
            "entries": entries,
        }
        body_bytes = canonical_json_str(body_payload).encode("utf-8")
        total_bytes = sum(
            int(item.get("size", 0)) for item in entries if isinstance(item, Mapping)
        )
        ledger.record_content_manifest(
            anchor=str(anchor),
            format="manifest-v1",
            entry_count=len(entries),
            total_bytes=total_bytes,
            body=body_bytes,
        )
    except Exception as exc:
        logging.debug(
            "[Consist] Failed to persist content manifest for %s: %s",
            anchor,
            exc,
        )


def _infer_driver_from_path(path: Path) -> str:
    driver = ARRAY_DRIVERS.resolve_driver(path)
    if driver:
        return driver
    driver = TABLE_DRIVERS.resolve_driver(path)
    if driver:
        return driver
    try:
        if path.is_dir():
            return "directory"
    except OSError:
        pass
    suffixes = [suffix.lower() for suffix in path.suffixes]
    suffix = suffixes[-1].lstrip(".") if suffixes else ""
    if suffix == "shp":
        return "shapefile"
    if suffix == "gpkg":
        return "geopackage"
    if suffix == "geojson":
        return "geojson"
    if suffix in {"h5", "hdf5"}:
        return "h5"
    return suffix or "unknown"


def _resolve_h5_dataset(obj: Any) -> Optional[Any]:
    if isinstance(obj, _HasDtypeAndShape):
        return obj
    if isinstance(obj, _HasKeys):
        preferred = ("table", "block0_values", "values")
        for key in preferred:
            try:
                candidate = obj[key]
            except Exception:
                continue
            if isinstance(candidate, _HasDtypeAndShape):
                return candidate

        for key in obj.keys():
            try:
                candidate = obj[key]
            except Exception:
                continue
            if isinstance(candidate, _HasDtypeAndShape):
                return candidate
    return None


def _hash_h5_dataset(
    dataset: Any,
    *,
    chunk_rows: Optional[int],
) -> Optional[str]:
    try:
        import numpy as np
    except ImportError:
        return None

    resolved = _resolve_h5_dataset(dataset)
    if resolved is None:
        return None

    hasher = hashlib.sha256()
    hasher.update(str(resolved.dtype).encode("utf-8"))
    hasher.update(str(resolved.shape).encode("utf-8"))

    shape = resolved.shape
    if shape == ():
        data = resolved[()]
        hasher.update(np.asarray(data).tobytes(order="C"))
        return hasher.hexdigest()

    total_rows = int(shape[0]) if len(shape) else 0
    if total_rows == 0:
        return hasher.hexdigest()

    rows_per_chunk = chunk_rows
    if rows_per_chunk is None:
        if resolved.chunks and resolved.chunks[0]:
            rows_per_chunk = int(resolved.chunks[0])
        else:
            rows_per_chunk = min(1024, total_rows)

    rows_per_chunk = max(1, rows_per_chunk)
    for start in range(0, total_rows, rows_per_chunk):
        end = min(total_rows, start + rows_per_chunk)
        chunk = resolved[start:end]
        hasher.update(np.asarray(chunk).tobytes(order="C"))

    return hasher.hexdigest()


def _normalize_h5_child_path(path: str) -> str:
    stripped = path.strip()
    if not stripped:
        return "/"
    return f"/{stripped.lstrip('/')}"


def _resolve_h5_child_spec(
    child_specs: Optional[Mapping[str, H5ChildSpec]],
    table_path: str,
) -> Optional[H5ChildSpec]:
    if not child_specs:
        return None
    normalized_path = _normalize_h5_child_path(table_path)
    return (
        child_specs.get(normalized_path)
        or child_specs.get(normalized_path.lstrip("/"))
        or child_specs.get(table_path)
    )


def _container_policy_updates(
    meta: Mapping[str, object],
) -> dict[str, ContainerPolicyMetaValue]:
    updates: dict[str, ContainerPolicyMetaValue] = {}
    recovery_unit = meta.get("container_recovery_unit")
    if recovery_unit == "parent_file":
        updates["container_recovery_unit"] = "parent_file"
    elif recovery_unit == "derived_children":
        updates["container_recovery_unit"] = "derived_children"
    child_policy = meta.get("child_recovery_policy")
    if child_policy == "descriptive_only":
        updates["child_recovery_policy"] = "descriptive_only"
    elif child_policy == "independent":
        updates["child_recovery_policy"] = "independent"
    representation_policy = meta.get("representation_policy")
    if isinstance(representation_policy, Mapping):
        updates["representation_policy"] = {
            str(key): value for key, value in representation_policy.items()
        }
    return updates


def _list_h5_dataset_names(
    path_obj: Path,
    *,
    filter_fn: Callable[[str], bool],
) -> list[str]:
    try:
        import h5py
    except ImportError:
        logging.warning("[Consist] h5py not installed. Cannot inspect HDF5 tables.")
        return []

    dataset_names: list[str] = []
    try:
        with open_h5py_read_only(str(path_obj), h5py_module=h5py) as handle:

            def visit_datasets(name: str, obj: Any) -> None:
                if isinstance(obj, h5py.Dataset) and filter_fn(name):
                    dataset_names.append(_normalize_h5_child_path(name))

            handle.visititems(visit_datasets)
    except Exception as exc:
        logging.warning("[Consist] Failed to inspect HDF5 tables: %s", exc)
        return []

    return dataset_names


class ArtifactManager:
    """
    Manage the lifecycle, virtualization, and identity hashing of Consist Artifacts.

    The ArtifactManager is responsible for transforming raw filesystem paths into
    virtualized, portable URIs and computing deterministic content hashes. It
    ensures that artifacts are correctly linked to runs and provides specialized
    handling for complex containers like HDF5 and Zarr.
    """

    def __init__(self, tracker: "Tracker"):
        self.tracker = tracker

    def _ledger_ref(self) -> Any:
        if isinstance(self.tracker, _HasLedger) and self.tracker.ledger is not None:
            return self.tracker.ledger.ref
        return None

    def _site_name(self) -> str:
        return "local"

    def _effective_mounts(self) -> dict[str, Any]:
        mounts: dict[str, Any] = {}
        if isinstance(self.tracker, _HasRunDir) and self.tracker.run_dir:
            mounts["workspace"] = self.tracker.run_dir
        if isinstance(self.tracker, _HasArchiveMounts) and isinstance(
            self.tracker.archive_mounts, Mapping
        ):
            mounts.update(self.tracker.archive_mounts)
        if isinstance(self.tracker, _HasMounts) and isinstance(
            self.tracker.mounts, Mapping
        ):
            mounts.update(self.tracker.mounts)
        return mounts

    def _build_identity_and_basis(
        self,
        *,
        driver: str,
        member: Optional[str],
        path_for_stat: Optional[Path],
        content_hash: Optional[str],
        validate_content_hash: bool,
        meta: Mapping[str, Any],
        compute_default_hash: Callable[[], Optional[str]],
        validate_disk_hash: Callable[[], None],
    ) -> tuple[Identity, ObservationBasis]:
        hashing_strategy_raw = self.tracker.identity.hashing_strategy
        hashing_strategy = (
            hashing_strategy_raw if isinstance(hashing_strategy_raw, str) else "full"
        )

        if meta.get("directory_artifact") is True and isinstance(
            meta.get("directory_manifest"), Mapping
        ):
            manifest = meta["directory_manifest"]
            tree_hex = _normalize_64hex(
                str(manifest.get("tree_hash") or content_hash or "")
            )
            anchor = Digest("sha256:manifest-v1", tree_hex)
            _record_manifest_if_writable(self.tracker, anchor, manifest)
            return Identity(anchor=anchor), "computed"

        if meta.get("file_bundle_artifact") is True and isinstance(
            meta.get("file_bundle_manifest"), Mapping
        ):
            manifest = meta["file_bundle_manifest"]
            tree_hex = _normalize_64hex(
                str(manifest.get("tree_hash") or content_hash or "")
            )
            anchor = Digest("sha256:manifest-v1", tree_hex)
            _record_manifest_if_writable(self.tracker, anchor, manifest)
            return Identity(anchor=anchor), "computed"

        if driver == "artifact_set" or meta.get("artifact_set") is True:
            raw_set_hash = content_hash or compute_default_hash()
            anchor = Digest("core_state/v1:sha256", _normalize_64hex(raw_set_hash))
            return Identity(anchor=anchor), "computed"

        if (
            driver == "h5_table"
            and isinstance(content_hash, str)
            and _HEX64_RE.fullmatch(content_hash)
        ):
            anchor = Digest("ctd1:sha256", content_hash.lower())
            return Identity(anchor=anchor), "computed"

        if isinstance(content_hash, str) and ":" in content_hash:
            try:
                parsed = Digest.parse(content_hash)
                if member is not None and parsed.scheme in {
                    "sha256:file",
                    "blake3:file",
                }:
                    parsed = Digest("legacy-v1:unverified", parsed.hex)
                if validate_content_hash:
                    validate_disk_hash()
                return (
                    Identity(anchor=parsed),
                    "computed" if validate_content_hash else "declared",
                )
            except ValueError:
                pass

        if content_hash is not None:
            if validate_content_hash:
                validate_disk_hash()
                if (
                    member is None
                    and hashing_strategy != "fast"
                    and path_for_stat is not None
                    and path_for_stat.is_file()
                    and _HEX64_RE.fullmatch(content_hash)
                ):
                    return (
                        Identity(anchor=Digest("sha256:file", content_hash.lower())),
                        "computed",
                    )
                return (
                    Identity(
                        anchor=Digest(
                            "legacy-v1:unverified", _normalize_64hex(content_hash)
                        )
                    ),
                    "computed",
                )
            return (
                Identity(
                    anchor=Digest(
                        "legacy-v1:unverified", _normalize_64hex(content_hash)
                    )
                ),
                "declared",
            )

        if member is None and hashing_strategy != "fast" and path_for_stat is not None:
            try:
                is_reg_file = path_for_stat.is_file()
                is_dir_path = path_for_stat.is_dir()
            except OSError:
                is_reg_file = False
                is_dir_path = False

            if is_reg_file:
                computed = compute_default_hash()
                if isinstance(computed, str) and _HEX64_RE.fullmatch(computed):
                    return (
                        Identity(anchor=Digest("sha256:file", computed.lower())),
                        "computed",
                    )
                return (
                    Identity(
                        anchor=Digest(
                            "legacy-v1:unverified", _normalize_64hex(computed)
                        )
                    ),
                    "computed",
                )

            if is_dir_path:
                try:
                    dir_manifest = build_manifest_v1(path_for_stat)
                    tree_hex = str(dir_manifest["tree_hash"])
                    anchor = Digest("sha256:manifest-v1", tree_hex)
                    _record_manifest_if_writable(self.tracker, anchor, dir_manifest)
                    return Identity(anchor=anchor), "computed"
                except (OSError, ValueError):
                    computed = compute_default_hash()
                    return (
                        Identity(
                            anchor=Digest(
                                "legacy-v1:unverified", _normalize_64hex(computed)
                            )
                        ),
                        "computed",
                    )

        computed = compute_default_hash()
        exists_on_disk = False
        if path_for_stat is not None:
            try:
                exists_on_disk = path_for_stat.exists()
            except OSError:
                exists_on_disk = False
        return (
            Identity(anchor=Digest("legacy-v1:unverified", _normalize_64hex(computed))),
            "computed" if exists_on_disk else "declared",
        )

    def create_artifact(
        self,
        path: ArtifactRef,
        run_id: Optional[str] = None,
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        driver: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        parent_artifact_id: Optional[uuid.UUID] = None,
        known_latest_artifact_at_uri: Optional[Artifact] = None,
        latest_artifact_lookup_done: bool = False,
        strict_schema: bool = False,
        **meta: Any,
    ) -> Artifact:
        """
        Instantiate or resolve a tracked Artifact for a given filesystem path.

        This method performs path virtualization against configured mounts and
        generates a unique identity for the data. If an identical artifact (by URI
        and hash) exists in the provenance database, it may be reused to maintain
        graph integrity.

        Parameters
        ----------
        path : ArtifactRef
            The filesystem path (string or Path object) or an existing Artifact
            to be registered.
        run_id : Optional[str], optional
            The unique identifier of the run producing this artifact. If None,
            the artifact is treated as an exogenous input.
        key : Optional[str], optional
            A semantic identifier for the artifact (e.g., 'household_survey').
            Required if `path` is a string.
        direction : str, default "output"
            The relationship of the artifact to the run: "input" or "output".
        schema : Optional[Type[SQLModel]], optional
            A SQLModel class used to tag expected structure and enable hybrid views.
        strict_schema : bool, default False
            Whether the schema tag should also mark the artifact for strict
            schema validation semantics.
        driver : Optional[str], optional
            The format handler (e.g., 'parquet', 'csv', 'zarr'). If omitted,
            the driver is inferred from the file extension.
        table_path : Optional[str], optional
            Internal path for tabular datasets within a container (e.g., HDF5).
        array_path : Optional[str], optional
            Internal path for multi-dimensional arrays within a container.
        content_hash : Optional[str], optional
            A precomputed SHA256 hash. If provided, Consist skips the
            expensive file hashing step unless validation is requested.
            When hashing is needed, it is computed lazily and reused within
            this single call.
        force_hash_override : bool, default False
            If True, permits overwriting an existing artifact's hash with a
            new value.
        validate_content_hash : bool, default False
            If True, re-computes the hash from disk to ensure it matches
            the provided `content_hash`.
        reuse_if_unchanged : bool, default False
            If True, attempts to link to a previously recorded artifact record
            if the content hash is identical.
        reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
            The breadth of the search for reusable artifacts.
        parent_artifact_id : Optional[uuid.UUID], default None
            Canonical parent artifact relation for container child artifacts.
        **meta : Any
            Flexible metadata payload stored within the artifact record.

        Returns
        -------
        Artifact
            A hydrated Artifact instance ready for persistence or chaining.
        """
        meta.pop("table_path", None)
        meta.pop("array_path", None)
        if "recovery_roots" in meta:
            meta["recovery_roots"] = self.tracker.fs.normalize_recovery_roots(
                meta["recovery_roots"]
            )
        meta.pop("hash_semantics", None)
        meta.pop("content_identity", None)
        meta.pop("observation_identity", None)

        resolved_abs_path: Optional[str] = None
        mount_scheme: Optional[str] = None
        mount_root: Optional[str] = None
        hash_state = _ContentHashState(effective_hash=content_hash)

        is_output_reuse = direction == "output" and reuse_if_unchanged
        if is_output_reuse:
            logging.warning(
                "[Consist] reuse_if_unchanged on outputs is deprecated; "
                "a new artifact row is always created now and identical bytes share "
                "verified content identity."
            )
            if reuse_scope == "any_uri":
                logging.warning(
                    "[Consist] reuse_scope='any_uri' is ignored for outputs; "
                    "content identity equality governs deduplication instead."
                )

        def _compute_content_hash(*, raise_on_error: bool) -> Optional[str]:
            if hash_state.computed_hash is not None or hash_state.attempted_compute:
                return hash_state.computed_hash
            if not resolved_abs_path:
                if raise_on_error:
                    raise ValueError(
                        "validate_content_hash=True requires a resolvable path."
                    )
                return None
            hash_state.attempted_compute = True
            try:
                hash_state.computed_hash = self.tracker.identity.compute_file_checksum(
                    resolved_abs_path
                )
            except Exception as e:
                if raise_on_error:
                    raise ValueError(
                        f"Failed to validate content_hash for {resolved_abs_path}: {e}"
                    ) from e
                logging.warning(
                    "[Consist Warning] Failed to compute hash for %s: %s",
                    resolved_abs_path,
                    e,
                )
            return hash_state.computed_hash

        def _ensure_content_hash(*, raise_on_error: bool = False) -> Optional[str]:
            if hash_state.effective_hash is None:
                hash_state.effective_hash = _compute_content_hash(
                    raise_on_error=raise_on_error
                )
            return hash_state.effective_hash

        def _validate_content_hash() -> None:
            if not validate_content_hash or hash_state.effective_hash is None:
                return
            computed = _compute_content_hash(raise_on_error=True)
            expected_tail = hash_state.effective_hash.rsplit(":", 1)[-1]
            if computed != hash_state.effective_hash and computed != expected_tail:
                raise ValueError(
                    f"content_hash does not match on-disk data for {resolved_abs_path}."
                )

        def _clone_and_update_existing(
            existing: Artifact,
            *,
            key_override: Optional[str],
            driver_override: Optional[str],
            table_path_override: Optional[str],
            array_path_override: Optional[str],
            parent_id_override: Optional[uuid.UUID],
            meta_updates: dict[str, Any],
        ) -> Artifact:
            eff_key = key_override if key_override is not None else existing.key
            validate_artifact_key(eff_key)
            eff_driver = (
                driver_override if driver_override is not None else existing.driver
            )
            eff_member = (
                table_path_override
                if table_path_override is not None
                else (
                    array_path_override
                    if array_path_override is not None
                    else existing.member
                )
            )
            eff_parent_id = (
                parent_id_override
                if parent_id_override is not None
                else existing.parent_artifact_id
            )
            eff_id = existing.id
            eff_run_id = existing.run_id
            eff_created_at = existing.created_at
            eff_identity = existing.identity
            eff_hints = list(existing.hints)

            primary_loc = existing.primary_location
            base_uri = primary_loc.uri if primary_loc is not None else ""

            if hash_state.effective_hash is not None:
                _validate_content_hash()
                if not _matches_artifact_hash(existing, hash_state.effective_hash):
                    if not force_hash_override:
                        logging.warning(
                            "[Consist Warning] Ignoring content_hash override for artifact key=%s uri=%s "
                            "(existing hash differs). Use force_hash_override=True to override.",
                            eff_key,
                            base_uri,
                        )
                    else:
                        path_obj = (
                            Path(resolved_abs_path) if resolved_abs_path else None
                        )
                        eff_identity, new_basis = self._build_identity_and_basis(
                            driver=eff_driver,
                            member=eff_member,
                            path_for_stat=path_obj,
                            content_hash=hash_state.effective_hash,
                            validate_content_hash=validate_content_hash,
                            meta=meta_updates,
                            compute_default_hash=_ensure_content_hash,
                            validate_disk_hash=_validate_content_hash,
                        )
                        eff_id = uuid.uuid4()
                        eff_run_id = None
                        eff_created_at = datetime.now(timezone.utc)
                        if base_uri:
                            obs = Observation(
                                location=Location(uri=base_uri, member=eff_member),
                                identity=eff_identity,
                                access_driver=eff_driver,
                                witness=_capture_witness(path_obj, member=eff_member),
                                basis=new_basis,
                                site=self._site_name(),
                            )
                            eff_hints = [obs]

            if (
                eff_member != existing.member or eff_driver != existing.driver
            ) and base_uri:
                path_obj = Path(resolved_abs_path) if resolved_abs_path else None
                obs = Observation(
                    location=Location(uri=base_uri, member=eff_member),
                    identity=eff_identity,
                    access_driver=eff_driver,
                    witness=_capture_witness(path_obj, member=eff_member),
                    basis=eff_hints[0].basis if eff_hints else "declared",
                    site=self._site_name(),
                )
                eff_hints = [
                    obs,
                    *(h for h in eff_hints if h.place_key != obs.place_key),
                ]

            merged_meta = dict(existing.meta)
            if meta_updates:
                merged_meta = merge_artifact_output_meta(
                    merged_meta, meta_updates, merge=True
                )
            if schema is not None:
                merged_meta["schema_name"] = schema.__name__
                if strict_schema:
                    merged_meta["has_strict_schema"] = True
                else:
                    merged_meta.pop("has_strict_schema", None)
            if mount_scheme and "mount_scheme" not in merged_meta:
                merged_meta["mount_scheme"] = mount_scheme
            if mount_root and "mount_root" not in merged_meta:
                merged_meta["mount_root"] = mount_root

            eff_hints = [
                h
                if h.identity.anchor == eff_identity.anchor
                else replace(
                    h,
                    identity=eff_identity,
                    basis="declared" if not eff_identity.verifiable else h.basis,
                )
                for h in eff_hints
            ]
            origin_ref = self._ledger_ref() or existing.origin
            rebuilt = Artifact(
                id=eff_id,
                key=eff_key,
                identity=eff_identity,
                driver=eff_driver,
                run_id=eff_run_id,
                parent_artifact_id=eff_parent_id,
                schema_id=existing.schema_id,
                created_at=eff_created_at,
                meta=FrozenMeta(merged_meta) if merged_meta else EMPTY_META,
                origin=origin_ref,
                hints=tuple(eff_hints),
            )
            return rebuilt.bind_tracker(self.tracker)

        if isinstance(path, Artifact):
            source_artifact = path
            located = source_artifact.locate(
                mounts=self._effective_mounts(),
                verify_on_stale=False,
            )
            if located is not None:
                resolved_abs_path = str(located)
            elif source_artifact.primary_location is not None:
                try:
                    resolved_abs_path = self.tracker.resolve_uri(
                        source_artifact.primary_location.uri
                    )
                except Exception:
                    resolved_abs_path = None
            return _clone_and_update_existing(
                source_artifact,
                key_override=key,
                driver_override=driver,
                table_path_override=table_path,
                array_path_override=array_path,
                parent_id_override=parent_artifact_id,
                meta_updates=meta,
            )

        if key is None:
            raise ValueError("Argument 'key' required when 'path' is a path-like.")
        validate_artifact_key(key)

        if isinstance(path, str) and path.startswith("db://"):
            container_uri = path
            effective_member = table_path if table_path is not None else array_path
            resolved_driver = driver if driver is not None else "db"
            anchor_val = (
                hash_state.effective_hash
                if hash_state.effective_hash is not None
                else hashlib.sha256(
                    f"{container_uri}|{effective_member or ''}".encode("utf-8")
                ).hexdigest()
            )
            if (
                hash_state.effective_hash is not None
                and ":" in hash_state.effective_hash
            ):
                db_anchor = Digest.parse(hash_state.effective_hash)
                db_basis: ObservationBasis = "computed"
            elif hash_state.effective_hash is not None:
                db_anchor = Digest("ctd1:sha256", anchor_val)
                db_basis = "computed"
            else:
                db_anchor = Digest("legacy-v1:unverified", anchor_val)
                db_basis = "declared"
            db_identity = Identity(anchor=db_anchor)
            merged_db_meta = dict(meta)
            if schema is not None:
                merged_db_meta["schema_name"] = schema.__name__
                if strict_schema:
                    merged_db_meta["has_strict_schema"] = True
                else:
                    merged_db_meta.pop("has_strict_schema", None)
            db_obs = Observation(
                location=Location(uri=container_uri, member=effective_member),
                identity=db_identity,
                access_driver=resolved_driver,
                witness=None,
                basis=db_basis,
                site=self._site_name(),
            )
            return Artifact(
                id=uuid.uuid4(),
                key=key,
                identity=db_identity,
                driver=resolved_driver,
                run_id=run_id,
                parent_artifact_id=parent_artifact_id,
                created_at=datetime.now(timezone.utc),
                meta=FrozenMeta(merged_db_meta) if merged_db_meta else EMPTY_META,
                origin=self._ledger_ref(),
                hints=(db_obs,),
            ).bind_tracker(self.tracker)

        path_obj = Path(path).expanduser().resolve()
        resolved_abs_path = str(path_obj)
        mounts_map = self._effective_mounts()
        container_uri, matched_mount = _engine_virtualize_path(path_obj, mounts_map)

        if matched_mount is not None and matched_mount.name != "workspace":
            mount_scheme = matched_mount.name
            mount_root = str(matched_mount.root.resolve())

        if driver is None:
            if meta.get("directory_artifact") is True:
                driver = "directory"
            else:
                driver = _infer_driver_from_path(Path(path))

        precomputed_identity_and_basis: Optional[tuple[Identity, ObservationBasis]] = (
            None
        )
        if direction == "input" and self.tracker.db:
            parent = known_latest_artifact_at_uri
            if not latest_artifact_lookup_done:
                parent = self.tracker.db.find_latest_artifact_at_uri(
                    container_uri,
                    driver=driver,
                    table_path=table_path,
                    array_path=array_path,
                )
            if parent is not None:
                should_reuse = True
                if driver in {"h5", "hdf5"}:
                    if hash_state.effective_hash is None:
                        computed_h5 = _compute_content_hash(raise_on_error=False)
                        should_reuse = _matches_artifact_hash(parent, computed_h5)
                    else:
                        should_reuse = _matches_artifact_hash(
                            parent, hash_state.effective_hash
                        )
                elif driver in {"h5_table", "gtfs"}:
                    should_reuse = _matches_artifact_hash(
                        parent, hash_state.effective_hash
                    )
                elif hash_state.effective_hash is None and path_obj.exists():
                    effective_member_check = (
                        table_path if table_path is not None else array_path
                    )
                    has_fresh_witness = parent.identity.verifiable and any(
                        h.location.uri == container_uri
                        and h.location.member == effective_member_check
                        and isinstance(h.witness, (FileStat, DirTreeStat))
                        and h.witness.check(path_obj) is Freshness.FRESH
                        for h in parent.hints
                    )
                    if has_fresh_witness:
                        should_reuse = True
                    else:
                        precomputed_identity_and_basis = self._build_identity_and_basis(
                            driver=driver,
                            member=effective_member_check,
                            path_for_stat=path_obj,
                            content_hash=None,
                            validate_content_hash=False,
                            meta=meta,
                            compute_default_hash=lambda: _compute_content_hash(
                                raise_on_error=False
                            ),
                            validate_disk_hash=_validate_content_hash,
                        )
                        should_reuse = (
                            precomputed_identity_and_basis[0].anchor
                            == parent.identity.anchor
                        )

                if should_reuse:
                    return _clone_and_update_existing(
                        parent,
                        key_override=key,
                        driver_override=driver,
                        table_path_override=table_path,
                        array_path_override=array_path,
                        parent_id_override=parent_artifact_id,
                        meta_updates=meta,
                    )

        effective_member = table_path if table_path is not None else array_path
        if precomputed_identity_and_basis is not None:
            identity, basis = precomputed_identity_and_basis
        else:
            identity, basis = self._build_identity_and_basis(
                driver=driver,
                member=effective_member,
                path_for_stat=path_obj,
                content_hash=hash_state.effective_hash,
                validate_content_hash=validate_content_hash,
                meta=meta,
                compute_default_hash=_ensure_content_hash,
                validate_disk_hash=_validate_content_hash,
            )

        merged_meta = dict(meta)
        if schema is not None:
            merged_meta["schema_name"] = schema.__name__
            if strict_schema:
                merged_meta["has_strict_schema"] = True
            else:
                merged_meta.pop("has_strict_schema", None)
        if mount_scheme and "mount_scheme" not in merged_meta:
            merged_meta["mount_scheme"] = mount_scheme
        if mount_root and "mount_root" not in merged_meta:
            merged_meta["mount_root"] = mount_root

        obs = Observation(
            location=Location(uri=container_uri, member=effective_member),
            identity=identity,
            access_driver=driver,
            witness=_capture_witness(path_obj, member=effective_member),
            basis=basis,
            site=self._site_name(),
        )

        artifact_obj = Artifact(
            id=uuid.uuid4(),
            key=key,
            identity=identity,
            driver=driver,
            run_id=run_id,
            parent_artifact_id=parent_artifact_id,
            created_at=datetime.now(timezone.utc),
            meta=FrozenMeta(merged_meta) if merged_meta else EMPTY_META,
            origin=self._ledger_ref(),
            hints=(obs,),
        )
        return artifact_obj.bind_tracker(self.tracker)

    def scan_h5_container(
        self,
        container: Artifact,
        path_obj: Path,
        key: str,
        direction: str,
        filter_fn: Callable[[str], bool],
        *,
        hash_tables: bool,
        table_hash_chunk_rows: Optional[int],
        child_specs: Optional[Mapping[str, H5ChildSpec]] = None,
    ) -> List[tuple[Artifact, Any]]:
        """
        Discover and log HDF5 tables contained within an artifact.

        Parameters
        ----------
        container : Artifact
            Parent artifact that holds the HDF5 container.
        path_obj : Path
            Path to the HDF5 file.
        key : str
            Base key used to name derived HDF5 table artifacts.
        direction : str
            Either ``"input"`` or ``"output"`` to tag derived artifacts.
        filter_fn : Callable[[str], bool]
            Predicate that selects which datasets should be turned into artifacts.
        hash_tables : bool
            Whether to compute per-table hashes for discovered datasets.
        table_hash_chunk_rows : Optional[int]
            Number of rows per hash chunk when computing table hashes.

        Returns
        -------
        List[tuple[Artifact, Any]]
            One tuple per selected dataset containing the child artifact and an
            optional prepared facet bundle.
        """
        try:
            import h5py
        except ImportError:
            logging.warning(
                "[Consist] h5py not installed. Cannot discover HDF5 tables."
            )
            return []

        table_artifacts: List[tuple[Artifact, Any]] = []

        try:
            with open_h5py_read_only(str(path_obj), h5py_module=h5py) as f:

                def visit_datasets(name: str, obj: Any) -> None:
                    if isinstance(obj, h5py.Dataset):
                        if filter_fn(name):
                            child_spec = _resolve_h5_child_spec(child_specs, name)
                            table_key = (
                                child_spec.key
                                if child_spec and child_spec.key
                                else f"{key}_{name.replace('/', '_')}"
                            )
                            table_hash = None
                            table_meta: dict[str, Any] = {
                                "parent_id": str(container.id),
                                "shape": list(obj.shape),
                                "dtype": str(obj.dtype),
                            }
                            if child_spec and child_spec.description:
                                table_meta["description"] = child_spec.description
                            if child_spec and child_spec.metadata:
                                table_meta.update(dict(child_spec.metadata))
                            if hash_tables:
                                try:
                                    table_hash = _hash_h5_dataset(
                                        obj, chunk_rows=table_hash_chunk_rows
                                    )
                                except Exception as e:
                                    logging.warning(
                                        "[Consist] Failed to hash HDF5 dataset %s: %s",
                                        name,
                                        e,
                                    )
                                if table_hash:
                                    table_meta["table_hash"] = table_hash
                                    table_meta["table_hash_algo"] = "sha256"
                                    if table_hash_chunk_rows is not None:
                                        table_meta["table_hash_chunk_rows"] = (
                                            table_hash_chunk_rows
                                        )
                            table_art = self.create_artifact(
                                str(path_obj),
                                run_id=container.run_id,
                                key=table_key,
                                direction=direction,
                                driver="h5_table",
                                table_path=name,
                                content_hash=table_hash,
                                parent_artifact_id=container.id,
                                **table_meta,
                            )
                            prepared_bundle = self.tracker._artifact_logging.prepare_artifact_facet_bundle(
                                artifact=table_art,
                                facet=child_spec.facet if child_spec else None,
                                facet_schema_version=(
                                    child_spec.facet_schema_version
                                    if child_spec
                                    else None
                                ),
                                facet_index=(
                                    child_spec.facet_index if child_spec else False
                                ),
                            )
                            table_artifacts.append((table_art, prepared_bundle))

                f.visititems(visit_datasets)
        except Exception as e:
            logging.warning(f"[Consist] Failed to discover HDF5 tables: {e}")

        return table_artifacts

    def log_h5_container(
        self,
        path: Union[str, Path],
        *,
        key: Optional[str] = None,
        direction: str = "output",
        discover_tables: bool = True,
        table_filter: Optional[Union[Callable[[str], bool], List[str]]] = None,
        hash_tables: Literal["always", "if_unchanged", "never"] = "if_unchanged",
        table_hash_chunk_rows: Optional[int] = None,
        child_specs: Optional[Mapping[str, H5ChildSpec]] = None,
        child_selection: H5ChildSelectionMode = "all",
        container_recovery_unit: ContainerRecoveryUnit | None = None,
        child_recovery_policy: ChildRecoveryPolicy | None = None,
        representation_policy: Optional[Mapping[str, Any]] = None,
        **meta: Any,
    ) -> tuple[Artifact, List[Artifact]]:
        """
        Log an HDF5 file and optionally discover its internal tables.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        """
        if not self.tracker.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")
        if child_selection not in {"all", "include_only"}:
            raise ValueError("child_selection must be 'all' or 'include_only'.")

        apply_container_policy_meta(
            meta,
            container_recovery_unit=container_recovery_unit,
            child_recovery_policy=child_recovery_policy,
            representation_policy=representation_policy,
        )

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem
        normalized_child_specs = (
            {
                _normalize_h5_child_path(path_key): spec
                for path_key, spec in child_specs.items()
            }
            if child_specs is not None
            else None
        )
        prior_container: Optional[Artifact] = None
        if self.tracker.db:
            try:
                container_uri, _ = _engine_virtualize_path(
                    path_obj.resolve(), self._effective_mounts()
                )
                prior_container = self.tracker.db.find_latest_artifact_at_uri(
                    container_uri, driver="h5", include_inputs=True
                )
            except Exception:
                prior_container = None

        with self.tracker.persistence.batch_artifact_writes():
            container = self.tracker.log_artifact(
                str(path_obj),
                key=key,
                direction=direction,
                driver="h5",
                is_container=True,
                **meta,
            )

            table_artifacts: List[Artifact] = []
            reusing_cached_children = False
            should_hash_tables = False
            skip_reason: Optional[str] = None
            if hash_tables not in {"always", "if_unchanged", "never"}:
                raise ValueError(
                    "hash_tables must be one of: 'always', 'if_unchanged', 'never'."
                )
            if hash_tables == "always":
                should_hash_tables = True
            elif hash_tables == "if_unchanged":
                if prior_container is None:
                    should_hash_tables = True
                elif (
                    prior_container.identity.verifiable
                    and container.identity.verifiable
                ):
                    should_hash_tables = (
                        prior_container.identity.anchor == container.identity.anchor
                    )
                    if not should_hash_tables:
                        skip_reason = "content_identity_changed"
                elif prior_container.hash and container.hash:
                    should_hash_tables = prior_container.hash == container.hash
                    if not should_hash_tables:
                        skip_reason = "file_hash_changed"
                else:
                    skip_reason = "file_hash_unavailable"

            container_meta_updates: dict[str, Any] = {
                "table_hashes_checked": should_hash_tables
            }
            if not should_hash_tables and hash_tables != "always":
                if hash_tables == "never":
                    container_meta_updates["table_hashes_skip_reason"] = "disabled"
                elif prior_container is not None and skip_reason is not None:
                    container_meta_updates["table_hashes_skip_reason"] = skip_reason
            container = container.with_meta(
                container_meta_updates, merge=True
            ).bind_tracker(self.tracker)

            if discover_tables:
                include_only_paths = (
                    (
                        set(normalized_child_specs.keys())
                        if normalized_child_specs is not None
                        else set()
                    )
                    if child_selection == "include_only"
                    else None
                )
                if table_filter is None:

                    def base_filter_fn(name: str) -> bool:
                        return True

                elif isinstance(table_filter, list):
                    filter_set = set(table_filter)

                    def base_filter_fn(name: str) -> bool:
                        stripped = name.lstrip("/")
                        return name in filter_set or stripped in filter_set

                else:
                    base_filter_fn = table_filter

                def filter_fn(name: str) -> bool:
                    if not base_filter_fn(name):
                        return False
                    if include_only_paths is None:
                        return True
                    return _normalize_h5_child_path(name) in include_only_paths

                if direction == "output" and self.tracker.is_cached:
                    cached_children = [
                        artifact
                        for artifact in self.tracker.current_consist.outputs
                        if artifact.id != container.id
                        and (
                            artifact.parent_artifact_id == container.id
                            or artifact.meta.get("parent_id") == str(container.id)
                        )
                        and artifact.driver == "h5_table"
                        and artifact.member is not None
                        and filter_fn(artifact.member)
                    ]
                    cached_child_paths = {
                        _normalize_h5_child_path(artifact.member)
                        for artifact in cached_children
                        if artifact.member is not None
                    }
                    requested_child_paths = set(
                        _list_h5_dataset_names(path_obj, filter_fn=filter_fn)
                    )
                    if cached_child_paths == requested_child_paths:
                        table_artifacts = list(cached_children)
                        reusing_cached_children = True
                        policy_updates = _container_policy_updates(meta)
                        if policy_updates:
                            container = container.with_meta(
                                policy_updates, merge=True
                            ).bind_tracker(self.tracker)
                            self.tracker._artifact_logging._replace_in_active_run(
                                container, direction=direction
                            )
                            if self.tracker.db is not None:
                                self.tracker.db.update_artifact_meta(
                                    container,
                                    dict(policy_updates),
                                    raise_on_error=True,
                                )
                    else:
                        _demote_cache_hit(
                            self.tracker.current_consist,
                            reason=(
                                "caller requested HDF5 child artifacts that do not "
                                "match the cached output set"
                            ),
                        )
                        container = self.tracker.log_artifact(
                            str(path_obj),
                            key=key,
                            direction=direction,
                            driver="h5",
                            is_container=True,
                            **meta,
                        )
                        container = container.with_meta(
                            container_meta_updates, merge=True
                        ).bind_tracker(self.tracker)

                if not table_artifacts:
                    scanned_children = self.scan_h5_container(
                        container,
                        path_obj,
                        key,
                        direction,
                        filter_fn,
                        hash_tables=should_hash_tables,
                        table_hash_chunk_rows=table_hash_chunk_rows,
                        child_specs=normalized_child_specs,
                    )

                    for table_artifact, prepared_bundle in scanned_children:
                        table_artifact = (
                            self.tracker._artifact_logging.apply_inherited_run_metadata(
                                table_artifact
                            )
                        )
                        if prepared_bundle is not None:
                            table_artifact = table_artifact.with_meta(
                                prepared_bundle.meta_updates,
                                merge=True,
                            )
                        table_artifact.bind_tracker(self.tracker)
                        self.tracker._artifact_logging.attach_logged_artifact(
                            artifact=table_artifact,
                            direction=direction,
                            register_with_coupler=False,
                        )
                        if prepared_bundle is not None:
                            table_artifact = self.tracker.persistence.sync_artifact_with_facet_bundle(
                                table_artifact,
                                direction,
                                meta_updates=None,
                                index_kv=prepared_bundle.index_kv,
                            )
                        else:
                            self.tracker.persistence.sync_artifact(
                                table_artifact,
                                direction,
                            )
                        table_artifacts.append(table_artifact)

                if reusing_cached_children:
                    return container, table_artifacts

            container = container.with_meta(
                {
                    "table_count": len(table_artifacts),
                    "table_ids": [str(t.id) for t in table_artifacts],
                },
                merge=True,
            ).bind_tracker(self.tracker)
            self.tracker._artifact_logging._replace_in_active_run(
                container, direction=direction
            )
            self.tracker.persistence.sync_artifact(container, direction)

            return container, table_artifacts

    def log_h5_table(
        self,
        path: Union[str, Path],
        *,
        table_path: str,
        key: Optional[str] = None,
        direction: str = "output",
        parent: Optional[Artifact] = None,
        hash_table: bool = True,
        table_hash_chunk_rows: Optional[int] = None,
        profile_file_schema: bool | Literal["if_changed"] = False,
        file_schema_sample_rows: Optional[int] = None,
        description: Optional[str] = None,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> Artifact:
        """
        Log a single HDF5 table as an artifact without scanning the container.
        """
        if not self.tracker.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            suffix = table_path.lstrip("/").replace("/", "_")
            key = f"{path_obj.stem}_{suffix}" if suffix else path_obj.stem

        table_hash = None
        if hash_table:
            try:
                import h5py
            except ImportError as e:
                raise ImportError("h5py is required to hash HDF5 tables.") from e
            with open_h5py_read_only(str(path_obj), h5py_module=h5py) as h5_file:
                try:
                    dataset = h5_file[table_path]
                except KeyError:
                    alt_path = (
                        f"/{table_path}" if not table_path.startswith("/") else None
                    )
                    if alt_path and alt_path in h5_file:
                        dataset = h5_file[alt_path]
                    else:
                        raise
                table_hash = _hash_h5_dataset(dataset, chunk_rows=table_hash_chunk_rows)

        table_meta: dict[str, Any] = {}
        if parent is not None:
            table_meta["parent_id"] = str(parent.id)
            parent_artifact_id = parent.id
        else:
            parent_artifact_id = None
        if table_hash:
            table_meta["table_hash"] = table_hash
            table_meta["table_hash_algo"] = "sha256"
            if table_hash_chunk_rows is not None:
                table_meta["table_hash_chunk_rows"] = table_hash_chunk_rows
        if description is not None:
            table_meta["description"] = description

        if meta:
            table_meta.update(meta)

        artifact = self.create_artifact(
            str(path_obj),
            run_id=(
                self.tracker.current_consist.run.id if direction == "output" else None
            ),
            key=key,
            direction=direction,
            driver="h5_table",
            table_path=table_path,
            content_hash=table_hash,
            parent_artifact_id=parent_artifact_id,
            **table_meta,
        )
        artifact = self.tracker._artifact_logging.apply_inherited_run_metadata(artifact)
        prepared_bundle = self.tracker._artifact_logging.prepare_artifact_facet_bundle(
            artifact=artifact,
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
        )
        if prepared_bundle is not None:
            artifact = artifact.with_meta(
                prepared_bundle.meta_updates,
                merge=True,
            )
        artifact.bind_tracker(self.tracker)
        self.tracker._artifact_logging.attach_logged_artifact(
            artifact=artifact,
            direction=direction,
            register_with_coupler=direction == "output",
        )

        self.tracker._flush_json()
        if prepared_bundle is not None:
            artifact = self.tracker.persistence.sync_artifact_with_facet_bundle(
                artifact,
                direction,
                meta_updates=None,
                index_kv=prepared_bundle.index_kv,
            )
        else:
            self.tracker.persistence.sync_artifact(artifact, direction)

        if (
            profile_file_schema
            and artifact.is_tabular
            and self.tracker.current_consist is not None
        ):
            try:
                if artifact.schema_id or artifact.meta.get("schema_id"):
                    return artifact
                located = artifact.locate(
                    mounts=self._effective_mounts(),
                    verify_on_stale=False,
                )
                resolved_path = str(located) if located is not None else None
                if resolved_path is None and path_obj.exists():
                    resolved_path = str(path_obj.resolve())
                if resolved_path:
                    self.tracker.artifact_schemas.profile_file_artifact(
                        artifact=artifact,
                        run=self.tracker.current_consist.run,
                        resolved_path=resolved_path,
                        driver="h5_table",
                        sample_rows=(
                            self.tracker.settings.schema_sample_rows
                            if file_schema_sample_rows is None
                            else file_schema_sample_rows
                        ),
                        source="file",
                        reuse_if_unchanged=profile_file_schema == "if_changed",
                    )
                    if self.tracker.db is not None:
                        refreshed = self.tracker.db.get_artifact(artifact.id)
                        if refreshed is not None:
                            artifact = refreshed.bind_tracker(self.tracker)
                            self.tracker._artifact_logging._replace_in_active_run(
                                artifact, direction=direction
                            )
            except FileNotFoundError:
                loc = artifact.primary_location
                logging.warning(
                    "[Consist] File schema capture skipped; file not found: %s",
                    loc.uri if loc is not None else str(path_obj),
                )

        return artifact
