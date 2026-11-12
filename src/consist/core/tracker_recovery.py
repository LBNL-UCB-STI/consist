from __future__ import annotations

import ctypes
import fcntl
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Mapping, Optional, Sequence, TYPE_CHECKING, Union
from typing_extensions import Protocol, runtime_checkable

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.directory_artifacts import (
    file_bundle_entry_name,
    is_directory_artifact,
    is_file_bundle_artifact,
    is_valid_directory_artifact_tree,
    is_valid_shapefile_bundle_source,
    resolve_artifact_entry_path,
    resolve_file_bundle_root,
)
from consist.core.materialize import (
    build_allowed_materialization_roots,
    fold_hydrated_run_outputs_result,
    materialize_artifacts,
    materialize_artifact as materialize_artifact_core,
    stage_artifact as stage_artifact_core,
    stage_inputs as stage_inputs_core,
    validate_allowed_materialization_destination,
)
from consist.core.materialize_options import (
    VALID_MATERIALIZE_DB_FALLBACK as _VALID_MATERIALIZE_DB_FALLBACK,
    VALID_MATERIALIZE_ON_MISSING as _VALID_MATERIALIZE_ON_MISSING,
    normalize_materialize_output_keys,
    validate_materialize_option,
)
from consist.digest import DIGEST_DRIVERS, verify_digest
from consist.location import (
    ArtifactLocationRow,
    Witness,
    WitnessFreshness,
    capture_witness,
)
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.materialize import (
        HydratedRunOutputsResult,
        MaterializedArtifact,
        MaterializationResult,
        StagedInput,
        StagedInputsResult,
    )
    from consist.core.tracker import Tracker


_NON_PORTABLE_DIGEST_SCHEMES = frozenset(
    {
        "stat_file_v1:sha256",
        "stat_dir_v1:sha256",
        "caller_supplied_v1:sha256",
    }
)
_LINUX_FICLONE = 0x40049409
_DARWIN_CLONEFILE: Any = None
_DARWIN_CLONEFILE_READY = False
_KNOWN_FULL_TRACKER_TYPES: set[type[Any]] = set()
_REALPATH_MEMO: dict[str, str] = {}


def _fast_realpath(path_str: str) -> str:
    cached = _REALPATH_MEMO.get(path_str)
    if cached is not None:
        return cached
    resolved = os.path.realpath(path_str)
    if len(_REALPATH_MEMO) >= 4096:
        _REALPATH_MEMO.clear()
    _REALPATH_MEMO[path_str] = resolved
    return resolved


@runtime_checkable
class _HasDb(Protocol):
    @property
    def db(self) -> Any: ...


@runtime_checkable
class _HasAbsPathOverride(Protocol):
    def artifact_abs_path_override(
        self, artifact: Any
    ) -> Optional[tuple[Optional[str], Optional[str]]]: ...


def _try_reflink_clone(source: Path, destination: Path) -> bool:
    """Attempt a zero-copy copy-on-write clone; return ``False`` if unsupported."""
    global _DARWIN_CLONEFILE, _DARWIN_CLONEFILE_READY
    if sys.platform == "darwin":
        if not _DARWIN_CLONEFILE_READY:
            try:
                libc = ctypes.CDLL(None, use_errno=True)
                fn = libc.clonefile
                fn.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint32]
                fn.restype = ctypes.c_int
                _DARWIN_CLONEFILE = fn
            except (AttributeError, OSError):
                _DARWIN_CLONEFILE = None
            _DARWIN_CLONEFILE_READY = True
        if _DARWIN_CLONEFILE is None:
            return False
        rc = _DARWIN_CLONEFILE(
            os.fsencode(os.fspath(source)),
            os.fsencode(os.fspath(destination)),
            0,
        )
        return rc == 0

    if sys.platform.startswith("linux") and source.is_file():
        src_fd = -1
        dst_fd = -1
        try:
            src_fd = os.open(source, os.O_RDONLY)
            dst_fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                stat_mode_or_default(source),
            )
            fcntl.ioctl(dst_fd, _LINUX_FICLONE, src_fd)
            shutil.copystat(source, destination, follow_symlinks=False)
            return True
        except OSError:
            try:
                destination.unlink(missing_ok=True)
            except OSError:
                pass
            return False
        finally:
            if dst_fd >= 0:
                os.close(dst_fd)
            if src_fd >= 0:
                os.close(src_fd)

    return False


def stat_mode_or_default(path: Path) -> int:
    try:
        return os.lstat(path).st_mode & 0o777
    except OSError:
        return 0o644


def is_unvalidated_caller_digest(artifact: Artifact) -> bool:
    """Return ``True`` when ``artifact.digest`` is an unvalidated caller-supplied token."""
    if (
        artifact.digest is not None
        and artifact.digest.scheme == "caller_supplied_v1:sha256"
    ):
        return True
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    semantics = meta.get("hash_semantics")
    return (
        isinstance(semantics, Mapping) and semantics.get("source") == "caller_supplied"
    )


def verify_candidate_against_artifact(path: Path, artifact: Artifact) -> bool:
    """Verify that ``path`` exists and matches ``artifact``'s recorded digest/manifest."""
    try:
        if not path.exists():
            return False
    except OSError:
        return False

    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    if is_file_bundle_artifact(artifact):
        return bool(is_valid_shapefile_bundle_source(path, artifact))

    if artifact.driver == "artifact_set" or meta.get("artifact_set") is True:
        if not path.is_dir():
            return False
        member_paths = meta.get("output_set_member_paths")
        if (
            isinstance(member_paths, Mapping)
            and artifact.digest is not None
            and artifact.digest.scheme in DIGEST_DRIVERS
            and not is_unvalidated_caller_digest(artifact)
        ):
            try:
                return verify_digest(
                    path,
                    artifact.digest,
                    driver=artifact.driver,
                    options={"member_paths": member_paths},
                )
            except (ValueError, OSError):
                return False
        return True

    if (
        is_directory_artifact(artifact)
        or artifact.driver in {"zarr", "directory"}
        or (
            artifact.digest is not None
            and artifact.digest.scheme
            in {"dir_manifest_v1:sha256", "stat_dir_v1:sha256"}
        )
        or (artifact.digest is None and path.is_dir())
    ):
        if not path.is_dir():
            return False
        valid_dir = is_valid_directory_artifact_tree(path, artifact)
        if valid_dir is not None:
            return valid_dir
        if (
            artifact.digest is not None
            and artifact.digest.scheme in DIGEST_DRIVERS
            and artifact.digest.scheme not in _NON_PORTABLE_DIGEST_SCHEMES
            and not is_unvalidated_caller_digest(artifact)
        ):
            try:
                return verify_digest(
                    path,
                    artifact.digest,
                    driver=artifact.driver,
                    member=artifact.member,
                )
            except (ValueError, OSError):
                return False
        return True

    if not path.is_file():
        return False
    if (
        artifact.digest is None
        or artifact.digest.scheme in _NON_PORTABLE_DIGEST_SCHEMES
        or artifact.digest.scheme not in DIGEST_DRIVERS
        or is_unvalidated_caller_digest(artifact)
    ):
        return True
    try:
        return verify_digest(
            path,
            artifact.digest,
            driver=artifact.driver,
            member=artifact.member,
        )
    except (ValueError, OSError):
        return False


def capture_artifact_witness(
    path: Path,
    artifact: Artifact,
    *,
    observed_at_ns: Optional[int] = None,
) -> Optional[Witness]:
    """Capture a ``FileStat`` or ``DirTreeStat`` witness for ``artifact`` at ``path``."""
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    is_dir = (
        is_directory_artifact(artifact)
        or artifact.driver == "artifact_set"
        or meta.get("artifact_set") is True
    )
    is_shp = is_file_bundle_artifact(artifact) or artifact.driver == "shapefile"
    entry_raw = meta.get("file_bundle_entry")
    entry_name = entry_raw if isinstance(entry_raw, str) and entry_raw else None
    try:
        return capture_witness(
            path,
            driver=artifact.driver,
            is_directory=is_dir,
            is_shapefile_bundle=is_shp,
            bundle_entry=entry_name,
            observed_at_ns=observed_at_ns,
        )
    except (OSError, ValueError):
        return None


def _load_cached_active_locations(
    tracker: "Tracker",
    artifact: Artifact,
) -> tuple[
    tuple[ArtifactLocationRow, Optional[Path], Optional[Path], Optional[Path], str],
    ...,
]:
    db = tracker.db
    if db is None:
        return ()
    cache_key = (
        db.db_uid,
        db.effective_location_epoch,
        tracker.fs.sorted_mounts_key,
        tracker.fs.workspace_instance_id,
        artifact.id,
        str(artifact.digest) if artifact.digest is not None else "",
        artifact.member or "",
    )
    cached = db._location_resolution_cache.get(cache_key)
    if cached is not None:
        return cached
    rows = db.get_active_artifact_locations(
        artifact_id=artifact.id,
        digest=artifact.digest,
        member=artifact.member,
    )
    historical_run_dir: Optional[str] = None
    historical_lookup_done = False
    primary_entries: list[
        tuple[ArtifactLocationRow, Optional[Path], Optional[Path], Optional[Path], str]
    ] = []
    historical_entries: list[
        tuple[ArtifactLocationRow, Optional[Path], Optional[Path], Optional[Path], str]
    ] = []
    for row in rows:
        primary_cand = tracker.fs.resolve_location_row_path(row)
        primary_entry = (
            Path(resolve_artifact_entry_path(artifact, primary_cand))
            if primary_cand is not None
            else None
        )
        primary_real = (
            _fast_realpath(str(primary_entry)) if primary_entry is not None else ""
        )
        hist_cand: Optional[Path] = None
        if (
            primary_cand is None
            and row.mount_name in ("workspace", "runs")
            and artifact.run_id is not None
        ):
            if not historical_lookup_done:
                historical_lookup_done = True
                owning_run = tracker.get_run(str(artifact.run_id))
                if owning_run is not None and isinstance(owning_run.meta, Mapping):
                    phys = owning_run.meta.get("_physical_run_dir")
                    if isinstance(phys, str) and phys:
                        historical_run_dir = phys
            if historical_run_dir is not None:
                hist_cand = tracker.fs.resolve_location_row_path(
                    row, historical_run_dir=historical_run_dir
                )
        entry_tuple = (row, primary_cand, hist_cand, primary_entry, primary_real)
        if primary_cand is not None or row.site in ("archive", "advisory"):
            primary_entries.append(entry_tuple)
        else:
            historical_entries.append(entry_tuple)

    resolved_tuple = (*primary_entries, *historical_entries)
    if len(db._location_resolution_cache) >= 4096:
        db._location_resolution_cache.clear()
    db._location_resolution_cache[cache_key] = resolved_tuple
    return resolved_tuple


def resolve_verified_local_path_with_diagnostics(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    allow_recovery_roots: bool = True,
    ignore_runtime_abs_path: bool = False,
    now_ns: Optional[int] = None,
) -> tuple[Optional[Path], bool]:
    """Resolve ``artifact`` to a witness/digest-verified local ``Path``.

    Returns ``(resolved_path, saw_digest_mismatch)``.
    """
    tracker_cls = type(tracker)
    is_full_tracker = tracker_cls in _KNOWN_FULL_TRACKER_TYPES
    if (
        not is_full_tracker
        and isinstance(tracker, _HasDb)
        and isinstance(tracker, _HasAbsPathOverride)
    ):
        _KNOWN_FULL_TRACKER_TYPES.add(tracker_cls)
        is_full_tracker = True

    db = (
        tracker.db
        if is_full_tracker
        else (tracker.db if isinstance(tracker, _HasDb) else None)
    )
    saw_mismatch = False

    if not ignore_runtime_abs_path and (
        is_full_tracker or isinstance(tracker, _HasAbsPathOverride)
    ):
        override = tracker.artifact_abs_path_override(artifact)
        if override is not None and override[0] == artifact._abs_path and override[1]:
            override_cand = Path(override[1])
            if override_cand.exists():
                if verify_candidate_against_artifact(override_cand, artifact):
                    return (
                        Path(resolve_artifact_entry_path(artifact, override_cand)),
                        saw_mismatch,
                    )
                saw_mismatch = True

    if db is not None:
        cached_entries = _load_cached_active_locations(tracker, artifact)

        if not ignore_runtime_abs_path and artifact._abs_path:
            attached = (
                artifact._resolver_ref() if artifact._resolver_ref is not None else None
            )
            if attached is None or attached is tracker:
                rt_str = artifact._abs_path
                rt_real: Optional[str] = None
                matches_primary = False
                for _row, _pc, _hc, primary_entry, primary_real in cached_entries:
                    if primary_entry is None:
                        continue
                    if str(primary_entry) == rt_str or primary_real == rt_str:
                        matches_primary = True
                        break
                    if rt_real is None:
                        rt_real = _fast_realpath(rt_str)
                    if primary_real == rt_real:
                        matches_primary = True
                        break
                if not matches_primary:
                    rt_cand = Path(rt_str)
                    if rt_cand.exists():
                        if verify_candidate_against_artifact(rt_cand, artifact):
                            return (
                                Path(resolve_artifact_entry_path(artifact, rt_cand)),
                                saw_mismatch,
                            )
                        saw_mismatch = True

        for (
            row,
            primary_cand,
            hist_cand,
            primary_entry,
            _primary_real,
        ) in cached_entries:
            if row.location_id in db._ephemeral_stale_locations:
                continue
            if not allow_recovery_roots and row.site in ("archive", "advisory"):
                continue

            if primary_cand is not None:
                candidate = primary_cand
                entry_fast = primary_entry
            elif allow_recovery_roots and hist_cand is not None:
                candidate = hist_cand
                entry_fast = None
            else:
                continue

            effective_witness = db._settled_witnesses.get(row.location_id, row.witness)
            if effective_witness is not None:
                check_res = effective_witness.check(candidate, now_ns=now_ns)
                if check_res.freshness == WitnessFreshness.FRESH:
                    return (
                        entry_fast
                        if entry_fast is not None
                        else Path(resolve_artifact_entry_path(artifact, candidate)),
                        saw_mismatch,
                    )
                if check_res.freshness == WitnessFreshness.MISSING:
                    db.mark_artifact_location_status(row.location_id, "stale")
                    continue
                if check_res.freshness == WitnessFreshness.RACY_NEEDS_VERIFY:
                    verify_started_ns = (
                        time.time_ns() if now_ns is None else int(now_ns)
                    )
                    if verify_candidate_against_artifact(candidate, artifact):
                        if check_res.can_settle_racy:
                            db.settle_artifact_location_witness(
                                row.location_id,
                                effective_witness.settled(),
                                observed_at_ns=verify_started_ns,
                            )
                        return (
                            Path(resolve_artifact_entry_path(artifact, candidate)),
                            saw_mismatch,
                        )
                    saw_mismatch = True
                    db.mark_artifact_location_status(row.location_id, "stale")
                    continue

                # STAT_MISMATCH: verify content digest (crucial for multi-member HDF5
                # containers where sibling appends alter FileStat without changing this
                # member's digest, as well as benign metadata touches).
                if verify_candidate_against_artifact(candidate, artifact):
                    verify_started_ns = (
                        time.time_ns() if now_ns is None else int(now_ns)
                    )
                    refreshed = capture_artifact_witness(
                        candidate,
                        artifact,
                        observed_at_ns=verify_started_ns,
                    )
                    if refreshed is not None:
                        db.settle_artifact_location_witness(
                            row.location_id,
                            refreshed,
                            observed_at_ns=verify_started_ns,
                        )
                    return (
                        Path(resolve_artifact_entry_path(artifact, candidate)),
                        saw_mismatch,
                    )
                saw_mismatch = True
                db.mark_artifact_location_status(row.location_id, "stale")
                continue

            # Unwitnessed row (advisory root or cold-migrated location mounted later)
            if not candidate.exists():
                continue
            verify_started_ns = time.time_ns() if now_ns is None else int(now_ns)
            if verify_candidate_against_artifact(candidate, artifact):
                captured = capture_artifact_witness(
                    candidate,
                    artifact,
                    observed_at_ns=verify_started_ns,
                )
                if captured is not None:
                    db.settle_artifact_location_witness(
                        row.location_id,
                        captured,
                        observed_at_ns=verify_started_ns,
                    )
                return (
                    Path(resolve_artifact_entry_path(artifact, candidate)),
                    saw_mismatch,
                )
            saw_mismatch = True
            if row.site != "advisory":
                db.mark_artifact_location_status(row.location_id, "stale")

        if allow_recovery_roots and artifact.parent_artifact_id is not None:
            parent = db.get_artifact(artifact.parent_artifact_id)
            rel_path = tracker.fs.get_remappable_relative_path(artifact.uri)
            if parent is not None and rel_path is not None:
                for root_str in tracker.artifact_recovery_roots(parent):
                    cand = (Path(root_str) / rel_path).resolve()
                    if not cand.exists():
                        continue
                    if verify_candidate_against_artifact(cand, artifact):
                        return (
                            Path(resolve_artifact_entry_path(artifact, cand)),
                            saw_mismatch,
                        )
                    saw_mismatch = True

        if cached_entries:
            return None, saw_mismatch

        # Artifact has zero active location rows in DB; check if it has stale/evicted rows
        # (meaning it WAS tracked in DB and its locations were invalidated/moved).
        existing_any = db.fetchone(
            "SELECT 1 FROM artifact_location WHERE artifact_id = ? LIMIT 1",
            [str(artifact.id)],
        )
        if existing_any is not None:
            return None, saw_mismatch

    # Unpersisted in-memory artifact or tracker without DB: verify direct path candidates
    fallback_candidates: list[Path] = []
    attached_fb = (
        artifact._resolver_ref() if artifact._resolver_ref is not None else None
    )
    if (
        not ignore_runtime_abs_path
        and artifact.abs_path
        and (attached_fb is None or attached_fb is tracker)
    ):
        fallback_candidates.append(Path(artifact.abs_path))
    try:
        fallback_candidates.append(Path(tracker.resolve_uri(artifact.uri)))
    except (OSError, RuntimeError, ValueError, TypeError):
        pass

    for cand in fallback_candidates:
        if not cand.exists():
            continue
        if verify_candidate_against_artifact(cand, artifact):
            return Path(resolve_artifact_entry_path(artifact, cand)), saw_mismatch
        saw_mismatch = True

    return None, saw_mismatch


def resolve_verified_local_path(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    allow_recovery_roots: bool = True,
    ignore_runtime_abs_path: bool = False,
    now_ns: Optional[int] = None,
) -> Optional[Path]:
    """Resolve ``artifact`` to a verified local ``Path`` or ``None`` if unavailable."""
    resolved, _ = resolve_verified_local_path_with_diagnostics(
        tracker,
        artifact,
        allow_recovery_roots=allow_recovery_roots,
        ignore_runtime_abs_path=ignore_runtime_abs_path,
        now_ns=now_ns,
    )
    return resolved


def _stage_single_file_atomic(
    source_file: Path,
    dest_file: Path,
    *,
    mutable: bool,
) -> None:
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = dest_file.parent / f".consist-stage-{uuid.uuid4().hex}"
    try:
        if _try_reflink_clone(source_file, tmp_file):
            if mutable and not os.access(tmp_file, os.W_OK):
                os.chmod(tmp_file, stat_mode_or_default(source_file) | 0o200)
        elif not mutable:
            try:
                os.link(source_file, tmp_file)
                os.chmod(tmp_file, 0o444)
            except OSError:
                shutil.copy2(source_file, tmp_file)
                os.chmod(tmp_file, 0o444)
        else:
            shutil.copy2(source_file, tmp_file)
            if not os.access(tmp_file, os.W_OK):
                os.chmod(tmp_file, stat_mode_or_default(source_file) | 0o200)
        os.replace(tmp_file, dest_file)
    except BaseException:
        try:
            tmp_file.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def stage_verified_source(
    artifact: Artifact,
    source: Path,
    destination: Path,
    *,
    mutable: bool = False,
    overwrite: bool = False,
) -> Path:
    """Atomically stage verified ``source`` to ``destination``.

    Strategy cascade:
      1. Copy-on-write reflink/clonefile when supported by the filesystem.
      2. Read-only ``0o444`` hardlink (or read-only copy cross-device) when ``mutable=False``.
      3. Writable atomic byte copy when ``mutable=True``.
    """
    src = Path(os.path.realpath(source))
    raw_dest = Path(destination).expanduser().absolute()
    raw_dest.parent.mkdir(parents=True, exist_ok=True)
    dest = Path(os.path.realpath(raw_dest.parent)) / raw_dest.name

    if dest.is_symlink():
        if not overwrite:
            raise FileExistsError(
                f"Refusing to overwrite symlink destination {dest} for artifact {artifact.key!r}."
            )
        dest.unlink()

    if is_file_bundle_artifact(artifact) or artifact.driver == "shapefile":
        entry = file_bundle_entry_name(artifact, fallback=dest.name)
        if entry is None:
            raise ValueError(
                f"Shapefile bundle artifact {artifact.key!r} has no entry."
            )
        dest_bundle_dir = dest if (dest.exists() and dest.is_dir()) else dest.parent
        dest_entry_path = dest_bundle_dir / entry
        if dest_entry_path.exists():
            if verify_candidate_against_artifact(dest_entry_path, artifact):
                if not mutable or os.access(dest_entry_path, os.W_OK):
                    return dest_entry_path.resolve()
            elif not overwrite:
                raise FileExistsError(
                    f"Refusing to overwrite modified shapefile bundle at {dest_entry_path} "
                    f"for artifact {artifact.key!r}."
                )

        src_bundle_root = resolve_file_bundle_root(src)
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        manifest = meta.get("file_bundle_manifest") or meta.get("directory_manifest")
        if isinstance(manifest, Mapping) and isinstance(manifest.get("entries"), list):
            member_names = [
                str(item["path"])
                for item in manifest["entries"]
                if isinstance(item, Mapping) and isinstance(item.get("path"), str)
            ]
        else:
            stem_prefix = f"{Path(entry).stem}."
            with os.scandir(src_bundle_root) as scan:
                member_names = sorted(
                    item.name
                    for item in scan
                    if item.is_file(follow_symlinks=False)
                    and (item.name == entry or item.name.startswith(stem_prefix))
                )

        dest_bundle_dir.mkdir(parents=True, exist_ok=True)
        staged_tmps: list[tuple[Path, Path]] = []
        try:
            for name in member_names:
                src_member = src_bundle_root / name
                target_member = dest_bundle_dir / name
                if src_member.resolve() == target_member.resolve():
                    continue
                tmp_member = (
                    dest_bundle_dir / f".consist-stage-{uuid.uuid4().hex}-{name}"
                )
                _stage_single_file_atomic(src_member, tmp_member, mutable=mutable)
                staged_tmps.append((tmp_member, target_member))
            for tmp_member, target_member in staged_tmps:
                os.replace(tmp_member, target_member)
        except BaseException:
            for tmp_member, _ in staged_tmps:
                try:
                    tmp_member.unlink(missing_ok=True)
                except OSError:
                    pass
            raise
        return dest_entry_path.resolve()

    if dest.exists():
        if verify_candidate_against_artifact(dest, artifact):
            if not mutable or os.access(dest, os.W_OK):
                return Path(resolve_artifact_entry_path(artifact, dest))
        elif not overwrite:
            raise FileExistsError(
                f"Refusing to overwrite modified destination {dest} for artifact {artifact.key!r}."
            )

    if src == dest:
        if mutable and not os.access(dest, os.W_OK):
            raise ValueError(
                f"Cannot stage mutable artifact onto read-only source path {src}."
            )
        return Path(resolve_artifact_entry_path(artifact, dest))

    if src.is_dir():
        tmp_dir = dest.parent / f".consist-stage-dir-{uuid.uuid4().hex}"
        try:
            if not _try_reflink_clone(src, tmp_dir):
                tmp_dir.mkdir(parents=True, exist_ok=False)
                for root_str, dirnames, filenames in os.walk(src):
                    rel_dir = Path(root_str).relative_to(src)
                    target_sub = tmp_dir / rel_dir if rel_dir != Path(".") else tmp_dir
                    target_sub.mkdir(parents=True, exist_ok=True)
                    for dname in dirnames:
                        (target_sub / dname).mkdir(parents=True, exist_ok=True)
                    for fname in filenames:
                        _stage_single_file_atomic(
                            Path(root_str) / fname,
                            target_sub / fname,
                            mutable=mutable,
                        )
            elif mutable:
                for root_str, _, filenames in os.walk(tmp_dir):
                    for fname in filenames:
                        fpath = Path(root_str) / fname
                        if not os.access(fpath, os.W_OK):
                            os.chmod(fpath, stat_mode_or_default(fpath) | 0o200)
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            os.replace(tmp_dir, dest)
        except BaseException:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
        return dest.resolve()

    if dest.exists() and dest.is_dir():
        shutil.rmtree(dest)
    _stage_single_file_atomic(src, dest, mutable=mutable)
    return dest.resolve()


class TrackerRecoveryService(_TrackerServiceBase):
    """Artifact staging and historical output recovery helpers for ``Tracker``.

    Notes
    -----
    ``Tracker`` owns the public API. This service centralizes the recovery
    implementation so staging and historical-output methods share validation,
    source resolution, and destination safety rules.
    """

    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        """Materialize one artifact to an exact destination path.

        Parameters
        ----------
        artifact : Artifact
            Artifact whose filesystem bytes should be restored.
        destination_path : str | Path
            Exact path that receives the materialized artifact.
        on_missing : {"warn", "raise"}, default "warn"
            Policy applied when no recoverable filesystem source is available.

        Returns
        -------
        str | None
            Destination path when materialization succeeds; otherwise ``None``.
        """
        result = materialize_artifacts(
            tracker=self._tracker,
            items=[(artifact, Path(destination_path))],
            on_missing=on_missing,
        )
        return result.get(artifact.key)

    def stage_artifact(
        self,
        artifact: Artifact,
        *,
        destination: str | Path,
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInput":
        """Stage one artifact for use outside a run lifecycle.

        Parameters
        ----------
        artifact : Artifact
            Resolved artifact to stage.
        destination : str | Path
            Requested local destination path.
        mode : {"copy", "hardlink", "symlink"}, default "copy"
            Filesystem operation used to create the staged input.
        overwrite : bool, default False
            Whether an existing destination may be replaced.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for staged bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.

        Returns
        -------
        StagedInput
            Per-artifact staging outcome and resolved destination.
        """
        return stage_artifact_core(
            self._tracker,
            artifact,
            Path(destination),
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    def materialize_artifact(
        self,
        artifact: Artifact,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
        scheme_prefixes: Optional[Mapping[str, Union[str, Path]]] = None,
    ) -> "MaterializedArtifact":
        """Materialize one artifact below a caller-selected target root.

        Parameters
        ----------
        artifact : Artifact
            Historical artifact to recover.
        target_root : str | Path
            Root beneath which Consist derives the artifact's canonical path.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        preserve_existing : bool, default True
            Whether an existing target may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for materialized bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.
        scheme_prefixes : mapping, optional
            Explicit mapping of URI scheme to relative path prefix.

        Returns
        -------
        MaterializedArtifact
            Keyed outcome containing the destination, status, and diagnostics.

        Raises
        ------
        TypeError
            If ``artifact`` is not an :class:`Artifact`.
        ValueError
            If an option is invalid or the destination is disallowed.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")

        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )

        allow_external = (
            self.allow_external_paths
            if allow_external_paths is None
            else allow_external_paths
        )
        allowed_roots = build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=allow_external,
        )
        destination_root = Path(target_root).resolve()
        validate_allowed_materialization_destination(destination_root, allowed_roots)
        source_root_override = (
            Path(source_root).resolve() if source_root is not None else None
        )
        return materialize_artifact_core(
            self._tracker,
            artifact,
            target_root=destination_root,
            source_root=source_root_override,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            validate_content_hash=validate_content_hash,
            scheme_prefixes=scheme_prefixes,
        )

    def stage_inputs(
        self,
        inputs_by_key: Mapping[str, Artifact],
        *,
        destinations_by_key: Mapping[str, str | Path],
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInputsResult":
        """Stage multiple keyed artifacts to exact destination paths.

        Parameters
        ----------
        inputs_by_key : Mapping[str, Artifact]
            Artifacts keyed by the destination mapping keys.
        destinations_by_key : Mapping[str, str | Path]
            Exact local destination for each input key.
        mode : {"copy", "hardlink", "symlink"}, default "copy"
            Filesystem operation used for staging.
        overwrite : bool, default False
            Whether existing destinations may be replaced.
        validate_content_hash : {"never", "if-present", "always"}, default "if-present"
            Content-hash validation policy for staged bytes.
        allow_external_paths : bool | None, optional
            Override for the tracker's external-destination policy.

        Returns
        -------
        StagedInputsResult
            Per-key staging outcomes in destination mapping order.
        """
        normalized_destinations = {
            str(key): Path(destination)
            for key, destination in destinations_by_key.items()
        }
        return stage_inputs_core(
            self._tracker,
            inputs_by_key,
            normalized_destinations,
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    def materialize_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "MaterializationResult":
        """Materialize selected outputs from a completed historical run.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be restored.
        target_root : str | Path
            Root beneath which canonical output paths are recreated.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        keys : Sequence[str] | None, optional
            Selected output keys; ``None`` selects all outputs.
        preserve_existing : bool, default True
            Whether existing target files may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for outputs whose bytes are unavailable.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        MaterializationResult
            Compatibility aggregate grouped by materialization outcome.
        """
        return fold_hydrated_run_outputs_result(
            self._tracker.hydrate_run_outputs(
                run_id,
                target_root=target_root,
                source_root=source_root,
                keys=keys,
                preserve_existing=preserve_existing,
                on_missing=on_missing,
                db_fallback=db_fallback,
            )
        )

    def hydrate_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """Hydrate selected historical outputs with per-key recovery outcomes.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be hydrated.
        target_root : str | Path
            Root beneath which canonical output paths are recreated.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        keys : Sequence[str] | None, optional
            Selected output keys; ``None`` selects all outputs.
        preserve_existing : bool, default True
            Whether existing targets may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        HydratedRunOutputsResult
            Mapping of output keys to detached artifacts, paths, and statuses.

        Raises
        ------
        KeyError
            If ``run_id`` is not found.
        RuntimeError
            If the tracker has no metadata database.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys,
            caller="hydrate_run_outputs",
        )
        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )
        validate_materialize_option(
            name="db_fallback",
            value=db_fallback,
            allowed=_VALID_MATERIALIZE_DB_FALLBACK,
        )

        if self.db is None:
            raise RuntimeError(
                "Cannot materialize run outputs: tracker has no database configured."
            )

        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        from consist.core import materialize as materialize_core

        destination_root = Path(target_root).resolve()
        allowed_roots = materialize_core.build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=self.allow_external_paths,
        )
        materialize_core.validate_allowed_materialization_destination(
            destination_root, allowed_roots
        )

        source_root_override = (
            Path(source_root).resolve() if source_root is not None else None
        )

        from consist.core import tracker as tracker_module

        return tracker_module.hydrate_run_outputs_core(
            tracker=self._tracker,
            run=run,
            target_root=destination_root,
            source_root=source_root_override,
            keys=normalized_keys,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )

    def hydrate_run_outputs_to_destinations(
        self,
        run_id: str,
        *,
        destinations_by_key: Mapping[str, str | Path],
        source_root: str | Path | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """Hydrate historical outputs to caller-selected exact destinations.

        Parameters
        ----------
        run_id : str
            Historical run whose outputs should be hydrated.
        destinations_by_key : Mapping[str, str | Path]
            Exact target path for every requested output key.
        source_root : str | Path | None, optional
            Preferred source root before historical and recovery-root probes.
        preserve_existing : bool, default True
            Whether existing destinations may be retained.
        on_missing : {"warn", "raise"}, default "warn"
            Policy for unavailable source bytes.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether eligible ingested tabular outputs may be rebuilt from the DB.

        Returns
        -------
        HydratedRunOutputsResult
            Mapping of requested keys to hydration outcomes.

        Raises
        ------
        TypeError
            If ``destinations_by_key`` is not a string-keyed path mapping.
        KeyError
            If ``run_id`` is not found.
        RuntimeError
            If the tracker has no metadata database.
        """
        if not isinstance(destinations_by_key, Mapping):
            raise TypeError(
                "destinations_by_key must be a mapping of output keys to paths."
            )

        requested_destinations: dict[str, Path] = {}
        for key, destination in destinations_by_key.items():
            if not isinstance(key, str):
                raise TypeError(
                    "destinations_by_key must contain only string output keys."
                )
            if not isinstance(destination, (str, Path)):
                raise TypeError(
                    "destinations_by_key values must be str or Path instances."
                )
            requested_destinations[key] = Path(destination).expanduser().absolute()

        validate_materialize_option(
            name="on_missing",
            value=on_missing,
            allowed=_VALID_MATERIALIZE_ON_MISSING,
        )
        validate_materialize_option(
            name="db_fallback",
            value=db_fallback,
            allowed=_VALID_MATERIALIZE_DB_FALLBACK,
        )

        if self.db is None:
            raise RuntimeError(
                "Cannot materialize run outputs: tracker has no database configured."
            )

        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        requested_outputs = self.get_run_outputs(run_id)
        strict_tree_keys = {
            key
            for key, artifact in requested_outputs.items()
            if isinstance(artifact.meta, Mapping)
            and (
                artifact.meta.get("directory_artifact") is True
                or artifact.meta.get("file_bundle_artifact") is True
            )
        }
        normalized_destinations = {
            key: (destination if key in strict_tree_keys else destination.resolve())
            for key, destination in requested_destinations.items()
        }

        from consist.core import materialize as materialize_core

        allowed_roots = materialize_core.build_allowed_materialization_roots(
            run_dir=self.run_dir,
            mounts=self.mounts,
            allow_external_paths=self.allow_external_paths,
        )
        for destination in normalized_destinations.values():
            materialize_core.validate_allowed_materialization_destination(
                destination, allowed_roots
            )

        source_root_override = None
        if source_root is not None:
            source_root_path = Path(source_root).expanduser().absolute()
            source_root_override = (
                source_root_path
                if strict_tree_keys and db_fallback == "never"
                else source_root_path.resolve()
            )

        from consist.core import tracker as tracker_module

        return tracker_module.hydrate_run_outputs_to_destinations_core(
            tracker=self._tracker,
            run=run,
            destinations_by_key=normalized_destinations,
            source_root=source_root_override,
            allowed_base=allowed_roots,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )
