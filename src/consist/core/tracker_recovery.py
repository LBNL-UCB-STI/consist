from __future__ import annotations

import ctypes
import fcntl
import logging
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
from consist.core.fs import build_verified_location_row, capture_artifact_witness
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
from consist.core.persistence import DatabaseManager
from consist.digest import DIGEST_DRIVERS, is_shapefile_sidecar_filename, verify_digest
from consist.location import (
    ArtifactLocationRow,
    WitnessFreshness,
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


def _clear_realpath_memo() -> None:
    _REALPATH_MEMO.clear()


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
class _HasHistoryService(Protocol):
    _history_service: Any
    current_consist: Any
    _last_consist: Any


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


def _artifact_has_portable_content_identity(artifact: Artifact) -> bool:
    """Return ``True`` when ``artifact`` carries a verifiable content digest or manifest."""
    if is_unvalidated_caller_digest(artifact):
        return False
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    if is_file_bundle_artifact(artifact):
        return isinstance(meta.get("file_bundle_manifest"), Mapping)
    if artifact.driver == "artifact_set" or meta.get("artifact_set") is True:
        return (
            isinstance(meta.get("output_set_member_paths"), Mapping)
            and artifact.digest is not None
            and artifact.digest.scheme in DIGEST_DRIVERS
            and artifact.digest.scheme not in _NON_PORTABLE_DIGEST_SCHEMES
        )
    if is_directory_artifact(artifact) or artifact.driver in {"zarr", "directory"}:
        if isinstance(meta.get("dir_manifest"), Mapping):
            return True
    return (
        artifact.digest is not None
        and artifact.digest.scheme in DIGEST_DRIVERS
        and artifact.digest.scheme not in _NON_PORTABLE_DIGEST_SCHEMES
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


_CachedEntryTuple = tuple[
    ArtifactLocationRow,
    Optional[Path],
    Optional[Path],
    Optional[Path],
    str,
    Optional[Path],
    str,
]


def _resolve_rows_for_cache(
    tracker: "Tracker",
    artifact: Artifact,
    rows: Sequence[ArtifactLocationRow],
    historical_run_dir_memo: dict[str, Optional[str]],
) -> tuple[_CachedEntryTuple, ...]:
    primary_entries: list[_CachedEntryTuple] = []
    historical_entries: list[_CachedEntryTuple] = []
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
        if row.site in ("workspace", "external") and row.mount_name != "abs":
            hist_dir: Optional[str] = None
            if artifact.run_id is not None and (
                primary_cand is None or row.mount_name not in ("workspace", "runs")
            ):
                rid_str = str(artifact.run_id)
                memo_key = f"{rid_str}:{row.mount_name}"
                if memo_key not in historical_run_dir_memo:
                    owning_run = tracker.get_run(rid_str)
                    run_meta = (
                        owning_run.meta
                        if owning_run is not None
                        and isinstance(owning_run.meta, Mapping)
                        else {}
                    )
                    mounts_meta = run_meta.get("mounts")
                    mount_override = (
                        mounts_meta.get(row.mount_name)
                        if isinstance(mounts_meta, Mapping)
                        else None
                    )
                    phys = (
                        mount_override
                        if isinstance(mount_override, str) and mount_override
                        else (
                            run_meta.get("_physical_run_dir")
                            if row.mount_name in ("workspace", "runs")
                            else None
                        )
                    )
                    historical_run_dir_memo[memo_key] = (
                        phys if isinstance(phys, str) and phys else None
                    )
                hist_dir = historical_run_dir_memo[memo_key]
            if (
                hist_dir is None
                and row.mount_name not in ("workspace", "runs")
                and isinstance(artifact.meta, Mapping)
            ):
                art_mount_root = artifact.meta.get("mount_root")
                if isinstance(art_mount_root, str) and art_mount_root:
                    hist_dir = art_mount_root
            if hist_dir is not None:
                cand_from_hist = tracker.fs.resolve_location_row_path(
                    row, historical_run_dir=hist_dir
                )
                if cand_from_hist is not None and cand_from_hist != primary_cand:
                    hist_cand = cand_from_hist
        hist_entry = (
            Path(resolve_artifact_entry_path(artifact, hist_cand))
            if hist_cand is not None
            else None
        )
        hist_real = _fast_realpath(str(hist_entry)) if hist_entry is not None else ""
        entry_tuple: _CachedEntryTuple = (
            row,
            primary_cand,
            hist_cand,
            primary_entry,
            primary_real,
            hist_entry,
            hist_real,
        )
        if primary_cand is not None or row.site in ("archive", "advisory"):
            primary_entries.append(entry_tuple)
        else:
            historical_entries.append(entry_tuple)
    return (*primary_entries, *historical_entries)


def _artifact_location_cache_key(
    tracker: "Tracker", artifact: Artifact, epoch: tuple[Any, ...]
) -> tuple[Any, ...]:
    db = tracker.db
    assert db is not None
    return (
        db.db_uid,
        epoch,
        tracker.fs.sorted_mounts_key,
        tracker.fs.workspace_instance_id,
        artifact.id,
        str(artifact.digest) if artifact.digest is not None else "",
        artifact.member or "",
    )


def warm_artifact_locations_cache(
    tracker: "Tracker", artifacts: Sequence[Artifact]
) -> None:
    """Batch-warm ``db._location_resolution_cache`` for ``artifacts`` in <=1 SQL query."""
    db = tracker.db
    if db is None or not artifacts:
        return
    if _clear_realpath_memo not in db._epoch_invalidation_hooks:
        db._epoch_invalidation_hooks.append(_clear_realpath_memo)
    epoch = db.sync_location_epoch_caches()
    mounts_key = tracker.fs.sorted_mounts_key
    ws_id = tracker.fs.workspace_instance_id
    missing: list[Artifact] = []
    for art in artifacts:
        if art.id is None and art.digest is None:
            continue
        ck = (
            db.db_uid,
            epoch,
            mounts_key,
            ws_id,
            art.id,
            str(art.digest) if art.digest is not None else "",
            art.member or "",
        )
        if ck not in db._location_resolution_cache:
            missing.append(art)
    if not missing:
        return
    rows_by_aid = db.get_active_artifact_locations_bulk(missing)
    hist_memo: dict[str, Optional[str]] = {}
    for art in missing:
        if art.id is None:
            continue
        ck = _artifact_location_cache_key(tracker, art, epoch)
        rows = rows_by_aid.get(art.id, [])
        if len(db._location_resolution_cache) >= 4096:
            db._location_resolution_cache.clear()
        db._location_resolution_cache[ck] = _resolve_rows_for_cache(
            tracker, art, rows, hist_memo
        )


def _find_sibling_outputs_for_prefetch(
    tracker: "Tracker", artifact: Artifact
) -> Optional[tuple[Artifact, ...]]:
    if not isinstance(tracker, _HasHistoryService):
        return None
    for rec in (tracker.current_consist, tracker._last_consist):
        if rec is not None and len(rec.outputs) > 1:
            if any(s.id == artifact.id for s in rec.outputs):
                return tuple(rec.outputs)
    cache_map = tracker._history_service._run_artifacts_cache
    if artifact.run_id is not None:
        run_arts = cache_map.get(str(artifact.run_id))
        if run_arts is not None and len(run_arts.outputs) > 1:
            siblings = tuple(run_arts.outputs.values())
            if any(s.id == artifact.id for s in siblings):
                return siblings
    for cached_arts in reversed(list(cache_map.values())):
        if len(cached_arts.outputs) > 1:
            siblings = tuple(cached_arts.outputs.values())
            if any(s.id == artifact.id for s in siblings):
                return siblings
    return None


def _load_cached_active_locations(
    tracker: "Tracker",
    artifact: Artifact,
) -> tuple[_CachedEntryTuple, ...]:
    db = tracker.db
    if db is None:
        return ()
    if _clear_realpath_memo not in db._epoch_invalidation_hooks:
        db._epoch_invalidation_hooks.append(_clear_realpath_memo)
    epoch = db.sync_location_epoch_caches()
    cache_key = _artifact_location_cache_key(tracker, artifact, epoch)
    cached = db._location_resolution_cache.get(cache_key)
    if cached is not None:
        return cached
    siblings = _find_sibling_outputs_for_prefetch(tracker, artifact)
    if siblings is not None:
        warm_artifact_locations_cache(tracker, siblings)
        warmed = db._location_resolution_cache.get(cache_key)
        if warmed is not None:
            return warmed
    rows = db.get_active_artifact_locations(
        artifact_id=artifact.id,
        digest=artifact.digest,
        member=artifact.member,
    )
    resolved_tuple = _resolve_rows_for_cache(tracker, artifact, rows, {})
    if len(db._location_resolution_cache) >= 4096:
        db._location_resolution_cache.clear()
    db._location_resolution_cache[cache_key] = resolved_tuple
    return resolved_tuple


def resolve_verified_local_path_detailed(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    allow_recovery_roots: bool = True,
    ignore_runtime_abs_path: bool = False,
    verify_digest: bool = False,
    now_ns: Optional[int] = None,
) -> tuple[Optional[Path], bool, Optional[ArtifactLocationRow]]:
    """Resolve ``artifact`` returning ``(resolved_path, saw_digest_mismatch, winning_row)``."""
    tracker_cls = type(tracker)
    is_full_tracker = tracker_cls in _KNOWN_FULL_TRACKER_TYPES
    if (
        not is_full_tracker
        and isinstance(tracker, _HasDb)
        and isinstance(tracker, _HasAbsPathOverride)
    ):
        _KNOWN_FULL_TRACKER_TYPES.add(tracker_cls)
        is_full_tracker = True

    raw_db = (
        tracker.db
        if is_full_tracker
        else (tracker.db if isinstance(tracker, _HasDb) else None)
    )
    db = raw_db if isinstance(raw_db, DatabaseManager) else None
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
                        None,
                    )
                saw_mismatch = True

    if db is not None:
        cached_entries = _load_cached_active_locations(tracker, artifact)

        rt_str: Optional[str] = None
        if not ignore_runtime_abs_path and artifact._abs_path:
            attached = (
                artifact._resolver_ref() if artifact._resolver_ref is not None else None
            )
            if attached is None or attached is tracker:
                rt_str = artifact._abs_path

        rt_real: Optional[str] = None
        matches_cached_coord = False
        matched_rows: list[
            tuple[ArtifactLocationRow, list[tuple[Path, Optional[Path], str]]]
        ] = []
        normal_rows: list[
            tuple[ArtifactLocationRow, list[tuple[Path, Optional[Path], str]]]
        ] = []

        for (
            row,
            primary_cand,
            hist_cand,
            primary_entry,
            _primary_real,
            hist_entry,
            _hist_real,
        ) in cached_entries:
            cand_options: list[tuple[Path, Optional[Path], str]] = []
            if primary_cand is not None:
                cand_options.append(
                    (
                        primary_cand,
                        primary_entry,
                        _primary_real or _fast_realpath(str(primary_cand)),
                    )
                )
            if allow_recovery_roots and hist_cand is not None:
                hr = _hist_real or _fast_realpath(str(hist_cand))
                if not cand_options or cand_options[0][2] != hr:
                    cand_options.append((hist_cand, hist_entry, hr))

            matched_idx: Optional[int] = None
            if rt_str is not None:
                for opt_idx, (_opt_cand, opt_entry, opt_real) in enumerate(
                    cand_options
                ):
                    if opt_entry is None:
                        continue
                    is_match = str(opt_entry) == rt_str or opt_real == rt_str
                    if not is_match:
                        if rt_real is None:
                            rt_real = _fast_realpath(rt_str)
                        is_match = opt_real == rt_real
                    if is_match:
                        matches_cached_coord = True
                        matched_idx = opt_idx
                        break
                if (
                    matched_idx is None
                    and not allow_recovery_roots
                    and hist_entry is not None
                ):
                    # Still detect a match on a disallowed recovery entry so
                    # uncached rt_cand cannot bypass allow_recovery_roots=False.
                    is_hist_match = str(hist_entry) == rt_str or _hist_real == rt_str
                    if not is_hist_match:
                        if rt_real is None:
                            rt_real = _fast_realpath(rt_str)
                        is_hist_match = _hist_real == rt_real
                    if is_hist_match:
                        matches_cached_coord = True

            if row.location_id in db._ephemeral_stale_locations:
                continue
            if not allow_recovery_roots and row.site in ("archive", "advisory"):
                continue
            if not cand_options:
                continue

            if matched_idx is not None:
                if matched_idx > 0:
                    matched_opt = cand_options.pop(matched_idx)
                    cand_options.insert(0, matched_opt)
                matched_rows.append((row, cand_options))
            else:
                normal_rows.append((row, cand_options))

        if rt_str is not None and not matches_cached_coord:
            rt_cand = Path(rt_str)
            if rt_cand.exists():
                if verify_candidate_against_artifact(rt_cand, artifact):
                    return (
                        Path(resolve_artifact_entry_path(artifact, rt_cand)),
                        saw_mismatch,
                        None,
                    )
                saw_mismatch = True

        def _verify_and_settle(
            cand: Path,
            loc_id: uuid.UUID,
            *,
            can_settle: bool = True,
            settled_override: Optional[Any] = None,
        ) -> bool:
            verify_started_ns = time.time_ns() if now_ns is None else int(now_ns)
            pre_w = capture_artifact_witness(
                cand, artifact, observed_at_ns=verify_started_ns
            )
            if not verify_candidate_against_artifact(cand, artifact):
                return False
            post_w = capture_artifact_witness(
                cand, artifact, observed_at_ns=verify_started_ns
            )
            if (
                can_settle
                and pre_w is not None
                and post_w is not None
                and pre_w.settled() == post_w.settled()
            ):
                db.settle_artifact_location_witness(
                    loc_id,
                    settled_override if settled_override is not None else post_w,
                    observed_at_ns=verify_started_ns,
                )
            return True

        checked_reals: set[str] = set()
        failed_reals: set[str] = set()
        for row, cand_options in (*matched_rows, *normal_rows):
            row_should_mark_stale = False
            for cand_idx, (candidate, entry_fast, cand_real) in enumerate(cand_options):
                is_last_option = cand_idx == len(cand_options) - 1
                if cand_real in failed_reals:
                    if is_last_option and row.site != "advisory":
                        row_should_mark_stale = True
                    continue
                checked_reals.add(cand_real)

                def _win(
                    ef: Optional[Path] = entry_fast,
                    c: Path = candidate,
                    r: ArtifactLocationRow = row,
                ) -> tuple[Path, bool, ArtifactLocationRow]:
                    return (
                        ef
                        if ef is not None
                        else Path(resolve_artifact_entry_path(artifact, c)),
                        saw_mismatch,
                        r,
                    )

                effective_witness = db._settled_witnesses.get(
                    row.location_id, row.witness
                )
                if effective_witness is not None:
                    check_res = effective_witness.check(candidate, now_ns=now_ns)
                    if check_res.freshness == WitnessFreshness.FRESH:
                        if not verify_digest or verify_candidate_against_artifact(
                            candidate, artifact
                        ):
                            return _win()
                    elif check_res.freshness == WitnessFreshness.MISSING:
                        failed_reals.add(cand_real)
                        if is_last_option:
                            row_should_mark_stale = True
                        continue
                    elif check_res.freshness == WitnessFreshness.RACY_NEEDS_VERIFY:
                        if _verify_and_settle(
                            candidate,
                            row.location_id,
                            can_settle=check_res.can_settle_racy,
                            settled_override=effective_witness.settled(),
                        ):
                            return _win()
                    elif _artifact_has_portable_content_identity(
                        artifact
                    ) and _verify_and_settle(candidate, row.location_id):
                        return _win()
                    saw_mismatch = True
                    failed_reals.add(cand_real)
                    if is_last_option:
                        row_should_mark_stale = True
                    continue

                if not candidate.exists():
                    if is_last_option and row.site != "advisory":
                        row_should_mark_stale = True
                    continue
                if _verify_and_settle(candidate, row.location_id):
                    return _win()
                saw_mismatch = True
                failed_reals.add(cand_real)
                if is_last_option and row.site != "advisory":
                    row_should_mark_stale = True

            if row_should_mark_stale:
                db.mark_artifact_location_status(row.location_id, "stale")

        if allow_recovery_roots and artifact.parent_artifact_id is not None:
            parent = db.get_artifact(artifact.parent_artifact_id)
            rel_path = tracker.fs.get_remappable_relative_path(artifact.uri)
            if parent is not None and rel_path is not None:
                for root_str in tracker.artifact_recovery_roots(parent):
                    if "://" in root_str and not root_str.startswith("file://"):
                        _, _, _, cand_target = tracker.fs.archive_coordinate_for_root(
                            artifact.uri, root_str
                        )
                        cand = cand_target
                    else:
                        cand = (Path(root_str) / rel_path).resolve()
                    if str(cand).startswith("/__unmounted__/") or not cand.exists():
                        continue
                    if verify_candidate_against_artifact(cand, artifact):
                        return (
                            Path(resolve_artifact_entry_path(artifact, cand)),
                            saw_mismatch,
                            None,
                        )
                    saw_mismatch = True

        if not _artifact_has_portable_content_identity(artifact):
            if cached_entries:
                return None, saw_mismatch, None
            if db.has_artifact_location_table():
                existing_any = db.fetchone(
                    "SELECT status FROM artifact_location WHERE artifact_id = ? "
                    "ORDER BY observed_at_ns DESC LIMIT 1",
                    [str(artifact.id)],
                )
                if existing_any is not None:
                    return (
                        None,
                        saw_mismatch or str(existing_any[0]) == "stale",
                        None,
                    )
        else:
            try:
                uri_cand = Path(tracker.resolve_uri(artifact.uri))
            except (OSError, RuntimeError, ValueError, TypeError):
                uri_cand = None
            if (
                uri_cand is not None
                and _fast_realpath(str(uri_cand)) not in checked_reals
                and uri_cand.exists()
            ):
                if verify_candidate_against_artifact(uri_cand, artifact):
                    return (
                        Path(resolve_artifact_entry_path(artifact, uri_cand)),
                        saw_mismatch,
                        None,
                    )
                saw_mismatch = True
            return None, saw_mismatch, None

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
            return Path(resolve_artifact_entry_path(artifact, cand)), saw_mismatch, None
        saw_mismatch = True

    return None, saw_mismatch, None


def resolve_verified_local_path_with_diagnostics(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    allow_recovery_roots: bool = True,
    ignore_runtime_abs_path: bool = False,
    verify_digest: bool = False,
    now_ns: Optional[int] = None,
) -> tuple[Optional[Path], bool]:
    """Resolve ``artifact`` returning ``(resolved_path, saw_digest_mismatch)``."""
    resolved, saw_mismatch, _row = resolve_verified_local_path_detailed(
        tracker,
        artifact,
        allow_recovery_roots=allow_recovery_roots,
        ignore_runtime_abs_path=ignore_runtime_abs_path,
        verify_digest=verify_digest,
        now_ns=now_ns,
    )
    return resolved, saw_mismatch


def resolve_verified_local_path(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    allow_recovery_roots: bool = True,
    ignore_runtime_abs_path: bool = False,
    verify_digest: bool = False,
    now_ns: Optional[int] = None,
) -> Optional[Path]:
    """Resolve ``artifact`` to a verified local ``Path`` or ``None`` if unavailable."""
    resolved, _, _ = resolve_verified_local_path_detailed(
        tracker,
        artifact,
        allow_recovery_roots=allow_recovery_roots,
        ignore_runtime_abs_path=ignore_runtime_abs_path,
        verify_digest=verify_digest,
        now_ns=now_ns,
    )
    return resolved


def _stage_single_file_atomic(
    source_file: Path,
    dest_file: Path,
    *,
    mutable: bool = False,
) -> None:
    _ = mutable
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = dest_file.parent / f".consist-stage-{uuid.uuid4().hex}"
    try:
        if not _try_reflink_clone(source_file, tmp_file):
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
    """Atomically stage verified ``source`` to ``destination`` via CoW clone or byte copy."""
    src = Path(os.path.realpath(source))
    if not verify_candidate_against_artifact(src, artifact):
        raise ValueError(
            f"Source {src} failed digest/manifest verification for artifact {artifact.key!r}."
        )
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
        stem = Path(entry).stem

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
            with os.scandir(src_bundle_root) as scan:
                member_names = sorted(
                    item.name
                    for item in scan
                    if item.is_file(follow_symlinks=False)
                    and (
                        item.name == entry
                        or is_shapefile_sidecar_filename(item.name, stem)
                    )
                )
        member_names_set = set(member_names)

        dest_verified = (
            dest_entry_path.exists()
            and not dest_entry_path.is_symlink()
            and verify_candidate_against_artifact(dest_entry_path, artifact)
        )
        if dest_verified:
            if not mutable or os.access(dest_entry_path, os.W_OK):
                return dest_entry_path.resolve()
        elif dest_bundle_dir.is_dir():
            existing_conflicts: list[Path] = []
            with os.scandir(dest_bundle_dir) as scan:
                for item in scan:
                    if item.name in member_names_set or is_shapefile_sidecar_filename(
                        item.name, stem
                    ):
                        existing_conflicts.append(Path(item.path))
            if existing_conflicts and not overwrite:
                raise FileExistsError(
                    f"Refusing to overwrite modified shapefile bundle at {dest_entry_path} "
                    f"for artifact {artifact.key!r}."
                )

        dest_bundle_dir.mkdir(parents=True, exist_ok=True)
        staged_tmps: list[tuple[Path, Path]] = []
        try:
            for name in member_names:
                src_member = src_bundle_root / name
                target_member = dest_bundle_dir / name
                if (
                    target_member.exists()
                    and not target_member.is_symlink()
                    and src_member.resolve() == target_member.resolve()
                ):
                    continue
                tmp_member = (
                    dest_bundle_dir / f".consist-stage-{uuid.uuid4().hex}-{name}"
                )
                _stage_single_file_atomic(src_member, tmp_member, mutable=mutable)
                staged_tmps.append((tmp_member, target_member))
            for tmp_member, target_member in staged_tmps:
                if target_member.is_symlink():
                    target_member.unlink()
                os.replace(tmp_member, target_member)
            if overwrite:
                with os.scandir(dest_bundle_dir) as scan:
                    for item in scan:
                        if (
                            item.name not in member_names_set
                            and is_shapefile_sidecar_filename(item.name, stem)
                        ):
                            p = Path(item.path)
                            if p.is_dir() and not p.is_symlink():
                                shutil.rmtree(p)
                            else:
                                p.unlink(missing_ok=True)
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
            else:
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


def ensure_local_artifact_path_core(
    tracker: "Tracker",
    artifact: Any,
    *,
    target_root: Optional[Path | str] = None,
    source_root: Optional[Path | str] = None,
    mutable: bool = False,
    on_missing: Literal["warn", "raise"] = "warn",
) -> Optional[Path]:
    """Ensure ``artifact`` has a verified local path, protecting archive/external sources when ``mutable=True``."""
    if isinstance(artifact, Artifact) and source_root is None:
        verified, _saw_mismatch, winning_row = resolve_verified_local_path_detailed(
            tracker, artifact
        )
        is_h5_container_member = artifact.driver == "h5_table" or (
            artifact.driver in {"h5", "hdf5"}
            and (artifact.parent_artifact_id is not None or artifact.member is not None)
        )
        m_name, _m_inst, _c_uri = tracker.fs.location_coordinate_for_uri(artifact.uri)
        is_workspace_uri = m_name in ("workspace", "runs")
        if mutable and target_root is None and not is_workspace_uri:
            raise ValueError(
                f"Cannot expose external-mount artifact {artifact.key!r} ({artifact.uri!r}) "
                "as mutable in-place; pass target_root=... to stage an independent copy."
            )
        if verified is not None and not is_h5_container_member:
            db = tracker.db if isinstance(tracker, _HasDb) else None

            def _stage_to_primary(dest_path: Path, *, is_mutable: bool) -> Path:
                staged = stage_verified_source(
                    artifact, verified, dest_path, mutable=is_mutable, overwrite=False
                )
                if db is not None and not db.read_only and artifact.id is not None:
                    db.upsert_artifact_locations(
                        [
                            build_verified_location_row(
                                tracker.fs, artifact, local_path=staged
                            )
                        ]
                    )
                tracker._record_artifact_abs_path_override(artifact, str(staged))
                return staged

            if target_root is None:
                primary: Optional[Path] = None
                try:
                    primary = Path(tracker.resolve_uri(artifact.uri)).resolve()
                except (ValueError, RuntimeError, OSError, TypeError):
                    primary = None

                if not mutable:
                    if primary is None or verified.resolve() == primary:
                        return verified
                    if (
                        is_workspace_uri
                        and not primary.exists()
                        and not primary.is_symlink()
                        and tracker.fs.get_remappable_relative_path(artifact.uri)
                        is not None
                    ):
                        return _stage_to_primary(primary, is_mutable=False)
                    return verified

                in_place_workspace = (
                    primary is not None
                    and verified.resolve() == primary
                    and os.access(verified, os.W_OK)
                    and (
                        (
                            winning_row is not None
                            and winning_row.site == "workspace"
                            and winning_row.mount_name in ("workspace", "runs")
                        )
                        or db is None
                    )
                )
                if in_place_workspace:
                    return verified
                if primary is not None and (primary.exists() or primary.is_symlink()):
                    raise FileExistsError(
                        f"Refusing to clobber modified workspace path {primary} or return "
                        f"non-workspace copy {verified} as mutable for artifact {artifact.key!r}; "
                        "pass target_root=... or remove the modified workspace file first."
                    )
                if (
                    primary is not None
                    and tracker.fs.get_remappable_relative_path(artifact.uri)
                    is not None
                ):
                    return _stage_to_primary(primary, is_mutable=True)
            else:
                rel = tracker.fs.get_remappable_relative_path(artifact.uri)
                if rel is not None:
                    dest = (Path(target_root).resolve() / rel).resolve()
                    try:
                        staged = stage_verified_source(
                            artifact,
                            verified,
                            dest,
                            mutable=mutable,
                            overwrite=False,
                        )
                        tracker._record_artifact_abs_path_override(
                            artifact, str(staged)
                        )
                        return staged
                    except (FileExistsError, ValueError, OSError) as exc:
                        if on_missing == "raise" or (
                            mutable and isinstance(exc, FileExistsError)
                        ):
                            raise
                        logging.warning(
                            "[Consist] Cannot stage artifact %r to %s: %s",
                            artifact.key,
                            dest,
                            exc,
                        )
                        return None

    resolved_target_root: Path | str = (
        target_root if target_root is not None else tracker.run_dir
    )
    result = tracker.materialize_artifact(
        artifact,
        target_root=resolved_target_root,
        source_root=source_root,
        preserve_existing=True,
        on_missing=on_missing,
        allow_external_paths=True if target_root is not None else None,
    )
    if result.path is not None and result.path.exists():
        if isinstance(artifact, Artifact):
            tracker._record_artifact_abs_path_override(artifact, str(result.path))
        return result.path
    if on_missing == "raise":
        key_repr = artifact.key if isinstance(artifact, Artifact) else artifact
        raise FileNotFoundError(
            f"[Consist] Cannot materialize artifact {key_repr!r}: "
            f"status={result.status}."
        )
    return None


class TrackerRecoveryService(_TrackerServiceBase):
    """Artifact staging and historical output recovery helpers for ``Tracker``."""

    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        """Materialize one artifact to an exact destination path."""
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
        """Stage one artifact for use outside a run lifecycle."""
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
        """Materialize one artifact below a caller-selected target root."""
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
        """Stage multiple keyed artifacts to exact destination paths."""
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
        """Materialize selected outputs from a completed historical run."""
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
        """Hydrate selected historical outputs with per-key recovery outcomes."""
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
        """Hydrate historical outputs to caller-selected exact destinations."""
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
