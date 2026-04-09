"""Historical output materialization and hydration helpers.

This module exposes two related recovery layers for prior run outputs:

- ``materialize_run_outputs(...)`` performs the physical copy/export work and
  returns the legacy aggregate ``MaterializationResult`` summary.
- ``hydrate_run_outputs(...)`` performs the same recovery work but returns a
  key-indexed ``HydratedRunOutputsResult`` with detached artifacts, resolved
  paths, and per-key statuses that are immediately usable in a new workspace.

The keyed hydration result is the higher-level API. The aggregate
``MaterializationResult`` remains the compatibility wrapper for callers that
only need summary buckets.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping as MappingABC
from dataclasses import dataclass, field
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Literal, Mapping, Sequence, TYPE_CHECKING, cast

import pandas as pd
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

from consist.core.stores import get_hot_data_engine
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager
    from consist.core.tracker import Tracker

HydrationStatus = Literal[
    "materialized_from_filesystem",
    "materialized_from_db",
    "preserved_existing",
    "skipped_unmapped",
    "missing_source",
    "failed",
]


@dataclass(slots=True)
class MaterializationResult:
    """Aggregate summary for historical run-output materialization.

    This is the compatibility result returned by
    ``Tracker.materialize_run_outputs(...)`` and the top-level
    ``consist.materialize_run_outputs(...)`` helper. It groups outcomes into
    summary buckets rather than returning one object per requested key.

    For new recovery flows, prefer ``HydratedRunOutputsResult`` because it
    preserves per-key status, detached artifacts, and directly usable paths.

    Attributes
    ----------
    materialized_from_filesystem : dict[str, str]
        Keys restored by copying bytes from historical filesystem locations.
        Values are the destination paths.
    materialized_from_db : dict[str, str]
        Keys reconstructed from DuckDB for ingested CSV/Parquet artifacts.
        Values are the destination paths.
    skipped_existing : list[str]
        Keys whose destination already existed and was preserved.
    skipped_unmapped : list[str]
        Keys that could not be mapped back to a historical relative path.
    skipped_missing_source : list[str]
        Keys whose historical bytes were unavailable and could not be recovered.
    failed : list[tuple[str, str]]
        Keys that failed during recovery, paired with the failure message.
    """
    materialized_from_filesystem: dict[str, str] = field(default_factory=dict)
    materialized_from_db: dict[str, str] = field(default_factory=dict)
    skipped_existing: list[str] = field(default_factory=list)
    skipped_unmapped: list[str] = field(default_factory=list)
    skipped_missing_source: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def materialized(self) -> dict[str, str]:
        return {
            **self.materialized_from_filesystem,
            **self.materialized_from_db,
        }

    @property
    def has_failures(self) -> bool:
        return bool(self.failed)

    @property
    def complete(self) -> bool:
        return not (self.skipped_unmapped or self.skipped_missing_source or self.failed)

    @property
    def summary(self) -> str:
        return (
            f"materialized_fs={len(self.materialized_from_filesystem)} "
            f"materialized_db={len(self.materialized_from_db)} "
            f"skipped_existing={len(self.skipped_existing)} "
            f"skipped_unmapped={len(self.skipped_unmapped)} "
            f"skipped_missing_source={len(self.skipped_missing_source)} "
            f"failed={len(self.failed)}"
        )


@dataclass(frozen=True, slots=True)
class HydratedRunOutput:
    """Per-key outcome for ``hydrate_run_outputs(...)``.

    Attributes
    ----------
    key : str
        Output key requested by the caller.
    artifact : Artifact
        Detached copy of the historical artifact record. When ``resolvable`` is
        ``True``, the detached artifact's runtime ``abs_path`` points at the
        hydrated destination so helpers like ``artifact.as_path()`` can be used
        directly in the new workspace.
    path : Path | None
        Destination path under the requested target root, when one is known.
        This can be populated even for non-resolvable outcomes to show the
        intended restore location.
    status : HydrationStatus
        One of:

        - ``"materialized_from_filesystem"``: copied from historical cold bytes.
        - ``"materialized_from_db"``: exported from DuckDB for an ingested
          CSV/Parquet artifact.
        - ``"preserved_existing"``: destination already existed and was reused.
        - ``"skipped_unmapped"``: no safe historical relative-path mapping was
          available.
        - ``"missing_source"``: historical bytes were unavailable and no DB
          fallback applied.
        - ``"failed"``: recovery attempted but failed due to a collision,
          policy check, or copy/export error.
    message : str | None
        Optional warning or error detail for ``"missing_source"`` and
        ``"failed"`` outcomes.
    resolvable : bool
        Whether the returned detached artifact is immediately usable from the
        hydrated workspace. When ``True``, ``path`` exists or is treated as the
        preserved reusable destination.
    """
    key: str
    artifact: Artifact
    path: Path | None
    status: HydrationStatus
    message: str | None = None
    resolvable: bool = False


@dataclass(slots=True)
class HydratedRunOutputsResult(MappingABC[str, HydratedRunOutput]):
    """Key-indexed recovery result for historical run-output hydration.

    This mapping is returned by ``hydrate_run_outputs(...)`` and preserves the
    caller's requested key order. Each value carries the detached artifact,
    destination path, and per-key status.

    New users should generally start with this result because it answers the
    three practical questions in one object: which keys were requested, what
    path each key resolved to, and whether the returned artifact is immediately
    usable in the current workspace.

    Examples
    --------
    >>> hydrated = tracker.hydrate_run_outputs(
    ...     "prior_run",
    ...     keys=["persons", "households"],
    ...     target_root="restored",
    ... )
    >>> hydrated["persons"].status
    'materialized_from_filesystem'
    >>> hydrated.paths["persons"].name
    'persons.parquet'
    """
    outputs: dict[str, HydratedRunOutput] = field(default_factory=dict)

    def __getitem__(self, key: str) -> HydratedRunOutput:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def items(self):
        return self.outputs.items()

    def keys(self):
        return self.outputs.keys()

    def values(self):
        return self.outputs.values()

    @property
    def paths(self) -> dict[str, Path]:
        return {
            key: output.path
            for key, output in self.outputs.items()
            if output.path is not None and output.resolvable
        }

    @property
    def resolvable(self) -> dict[str, HydratedRunOutput]:
        return {
            key: output for key, output in self.outputs.items() if output.resolvable
        }

    @property
    def failed_keys(self) -> list[str]:
        return [
            key for key, output in self.outputs.items() if output.status == "failed"
        ]

    @property
    def complete(self) -> bool:
        incomplete_statuses = {"skipped_unmapped", "missing_source", "failed"}
        return all(
            output.status not in incomplete_statuses for output in self.outputs.values()
        )

    @property
    def summary(self) -> str:
        counts: dict[str, int] = {
            "materialized_from_filesystem": 0,
            "materialized_from_db": 0,
            "preserved_existing": 0,
            "skipped_unmapped": 0,
            "missing_source": 0,
            "failed": 0,
        }
        for output in self.outputs.values():
            counts[output.status] += 1
        return (
            f"materialized_fs={counts['materialized_from_filesystem']} "
            f"materialized_db={counts['materialized_from_db']} "
            f"preserved_existing={counts['preserved_existing']} "
            f"skipped_unmapped={counts['skipped_unmapped']} "
            f"missing_source={counts['missing_source']} "
            f"failed={counts['failed']}"
        )


@dataclass(frozen=True, slots=True)
class PlannedMaterialization:
    artifact: Artifact
    keys: tuple[str, ...]
    source_kind: Literal["filesystem", "db_export"]
    source_path: Path | None
    destination: Path
    relative_path: Path
    artifacts_by_key: Mapping[str, Artifact] | None = None

    def __post_init__(self) -> None:
        if self.artifacts_by_key is None:
            object.__setattr__(
                self,
                "artifacts_by_key",
                {key: self.artifact for key in self.keys},
            )


@dataclass(frozen=True, slots=True)
class PlannedRunOutputHydration:
    key_order: tuple[str, ...]
    plan: list[PlannedMaterialization]
    preflight_results: HydratedRunOutputsResult


def _detach_artifact_for_hydration(
    artifact: Artifact,
    *,
    hydrated_path: Path | None,
) -> Artifact:
    clone = artifact.model_copy(deep=True)
    private_state = getattr(clone, "__pydantic_private__", None)
    if private_state is None:
        try:
            object.__setattr__(clone, "__pydantic_private__", {})
        except Exception:
            pass
        private_state = getattr(clone, "__pydantic_private__", None)
    if isinstance(private_state, dict):
        private_state["_tracker"] = None
        private_state["_abs_path"] = (
            str(hydrated_path.resolve()) if hydrated_path is not None else None
        )
    try:
        object.__setattr__(clone, "_tracker", None)
    except Exception:
        pass
    try:
        object.__setattr__(
            clone,
            "_abs_path",
            str(hydrated_path.resolve()) if hydrated_path is not None else None,
        )
    except Exception:
        pass
    return clone


def _make_hydrated_output(
    artifact: Artifact,
    *,
    status: HydrationStatus,
    path: Path | None,
    message: str | None = None,
    resolvable: bool,
) -> HydratedRunOutput:
    normalized_path = path.resolve() if path is not None else None
    detached = _detach_artifact_for_hydration(
        artifact,
        hydrated_path=normalized_path if resolvable else None,
    )
    return HydratedRunOutput(
        key=artifact.key,
        artifact=detached,
        path=normalized_path,
        status=status,
        message=message,
        resolvable=resolvable,
    )


def _ordered_hydrated_results(
    key_order: Sequence[str],
    outputs: Mapping[str, HydratedRunOutput],
) -> HydratedRunOutputsResult:
    ordered: dict[str, HydratedRunOutput] = {}
    for key in key_order:
        output = outputs.get(key)
        if output is not None:
            ordered[key] = output
    for key, output in outputs.items():
        if key not in ordered:
            ordered[key] = output
    return HydratedRunOutputsResult(outputs=ordered)


def fold_hydrated_run_outputs_result(
    result: HydratedRunOutputsResult,
) -> MaterializationResult:
    """Fold keyed hydration outcomes into the legacy summary result.

    Parameters
    ----------
    result : HydratedRunOutputsResult
        Key-indexed hydration outcomes produced by the shared recovery core.

    Returns
    -------
    MaterializationResult
        Aggregate summary that preserves the older materialization API shape.
    """
    folded = MaterializationResult()
    for key, output in result.items():
        if output.status == "materialized_from_filesystem" and output.path is not None:
            folded.materialized_from_filesystem[key] = str(output.path)
        elif output.status == "materialized_from_db" and output.path is not None:
            folded.materialized_from_db[key] = str(output.path)
        elif output.status == "preserved_existing":
            folded.skipped_existing.append(key)
        elif output.status == "skipped_unmapped":
            folded.skipped_unmapped.append(key)
        elif output.status == "missing_source":
            folded.skipped_missing_source.append(key)
        elif output.status == "failed":
            folded.failed.append((key, output.message or "unknown failure"))
    return folded


def _ensure_destination_not_symlink(path: Path) -> None:
    if path.exists() and path.is_symlink():
        raise ValueError(f"Symlink detected in destination path: {path}")


def _copy_file_atomic(source: Path, destination: Path) -> bool:
    fd, tmp_path = tempfile.mkstemp(dir=str(destination.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        shutil.copy2(source, tmp_path_obj)
        try:
            os.link(tmp_path_obj, destination)
        except FileExistsError:
            return False
        return True
    finally:
        try:
            tmp_path_obj.unlink()
        except FileNotFoundError:
            pass


def _copy_dir_safe(source: Path, destination: Path) -> bool:
    try:
        destination.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        return False

    try:
        shutil.copytree(source, destination, dirs_exist_ok=True)
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    return True


def _copy_dir_to_temp(source: Path, parent: Path) -> Path:
    temporary_root = Path(tempfile.mkdtemp(dir=str(parent), prefix=".consist-dir-"))
    tmp_destination = temporary_root / "payload"
    shutil.copytree(source, tmp_destination, dirs_exist_ok=False)
    return tmp_destination


def _cleanup_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _replace_path(source: Path, destination: Path) -> None:
    backup_path: Path | None = None
    backup_root: Path | None = None
    if destination.exists():
        backup_root = Path(
            tempfile.mkdtemp(
                dir=str(destination.parent),
                prefix=f".consist-backup-{destination.name}-",
            )
        )
        backup_path = backup_root / "payload"
        destination.rename(backup_path)

    try:
        os.replace(source, destination)
    except Exception:
        if backup_path is not None and backup_path.exists():
            os.replace(backup_path, destination)
        raise
    else:
        if backup_root is not None:
            _cleanup_path(backup_root)


def _materialize_path(
    *,
    source: Path,
    destination: Path,
    preserve_existing: bool,
) -> tuple[bool, bool]:
    """
    Return ``(materialized, skipped_existing)`` for one filesystem operation.
    """
    _ensure_destination_not_symlink(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if source == destination:
        return True, False

    if destination.exists():
        if preserve_existing:
            if source.is_dir() != destination.is_dir():
                raise ValueError(
                    f"Destination type mismatch for {destination}; refusing to overwrite."
                )
            return False, True

        if destination.is_symlink():
            raise ValueError(f"Symlink detected in destination path: {destination}")

    if preserve_existing:
        if source.is_dir():
            copied = _copy_dir_safe(source, destination)
        else:
            copied = _copy_file_atomic(source, destination)
        if copied or destination.exists():
            return True, False
        return False, True

    if source.is_dir():
        tmp_copy = _copy_dir_to_temp(source, destination.parent)
        _replace_path(tmp_copy, destination)
        _cleanup_path(tmp_copy.parent)
        return True, False

    fd, tmp_path = tempfile.mkstemp(dir=str(destination.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        shutil.copy2(source, tmp_path_obj)
        _replace_path(tmp_path_obj, destination)
    finally:
        try:
            tmp_path_obj.unlink()
        except FileNotFoundError:
            pass
    return True, False


def build_allowed_materialization_roots(
    *,
    run_dir: Path,
    mounts: Mapping[str, str] | None = None,
    allow_external_paths: bool,
) -> tuple[Path, ...] | None:
    if allow_external_paths:
        return None

    roots: list[Path] = []
    for candidate in [Path(run_dir), *(Path(root) for root in (mounts or {}).values())]:
        resolved = candidate.resolve()
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots)


def _normalize_allowed_roots(
    allowed_base: Path | Sequence[Path] | None,
) -> tuple[Path, ...] | None:
    if allowed_base is None:
        return None
    if isinstance(allowed_base, Path):
        return (allowed_base.resolve(),)

    roots: list[Path] = []
    for root in allowed_base:
        resolved = Path(root).resolve()
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots)


def validate_allowed_materialization_destination(
    destination: Path,
    allowed_base: Path | Sequence[Path] | None,
) -> None:
    allowed_roots = _normalize_allowed_roots(allowed_base)
    if not allowed_roots:
        return

    resolved_destination = destination.resolve()
    for root in allowed_roots:
        try:
            resolved_destination.relative_to(root)
            return
        except ValueError:
            continue

    if len(allowed_roots) == 1:
        allowed_root = allowed_roots[0]
        raise ValueError(
            f"Destination path {resolved_destination} is outside allowed base "
            f"{allowed_root}. Set allow_external_paths=True or "
            "CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
        )

    allowed_display = ", ".join(str(root) for root in allowed_roots)
    raise ValueError(
        f"Destination path {resolved_destination} is outside allowed roots: "
        f"{allowed_display}. Set allow_external_paths=True or "
        "CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
    )


def _validate_allowed_base(
    destination: Path, allowed_base: Path | Sequence[Path] | None
) -> None:
    validate_allowed_materialization_destination(destination, allowed_base)


def _get_output_artifacts_for_run(tracker: "Tracker", run_id: str) -> list[Artifact]:
    db = cast("DatabaseManager | None", getattr(tracker, "db", None))
    if db is None:
        return []

    get_raw_outputs = getattr(db, "get_output_artifacts_for_run", None)
    if callable(get_raw_outputs):
        return list(get_raw_outputs(run_id))

    raw_rows = db.get_artifacts_for_run(run_id)
    return [artifact for artifact, direction in raw_rows if direction == "output"]


def _get_artifact_owning_run(
    tracker: "Tracker",
    *,
    selected_run,
    artifact: Artifact,
):
    artifact_run_id = getattr(artifact, "run_id", None)
    if not artifact_run_id or artifact_run_id == selected_run.id:
        return selected_run
    db = cast("DatabaseManager | None", getattr(tracker, "db", None))
    if db is None:
        return selected_run
    producing_run = db.get_run(str(artifact_run_id))
    return producing_run or selected_run


def _derive_historical_remap(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    run,
) -> tuple[Path, Path] | None:
    meta = run.meta if isinstance(run.meta, dict) else {}
    artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}

    helper = getattr(tracker.fs, "get_historical_remap", None)
    if callable(helper):
        return helper(
            artifact.container_uri,
            original_run_dir=meta.get("_physical_run_dir"),
            mounts_snapshot=meta.get("mounts"),
            artifact_mount_root=artifact_meta.get("mount_root"),
        )

    return None


def _source_identity(item: PlannedMaterialization) -> tuple[str, str]:
    if item.source_kind == "filesystem":
        assert item.source_path is not None
        return ("filesystem", str(item.source_path.resolve()))
    return ("db_export", str(item.artifact.id))


def plan_run_output_hydration(
    tracker: "Tracker",
    run,
    *,
    target_root: Path,
    source_root: Path | None,
    keys: Sequence[str] | None,
    preserve_existing: bool,
    db_fallback: Literal["never", "if_ingested"],
) -> PlannedRunOutputHydration:
    """Plan historical output hydration for a run without mutating the target.

    The planner resolves which artifacts belong to the requested keys, derives
    their historical relative paths, chooses a recovery source for each output,
    and emits preflight results for outcomes that do not need execution.

    Preflight outcomes include:

    - unknown keys (raised immediately)
    - ambiguous duplicate output keys on the run (raised immediately)
    - unmapped historical outputs
    - destination collisions
    - ``preserve_existing=True`` short-circuits

    Parameters
    ----------
    tracker : Tracker
        Tracker providing DB access, filesystem remapping, and mount policy.
    run
        Historical run record whose outputs are being restored.
    target_root : Path
        Root directory under which historical relative layout is recreated.
    source_root : Path | None
        Optional override root to probe before the original historical source.
    keys : Sequence[str] | None
        Specific output keys to restore. ``None`` means all run outputs.
    preserve_existing : bool
        Whether existing destinations should be treated as reusable.
    db_fallback : {"never", "if_ingested"}
        Whether ingested CSV/Parquet artifacts may be exported from DuckDB when
        cold bytes are unavailable.
    """
    outputs: dict[str, HydratedRunOutput] = {}
    raw_outputs = _get_output_artifacts_for_run(tracker, run.id)

    key_counts: dict[str, int] = {}
    for artifact in raw_outputs:
        key_counts[artifact.key] = key_counts.get(artifact.key, 0) + 1
    duplicate_keys = sorted(key for key, count in key_counts.items() if count > 1)
    if duplicate_keys:
        raise ValueError(
            "Run has duplicate output keys; run-scoped materialization is ambiguous: "
            + ", ".join(repr(key) for key in duplicate_keys)
        )

    outputs_by_key = {artifact.key: artifact for artifact in raw_outputs}
    key_order: tuple[str, ...]
    if keys is not None:
        requested_keys = list(keys)
        missing_keys = [key for key in requested_keys if key not in outputs_by_key]
        if missing_keys:
            raise KeyError(
                "Requested output keys were not found for run "
                f"{run.id!r}: {', '.join(repr(key) for key in missing_keys)}"
            )
        selected_outputs = [outputs_by_key[key] for key in requested_keys]
        key_order = tuple(requested_keys)
    else:
        selected_outputs = list(raw_outputs)
        key_order = tuple(artifact.key for artifact in selected_outputs)

    planned_by_destination: dict[Path, PlannedMaterialization] = {}
    conflicted_destinations: set[Path] = set()

    for artifact in selected_outputs:
        owning_run = _get_artifact_owning_run(
            tracker, selected_run=run, artifact=artifact
        )
        remap = _derive_historical_remap(tracker, artifact=artifact, run=owning_run)
        if remap is None:
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="skipped_unmapped",
                path=None,
                resolvable=False,
            )
            continue

        historical_root, relative_path = remap
        destination = target_root / relative_path

        historical_source = (historical_root / relative_path).resolve()
        override_source = (
            (source_root / relative_path).resolve() if source_root is not None else None
        )

        source_kind: Literal["filesystem", "db_export"]
        source_path: Path | None
        if override_source is not None and override_source.exists():
            source_kind = "filesystem"
            source_path = override_source
        elif historical_source.exists():
            source_kind = "filesystem"
            source_path = historical_source
        else:
            artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}
            if db_fallback == "if_ingested" and artifact_meta.get("is_ingested", False):
                driver = str(artifact.driver or "").lower()
                if driver not in {"csv", "parquet"}:
                    outputs[artifact.key] = _make_hydrated_output(
                        artifact,
                        status="failed",
                        path=destination,
                        message=(
                            "unsupported DB export for ingested driver "
                            f"{artifact.driver!r}"
                        ),
                        resolvable=False,
                    )
                    continue
                source_kind = "db_export"
                source_path = None
            else:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="missing_source",
                    path=destination,
                    resolvable=False,
                )
                continue

        planned = PlannedMaterialization(
            artifact=artifact,
            keys=(artifact.key,),
            source_kind=source_kind,
            source_path=source_path,
            destination=destination,
            relative_path=relative_path,
            artifacts_by_key={artifact.key: artifact},
        )

        if destination in conflicted_destinations:
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="failed",
                path=destination,
                message=f"destination collision at {destination}",
                resolvable=False,
            )
            continue

        existing = planned_by_destination.get(destination)
        if existing is None:
            planned_by_destination[destination] = planned
            continue

        if _source_identity(existing) == _source_identity(planned):
            combined_artifacts = dict(existing.artifacts_by_key or {})
            combined_artifacts[artifact.key] = artifact
            planned_by_destination[destination] = PlannedMaterialization(
                artifact=existing.artifact,
                keys=existing.keys + (artifact.key,),
                source_kind=existing.source_kind,
                source_path=existing.source_path,
                destination=existing.destination,
                relative_path=existing.relative_path,
                artifacts_by_key=combined_artifacts,
            )
            continue

        for key in existing.keys + (artifact.key,):
            collision_artifact = (
                (
                    existing.artifacts_by_key
                    or {existing.artifact.key: existing.artifact}
                )[key]
                if key in (existing.artifacts_by_key or {})
                else artifact
            )
            outputs[key] = _make_hydrated_output(
                collision_artifact,
                status="failed",
                path=destination,
                message=f"destination collision at {destination}",
                resolvable=False,
            )
        conflicted_destinations.add(destination)
        del planned_by_destination[destination]

    plan: list[PlannedMaterialization] = []
    for item in sorted(
        planned_by_destination.values(),
        key=lambda item: (str(item.destination), item.keys),
    ):
        if preserve_existing and item.destination.exists():
            if item.destination.is_symlink():
                for key in item.keys:
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status="failed",
                        path=item.destination,
                        message=f"Symlink detected in destination path: {item.destination}",
                        resolvable=False,
                    )
            else:
                for key in item.keys:
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status="preserved_existing",
                        path=item.destination,
                        resolvable=True,
                    )
            continue
        plan.append(item)
    return PlannedRunOutputHydration(
        key_order=key_order,
        plan=plan,
        preflight_results=_ordered_hydrated_results(key_order, outputs),
    )


def build_run_output_materialize_plan(
    tracker: "Tracker",
    run,
    *,
    target_root: Path,
    source_root: Path | None,
    keys: Sequence[str] | None,
    preserve_existing: bool,
    db_fallback: Literal["never", "if_ingested"],
) -> tuple[list[PlannedMaterialization], MaterializationResult]:
    """Build the legacy materialization plan and aggregate preflight result."""
    planned = plan_run_output_hydration(
        tracker,
        run,
        target_root=target_root,
        source_root=source_root,
        keys=keys,
        preserve_existing=preserve_existing,
        db_fallback=db_fallback,
    )
    return planned.plan, fold_hydrated_run_outputs_result(planned.preflight_results)


def execute_planned_output_hydration(
    plan: Sequence[PlannedMaterialization],
    *,
    tracker: "Tracker",
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
    preserve_existing: bool = True,
) -> HydratedRunOutputsResult:
    """Execute a planned historical output hydration.

    Parameters
    ----------
    plan : Sequence[PlannedMaterialization]
        Planned filesystem copies or DuckDB exports to execute.
    tracker : Tracker
        Tracker used for DB exports and path policy.
    allowed_base : Path | Sequence[Path] | None
        Allowed destination roots. ``None`` disables destination-root checks.
    on_missing : {"warn", "raise"}, default "warn"
        Error policy for missing source bytes and execution failures.
    preserve_existing : bool, default True
        Whether existing destinations should be kept in place.

    Returns
    -------
    HydratedRunOutputsResult
        Per-key execution outcomes for the plan. Preflight-only results are not
        included; callers merge them separately.
    """
    outputs: dict[str, HydratedRunOutput] = {}

    for item in plan:
        try:
            _validate_allowed_base(item.destination, allowed_base)

            if item.source_kind == "filesystem":
                if item.source_path is None or not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                materialized, skipped_existing = _materialize_path(
                    source=item.source_path,
                    destination=item.destination,
                    preserve_existing=preserve_existing,
                )
                if skipped_existing:
                    for key in item.keys:
                        artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[
                            key
                        ]
                        outputs[key] = _make_hydrated_output(
                            artifact,
                            status="preserved_existing",
                            path=item.destination,
                            resolvable=True,
                        )
                    continue
                if materialized:
                    for key in item.keys:
                        artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[
                            key
                        ]
                        outputs[key] = _make_hydrated_output(
                            artifact,
                            status="materialized_from_filesystem",
                            path=item.destination,
                            resolvable=True,
                        )
                continue

            destination_str = materialize_ingested_artifact_from_db(
                artifact=item.artifact,
                tracker=tracker,
                destination=item.destination,
                overwrite=not preserve_existing,
            )
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="materialized_from_db",
                    path=Path(destination_str),
                    resolvable=True,
                )

        except FileNotFoundError as exc:
            if on_missing == "raise":
                raise
            logging.warning(
                "[Consist] Cannot materialize run output %s: %s",
                ",".join(repr(key) for key in item.keys),
                exc,
            )
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="missing_source",
                    path=item.destination,
                    message=str(exc),
                    resolvable=False,
                )
        except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
            if on_missing == "raise":
                raise RuntimeError(
                    f"Failed to materialize run outputs for keys {item.keys!r}: {exc}"
                ) from exc
            logging.warning(
                "[Consist] Failed to materialize run output %s -> %s: %s",
                ",".join(repr(key) for key in item.keys),
                item.destination,
                exc,
            )
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="failed",
                    path=item.destination,
                    message=str(exc),
                    resolvable=False,
                )

    key_order = tuple(key for item in plan for key in item.keys)
    return _ordered_hydrated_results(key_order, outputs)


def materialize_planned_outputs(
    plan: Sequence[PlannedMaterialization],
    *,
    tracker: "Tracker",
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
    preserve_existing: bool = True,
) -> MaterializationResult:
    """Execute a plan and fold results into ``MaterializationResult``."""
    return fold_hydrated_run_outputs_result(
        execute_planned_output_hydration(
            plan,
            tracker=tracker,
            allowed_base=allowed_base,
            on_missing=on_missing,
            preserve_existing=preserve_existing,
        )
    )


def hydrate_run_outputs(
    tracker: "Tracker",
    run,
    *,
    target_root: Path,
    source_root: Path | None,
    keys: Sequence[str] | None,
    allowed_base: Path | Sequence[Path] | None,
    preserve_existing: bool,
    on_missing: Literal["warn", "raise"],
    db_fallback: Literal["never", "if_ingested"],
) -> HydratedRunOutputsResult:
    """Hydrate selected historical outputs into a target workspace root.

    This is the shared core behind ``Tracker.hydrate_run_outputs(...)`` and the
    top-level ``consist.hydrate_run_outputs(...)`` helper. It performs planning
    and execution, then returns a keyed result that combines preflight outcomes
    with executed copy/export work.

    Unlike ``materialize_run_outputs(...)``, this function returns detached
    artifacts whose runtime ``abs_path`` points at the hydrated destination when
    the outcome is resolvable. That makes the result suitable for restart and
    cross-workspace recovery flows that want to use the returned artifact/path
    directly instead of separately looking it up again.
    """
    planned = plan_run_output_hydration(
        tracker,
        run,
        target_root=target_root,
        source_root=source_root,
        keys=keys,
        preserve_existing=preserve_existing,
        db_fallback=db_fallback,
    )
    executed = execute_planned_output_hydration(
        planned.plan,
        tracker=tracker,
        allowed_base=allowed_base,
        on_missing=on_missing,
        preserve_existing=preserve_existing,
    )
    merged = dict(planned.preflight_results.items())
    merged.update(executed.items())
    return _ordered_hydrated_results(planned.key_order, merged)


def materialize_artifacts(
    tracker: "Tracker",
    items: Sequence[tuple[Artifact, Path]],
    *,
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """
    Synchronize cached artifact bytes to specific filesystem destinations.

    This function performs physical materialization by copying the resolved source
    data of cached artifacts to the requested locations. It ensures atomic
    file writes and safe directory recursion. If the destination already contains
    matching data, the operation is bypassed to prevent redundant I/O.

    Parameters
    ----------
    tracker : Tracker
        The active Tracker instance used to resolve virtualized artifact URIs
        to physical host filesystem paths.
    items : Sequence[tuple[Artifact, Path]]
        A collection of (Artifact, Path) pairs defining the source artifacts
        and their respective target destinations.
    on_missing : {"warn", "raise"}, default "warn"
        The error handling policy for cases where the resolved source path
        is absent from the filesystem.

    Returns
    -------
    dict[str, str]
        A mapping of artifact keys to their successfully materialized
        absolute filesystem paths.
    """
    materialized: dict[str, str] = {}

    for artifact, destination in items:
        destination_path = Path(destination).resolve()
        _ensure_destination_not_symlink(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            source_path = Path(tracker.resolve_uri(artifact.container_uri)).resolve()
        except (OSError, ValueError) as e:
            msg = f"[Consist] Failed to resolve URI for artifact {artifact.key!r}: {e}"
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)
            continue

        if not source_path.exists():
            msg = (
                f"[Consist] Cannot materialize cached output {artifact.key!r}: "
                f"resolved source path missing ({source_path})."
            )
            if on_missing == "raise":
                raise FileNotFoundError(msg)
            logging.warning(msg)
            continue

        try:
            if destination_path.exists():
                if destination_path.is_symlink():
                    raise ValueError(
                        f"Symlink detected in destination path: {destination_path}"
                    )
                if source_path.is_dir() != destination_path.is_dir():
                    raise ValueError(
                        f"Destination type mismatch for {destination_path}; "
                        "refusing to overwrite."
                    )
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path.is_dir():
                copied = _copy_dir_safe(source_path, destination_path)
            else:
                copied = _copy_file_atomic(source_path, destination_path)
            if copied:
                materialized[artifact.key] = str(destination_path)
            elif destination_path.exists():
                materialized[artifact.key] = str(destination_path)
        except (OSError, shutil.Error, ValueError) as e:
            msg = (
                f"[Consist] Failed to materialize cached output {artifact.key!r} "
                f"from {source_path} -> {destination_path}: {e}"
            )
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)

    return materialized


def materialize_artifacts_from_sources(
    items: Sequence[tuple[Artifact, Path, Path]],
    *,
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """
    Rehydrate artifacts from explicit source paths to specified destinations.

    This specialized materialization utility is typically employed during
    cross-run hydration (e.g., 'inputs-missing' mode) where the original
    on-disk source is located in a historical run directory. It enforces
    directory containment security via the `allowed_base` parameter.

    Parameters
    ----------
    items : Sequence[tuple[Artifact, Path, Path]]
        A collection of (Artifact, SourcePath, DestinationPath) triples
        defining the explicit rehydration mapping.
    allowed_base : Path | None
        A security boundary for the materialization. If provided, all
        destination paths must reside within this directory tree.
    on_missing : {"warn", "raise"}, default "warn"
        The error handling policy for cases where the explicit source path
        is absent from the filesystem.

    Returns
    -------
    dict[str, str]
        A mapping of artifact keys to their successfully materialized
        absolute filesystem paths.
    """
    materialized: dict[str, str] = {}
    for artifact, source, destination in items:
        source_path = Path(source).resolve()
        destination_path = Path(destination).resolve()
        validate_allowed_materialization_destination(destination_path, allowed_base)
        _ensure_destination_not_symlink(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        if not source_path.exists():
            msg = (
                f"[Consist] Cannot materialize cached input {artifact.key!r}: "
                f"source path missing ({source_path})."
            )
            if on_missing == "raise":
                raise FileNotFoundError(msg)
            logging.warning(msg)
            continue

        try:
            if destination_path.exists():
                if destination_path.is_symlink():
                    raise ValueError(
                        f"Symlink detected in destination path: {destination_path}"
                    )
                if source_path.is_dir() != destination_path.is_dir():
                    raise ValueError(
                        f"Destination type mismatch for {destination_path}; "
                        "refusing to overwrite."
                    )
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path.is_dir():
                copied = _copy_dir_safe(source_path, destination_path)
            else:
                copied = _copy_file_atomic(source_path, destination_path)
            if copied:
                materialized[artifact.key] = str(destination_path)
            elif destination_path.exists():
                materialized[artifact.key] = str(destination_path)
        except (OSError, shutil.Error, ValueError) as e:
            msg = (
                f"[Consist] Failed to materialize cached input {artifact.key!r} "
                f"from {source_path} -> {destination_path}: {e}"
            )
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)

    return materialized


def build_materialize_items_for_keys(
    outputs: Iterable[Artifact],
    *,
    destinations_by_key: dict[str, Path],
) -> list[tuple[Artifact, Path]]:
    """
    Construct materialization mappings by correlating artifacts with target keys.

    This helper facilitates the preparation of materialization payloads by
    matching a collection of candidate artifacts against a set of desired
    destination keys.

    Parameters
    ----------
    outputs : Iterable[Artifact]
        The collection of candidate artifacts available for materialization.
    destinations_by_key : dict[str, Path]
        A mapping of artifact keys to their intended target filesystem paths.

    Returns
    -------
    list[tuple[Artifact, Path]]
        A list of (Artifact, Path) pairs ready for the `materialize_artifacts`
        utility.
    """
    outputs_by_key = {a.key: a for a in outputs}
    items: list[tuple[Artifact, Path]] = []
    for key, dest in destinations_by_key.items():
        artifact = outputs_by_key.get(key)
        if artifact is not None:
            items.append((artifact, Path(dest)))
    return items


def materialize_ingested_artifact_from_db(
    *,
    artifact: Artifact,
    tracker: "Tracker",
    destination: Path,
    overwrite: bool = False,
) -> str:
    """
    Reconstruct an ingested artifact from the analytical database.

    This recovery mechanism is utilized when a cached artifact is required but
    its physical materialization has been purged from the filesystem. If the
    artifact was previously ingested, its data is exported from DuckDB to
    the specified destination.

    Parameters
    ----------
    artifact : Artifact
        The metadata record of the artifact to be reconstructed.
    tracker : Tracker
        The active Tracker instance containing the analytical engine and
        database connection.
    destination : Path
        The target filesystem path where the reconstructed data will be
        materialized.

    Returns
    -------
    str
        The absolute filesystem path of the reconstructed artifact.

    Raises
    ------
    RuntimeError
        If the database engine is unavailable or the source table cannot be resolved.
    ValueError
        If the artifact is not marked as ingested or uses an unsupported
        materialization driver.
    """
    hot_data_engine = get_hot_data_engine(tracker)
    if not tracker or hot_data_engine is None:
        raise RuntimeError(
            "Cannot materialize ingested artifact: tracker has no DB engine."
        )
    if not artifact.meta.get("is_ingested", False):
        raise ValueError(
            f"Artifact {artifact.key!r} is not marked as ingested; "
            "cannot reconstruct from DB."
        )

    driver = str(artifact.driver or "").lower()
    if driver not in {"csv", "parquet"}:
        raise ValueError(
            "Only csv/parquet artifacts can be reconstructed from DuckDB "
            f"(got driver={artifact.driver!r})."
        )

    table_name = artifact.meta.get("dlt_table_name") or artifact.key
    if not isinstance(table_name, str) or not table_name:
        raise ValueError("Artifact table name is missing; cannot reconstruct from DB.")

    metadata = MetaData()
    try:
        table = Table(
            table_name,
            metadata,
            schema="global_tables",
            autoload_with=hot_data_engine,
        )
    except SQLAlchemyError as e:
        raise RuntimeError(
            f"Failed to reflect table 'global_tables.{table_name}': {e}"
        ) from e

    if "consist_artifact_id" not in table.c:
        raise RuntimeError(
            f"Table 'global_tables.{table_name}' is missing consist_artifact_id."
        )

    stmt = select(table).where(table.c.consist_artifact_id == str(artifact.id))
    df = pd.read_sql(stmt, hot_data_engine)
    if df.empty:
        raise FileNotFoundError(
            f"No ingested rows found for artifact {artifact.key!r} ({artifact.id})."
        )

    destination_path = Path(destination).resolve()
    _ensure_destination_not_symlink(destination_path)
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if destination_path.exists() and destination_path.is_dir():
        if not overwrite:
            raise ValueError(
                f"Destination type mismatch for {destination_path}; refusing to overwrite."
            )

    fd, tmp_path = tempfile.mkstemp(dir=str(destination_path.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        if driver == "csv":
            df.to_csv(tmp_path_obj, index=False)
        else:
            df.to_parquet(tmp_path_obj, index=False)

        if destination_path.exists() and not overwrite:
            tmp_path_obj.unlink(missing_ok=True)
            return str(destination_path)

        if overwrite:
            _replace_path(tmp_path_obj, destination_path)
        else:
            try:
                os.link(tmp_path_obj, destination_path)
            except FileExistsError:
                return str(destination_path)
    finally:
        try:
            tmp_path_obj.unlink()
        except FileNotFoundError:
            pass

    return str(destination_path)
