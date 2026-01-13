from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Literal, Sequence, TYPE_CHECKING

import pandas as pd
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


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


def materialize_artifacts(
    tracker: "Tracker",
    items: Sequence[tuple[Artifact, Path]],
    *,
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """
    Copy cached artifact bytes onto the filesystem at caller-specified destinations.

    This is intentionally *copy-only* materialization:
    - If the artifact's resolved source path exists, it is copied to the destination.
    - If it does not exist, behavior depends on `on_missing`.
    - If the destination already exists, it is left untouched.
    - Destination paths cannot be symlinks and will not be overwritten if the type differs.

    This function does not attempt database-backed reconstruction. Higher-level code
    can decide when to call `consist.load(...)` or enable DB recovery.

    Parameters
    ----------
    tracker : Tracker
        Tracker used to resolve portable artifact URIs to host filesystem paths.
    items : Sequence[tuple[Artifact, Path]]
        A list of `(artifact, destination_path)` pairs to materialize.
    on_missing : {"warn", "raise"}, default "warn"
        What to do when a resolved source path does not exist.

    Returns
    -------
    dict[str, str]
        Map of `artifact.key -> destination_path` for successfully materialized items.
    """
    materialized: dict[str, str] = {}

    for artifact, destination in items:
        destination_path = Path(destination).resolve()
        _ensure_destination_not_symlink(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            source_path = Path(tracker.resolve_uri(artifact.uri)).resolve()
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
    allowed_base: Path | None,
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """
    Copy artifact bytes from explicit source paths to caller-specified destinations.

    This is useful for rehydrating cached inputs from historical run directories
    when the current run directory is different from the original producer.
    Existing destination paths are left untouched.
    Destination paths cannot be symlinks and will not be overwritten if the type differs.

    Parameters
    ----------
    allowed_base : Path | None
        Base directory that destination paths must remain within. When None,
        no base containment check is performed.
    """
    materialized: dict[str, str] = {}
    allowed_base_path = Path(allowed_base).resolve() if allowed_base else None

    for artifact, source, destination in items:
        source_path = Path(source).resolve()
        destination_path = Path(destination).resolve()
        if allowed_base_path is not None:
            try:
                destination_path.relative_to(allowed_base_path)
            except ValueError as exc:
                raise ValueError(
                    f"Destination path {destination_path} is outside allowed base "
                    f"{allowed_base_path}. Set allow_external_paths=True or "
                    "CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
                ) from exc
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
    Convenience helper to construct `(artifact, destination)` pairs by artifact key.

    Any keys not present in `outputs` are ignored (caller decides whether to treat
    this as an error).
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
) -> str:
    """
    Reconstruct a CSV/Parquet artifact from DuckDB and write it to disk.

    This is intended for `cache_hydration="inputs-missing"` when the original
    on-disk source is missing but the artifact is ingested (`is_ingested=True`).

    Supported drivers: csv, parquet. All other drivers raise ValueError.
    """
    if not tracker or not tracker.engine:
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
            autoload_with=tracker.engine,
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
    df = pd.read_sql(stmt, tracker.engine)

    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if driver == "csv":
        df.to_csv(destination_path, index=False)
    else:
        df.to_parquet(destination_path, index=False)

    return str(destination_path)
