from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Iterable, Literal, Sequence, TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


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
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            source_path = Path(tracker.resolve_uri(artifact.uri)).resolve()
        except Exception as e:
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
            # No-op if the file/dir is already at the requested destination.
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path.is_dir():
                shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
            else:
                shutil.copy2(source_path, destination_path)
            materialized[artifact.key] = str(destination_path)
        except Exception as e:
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
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """
    Copy artifact bytes from explicit source paths to caller-specified destinations.

    This is useful for rehydrating cached inputs from historical run directories
    when the current run directory is different from the original producer.
    """
    materialized: dict[str, str] = {}

    for artifact, source, destination in items:
        source_path = Path(source).resolve()
        destination_path = Path(destination).resolve()
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
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path.is_dir():
                shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
            else:
                shutil.copy2(source_path, destination_path)
            materialized[artifact.key] = str(destination_path)
        except Exception as e:
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
    query = (
        f"SELECT * FROM global_tables.{table_name} "
        f"WHERE consist_artifact_id = '{artifact.id}'"
    )
    df = pd.read_sql(text(query), tracker.engine)

    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if driver == "csv":
        df.to_csv(destination_path, index=False)
    else:
        df.to_parquet(destination_path, index=False)

    return str(destination_path)
