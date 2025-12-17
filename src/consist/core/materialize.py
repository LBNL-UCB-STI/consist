from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Iterable, Literal, Sequence, TYPE_CHECKING

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
