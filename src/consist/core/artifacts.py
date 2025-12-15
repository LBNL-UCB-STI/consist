import logging
from pathlib import Path
from typing import List, Optional, Any, Type, TYPE_CHECKING, Callable

from sqlmodel import SQLModel
from consist.models.artifact import Artifact
from consist.types import ArtifactRef

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class ArtifactManager:
    """
    Handles creation and discovery of Consist ``Artifact`` objects within a run.

    The manager virtualizes paths, computes hashes, ties artifacts to existing
    provenance records, and provides helpers for discovering nested HDF5 tables.
    """

    def __init__(self, tracker: "Tracker"):
        self.tracker = tracker

    def create_artifact(
        self,
        path: ArtifactRef,
        run_id: Optional[str] = None,
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        driver: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Construct or reuse an ``Artifact`` instance for the given path.

        Parameters
        ----------
        path : ArtifactRef
            File system path or an existing ``Artifact`` reference to log.
        run_id : Optional[str], optional
            Identifier of the run that produced the artifact.
        key : Optional[str], optional
            Logical name for the artifact; required when ``path`` is a string.
        direction : str, default "output"
            Whether the artifact is treated as an "input" or "output" of the run.
        schema : Optional[Type[SQLModel]], optional
            Schema class to record in ``meta["schema_name"]`` and ``meta["has_strict_schema"]``.
        driver : Optional[str], optional
            Explicit driver identifier (e.g., ``"h5_table"``); inferred from suffix if omitted.
        **meta : Any
            Additional metadata key/value pairs to attach to the artifact.

        Returns
        -------
        Artifact
            The persisted or reused artifact object carrying virtualized URI information.

        Raises
        ------
        ValueError
            If ``path`` is a string and ``key`` is not provided.
        """
        artifact_obj = None
        resolved_abs_path = None
        mount_scheme: Optional[str] = None
        mount_root: Optional[str] = None

        if isinstance(path, Artifact):
            artifact_obj = path
            resolved_abs_path = artifact_obj.abs_path or self.tracker.resolve_uri(
                artifact_obj.uri
            )
            if key is None:
                key = artifact_obj.key
            if driver:
                artifact_obj.driver = driver
            if meta:
                artifact_obj.meta.update(meta)
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a path-like.")

            resolved_abs_path = str(Path(path).resolve())
            uri = self.tracker.fs.virtualize_path(resolved_abs_path)

            if "://" in uri:
                scheme = uri.split("://", 1)[0]
                if scheme in self.tracker.mounts:
                    mount_scheme = scheme
                    mount_root = str(Path(self.tracker.mounts[scheme]).resolve())

            if direction == "input" and self.tracker.db:
                parent = self.tracker.db.find_latest_artifact_at_uri(uri)
                if parent:
                    artifact_obj = parent
                    if driver:
                        artifact_obj.driver = driver
                    if meta:
                        artifact_obj.meta.update(meta)

            if artifact_obj is None:
                if driver is None:
                    driver = Path(path).suffix.lstrip(".").lower() or "unknown"

                content_hash = None
                try:
                    content_hash = self.tracker.identity.compute_file_checksum(
                        resolved_abs_path
                    )
                except Exception as e:
                    logging.warning(
                        f"[Consist Warning] Failed to compute hash for {path}: {e}"
                    )

                artifact_obj = Artifact(
                    key=key,
                    uri=uri,
                    driver=driver,
                    hash=content_hash,
                    run_id=run_id,
                    meta=meta,
                )

        if schema:
            artifact_obj.meta["schema_name"] = schema.__name__
            artifact_obj.meta["has_strict_schema"] = True

        if artifact_obj.meta is None:
            artifact_obj.meta = {}
        if mount_scheme and "mount_scheme" not in artifact_obj.meta:
            artifact_obj.meta["mount_scheme"] = mount_scheme
        if mount_root and "mount_root" not in artifact_obj.meta:
            artifact_obj.meta["mount_root"] = mount_root

        artifact_obj.abs_path = resolved_abs_path
        return artifact_obj

    def scan_h5_container(
        self,
        container: Artifact,
        path_obj: Path,
        key: str,
        direction: str,
        filter_fn: Callable[[str], bool],
    ) -> List[Artifact]:
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

        Returns
        -------
        List[Artifact]
            One artifact per selected dataset.
        """
        try:
            import h5py
        except ImportError:
            logging.warning(
                "[Consist] h5py not installed. Cannot discover HDF5 tables."
            )
            return []

        table_artifacts: List[Artifact] = []

        try:
            with h5py.File(str(path_obj), "r") as f:

                def visit_datasets(name: str, obj: Any) -> None:
                    if isinstance(obj, h5py.Dataset):
                        if filter_fn(name):
                            table_key = f"{key}_{name.replace('/', '_')}"
                            table_art = self.create_artifact(
                                str(path_obj),
                                run_id=container.run_id,  # FIX: Inherit Run ID
                                key=table_key,
                                direction=direction,
                                driver="h5_table",
                                parent_id=str(container.id),
                                table_path=name,
                                shape=list(obj.shape),
                                dtype=str(obj.dtype),
                            )
                            table_artifacts.append(table_art)

                f.visititems(visit_datasets)
        except Exception as e:
            logging.warning(f"[Consist] Failed to discover HDF5 tables: {e}")

        return table_artifacts
