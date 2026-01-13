import logging
from pathlib import Path
from typing import List, Optional, Any, Type, TYPE_CHECKING, Callable, Union

from sqlmodel import SQLModel
from consist.models.artifact import Artifact
from consist.core.validation import validate_artifact_key
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
            Must follow artifact-key rules (no empty key, no ``..``/``//``, length <= 256).
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
        TypeError
            If ``key`` is not a string when provided.
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
            if key is not None:
                validate_artifact_key(key)
            if driver:
                artifact_obj.driver = driver
            if meta:
                artifact_obj.meta.update(meta)
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a path-like.")
            validate_artifact_key(key)

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

    def log_h5_container(
        self,
        path: Union[str, Path],
        *,
        key: Optional[str] = None,
        direction: str = "output",
        discover_tables: bool = True,
        table_filter: Optional[Union[Callable[[str], bool], List[str]]] = None,
        **meta: Any,
    ) -> tuple[Artifact, List[Artifact]]:
        """
        Log an HDF5 file and optionally discover its internal tables.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the HDF5 file.
        key : Optional[str], optional
            Semantic name for the container. If not provided, uses the file stem.
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        discover_tables : bool, default True
            If True, scan the file and create child artifacts for each table/dataset.
        table_filter : Optional[Union[Callable[[str], bool], List[str]]], optional
            Filter which tables to log. Can be:
            - A callable that takes a table name and returns True to include
            - A list of table names to include (exact match)
            If None, all tables are included.
        **meta : Any
            Additional metadata for the container artifact.

        Returns
        -------
        tuple[Artifact, List[Artifact]]
            A tuple of (container_artifact, list_of_table_artifacts).

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        """
        if not self.tracker.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem

        container = self.tracker.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="h5",
            is_container=True,
            **meta,
        )

        table_artifacts: List[Artifact] = []

        if discover_tables:
            if table_filter is None:

                def filter_fn(name: str) -> bool:
                    return True

            elif isinstance(table_filter, list):
                filter_set = set(table_filter)

                def filter_fn(name: str) -> bool:
                    stripped = name.lstrip("/")
                    return name in filter_set or stripped in filter_set

            else:
                filter_fn = table_filter

            table_artifacts = self.scan_h5_container(
                container, path_obj, key, direction, filter_fn
            )

            for table_artifact in table_artifacts:
                self.tracker.current_consist.outputs.append(table_artifact)
                self.tracker.persistence.sync_artifact(table_artifact, direction)

        container.meta["table_count"] = len(table_artifacts)
        container.meta["table_ids"] = [str(t.id) for t in table_artifacts]

        self.tracker.persistence.flush_json()
        self.tracker.persistence.sync_artifact(container, direction)

        return container, table_artifacts
