import logging
from pathlib import Path
from typing import Union, List, Optional, Any, Type, TYPE_CHECKING, Callable

from sqlmodel import SQLModel
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class ArtifactManager:
    def __init__(self, tracker: "Tracker"):
        self.tracker = tracker

    def create_artifact(
            self,
            path: Union[str, Artifact],
            run_id: Optional[str] = None,
            key: Optional[str] = None,
            direction: str = "output",
            schema: Optional[Type[SQLModel]] = None,
            driver: Optional[str] = None,
            **meta: Any,
    ) -> Artifact:
        artifact_obj = None
        resolved_abs_path = None

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
                raise ValueError("Argument 'key' required when 'path' is a string.")

            resolved_abs_path = str(Path(path).resolve())
            uri = self.tracker.fs.virtualize_path(resolved_abs_path)

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
                    content_hash = self.tracker.identity._compute_file_checksum(
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
        try:
            import h5py
        except ImportError:
            logging.warning("[Consist] h5py not installed. Cannot discover HDF5 tables.")
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