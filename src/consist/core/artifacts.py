import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Any, Type, TYPE_CHECKING, Callable, Union, Literal

from sqlmodel import SQLModel
from consist.models.artifact import Artifact
from consist.core.drivers import ARRAY_DRIVERS, TABLE_DRIVERS
from consist.core.validation import validate_artifact_key
from consist.types import ArtifactRef

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


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


def _infer_driver_from_path(path: Path) -> str:
    driver = ARRAY_DRIVERS.resolve_driver(path)
    if driver:
        return driver
    driver = TABLE_DRIVERS.resolve_driver(path)
    if driver:
        return driver
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
    if hasattr(obj, "dtype") and hasattr(obj, "shape"):
        return obj
    if hasattr(obj, "keys"):
        preferred = ("table", "block0_values", "values")
        for key in preferred:
            try:
                candidate = obj[key]
            except Exception:
                continue
            if hasattr(candidate, "dtype") and hasattr(candidate, "shape"):
                return candidate

        for key in obj.keys():
            try:
                candidate = obj[key]
            except Exception:
                continue
            if hasattr(candidate, "dtype") and hasattr(candidate, "shape"):
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
            A SQLModel class used to enforce structure and enable hybrid views.
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
        **meta : Any
            Flexible metadata payload stored within the artifact record.

        Returns
        -------
        Artifact
            A hydrated Artifact instance ready for persistence or chaining.
        """
        if "table_path" in meta:
            meta.pop("table_path")
        if "array_path" in meta:
            meta.pop("array_path")
        artifact_obj = None
        resolved_abs_path = None
        mount_scheme: Optional[str] = None
        mount_root: Optional[str] = None
        # Shared mutable state for nested helpers in this single create call.
        hash_state = _ContentHashState(effective_hash=content_hash)

        is_output_reuse = direction == "output" and reuse_if_unchanged

        def _warn_output_reuse_deprecated() -> None:
            if not is_output_reuse:
                return
            logging.warning(
                "[Consist] reuse_if_unchanged on outputs is deprecated; "
                "a new artifact row is always created now and identical bytes share "
                "identity via ArtifactContent (content_id)."
            )
            if reuse_scope == "any_uri":
                logging.warning(
                    "[Consist] reuse_scope='any_uri' is ignored for outputs; "
                    "content_id equality governs deduplication instead."
                )

        def _attach_content_id(artifact: Artifact) -> None:
            hash_value = hash_state.effective_hash or artifact.hash
            effective_driver = driver or artifact.driver
            if (
                hash_value is None
                or effective_driver is None
                or self.tracker.db is None
            ):
                return
            try:
                content_row = self.tracker.db.get_or_create_artifact_content(
                    content_hash=hash_value,
                    driver=effective_driver,
                )
                artifact.content_id = content_row.id
            except Exception as exc:
                logging.warning(
                    "[Consist] Failed to record artifact content identity for %s: %s",
                    artifact.key,
                    exc,
                )

        def _compute_content_hash(
            *,
            raise_on_error: bool,
        ) -> Optional[str]:
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
            if computed != hash_state.effective_hash:
                raise ValueError(
                    f"content_hash does not match on-disk data for {resolved_abs_path}."
                )

        def _apply_content_hash_override(artifact: Artifact) -> None:
            if hash_state.effective_hash is None:
                return
            _validate_content_hash()
            existing_hash = getattr(artifact, "hash", None)
            if (
                existing_hash
                and existing_hash != hash_state.effective_hash
                and not force_hash_override
            ):
                logging.warning(
                    "[Consist Warning] Ignoring content_hash override for artifact key=%s uri=%s "
                    "(existing hash differs). Use force_hash_override=True to override.",
                    getattr(artifact, "key", None),
                    getattr(artifact, "container_uri", None),
                )
                return
            artifact.hash = hash_state.effective_hash

        _warn_output_reuse_deprecated()

        if isinstance(path, Artifact):
            artifact_obj = path
            resolved_abs_path = artifact_obj.abs_path or self.tracker.resolve_uri(
                artifact_obj.container_uri
            )
            if key is None:
                key = artifact_obj.key
            if key is not None:
                validate_artifact_key(key)
            if driver:
                artifact_obj.driver = driver
            if table_path is not None:
                artifact_obj.table_path = table_path
            if array_path is not None:
                artifact_obj.array_path = array_path
            _apply_content_hash_override(artifact_obj)
            if meta:
                artifact_obj.meta.update(meta)
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a path-like.")
            validate_artifact_key(key)

            resolved_abs_path = str(Path(path).resolve())
            container_uri = self.tracker.fs.virtualize_path(resolved_abs_path)

            if "://" in container_uri:
                scheme = container_uri.split("://", 1)[0]
                if scheme in self.tracker.mounts:
                    mount_scheme = scheme
                    mount_root = str(Path(self.tracker.mounts[scheme]).resolve())

            if driver is None:
                driver = _infer_driver_from_path(Path(path))

            if direction == "input" and self.tracker.db:
                parent = self.tracker.db.find_latest_artifact_at_uri(
                    container_uri,
                    driver=driver,
                    table_path=table_path,
                    array_path=array_path,
                )
                if parent:
                    should_reuse = True
                    if driver in {"h5", "hdf5"}:
                        if hash_state.effective_hash is None:
                            _ensure_content_hash()
                        should_reuse = (
                            hash_state.effective_hash is not None
                            and parent.hash == hash_state.effective_hash
                        )
                    elif driver == "h5_table":
                        should_reuse = (
                            hash_state.effective_hash is not None
                            and parent.hash == hash_state.effective_hash
                        )

                    if should_reuse:
                        artifact_obj = parent
                        if driver:
                            artifact_obj.driver = driver
                        _apply_content_hash_override(artifact_obj)
                        if meta:
                            artifact_obj.meta.update(meta)

            if artifact_obj is None:
                if hash_state.effective_hash is not None and validate_content_hash:
                    _validate_content_hash()
                elif hash_state.effective_hash is None:
                    _ensure_content_hash()

                artifact_obj = Artifact(
                    key=key,
                    container_uri=container_uri,
                    driver=driver,
                    table_path=table_path,
                    array_path=array_path,
                    hash=hash_state.effective_hash,
                    run_id=run_id,
                    meta=meta,
                )

        if artifact_obj.hash and hash_state.effective_hash is None:
            hash_state.effective_hash = artifact_obj.hash

        _attach_content_id(artifact_obj)

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
        *,
        hash_tables: bool,
        table_hash_chunk_rows: Optional[int],
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
        hash_tables : bool
            Whether to compute per-table hashes for discovered datasets.
        table_hash_chunk_rows : Optional[int]
            Number of rows per hash chunk when computing table hashes.

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
                            table_hash = None
                            table_meta: dict[str, Any] = {
                                "parent_id": str(container.id),
                                "shape": list(obj.shape),
                                "dtype": str(obj.dtype),
                            }
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
                                run_id=container.run_id,  # FIX: Inherit Run ID
                                key=table_key,
                                direction=direction,
                                driver="h5_table",
                                table_path=name,
                                content_hash=table_hash,
                                **table_meta,
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
        hash_tables: Literal["always", "if_unchanged", "never"] = "if_unchanged",
        table_hash_chunk_rows: Optional[int] = None,
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
        hash_tables : {"always", "if_unchanged", "never"}, default "if_unchanged"
            Controls whether per-table hashes are computed. When set to
            ``"if_unchanged"``, Consist prefers content identity to decide whether
            the container is unchanged: if ``prior_container.content_id`` equals the
            new container's ``content_id``, table hashing is performed even if one
            side is missing a file hash. If content identity is unavailable,
            legacy hash-based behavior is used as a fallback.
        table_hash_chunk_rows : Optional[int], optional
            Rows per chunk when hashing tables (defaults to dataset chunking or 1024).
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
        prior_container = None
        if self.tracker.db:
            try:
                resolved_abs_path = str(path_obj.resolve())
                container_uri = self.tracker.fs.virtualize_path(resolved_abs_path)
                # Include inputs so we can compare against the last observed file hash,
                # even when the container was logged as an input (run_id=None).
                prior_container = self.tracker.db.find_latest_artifact_at_uri(
                    container_uri, driver="h5", include_inputs=True
                )
            except Exception:
                prior_container = None

        container = self.tracker.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="h5",
            is_container=True,
            **meta,
        )

        table_artifacts: List[Artifact] = []
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
            else:
                # Prefer content identity when available. This allows table hashing
                # to proceed even if a file hash is missing on one side.
                if (
                    getattr(prior_container, "content_id", None) is not None
                    and getattr(container, "content_id", None) is not None
                ):
                    should_hash_tables = (
                        prior_container.content_id == container.content_id
                    )
                    if not should_hash_tables:
                        skip_reason = "content_identity_changed"
                elif prior_container.hash and container.hash:
                    # Fallback to legacy hash equality when content identity is not available.
                    should_hash_tables = prior_container.hash == container.hash
                    if not should_hash_tables:
                        skip_reason = "file_hash_changed"
                else:
                    skip_reason = "file_hash_unavailable"

        container.meta["table_hashes_checked"] = should_hash_tables
        if not should_hash_tables and hash_tables != "always":
            if hash_tables == "never":
                container.meta["table_hashes_skip_reason"] = "disabled"
            elif prior_container is not None and skip_reason is not None:
                container.meta["table_hashes_skip_reason"] = skip_reason

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
                container,
                path_obj,
                key,
                direction,
                filter_fn,
                hash_tables=should_hash_tables,
                table_hash_chunk_rows=table_hash_chunk_rows,
            )

            target = (
                self.tracker.current_consist.inputs
                if direction == "input"
                else self.tracker.current_consist.outputs
            )
            for table_artifact in table_artifacts:
                target.append(table_artifact)
                self.tracker.persistence.sync_artifact(table_artifact, direction)

        container.meta["table_count"] = len(table_artifacts)
        container.meta["table_ids"] = [str(t.id) for t in table_artifacts]

        self.tracker.persistence.flush_json()
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
        **meta: Any,
    ) -> Artifact:
        """
        Log a single HDF5 table as an artifact without scanning the container.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the HDF5 file.
        table_path : str
            Internal HDF5 dataset path (e.g., "/households").
        key : Optional[str], optional
            Artifact key; defaults to "<file_stem>_<table_path>".
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        parent : Optional[Artifact], optional
            Optional container artifact to link via parent_id.
        hash_table : bool, default True
            Whether to compute a per-table hash.
        table_hash_chunk_rows : Optional[int], optional
            Rows per chunk when hashing tables (defaults to dataset chunking or 1024).
        profile_file_schema : bool, default False
            Whether to profile and persist a lightweight schema for the table.
            Use "if_changed" to skip profiling when matching content identity already
            has a stored schema (prefers content_id; falls back to hash for legacy rows).
        file_schema_sample_rows : Optional[int], optional
            Maximum rows to sample when profiling the schema.
        **meta : Any
            Additional metadata for the table artifact.

        Returns
        -------
        Artifact
            The logged table artifact.
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
            with h5py.File(str(path_obj), "r") as h5_file:
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
        if table_hash:
            table_meta["table_hash"] = table_hash
            table_meta["table_hash_algo"] = "sha256"
            if table_hash_chunk_rows is not None:
                table_meta["table_hash_chunk_rows"] = table_hash_chunk_rows

        if meta:
            table_meta.update(meta)

        return self.tracker.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="h5_table",
            table_path=table_path,
            content_hash=table_hash,
            profile_file_schema=profile_file_schema,
            file_schema_sample_rows=file_schema_sample_rows,
            **table_meta,
        )
