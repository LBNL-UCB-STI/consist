"""
Database model for artifacts in the Consist main database.

Artifacts represent tracked files/datasets and are linked to runs via the
run_artifact_link table for provenance and caching.
"""

from __future__ import annotations

import uuid
import weakref
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Protocol, cast

from pydantic import PrivateAttr
from sqlalchemy import JSON, Column, UniqueConstraint
from sqlalchemy.dialects.postgresql import (
    UUID as PG_UUID,
)  # Import for potential postgresql compatibility
from sqlalchemy.types import CHAR, TypeDecorator

from sqlmodel import Field, SQLModel

from consist.types import DriverType

UTC = timezone.utc

if TYPE_CHECKING:
    import pandas as pd

    from consist.core.tracker import Tracker


class _ArtifactTrackerRef(Protocol):
    def resolve_uri(self, uri: str) -> str: ...


def _private_state(artifact: object) -> Optional[dict[str, object]]:
    """Return Pydantic's private-attribute storage when it exists.

    ``Artifact`` uses ``PrivateAttr`` for runtime-only state such as the tracker
    weakref and resolved absolute path. In practice, ORM-loaded instances and
    copied models do not always present that storage consistently through normal
    attribute access, so this helper reads ``__pydantic_private__`` directly via
    ``object.__getattribute__``.

    Returning ``None`` means "treat this instance as if it has no initialized
    private state yet" rather than failing the caller.
    """
    try:
        private_state = object.__getattribute__(artifact, "__pydantic_private__")
    except AttributeError:
        return None
    return private_state if isinstance(private_state, dict) else None


def get_tracker_ref(
    artifact: object,
) -> Optional[weakref.ReferenceType[_ArtifactTrackerRef]]:
    """Read the runtime tracker weakref without depending on normal model access.

    The tracker reference is a convenience link used only at runtime for path
    resolution. It is intentionally not part of the persisted model schema.

    We first check Pydantic's private-state dictionary, which is the preferred
    storage for ``PrivateAttr`` values. If that storage is unavailable, we fall
    back to a direct ``object.__getattribute__`` lookup so partially initialized
    or copied objects still work. Missing state is treated as a normal case and
    returns ``None``.
    """
    private_state = _private_state(artifact)
    if private_state is not None:
        tracker_ref = private_state.get("_tracker")
        if tracker_ref is not None:
            return cast(weakref.ReferenceType[_ArtifactTrackerRef], tracker_ref)
    try:
        return cast(
            weakref.ReferenceType[_ArtifactTrackerRef],
            object.__getattribute__(artifact, "_tracker"),
        )
    except AttributeError:
        return None


def set_tracker_ref(artifact: object, tracker: _ArtifactTrackerRef) -> None:
    """Store a runtime tracker weakref on an artifact when possible.

    This helper mirrors :func:`get_tracker_ref`: prefer Pydantic's private-state
    dictionary, but fall back to ``object.__setattr__`` when that storage has not
    been initialized. The weakref avoids keeping the tracker alive solely because
    an artifact still points at it.

    Failing to attach the ref is non-fatal. The artifact can still function as a
    plain data object; it just loses the convenience of tracker-backed path
    resolution until another code path reattaches the tracker.
    """
    tracker_ref = weakref.ref(tracker)
    private_state = _private_state(artifact)
    if private_state is not None:
        private_state["_tracker"] = tracker_ref
        return
    try:
        object.__setattr__(artifact, "_tracker", tracker_ref)
    except (AttributeError, TypeError):
        return


class UUIDType(TypeDecorator):
    """Platform-independent UUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(36), storing UUIDs as strings."""

    impl = CHAR(36)  # Explicitly use CHAR(36) for storage

    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if not isinstance(value, uuid.UUID):
            raise ValueError(f"Expected UUID object, got {type(value)}")
        return str(value)  # Convert UUID object to string for storage

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if isinstance(
            value, uuid.UUID
        ):  # If the dialect already returned a UUID object
            return value
        return uuid.UUID(value)  # Convert string to UUID object


class Artifact(SQLModel, table=True):
    """
    Represents a physical data object in the Consist database.

    This table stores canonical metadata for any file/dataset Consist tracks. It is
    linked to runs via ``run_artifact_link`` to record whether an artifact was an
    input or output. The ``run_id`` field records the producing run (if any) and
    is often ``None`` for external inputs.

    Artifacts are the core building blocks of provenance and caching. Each artifact
    has a unique identity, a virtualized location, and rich metadata, supporting
    both "hot" (ingested) and "cold" (file-based) data strategies.

    The public fingerprint surface for downstream consumers is ``Artifact.hash``.
    Consist preserves that field across logging, querying, replay, staging, and
    hydration so external provenance/bookkeeping code can read one stable
    fingerprint without probing wrapper-specific fields. Fingerprint semantics
    follow the configured hashing strategy: ``full`` is generally content-based,
    while ``fast`` may use metadata-based hashing for files or directories.
    ``Artifact.content_id`` is a database-local deduplication key and is not the
    public fingerprint contract.

    Attributes:
        id (uuid.UUID): A unique identifier for the artifact.
        key (str): A semantic, human-readable name for the artifact (e.g., "households", "parcels").
        container_uri (str): A portable, virtualized Uniform Resource Identifier (URI) for the
                   artifact's location (e.g., "inputs://land_use.csv").
        table_path (Optional[str]): Optional path inside a container (e.g., "/tables/households").
        array_path (Optional[str]): Optional path inside a container for array artifacts.
        driver (str): The name of the format handler used to read or write the artifact
                      (e.g., "parquet", "csv", "zarr").
        hash (Optional[str]): Canonical portable artifact fingerprint. This is usually a SHA256
                              digest and remains the stable public identity field across logging,
                              replay, staging, and hydration.
        parent_artifact_id (Optional[uuid.UUID]): Canonical parent artifact relation for
                              container child artifacts. Null for standalone artifacts.
        run_id (Optional[str]): The ID of the run that generated this artifact. Null for inputs.
        meta (dict[str, object]): A flexible JSON field for storing arbitrary metadata, such as
                               schema signatures, or data dimensions.
        created_at (datetime): The timestamp when the artifact was first logged.
    """

    __tablename__ = "artifact"

    # Core Identity
    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        sa_type=UUIDType,
        sa_column_kwargs={"autoincrement": False},
    )
    key: str = Field(index=True, description="Semantic name, e.g., 'households'")

    # Location (Virtualized)
    container_uri: str = Field(
        index=True, description="Portable URI, e.g., 'inputs://land_use.csv'"
    )
    table_path: Optional[str] = Field(
        default=None,
        description="Optional path inside a container for tabular artifacts.",
    )
    array_path: Optional[str] = Field(
        default=None,
        description="Optional path inside a container for array artifacts.",
    )

    # Driver Info
    # Valid drivers: parquet, csv, zarr, json, h5_table, h5, hdf5, geojson, shapefile, geopackage, other
    # (See DriverType enum in consist.types for the authoritative list)
    driver: str = Field(
        description="Format handler: parquet, csv, zarr, json, h5_table, h5, hdf5, geojson, shapefile, geopackage, or other"
    )

    # Canonical artifact fingerprint (portable) plus DB-local content identity.
    hash: Optional[str] = Field(
        default=None,
        index=True,
        description=(
            "Canonical portable artifact fingerprint. Semantics follow the active "
            "hashing strategy and may be content- or metadata-based."
        ),
    )
    content_id: Optional[uuid.UUID] = Field(
        default=None,
        index=True,
        sa_type=UUIDType,
        description=(
            "Database-local pointer to shared ArtifactContent metadata used for "
            "deduplication; not the public artifact fingerprint surface."
        ),
    )
    parent_artifact_id: Optional[uuid.UUID] = Field(
        default=None,
        index=True,
        sa_type=UUIDType,
        description=(
            "Canonical parent artifact relation for container child artifacts."
        ),
    )

    # Lineage
    run_id: Optional[str] = Field(default=None, index=True)

    # Metadata (Flexible JSON bag)
    # Stores: schema signatures, matrix shapes, etc.
    # Uses SQLAlchemy's JSON type for efficient persistence of arbitrary JSON structures.
    meta: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))

    # Audit
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # --- Runtime State (Not persisted) ---
    # PrivateAttr is used here to store data that is not part of the SQLModel schema
    # but is needed at runtime, aligning with "Path Resolution & Mounts" and "Artifact Chaining"
    # as described in the architecture documentation.
    _abs_path: Optional[str] = PrivateAttr(default=None)
    _tracker: Optional[weakref.ReferenceType[_ArtifactTrackerRef]] = PrivateAttr(
        default=None
    )

    @property
    def abs_path(self) -> Optional[str]:
        """
        Runtime-only helper to access the absolute path of this artifact.

        This property provides the resolved absolute file system path for the artifact.
        It is not persisted to the database but is crucial for local file operations
        and for chaining Consist runs within the same script or environment.

        Returns
        -------
        Optional[str]
            The absolute file system path of the artifact, or `None` if it has not
            yet been resolved or set.
        """
        private_state = _private_state(self)
        if private_state is None:
            return None
        abs_path = private_state.get("_abs_path")
        return abs_path if isinstance(abs_path, str) else None

    @property
    def recovery_roots(self) -> list[str]:
        """
        Ordered advisory recovery roots recorded on this artifact.

        This is a typed convenience view over ``meta["recovery_roots"]``. Use
        ``Tracker.set_artifact_recovery_roots(...)`` to persist changes.
        """
        roots = (self.meta or {}).get("recovery_roots")
        if roots is None:
            return []
        if isinstance(roots, (str, Path)):
            return [str(roots)]
        if isinstance(roots, Sequence):
            return [str(item) for item in roots if isinstance(item, (str, Path))]
        return []

    @abs_path.setter
    def abs_path(self, value: str) -> None:
        """
        Sets the runtime-only absolute path of this artifact.

        Parameters
        ----------
        value : str
            The absolute file system path to set for the artifact.
        """
        self._abs_path = value

    @property
    def path(self) -> Path:
        """
        Resolve this artifact to a filesystem Path.

        Uses the tracker when available to handle mount-aware URIs. If that
        canonical resolution does not exist but a runtime ``abs_path`` is set
        (for example after historical hydration or archive ``move``), this
        falls back to ``abs_path`` before returning the raw URI.
        """
        tracker_ref = get_tracker_ref(self)
        if tracker_ref is not None:
            tracker_obj = tracker_ref()
            if tracker_obj is not None:
                try:
                    resolved = Path(tracker_obj.resolve_uri(self.container_uri))
                    if resolved.exists() or not self.abs_path:
                        return resolved
                except Exception:
                    resolved = None

        if self.abs_path:
            return Path(self.abs_path)
        return Path(self.container_uri)

    def as_path(self, tracker: Optional["Tracker"] = None) -> Path:
        """
        Resolve this artifact to a filesystem path.

        When ``tracker`` is provided, URI resolution uses that tracker
        explicitly. Otherwise this falls back to the attached tracker context
        (if any), then any runtime ``abs_path`` captured during hydration or
        archive ``move``, then the raw URI path.
        """
        if tracker is not None:
            return Path(tracker.resolve_uri(self.container_uri))
        return self.path

    def as_df(
        self,
        tracker: Optional["Tracker"] = None,
        *,
        db_fallback: Literal["inputs-only", "always", "never"] = "inputs-only",
        close: bool = True,
        **kwargs: Any,
    ) -> "pd.DataFrame":
        """
        Load this artifact as a pandas DataFrame.

        This is equivalent to ``consist.load_df(self, ...)`` and supports the same
        loader options.
        """
        # reason: keep the import local to avoid a circular dependency at module import time.
        from consist.api import load_df

        return load_df(
            self,
            tracker=tracker,
            db_fallback=db_fallback,
            close=close,
            **kwargs,
        )

    # --- Format Helpers ---

    @property
    def is_matrix(self) -> bool:
        """
        Indicates if the artifact represents a multi-dimensional array or matrix-like data.

        This property helps in dispatching to appropriate data loaders or processing
        functions that handle array-based data structures, such as those typically
        found in scientific computing.

        Returns
        -------
        bool
            True if the artifact's driver is associated with matrix-like data formats
            (e.g., Zarr, HDF5, NetCDF, OpenMatrix), False otherwise.
        """
        return self.driver in ("zarr", "h5", "netcdf", "openmatrix")

    @property
    def is_tabular(self) -> bool:
        """
        Indicates if the artifact represents tabular data (rows and columns).

        This property assists in identifying artifacts that can be loaded and processed
        using tools designed for structured, record-based data, such as Pandas DataFrames.

        Returns
        -------
        bool
            True if the artifact's driver is associated with tabular data formats
            (e.g., Parquet, CSV, SQL), False otherwise.
        """
        return (
            self.driver in DriverType.tabular_drivers()
            or self.driver in DriverType.spatial_drivers()
            or self.driver == "sql"
        )

    @property
    def created_at_iso(self) -> Optional[str]:
        """
        Return created_at as an ISO 8601 formatted string.

        Useful for serialization to external systems that expect string timestamps.

        Returns
        -------
        Optional[str]
            The created_at timestamp as ISO 8601 string, or None if not set.
        """
        return self.created_at.isoformat() if self.created_at else None

    def get_meta(self, key: str, default: Any = None) -> Any:
        """
        Safely retrieves a value from the 'meta' dictionary.

        Args:
            key (str): The key to look up in the metadata.
            default (Any, optional): The default value to return if the key is not found.
                                     Defaults to None.

        Returns:
            Any: The value associated with the key, or the default value if the key is not present.
        """
        return self.meta.get(key, default)

    def __repr__(self) -> str:
        """
        Returns a developer-friendly string representation of the Artifact object.

        This representation is designed for debugging and logging, providing a concise
        summary of the artifact's key attributes.

        Returns
        -------
        str
            A string in the format "<Artifact key='...' driver='...' uri='...'>".
        """
        return (
            f"<Artifact key='{self.key}' driver='{self.driver}' "
            f"uri='{self.container_uri}'>"
        )

    def __str__(self) -> str:
        """
        Returns a user-friendly string representation of the Artifact object.

        This representation is suitable for display to end-users, focusing on
        the artifact's key and its portable URI.

        Returns
        -------
        str
            A string in the format "Artifact(key=..., path=...)".
        """
        return f"Artifact(key={self.key}, path={self.container_uri})"


class ArtifactContent(SQLModel, table=True):
    """
    Content-centric metadata for deduplicated bytes shared across artifacts.

    Each row represents a unique `(content_hash, driver)` tuple that multiple artifact
    occurrences can reference.
    """

    __tablename__ = "artifact_content"
    __table_args__ = (UniqueConstraint("content_hash", "driver"),)

    id: uuid.UUID = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        sa_type=UUIDType,
        sa_column_kwargs={"autoincrement": False},
    )
    content_hash: str = Field(
        index=True, description="SHA256 hash identifying the content payload."
    )
    driver: str = Field(
        index=True, description="Driver used to interpret the content (e.g., parquet)."
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when this content identity was first recorded.",
    )
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON),
        description="Optional content-scoped metadata such as schema fingerprints.",
    )

    def __str__(self) -> str:
        return (
            f"ArtifactContent(hash={self.content_hash}, "
            f"driver={self.driver}, id={self.id})"
        )
