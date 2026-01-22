from __future__ import annotations

import uuid
import weakref
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import PrivateAttr
from sqlalchemy import JSON, Column
from sqlalchemy.dialects.postgresql import (
    UUID as PG_UUID,
)  # Import for potential postgresql compatibility
from sqlalchemy.types import CHAR, TypeDecorator

from sqlmodel import Field, SQLModel

UTC = timezone.utc


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
    Represents a physical data object, such as a file, directory, or database table, central to Consist's provenance tracking and caching.

    Artifacts are the core building blocks of the provenance system, tracking the inputs
    and outputs of runs. Each artifact has a unique identity, a virtualized location,
    and rich metadata, supporting both "hot" (ingested) and "cold" (file-based) data strategies.

    Attributes:
        id (uuid.UUID): A unique identifier for the artifact.
        key (str): A semantic, human-readable name for the artifact (e.g., "households", "parcels").
        uri (str): A portable, virtualized Uniform Resource Identifier (URI) for the artifact's
                   location (e.g., "inputs://land_use.csv").
        driver (str): The name of the format handler used to read or write the artifact
                      (e.g., "parquet", "csv", "zarr").
        hash (Optional[str]): SHA256 content hash of the artifact's data, enabling content-addressable
                              lookups and deduplication.
        run_id (Optional[str]): The ID of the run that generated this artifact. Null for inputs.
        meta (Dict[str, Any]): A flexible JSON field for storing arbitrary metadata, such as
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
    uri: str = Field(
        index=True, description="Portable URI, e.g., 'inputs://land_use.csv'"
    )

    # Driver Info
    # Valid drivers: parquet, csv, zarr, json, h5_table, h5, hdf5, other
    # (See DriverType enum in consist.types for the authoritative list)
    driver: str = Field(
        description="Format handler: parquet, csv, zarr, json, h5_table, h5, hdf5, or other"
    )

    # Content Hash (for deduplication and content-addressable lookups)
    hash: Optional[str] = Field(
        default=None,
        index=True,
        description="SHA256 content hash of the artifact's data",
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
    _tracker: Optional[weakref.ReferenceType[Any]] = PrivateAttr(default=None)

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
        # Some ORM-loaded artifacts may not initialize private state; guard for None.
        private_state = getattr(self, "__pydantic_private__", None)
        if private_state is None:
            return None
        try:
            return self._abs_path
        except Exception:
            return None

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

        Uses the tracker when available to handle mount-aware URIs; otherwise falls
        back to the cached absolute path or the raw URI.
        """
        tracker_ref = getattr(self, "_tracker", None)
        if tracker_ref is not None:
            tracker_obj = tracker_ref()
            if tracker_obj is not None:
                try:
                    return Path(tracker_obj.resolve_uri(self.uri))
                except Exception:
                    pass

        if self.abs_path:
            return Path(self.abs_path)
        return Path(self.uri)

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
        return self.driver in ("parquet", "csv", "sql")

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
        return f"<Artifact key='{self.key}' driver='{self.driver}' uri='{self.uri}'>"

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
        return f"Artifact(key={self.key}, path={self.uri})"
