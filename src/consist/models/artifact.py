import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel
from pydantic import PrivateAttr


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
        run_id (Optional[str]): The ID of the run that generated this artifact. Null for inputs.
        meta (Dict[str, Any]): A flexible JSON field for storing arbitrary metadata, such as
                               checksums, schema signatures, or data dimensions.
        created_at (datetime): The timestamp when the artifact was first logged.
    """

    __tablename__ = "artifact"

    # Core Identity
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    key: str = Field(index=True, description="Semantic name, e.g., 'households'")

    # Location (Virtualized)
    uri: str = Field(
        index=True, description="Portable URI, e.g., 'inputs://land_use.csv'"
    )

    # Driver Info
    driver: str = Field(description="Format handler: parquet, csv, zarr, h5, git")

    # Lineage
    run_id: Optional[str] = Field(default=None, index=True)

    # Metadata (Flexible JSON bag)
    # Stores: checksums, schema signatures, matrix shapes, etc.
    # Uses SQLAlchemy's JSON type for efficient persistence of arbitrary JSON structures.
    meta: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))

    # Audit
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # --- Runtime State (Not persisted) ---
    # PrivateAttr is used here to store data that is not part of the SQLModel schema
    # but is needed at runtime, aligning with "Path Resolution & Mounts" and "Artifact Chaining"
    # as described in the architecture documentation.
    _abs_path: Optional[str] = PrivateAttr(default=None)

    @property
    def abs_path(self) -> Optional[str]:
        """
        Runtime-only helper to access the absolute path of this artifact.
        Useful when chaining runs in the same script.
        """
        return self._abs_path

    @abs_path.setter
    def abs_path(self, value: str):
        self._abs_path = value

    # --- Format Helpers ---

    @property
    def is_matrix(self) -> bool:
        return self.driver in ("zarr", "h5", "netcdf")

    @property
    def is_tabular(self) -> bool:
        return self.driver in ("parquet", "csv", "sql")

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
