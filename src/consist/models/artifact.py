import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel


class Artifact(SQLModel, table=True):
    """
    Represents a physical data object (File, Directory, Zarr store).
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
    run_id: Optional[str] = Field(default=None, foreign_key="run.id", index=True)

    # Metadata (Flexible JSON bag)
    # Stores: checksums, schema signatures, matrix shapes, etc.
    meta: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))

    # Audit
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # --- Format Helpers (Python logic, not new columns) ---

    @property
    def is_matrix(self) -> bool:
        return self.driver in ("zarr", "h5", "netcdf")

    @property
    def is_tabular(self) -> bool:
        return self.driver in ("parquet", "csv", "sql")

    def get_meta(self, key: str, default: Any = None) -> Any:
        """Safe access to the JSON metadata bag."""
        return self.meta.get(key, default)
