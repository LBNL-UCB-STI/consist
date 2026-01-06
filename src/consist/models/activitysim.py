from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import Column, Float, JSON, String
from sqlmodel import Field, SQLModel


class ActivitySimConstantsCache(SQLModel, table=True):
    """
    Deduplicated ActivitySim constants keyed by content hash.
    """

    __tablename__ = "activitysim_constants_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    key: str = Field(primary_key=True, index=True)
    value_type: str = Field(index=True)
    value_str: Optional[str] = Field(default=None, sa_column=Column(String))
    value_num: Optional[float] = Field(default=None, sa_column=Column(Float))
    value_bool: Optional[bool] = Field(default=None)
    value_json: Optional[Any] = Field(default=None, sa_column=Column(JSON))


class ActivitySimCoefficientsCache(SQLModel, table=True):
    """
    Deduplicated ActivitySim coefficients keyed by content hash.
    """

    __tablename__ = "activitysim_coefficients_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    coefficient_name: str = Field(primary_key=True, index=True)
    segment: str = Field(default="", primary_key=True)
    source_type: str = Field(index=True)
    value_raw: str = Field(sa_column=Column(String))
    value_num: Optional[float] = Field(default=None, sa_column=Column(Float))
    dims_json: Optional[str] = Field(default=None, sa_column=Column(String))
    constrain: Optional[str] = Field(default=None, sa_column=Column(String))
    is_constrained: Optional[bool] = Field(default=None)


class ActivitySimCoefficientTemplateRefsCache(SQLModel, table=True):
    """
    Deduplicated template references keyed by content hash.
    """

    __tablename__ = "activitysim_coefficients_template_refs_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    coefficient_name: str = Field(primary_key=True, index=True)
    segment: str = Field(primary_key=True)
    referenced_coefficient: str = Field(sa_column=Column(String))


class ActivitySimProbabilitiesCache(SQLModel, table=True):
    """
    Deduplicated probabilities keyed by content hash.
    """

    __tablename__ = "activitysim_probabilities_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    row_index: int = Field(primary_key=True)
    dims: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    probs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class ActivitySimProbabilitiesEntriesCache(SQLModel, table=True):
    """
    Deduplicated probability entries keyed by content hash.
    """

    __tablename__ = "activitysim_probabilities_entries_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    row_index: int = Field(primary_key=True)
    key: str = Field(primary_key=True, index=True)
    value_num: float = Field(sa_column=Column(Float))


class ActivitySimProbabilitiesMetaEntriesCache(SQLModel, table=True):
    """
    Deduplicated probability metadata entries keyed by content hash.
    """

    __tablename__ = "activitysim_probabilities_meta_entries_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    row_index: int = Field(primary_key=True)
    key: str = Field(primary_key=True, index=True)
    value_num: float = Field(sa_column=Column(Float))


class ActivitySimConfigIngestRunLink(SQLModel, table=True):
    """
    Link runs to deduplicated config ingests by content hash.
    """

    __tablename__ = "activitysim_config_ingest_run_link"
    __table_args__ = {"schema": "global_tables"}

    run_id: str = Field(primary_key=True, index=True)
    table_name: str = Field(primary_key=True, index=True)
    content_hash: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
