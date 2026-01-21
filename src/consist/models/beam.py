from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, String
from sqlmodel import Field, SQLModel


class BeamConfigCache(SQLModel, table=True):
    __tablename__ = "beam_config_cache"
    __table_args__ = {"schema": "global_tables"}

    content_hash: str = Field(primary_key=True, index=True)
    key: str = Field(primary_key=True, index=True)
    value_type: str = Field(index=True)
    value_str: Optional[str] = Field(default=None, sa_column=Column(String))
    value_num: Optional[float] = Field(default=None)
    value_bool: Optional[bool] = Field(default=None)
    value_json_str: Optional[str] = Field(default=None, sa_column=Column(String))


class BeamConfigIngestRunLink(SQLModel, table=True):
    __tablename__ = "beam_config_ingest_run_link"
    __table_args__ = {"schema": "global_tables"}

    run_id: str = Field(primary_key=True, index=True)
    table_name: str = Field(primary_key=True, index=True)
    content_hash: str = Field(primary_key=True, index=True)
    config_name: str = Field(primary_key=True, index=True)
