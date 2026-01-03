from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import Column, Float, JSON, String
from sqlmodel import Field, SQLModel


class ActivitySimConstants(SQLModel, table=True):
    """
    Materialized ActivitySim constants and settings for query.

    Attributes
    ----------
    run_id : str
        Consist run identifier.
    file_name : str
        Source YAML file name.
    key : str
        Constant key (e.g., ``CONSTANTS.AUTO_TIME``).
    value_type : str
        Type tag for the value (e.g., ``num``, ``str``, ``bool``).
    value_str : Optional[str]
        String representation when value_type == ``str``.
    value_num : Optional[float]
        Numeric representation when value_type == ``num``.
    value_bool : Optional[bool]
        Boolean representation when value_type == ``bool``.
    value_json : Optional[Any]
        JSON representation for complex values.
    """

    __tablename__ = "activitysim_constants"
    __table_args__ = {"schema": "global_tables"}

    run_id: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    key: str = Field(primary_key=True, index=True)
    value_type: str = Field(index=True)
    value_str: Optional[str] = Field(default=None, sa_column=Column(String))
    value_num: Optional[float] = Field(default=None, sa_column=Column(Float))
    value_bool: Optional[bool] = Field(default=None)
    value_json: Optional[Any] = Field(default=None, sa_column=Column(JSON))


class ActivitySimCoefficients(SQLModel, table=True):
    """
    Materialized ActivitySim coefficient rows for query.

    Attributes
    ----------
    run_id : str
        Consist run identifier.
    file_name : str
        Source CSV file name.
    coefficient_name : str
        Coefficient identifier.
    segment : str
        Segment name for template coefficients, empty for direct.
    source_type : str
        ``direct`` for value columns, ``template`` for segment columns.
    value_raw : str
        Raw CSV cell value.
    value_num : Optional[float]
        Parsed numeric value when available.
    constrain : Optional[str]
        Constraint flag from CSV, if present.
    is_constrained : Optional[bool]
        Parsed constraint flag when available.
    """

    __tablename__ = "activitysim_coefficients"
    __table_args__ = {"schema": "global_tables"}

    run_id: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    coefficient_name: str = Field(primary_key=True, index=True)
    segment: str = Field(default="", primary_key=True)
    source_type: str = Field(index=True)
    value_raw: str = Field(sa_column=Column(String))
    value_num: Optional[float] = Field(default=None, sa_column=Column(Float))
    constrain: Optional[str] = Field(default=None, sa_column=Column(String))
    is_constrained: Optional[bool] = Field(default=None)


class ActivitySimProbabilities(SQLModel, table=True):
    """
    Materialized ActivitySim probability tables for query.

    Attributes
    ----------
    run_id : str
        Consist run identifier.
    file_name : str
        Source CSV file name.
    row_index : int
        Row index in the source file.
    dims : Dict[str, Any]
        Non-numeric dimension values.
    probs : Dict[str, Any]
        Numeric probability values.
    """

    __tablename__ = "activitysim_probabilities"
    __table_args__ = {"schema": "global_tables"}

    run_id: str = Field(primary_key=True, index=True)
    file_name: str = Field(primary_key=True, index=True)
    row_index: int = Field(primary_key=True)
    dims: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    probs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
