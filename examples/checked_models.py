from __future__ import annotations

from typing import Optional

from sqlmodel import Field, SQLModel


class PredatorPreySeriesChecked(SQLModel, table=True):
    """
    Checked-in “contract” model for the sampled per-simulation time series.

    The primary key is `t`; the hybrid view adds `consist_run_id` so rows can
    be grouped per run without embedding parameters in the artifact itself.
    """

    __tablename__ = "sim_series"
    __table_args__ = {"extend_existing": True}

    t: int = Field(primary_key=True, index=True)

    prey: int
    predator: int

    prey_births: int
    prey_eaten: int
    predator_births: int
    predator_deaths: int

    # Optional convenience field if users later compute/attach it in their pipeline
    # (kept nullable to avoid forcing it in every artifact).
    phase: Optional[str] = Field(default=None, index=True)


class PredatorPreySeriesHot(SQLModel, table=True):
    """
    Minimal SQLModel mapping for the ingested (hot) series table.

    This targets the `global_tables.sim_series` table created by ingestion.
    """

    __tablename__ = "sim_series"
    __table_args__ = {"schema": "global_tables", "extend_existing": True}

    consist_run_id: str = Field(primary_key=True)
    t: int = Field(primary_key=True)
