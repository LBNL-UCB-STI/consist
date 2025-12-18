from __future__ import annotations

from typing import Optional

from sqlmodel import Field, SQLModel


class PredatorPreySweepRegistryChecked(SQLModel, table=True):
    """
    Checked-in “contract” model for the sweep registry.

    Notes:
    - This is intended as an example of what a reviewed schema looks like.
    - The generated stubs from `schema export` are intentionally conservative; users
      should typically add keys/constraints/indexes like these.
    """

    __tablename__ = "sweep_registry"
    __table_args__ = {"extend_existing": True}

    sim_id: int = Field(primary_key=True)
    setting_id: int = Field(index=True)
    replicate_id: int = Field(index=True)

    prey_birth_rate: float = Field(index=True)
    predation_rate: float = Field(index=True)
    predator_death_rate: float = Field(index=True)
    predator_birth_efficiency: float = Field(index=True)

    seed: int


class PredatorPreyMetricsChecked(SQLModel, table=True):
    """
    Checked-in “contract” model for per-simulation metrics.
    """

    __tablename__ = "sim_metrics"
    __table_args__ = {"extend_existing": True}

    sim_id: int = Field(
        primary_key=True, foreign_key="sweep_registry.sim_id", index=True
    )
    setting_id: int = Field(index=True)
    replicate_id: int = Field(index=True)

    steps: int
    dt: float
    seed: int

    prey_init: int
    predator_init: int
    prey_birth_rate: float = Field(index=True)
    predation_rate: float = Field(index=True)
    predator_birth_efficiency: float = Field(index=True)
    predator_death_rate: float = Field(index=True)

    prey_mean: float
    predator_mean: float
    prey_std: float
    predator_std: float
    prey_final: int
    predator_final: int
    prey_extinct: int = Field(index=True)
    predator_extinct: int = Field(index=True)


class PredatorPreySeriesChecked(SQLModel, table=True):
    """
    Checked-in “contract” model for the sampled per-simulation time series.

    The primary key is `(sim_id, t)` for stable per-sim time indexing.
    """

    __tablename__ = "sim_series"
    __table_args__ = {"extend_existing": True}

    sim_id: int = Field(
        primary_key=True, foreign_key="sweep_registry.sim_id", index=True
    )
    t: int = Field(primary_key=True, index=True)

    setting_id: int = Field(index=True)
    replicate_id: int = Field(index=True)

    prey: int
    predator: int

    prey_births: int
    prey_eaten: int
    predator_births: int
    predator_deaths: int

    # Optional convenience field if users later compute/attach it in their pipeline
    # (kept nullable to avoid forcing it in every artifact).
    phase: Optional[str] = Field(default=None, index=True)
