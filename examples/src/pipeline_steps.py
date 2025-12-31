from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from synth_simulation import (
    PredatorPreyConfig,
    make_parameter_sweep,
    run_predator_prey,
)


def _py_scalar(value: Any) -> Any:
    return value.item() if hasattr(value, "item") else value


def build_sweep_registry(
    *,
    prey_birth_rates: Iterable[float],
    predation_rates: Iterable[float],
    predator_death_rates: Iterable[float],
    predator_birth_efficiency: float,
    replicates_per_setting: int,
    seed: int,
) -> pd.DataFrame:
    df = make_parameter_sweep(
        prey_birth_rates=list(prey_birth_rates),
        predation_rates=list(predation_rates),
        predator_death_rates=list(predator_death_rates),
        predator_birth_efficiency=float(predator_birth_efficiency),
        replicates_per_setting=int(replicates_per_setting),
        seed=seed,
    )
    return df.apply(lambda col: col.map(_py_scalar))


def make_run_id(*, scenario_id: str, sim_id: int) -> str:
    return f"{scenario_id}_sim_{sim_id:04d}"


def run_one_simulation(
    *,
    base_config: PredatorPreyConfig,
    registry_row: Mapping[str, Any],
) -> pd.DataFrame:
    """
    Execute exactly one simulation described by a sweep registry row.

    Returns:
        sim_time_series: sampled time-series dataframe
    """
    cfg = PredatorPreyConfig(
        steps=int(base_config.steps),
        dt=float(base_config.dt),
        seed=int(registry_row["seed"]),
        prey_init=int(base_config.prey_init),
        predator_init=int(base_config.predator_init),
        prey_birth_rate=float(registry_row["prey_birth_rate"]),
        predation_rate=float(registry_row["predation_rate"]),
        predator_birth_efficiency=float(registry_row["predator_birth_efficiency"]),
        predator_death_rate=float(registry_row["predator_death_rate"]),
        sample_every=int(base_config.sample_every),
    )
    time_series = run_predator_prey(cfg)
    return time_series


def aggregate_parquet(
    *,
    paths: Iterable[Path],
) -> pd.DataFrame:
    frames = []
    for path in paths:
        frames.append(pd.read_parquet(path))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
