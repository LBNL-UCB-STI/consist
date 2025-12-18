from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

import numpy as np
import pandas as pd

from .synth_simulation import (
    PredatorPreyConfig,
    make_parameter_sweep,
    run_predator_prey,
)


def build_sweep_registry(
    *,
    prey_birth_rates: Iterable[float],
    predation_rates: Iterable[float],
    predator_death_rates: Iterable[float],
    predator_birth_efficiency: float,
    replicates_per_setting: int,
    seed: int,
) -> pd.DataFrame:
    return make_parameter_sweep(
        prey_birth_rates=list(prey_birth_rates),
        predation_rates=list(predation_rates),
        predator_death_rates=list(predator_death_rates),
        predator_birth_efficiency=float(predator_birth_efficiency),
        replicates_per_setting=int(replicates_per_setting),
        seed=seed,
    )


def make_run_id(*, scenario_id: str, sim_id: int) -> str:
    return f"{scenario_id}_sim_{sim_id:04d}"


def run_one_simulation(
    *,
    base_config: PredatorPreyConfig,
    registry_row: Mapping[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Execute exactly one simulation described by a sweep registry row.

    Returns:
        sim_metrics: single-row dataframe with summary metrics and parameters
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
    _raw, time_series, metrics = run_predator_prey(cfg)

    sim_id = int(registry_row["sim_id"])
    setting_id = int(registry_row["setting_id"])
    replicate_id = int(registry_row["replicate_id"])

    metrics = metrics.copy()
    metrics.insert(0, "sim_id", sim_id)
    metrics.insert(1, "setting_id", setting_id)
    metrics.insert(2, "replicate_id", replicate_id)

    time_series = time_series.copy()
    time_series.insert(0, "sim_id", sim_id)
    time_series.insert(1, "setting_id", setting_id)
    time_series.insert(2, "replicate_id", replicate_id)

    return metrics, time_series


def run_one_simulation_with_raw(
    *,
    base_config: PredatorPreyConfig,
    registry_row: Mapping[str, Any],
) -> Tuple[Dict[str, np.ndarray], pd.DataFrame, pd.DataFrame]:
    """
    Same as `run_one_simulation`, but also returns raw arrays for optional storage.
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
    raw, time_series, metrics = run_predator_prey(cfg)

    sim_id = int(registry_row["sim_id"])
    setting_id = int(registry_row["setting_id"])
    replicate_id = int(registry_row["replicate_id"])

    metrics = metrics.copy()
    metrics.insert(0, "sim_id", sim_id)
    metrics.insert(1, "setting_id", setting_id)
    metrics.insert(2, "replicate_id", replicate_id)

    time_series = time_series.copy()
    time_series.insert(0, "sim_id", sim_id)
    time_series.insert(1, "setting_id", setting_id)
    time_series.insert(2, "replicate_id", replicate_id)

    raw_arrays: Dict[str, np.ndarray] = {
        "prey": np.asarray(raw["prey"], dtype=np.int32),
        "predator": np.asarray(raw["predator"], dtype=np.int32),
    }
    return raw_arrays, time_series, metrics


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


def write_npz(raw: Dict[str, np.ndarray], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, **raw)
    return path
