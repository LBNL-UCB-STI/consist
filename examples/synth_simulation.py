from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class PredatorPreyConfig:
    """
    A discrete-time stochastic predator–prey model meant to be intuitive:

    - Prey reproduce (random births) proportional to current prey.
    - Predators eat prey (random predation) proportional to prey*predators.
    - Predators reproduce from successful predation (random births).
    - Predators die (random deaths) proportional to current predators.

    This is NOT a continuous ODE solver; it is a simple Monte Carlo process.
    """

    steps: int = 250
    dt: float = 1.0
    seed: int = 7

    prey_init: int = 80
    predator_init: int = 25

    prey_birth_rate: float = 0.55
    predation_rate: float = 0.015
    predator_birth_efficiency: float = 0.20
    predator_death_rate: float = 0.35

    prey_carrying_capacity: int = 2_000
    max_population: int = 5_000_000

    sample_every: int = 1


def make_parameter_sweep(
    *,
    prey_birth_rates: Iterable[float],
    predation_rates: Iterable[float],
    predator_death_rates: Iterable[float],
    predator_birth_efficiency: float,
    replicates_per_setting: int,
    seed: int,
) -> pd.DataFrame:
    """
    Create a sweep registry: one row per (parameter setting, replicate).
    """
    rng = np.random.default_rng(seed)
    rows = []
    setting_id = 0
    for prey_birth_rate in prey_birth_rates:
        for predation_rate in predation_rates:
            for predator_death_rate in predator_death_rates:
                for rep in range(int(replicates_per_setting)):
                    run_seed = int(rng.integers(0, 2**31 - 1))
                    rows.append(
                        {
                            "setting_id": int(setting_id),
                            "replicate_id": int(rep),
                            "prey_birth_rate": float(prey_birth_rate),
                            "predation_rate": float(predation_rate),
                            "predator_death_rate": float(predator_death_rate),
                            "predator_birth_efficiency": float(
                                predator_birth_efficiency
                            ),
                            "seed": int(run_seed),
                        }
                    )
                setting_id += 1
    df = pd.DataFrame(rows)
    df.insert(0, "sim_id", np.arange(len(df), dtype=int))
    return df


def run_predator_prey(
    config: PredatorPreyConfig,
) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """
    Run one Monte Carlo predator–prey simulation.

    Returns:
        raw: dict containing full-resolution arrays for optional storage
        time_series: sampled time series (tabular, for DuckDB ingestion)
        metrics: single-row per-sim summary metrics (tabular, for DuckDB ingestion)
    """
    rng = np.random.default_rng(config.seed)
    steps = int(config.steps)
    dt = float(config.dt)

    prey = np.empty(steps + 1, dtype=np.int64)
    predator = np.empty(steps + 1, dtype=np.int64)
    prey[0] = int(config.prey_init)
    predator[0] = int(config.predator_init)

    prey_birth_rate = float(config.prey_birth_rate)
    predation_rate = float(config.predation_rate)
    predator_birth_efficiency = float(config.predator_birth_efficiency)
    predator_death_rate = float(config.predator_death_rate)
    prey_carrying_capacity = int(config.prey_carrying_capacity)
    max_population = int(config.max_population)

    samples = []
    for t in range(steps):
        x = int(prey[t])
        y = int(predator[t])

        # Logistic-style prey growth to avoid runaway explosion while staying intuitive.
        # When x approaches K, births taper toward ~0.
        capacity_factor = max(0.0, 1.0 - (x / max(1.0, float(prey_carrying_capacity))))
        expected_prey_births = max(0.0, prey_birth_rate * x * capacity_factor * dt)
        prey_births = int(rng.poisson(min(expected_prey_births, 1e8)))

        expected_encounters = max(0.0, predation_rate * x * y * dt)
        prey_eaten = int(min(x, rng.poisson(min(expected_encounters, 1e8))))

        expected_predator_births = max(0.0, predator_birth_efficiency * prey_eaten)
        predator_births = int(rng.poisson(min(expected_predator_births, 1e8)))

        predator_deaths = int(rng.binomial(n=y, p=min(1.0, predator_death_rate * dt)))

        x_next = max(0, x + prey_births - prey_eaten)
        y_next = max(0, y + predator_births - predator_deaths)

        # Safety clamp: keeps the process numerically stable even under extreme parameters.
        if x_next > max_population:
            x_next = max_population
        if y_next > max_population:
            y_next = max_population
        prey[t + 1] = x_next
        predator[t + 1] = y_next

        if t % int(config.sample_every) == 0:
            samples.append(
                {
                    "t": int(t),
                    "prey": int(x),
                    "predator": int(y),
                    "prey_births": int(prey_births),
                    "prey_eaten": int(prey_eaten),
                    "predator_births": int(predator_births),
                    "predator_deaths": int(predator_deaths),
                }
            )

    time_series = pd.DataFrame(samples)

    prey_final = int(prey[-1])
    predator_final = int(predator[-1])
    prey_extinct = int((prey == 0).any())
    predator_extinct = int((predator == 0).any())

    prey_mean = float(prey.mean())
    predator_mean = float(predator.mean())
    prey_std = float(prey.std())
    predator_std = float(predator.std())

    metrics = pd.DataFrame(
        [
            {
                "steps": int(config.steps),
                "dt": float(config.dt),
                "seed": int(config.seed),
                "prey_init": int(config.prey_init),
                "predator_init": int(config.predator_init),
                "prey_birth_rate": prey_birth_rate,
                "predation_rate": predation_rate,
                "predator_birth_efficiency": predator_birth_efficiency,
                "predator_death_rate": predator_death_rate,
                "prey_mean": prey_mean,
                "predator_mean": predator_mean,
                "prey_std": prey_std,
                "predator_std": predator_std,
                "prey_final": prey_final,
                "predator_final": predator_final,
                "prey_extinct": prey_extinct,
                "predator_extinct": predator_extinct,
            }
        ]
    )

    raw: Dict[str, Any] = {
        "prey": prey,
        "predator": predator,
    }
    return raw, time_series, metrics
