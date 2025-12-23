"""
Transportation equilibrium model for demonstrating deep provenance chains.

A simplified model for a linear 5-zone city:
1. Trip Distribution: Logsum-based gravity model (accessibility-weighted)
2. Mode Choice: Multinomial logit (car, transit, walk)
3. Traffic Assignment: All-or-nothing with BPR congestion

Functions read and write DataFrames and xarray Datasets. No framework glue—
iteration control, convergence checking, and provenance tracking are handled
by the calling code.

Skims are stored as xarray Datasets in zarr format to demonstrate array handling.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


# =============================================================================
# Configuration (plain dataclasses, JSON-serializable)
# =============================================================================


@dataclass
class ZoneParams:
    """Parameters defining the 5-zone linear city."""

    populations: tuple[int, ...] = (2000, 2000, 2000, 2000, 2000)
    jobs: tuple[int, ...] = (1000, 1500, 5000, 1500, 1000)
    parking_costs: tuple[float, ...] = (0.0, 5.0, 15.0, 5.0, 0.0)
    has_transit: tuple[bool, ...] = (True, True, True, True, False)

    within_zone_distance: float = 1.0
    adjacent_zone_distance: float = 2.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ModeChoiceParams:
    """Parameters for multinomial logit mode choice."""

    beta_time: float = -0.10
    beta_cost: float = -0.50

    asc_transit: float = -1.5
    asc_walk: float = -0.5

    fuel_cost_per_mile: float = 0.20
    transit_fare: float = 2.50

    walk_speed_mph: float = 3.0
    transit_speed_mph: float = 20.0
    free_flow_car_speed_mph: float = 35.0

    max_walk_distance: float = 2.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DestinationChoiceParams:
    """Parameters for logsum-based destination choice."""

    beta_size: float = 1.0  # coefficient on log(jobs)
    beta_access: float = 0.5  # coefficient on mode choice logsum

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AssignmentParams:
    """Parameters for traffic assignment (BPR function)."""

    bpr_alpha: float = 0.15
    bpr_beta: float = 4.0
    base_capacity: float = 1500.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TravelDemandScenarioConfig:
    """High-level scenario configuration."""

    n_iterations: int = 10
    seed: int = 0
    zone_params: ZoneParams = field(default_factory=ZoneParams)
    mode_params: ModeChoiceParams = field(default_factory=ModeChoiceParams)
    dest_params: DestinationChoiceParams = field(
        default_factory=DestinationChoiceParams
    )
    assignment_params: AssignmentParams = field(default_factory=AssignmentParams)

    def to_dict(self) -> dict:
        return asdict(self)


# =============================================================================
# Skim I/O (xarray + zarr)
# =============================================================================


def create_skims_dataset(
    zones: pd.DataFrame,
    distances: pd.DataFrame,
    mode_params: ModeChoiceParams | None = None,
) -> xr.Dataset:
    """Create initial skims as an xarray Dataset.

    Dimensions: (origin, destination)
    Variables: distance_miles, time_car_mins, time_transit_mins, time_walk_mins

    Also stores free-flow car time for BPR calculations.
    """
    mode_params = mode_params or ModeChoiceParams()

    zone_ids = sorted(zones["zone_id"].tolist())
    n_zones = len(zone_ids)

    # Build distance matrix
    dist_matrix = np.zeros((n_zones, n_zones))
    for _, row in distances.iterrows():
        o_idx = zone_ids.index(int(row["origin"]))
        d_idx = zone_ids.index(int(row["destination"]))
        dist_matrix[o_idx, d_idx] = row["distance_miles"]

    # Compute times
    time_car = dist_matrix / mode_params.free_flow_car_speed_mph * 60
    time_transit = dist_matrix / mode_params.transit_speed_mph * 60
    time_walk = dist_matrix / mode_params.walk_speed_mph * 60

    ds = xr.Dataset(
        {
            "distance_miles": (["origin", "destination"], dist_matrix),
            "time_car_mins": (["origin", "destination"], time_car),
            "time_car_ff_mins": (["origin", "destination"], time_car.copy()),
            "time_transit_mins": (["origin", "destination"], time_transit),
            "time_walk_mins": (["origin", "destination"], time_walk),
        },
        coords={
            "origin": zone_ids,
            "destination": zone_ids,
        },
    )

    return ds


def save_skims(ds: xr.Dataset, path: Path | str) -> None:
    """Save skims to zarr format."""
    path = Path(path)
    if path.exists():
        import shutil

        shutil.rmtree(path)
    ds.to_zarr(path)


def load_skims(path: Path | str) -> xr.Dataset:
    """Load skims from zarr format."""
    return xr.open_zarr(path)


def skims_to_dataframe(ds: xr.Dataset) -> pd.DataFrame:
    """Convert skims Dataset to long-form DataFrame for joins."""
    df = ds.to_dataframe().reset_index()
    return df


# =============================================================================
# Data Generation
# =============================================================================


def generate_zones(params: ZoneParams | None = None) -> pd.DataFrame:
    """Generate zone characteristics table."""
    params = params or ZoneParams()
    n_zones = len(params.populations)

    return pd.DataFrame(
        {
            "zone_id": range(1, n_zones + 1),
            "population": params.populations,
            "jobs": params.jobs,
            "parking_cost": params.parking_costs,
            "has_transit": params.has_transit,
        }
    )


def generate_distances(params: ZoneParams | None = None) -> pd.DataFrame:
    """Generate O-D distance matrix for linear city."""
    params = params or ZoneParams()
    n_zones = len(params.populations)

    rows = []
    for o in range(1, n_zones + 1):
        for d in range(1, n_zones + 1):
            if o == d:
                dist = params.within_zone_distance
            else:
                dist = params.adjacent_zone_distance * abs(o - d)
            rows.append({"origin": o, "destination": d, "distance_miles": dist})

    return pd.DataFrame(rows)


def generate_population(
    zones: pd.DataFrame,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic population with home zone assignments.

    Returns DataFrame with: person_id, home_zone
    """
    rng = np.random.default_rng(seed)

    persons = []
    person_id = 0

    for _, zone in zones.iterrows():
        for _ in range(int(zone["population"])):
            persons.append(
                {
                    "person_id": person_id,
                    "home_zone": int(zone["zone_id"]),
                }
            )
            person_id += 1

    df = pd.DataFrame(persons)
    return df.sample(frac=1, random_state=int(rng.integers(0, 2**31))).reset_index(
        drop=True
    )


# =============================================================================
# Mode Choice Logsum (needed for destination choice)
# =============================================================================


def compute_od_logsums(
    skims: xr.Dataset,
    zones: pd.DataFrame,
    mode_params: ModeChoiceParams | None = None,
) -> xr.DataArray:
    """Compute mode choice logsum for each O-D pair.

    Logsum = log(sum_m exp(V_m)) for available modes.

    Returns DataArray with dims (origin, destination).
    """
    mode_params = mode_params or ModeChoiceParams()

    zone_ids = list(skims.coords["origin"].values)
    n_zones = len(zone_ids)

    # Build zone lookup arrays
    parking = np.array(
        [zones.loc[zones["zone_id"] == z, "parking_cost"].iloc[0] for z in zone_ids]
    )
    has_transit_arr = np.array(
        [zones.loc[zones["zone_id"] == z, "has_transit"].iloc[0] for z in zone_ids]
    )

    distance = skims["distance_miles"].values
    time_car = skims["time_car_mins"].values
    time_transit = skims["time_transit_mins"].values
    time_walk = skims["time_walk_mins"].values

    # Compute utilities
    # V_car = beta_time * time + beta_cost * (fuel_cost * dist + parking[dest])
    cost_car = mode_params.fuel_cost_per_mile * distance + parking[np.newaxis, :]
    u_car = mode_params.beta_time * time_car + mode_params.beta_cost * cost_car

    # V_transit = beta_time * time + beta_cost * fare + ASC
    u_transit = (
        mode_params.beta_time * time_transit
        + mode_params.beta_cost * mode_params.transit_fare
        + mode_params.asc_transit
    )

    # V_walk = beta_time * time + ASC (if within max distance)
    u_walk = mode_params.beta_time * time_walk + mode_params.asc_walk

    # Availability masks
    # Transit: both origin and destination must have transit
    transit_avail = has_transit_arr[:, np.newaxis] & has_transit_arr[np.newaxis, :]
    walk_avail = distance <= mode_params.max_walk_distance

    # Mask unavailable modes
    u_transit = np.where(transit_avail, u_transit, -np.inf)
    u_walk = np.where(walk_avail, u_walk, -np.inf)

    # Stack and compute logsum with numerical stability
    utilities = np.stack([u_car, u_transit, u_walk], axis=-1)
    max_u = np.max(utilities, axis=-1, keepdims=True)
    max_u = np.where(np.isinf(max_u), 0, max_u)

    logsum = max_u.squeeze(-1) + np.log(np.sum(np.exp(utilities - max_u), axis=-1))

    return xr.DataArray(
        logsum,
        dims=["origin", "destination"],
        coords={"origin": zone_ids, "destination": zone_ids},
        name="logsum",
    )


# =============================================================================
# Trip Distribution (Logsum-based Gravity Model)
# =============================================================================


def distribute_trips(
    population: pd.DataFrame,
    zones: pd.DataFrame,
    logsums: xr.DataArray,
    dest_params: DestinationChoiceParams | None = None,
    seed: int = 42,
    prev_trips: pd.DataFrame | None = None,
    update_share: float = 1.0,
) -> pd.DataFrame:
    """Assign work locations using logsum-based gravity model.

    Utility(dest j | origin i) = β_size * log(jobs_j) + β_access * logsum_ij

    Returns DataFrame with: person_id, home_zone, work_zone
    """
    dest_params = dest_params or DestinationChoiceParams()
    rng = np.random.default_rng(seed)
    if not 0.0 <= update_share <= 1.0:
        raise ValueError("update_share must be between 0.0 and 1.0.")

    zone_ids = list(logsums.coords["origin"].values)
    jobs_by_zone = zones.set_index("zone_id")["jobs"].to_dict()

    # Precompute destination utilities for each origin
    # U[o, d] = beta_size * log(jobs[d]) + beta_access * logsum[o, d]
    log_jobs = np.array([np.log(jobs_by_zone[z]) for z in zone_ids])
    dest_utility = (
        dest_params.beta_size * log_jobs[np.newaxis, :]
        + dest_params.beta_access * logsums.values
    )

    # Convert to probabilities
    dest_utility_shifted = dest_utility - dest_utility.max(axis=1, keepdims=True)
    exp_u = np.exp(dest_utility_shifted)
    dest_probs = exp_u / exp_u.sum(axis=1, keepdims=True)

    # Create lookup from zone_id to index
    zone_to_idx = {z: i for i, z in enumerate(zone_ids)}

    trips = []
    for _, person in population.iterrows():
        home = int(person["home_zone"])
        home_idx = zone_to_idx[home]

        work_zone = int(rng.choice(zone_ids, p=dest_probs[home_idx]))

        trips.append(
            {
                "person_id": int(person["person_id"]),
                "home_zone": home,
                "work_zone": work_zone,
            }
        )

    trips_df = pd.DataFrame(trips)

    if prev_trips is not None and update_share < 1.0:
        prev_dest = prev_trips[["person_id", "work_zone"]].drop_duplicates("person_id")
        blended = trips_df.merge(
            prev_dest,
            on="person_id",
            how="left",
            suffixes=("", "_prev"),
            sort=False,
        )
        update_mask = rng.random(len(blended)) < update_share
        keep_prev_mask = blended["work_zone_prev"].notna() & ~update_mask
        blended.loc[keep_prev_mask, "work_zone"] = blended.loc[
            keep_prev_mask, "work_zone_prev"
        ]
        trips_df = blended.drop(columns=["work_zone_prev"])

    return trips_df


# =============================================================================
# Mode Choice
# =============================================================================


def compute_mode_utilities(
    trips: pd.DataFrame,
    skims: xr.Dataset,
    zones: pd.DataFrame,
    mode_params: ModeChoiceParams | None = None,
) -> pd.DataFrame:
    """Compute utility and availability of each mode for each trip.

    Returns trips DataFrame augmented with utility_* and available_* columns.
    """
    mode_params = mode_params or ModeChoiceParams()

    # Convert skims to DataFrame for merging
    skims_df = skims_to_dataframe(skims)

    df = trips.copy()

    # Merge travel times and distances
    df = df.merge(
        skims_df[
            [
                "origin",
                "destination",
                "distance_miles",
                "time_car_mins",
                "time_transit_mins",
                "time_walk_mins",
            ]
        ],
        left_on=["home_zone", "work_zone"],
        right_on=["origin", "destination"],
    ).drop(columns=["origin", "destination"])

    # Merge destination parking cost and transit access
    df = df.merge(
        zones[["zone_id", "parking_cost"]].rename(columns={"zone_id": "work_zone"}),
        on="work_zone",
    )
    df = df.merge(
        zones[["zone_id", "has_transit"]].rename(
            columns={"zone_id": "work_zone", "has_transit": "dest_has_transit"}
        ),
        on="work_zone",
    )
    df = df.merge(
        zones[["zone_id", "has_transit"]].rename(
            columns={"zone_id": "home_zone", "has_transit": "orig_has_transit"}
        ),
        on="home_zone",
    )

    # Compute costs
    df["cost_car"] = (
        df["distance_miles"] * mode_params.fuel_cost_per_mile + df["parking_cost"]
    )
    df["cost_transit"] = mode_params.transit_fare
    df["cost_walk"] = 0.0

    # Compute utilities
    df["utility_car"] = (
        mode_params.beta_time * df["time_car_mins"]
        + mode_params.beta_cost * df["cost_car"]
    )
    df["utility_transit"] = (
        mode_params.beta_time * df["time_transit_mins"]
        + mode_params.beta_cost * df["cost_transit"]
        + mode_params.asc_transit
    )
    df["utility_walk"] = (
        mode_params.beta_time * df["time_walk_mins"]
        + mode_params.beta_cost * df["cost_walk"]
        + mode_params.asc_walk
    )

    # Availability
    df["available_car"] = True
    df["available_transit"] = df["orig_has_transit"] & df["dest_has_transit"]
    df["available_walk"] = df["distance_miles"] <= mode_params.max_walk_distance

    return df


def apply_mode_choice(
    utilities_df: pd.DataFrame,
    seed: int = 42,
    prev_trips: pd.DataFrame | None = None,
    update_share: float = 1.0,
) -> pd.DataFrame:
    """Apply multinomial logit to select mode for each trip.

    If prev_trips is provided, update_share controls the fraction of trips
    that are resampled; the remainder keep their previous mode.

    Returns DataFrame with: person_id, home_zone, work_zone, distance_miles,
                           mode, time_mins, cost
    """
    if not 0.0 <= update_share <= 1.0:
        raise ValueError("update_share must be between 0.0 and 1.0.")

    rng = np.random.default_rng(seed)

    # Build utility matrix, masking unavailable modes
    u_car = utilities_df["utility_car"].values.copy()
    u_transit = utilities_df["utility_transit"].values.copy()
    u_walk = utilities_df["utility_walk"].values.copy()

    u_transit = np.where(utilities_df["available_transit"], u_transit, -np.inf)
    u_walk = np.where(utilities_df["available_walk"], u_walk, -np.inf)

    u_matrix = np.column_stack([u_car, u_transit, u_walk])

    # Softmax with numerical stability
    u_matrix = np.where(np.isinf(u_matrix), -700, u_matrix)
    u_shifted = u_matrix - u_matrix.max(axis=1, keepdims=True)
    exp_u = np.exp(u_shifted)
    probs = exp_u / exp_u.sum(axis=1, keepdims=True)

    # Sample
    mode_names = ["car", "transit", "walk"]
    modes = [mode_names[rng.choice(3, p=p)] for p in probs]

    # Build result
    result = utilities_df[
        ["person_id", "home_zone", "work_zone", "distance_miles"]
    ].copy()
    result["mode"] = modes

    if prev_trips is not None and update_share < 1.0:
        key_cols = ["person_id", "home_zone", "work_zone"]
        prev_modes = prev_trips[key_cols + ["mode"]].drop_duplicates(key_cols)
        blended = result.merge(
            prev_modes, on=key_cols, how="left", suffixes=("", "_prev"), sort=False
        )
        update_mask = rng.random(len(blended)) < update_share
        keep_prev_mask = blended["mode_prev"].notna() & ~update_mask
        blended.loc[keep_prev_mask, "mode"] = blended.loc[keep_prev_mask, "mode_prev"]
        result = blended.drop(columns=["mode_prev"])

    mode_arr = result["mode"].to_numpy()
    result["time_mins"] = np.select(
        [mode_arr == "car", mode_arr == "transit", mode_arr == "walk"],
        [
            utilities_df["time_car_mins"],
            utilities_df["time_transit_mins"],
            utilities_df["time_walk_mins"],
        ],
    )
    result["cost"] = np.select(
        [mode_arr == "car", mode_arr == "transit", mode_arr == "walk"],
        [
            utilities_df["cost_car"],
            utilities_df["cost_transit"],
            utilities_df["cost_walk"],
        ],
    )

    return result


def compute_mode_shares(trips: pd.DataFrame) -> dict[str, float]:
    """Compute mode share fractions from trips with modes."""
    counts = trips["mode"].value_counts()
    total = len(trips)
    return {
        "car": float(counts.get("car", 0) / total),
        "transit": float(counts.get("transit", 0) / total),
        "walk": float(counts.get("walk", 0) / total),
    }


# =============================================================================
# Traffic Assignment
# =============================================================================


def compute_od_volumes(trips: pd.DataFrame) -> pd.DataFrame:
    """Aggregate car trips into O-D volumes.

    Returns DataFrame with: origin, destination, volume
    """
    car_trips = trips[trips["mode"] == "car"]

    if len(car_trips) == 0:
        return pd.DataFrame(columns=["origin", "destination", "volume"])

    return (
        car_trips.groupby(["home_zone", "work_zone"])
        .size()
        .reset_index(name="volume")
        .rename(columns={"home_zone": "origin", "work_zone": "destination"})
    )


def apply_congestion(
    skims: xr.Dataset,
    volumes: pd.DataFrame,
    assignment_params: AssignmentParams | None = None,
) -> xr.Dataset:
    """Update car travel times using BPR function.

    time = free_flow_time * (1 + α * (V/C)^β)

    Returns new Dataset with updated time_car_mins.
    """
    assignment_params = assignment_params or AssignmentParams()

    zone_ids = list(skims.coords["origin"].values)
    zone_to_idx = {z: i for i, z in enumerate(zone_ids)}
    n_zones = len(zone_ids)

    # Build volume matrix
    vol_matrix = np.zeros((n_zones, n_zones))
    for _, row in volumes.iterrows():
        o_idx = zone_to_idx[int(row["origin"])]
        d_idx = zone_to_idx[int(row["destination"])]
        vol_matrix[o_idx, d_idx] = row["volume"]

    # BPR function
    free_flow = skims["time_car_ff_mins"].values
    vc_ratio = vol_matrix / assignment_params.base_capacity
    congestion_factor = 1 + assignment_params.bpr_alpha * (
        vc_ratio**assignment_params.bpr_beta
    )
    congested_time = free_flow * congestion_factor

    # Create new dataset with updated times
    new_skims = skims.copy()
    new_skims["time_car_mins"] = (["origin", "destination"], congested_time)

    return new_skims


# =============================================================================
# Convergence Checking
# =============================================================================


def compute_max_share_change(
    current_shares: dict[str, float],
    previous_shares: dict[str, float] | None,
) -> float:
    """Compute maximum absolute change in mode shares."""
    if previous_shares is None:
        return 1.0
    return max(
        abs(current_shares[m] - previous_shares.get(m, 0)) for m in current_shares
    )


def check_convergence(max_change: float, threshold: float) -> bool:
    """Check if mode shares have converged."""
    return max_change < threshold


# =============================================================================
# Analysis Helpers
# =============================================================================


def compute_vmt(trips: pd.DataFrame) -> float:
    """Compute total vehicle miles traveled (car only)."""
    car_trips = trips[trips["mode"] == "car"]
    return float(car_trips["distance_miles"].sum())


def compute_pmt(trips: pd.DataFrame) -> float:
    """Compute total person miles traveled (all modes)."""
    return float(trips["distance_miles"].sum())


def compute_average_times(trips: pd.DataFrame) -> dict[str, float]:
    """Compute average travel time by mode."""
    result = {}
    for mode in ["car", "transit", "walk"]:
        mode_trips = trips[trips["mode"] == mode]
        if len(mode_trips) > 0:
            result[mode] = float(mode_trips["time_mins"].mean())
    return result


def compute_destination_shares(trips: pd.DataFrame) -> pd.Series:
    """Compute share of trips going to each destination zone."""
    return trips["work_zone"].value_counts(normalize=True).sort_index()


def summarize_iteration(
    iteration: int,
    trips: pd.DataFrame,
    mode_shares: dict[str, float],
    max_change: float,
    converged: bool,
) -> dict:
    """Create a summary dict for one iteration."""
    return {
        "iteration": iteration,
        "converged": converged,
        "max_share_change": max_change,
        "car_share": mode_shares["car"],
        "transit_share": mode_shares["transit"],
        "walk_share": mode_shares["walk"],
        "vmt": compute_vmt(trips),
        "total_trips": len(trips),
        "car_trips": int((trips["mode"] == "car").sum()),
        "transit_trips": int((trips["mode"] == "transit").sum()),
        "walk_trips": int((trips["mode"] == "walk").sum()),
    }
