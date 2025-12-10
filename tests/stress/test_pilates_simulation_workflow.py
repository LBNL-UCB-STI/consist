import logging
import pandas as pd
import numpy as np
from sqlmodel import SQLModel, Field, select, func, Session
from consist.core.tracker import Tracker
import consist


# --- 1. Domain Models ---
class Household(SQLModel, table=True):
    __tablename__ = "households"
    __table_args__ = {"extend_existing": True}
    household_id: int = Field(primary_key=True)
    region: str
    income_segment: str


class Person(SQLModel, table=True):
    __tablename__ = "persons"
    __table_args__ = {"extend_existing": True}
    person_id: int = Field(primary_key=True)
    household_id: int = Field(foreign_key="households.household_id")
    age: int
    number_of_trips: int


# --- 2. Simulation Helper ---
def run_simulation_scenario(tracker, scenario_name, base_trips, years, storage_mode):
    rng = np.random.default_rng(42)
    n_hh = 100
    n_per = 300

    # Header
    scenario_id = f"scenario_{scenario_name}"
    with tracker.scenario(
        scenario_name,
        config={"mode": storage_mode},
        model="pilates_orchestrator",
        tags=["scenario_header"],
    ) as scenario:

        # Init
        with scenario.step(
            run_id=f"{scenario_id}_init",
            name="pop_synth",
            tags=["init"],
        ):
            df_hh = pd.DataFrame(
                {
                    "household_id": np.arange(n_hh),
                    "region": rng.choice(["North", "South", "East"], size=n_hh),
                    "income_segment": rng.choice(["Low", "Med", "High"], size=n_hh),
                }
            )
            path = tracker.run_dir / f"{scenario_name}_households.parquet"
            df_hh.to_parquet(path)
            art = consist.log_artifact(str(path), key="households")
            if storage_mode == "hot":
                consist.ingest(art, df_hh, schema=Household)

        # Years
        for year in years:
            with scenario.step(
                run_id=f"{scenario_id}_year_{year}",
                name="travel_demand",
                year=year,
                tags=["simulation"],
            ):
                trips = rng.poisson(lam=base_trips, size=n_per)
                df_per = pd.DataFrame(
                    {
                        "person_id": np.arange(n_per),
                        "household_id": rng.integers(0, n_hh, size=n_per),
                        "age": rng.integers(18, 80, size=n_per),
                        "number_of_trips": trips,
                    }
                )
                path = tracker.run_dir / f"{scenario_name}_persons_{year}.parquet"
                df_per.to_parquet(path)
                art = tracker.log_artifact(str(path), key="persons")
                if storage_mode == "hot":
                    tracker.ingest(art, df_per, schema=Person)


def test_pilates_header_pattern(tmp_path):
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path, schemas=[Household, Person])
    tracker.identity.hashing_strategy = "fast"

    years = [2020, 2030]
    run_simulation_scenario(tracker, "baseline", 10, years, "cold")
    run_simulation_scenario(tracker, "high_gas", 5, years, "hot")

    # Generate Dynamic View Models
    VPerson = tracker.views.Person
    VHousehold = tracker.views.Household

    # Q1: Comparing Scenarios
    logging.info("\n--- Query 1: Scenario Comparison ---")
    query = (
        select(
            VPerson.consist_scenario_id.label("scenario_id"),
            VPerson.consist_year,
            func.avg(VPerson.number_of_trips).label("avg_trips"),
        )
        .select_from(VPerson)
        .where(VPerson.consist_scenario_id.in_(["baseline", "high_gas"]))
        .group_by(VPerson.consist_scenario_id, VPerson.consist_year)
        .order_by("scenario_id", "consist_year")
    )
    results = consist.run_query(query, tracker=tracker)
    for r in results:
        logging.info(f"{r.scenario_id:<20} {r.consist_year}: {r.avg_trips:.2f}")

    assert len(results) == 4

    # Validation Logic
    res_map = {(r.scenario_id, r.consist_year): r.avg_trips for r in results}
    assert res_map[("high_gas", 2030)] < res_map[("baseline", 2030)]

    # Q2: Complex Join
    logging.info("\n--- Query 2: Trips by Region ---")
    with Session(tracker.engine) as session:
        query = (
            select(
                VPerson.consist_scenario_id.label("scenario"),
                VHousehold.region,
                VPerson.consist_year,
                func.sum(VPerson.number_of_trips).label("total_trips"),
            )
            .select_from(VPerson)
            .join(VHousehold, VPerson.household_id == VHousehold.household_id)
            # Ensure we are joining data from same scenario context
            .where(VPerson.consist_scenario_id == VHousehold.consist_scenario_id)
            .group_by(
                VPerson.consist_scenario_id, VHousehold.region, VPerson.consist_year
            )
            .order_by("scenario", VHousehold.region, VPerson.consist_year)
        )

        results = session.exec(query).all()
        assert len(results) > 0

    # Q3: Helper Method Usage
    # We want the run from 2030 specifically
    target_run = tracker.find_run(parent_id="baseline", year=2030)

    assert target_run.year == 2030
    assert target_run.model_name == "travel_demand"

    # Indexing example
    # Get all runs for baseline, indexed by year
    # (Note: init/pop_synth has year=None, so it keys to None)
    runs_by_year = tracker.find_runs(parent_id="baseline", index_by="year")
    assert 2020 in runs_by_year
    assert 2030 in runs_by_year
    assert runs_by_year[2030].id == target_run.id

    # Q4: Snapshot Retrieval
    # Structured access
    artifacts = tracker.get_artifacts_for_run(target_run.id)
    # Dict access by key!
    person_art = artifacts.outputs["persons"]

    df = consist.load(person_art, tracker=tracker)
    assert len(df) == 300


def test_pilates_header_pattern_api(tmp_path):
    """
    Same workflow as above, but exercising the top-level API shortcuts.
    """
    run_dir = tmp_path / "runs_api"
    db_path = str(tmp_path / "provenance_api.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path, schemas=[Household, Person])
    tracker.identity.hashing_strategy = "fast"

    years = [2025, 2035]
    scenarios = [("baseline_api", 10, "cold"), ("high_gas_api", 5, "hot")]

    rng = np.random.default_rng(7)
    n_hh = 60
    n_per = 200

    for name, base_trips, mode in scenarios:
        with consist.scenario(
            name,
            tracker=tracker,
            model="pilates_orchestrator",
            tags=["scenario_header"],
        ) as sc:
            with sc.step(name="pop_synth", run_id=f"{name}_init", tags=["init"]):
                df_hh = pd.DataFrame(
                    {
                        "household_id": np.arange(n_hh),
                        "region": rng.choice(["North", "South", "East"], size=n_hh),
                        "income_segment": rng.choice(["Low", "Med", "High"], size=n_hh),
                    }
                )
                consist.log_dataframe(df_hh, key="households", schema=Household)

            for year in years:
                with sc.step(
                    name="travel_demand",
                    run_id=f"{name}_{year}",
                    year=year,
                    tags=["simulation"],
                ):
                    trips = rng.poisson(lam=base_trips, size=n_per)
                    df_per = pd.DataFrame(
                        {
                            "person_id": np.arange(n_per),
                            "household_id": rng.integers(0, n_hh, size=n_per),
                            "age": rng.integers(18, 80, size=n_per),
                            "number_of_trips": trips,
                        }
                    )
                    consist.log_dataframe(
                        df_per, key="persons", schema=Person, direction="output"
                    )

    target_run = consist.find_run(tracker=tracker, parent_id="baseline_api", year=2035)
    assert target_run is not None
    assert target_run.year == 2035
    assert target_run.model_name == "travel_demand"

    runs_by_year = consist.find_runs(
        tracker=tracker, parent_id="baseline_api", index_by="year"
    )
    assert runs_by_year[2035].id == target_run.id

    artifacts = tracker.get_artifacts_for_run(target_run.id)
    person_art = artifacts.outputs["persons"]
    df = consist.load(person_art, tracker=tracker)
    assert len(df) == n_per
