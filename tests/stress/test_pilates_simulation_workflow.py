import logging
import pandas as pd
import numpy as np
from sqlmodel import SQLModel, Field, select, func, Session
from consist.core.tracker import Tracker
from consist.models.run import Run
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
    with tracker.start_run(
        run_id=scenario_id,
        model="pilates_orchestrator",
        tags=["scenario_header"],
        config={"mode": storage_mode},
    ):
        pass

    # Init
    with tracker.start_run(
        run_id=f"{scenario_id}_init",
        parent_run_id=scenario_id,
        model="pop_synth",
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
        with tracker.start_run(
            run_id=f"{scenario_id}_year_{year}",
            parent_run_id=scenario_id,
            model="travel_demand",
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
            art = consist.log_artifact(str(path), key="persons")
            if storage_mode == "hot":
                consist.ingest(art, df_per, schema=Person)


def test_pilates_header_pattern(tmp_path):
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    years = [2020, 2030]
    run_simulation_scenario(tracker, "baseline", 10, years, "cold")
    run_simulation_scenario(tracker, "high_gas", 5, years, "hot")

    tracker.create_view("v_persons", "persons")
    tracker.create_view("v_households", "households")

    # Generate Dynamic View Models
    VPerson = consist.view(Person)
    VHousehold = consist.view(Household)

    # Q1: Comparing Scenarios
    logging.info("\n--- Query 1: Scenario Comparison ---")
    with Session(tracker.engine) as session:
        query = (
            select(
                Run.parent_run_id.label("scenario_id"),
                VPerson.consist_year,
                func.avg(VPerson.number_of_trips).label("avg_trips"),
            )
            .select_from(VPerson)
            .join(Run, VPerson.consist_run_id == Run.id)
            .where(Run.parent_run_id.in_(["scenario_baseline", "scenario_high_gas"]))
            .group_by(Run.parent_run_id, VPerson.consist_year)
            .order_by("scenario_id", "consist_year")
        )

        results = session.exec(query).all()
        for r in results:
            logging.info(f"{r.scenario_id:<20} {r.consist_year}: {r.avg_trips:.2f}")

        assert len(results) == 4

        # Validation Logic
        res_map = {(r.scenario_id, r.consist_year): r.avg_trips for r in results}
        assert (
            res_map[("scenario_high_gas", 2030)] < res_map[("scenario_baseline", 2030)]
        )

    # Q2: Complex Join
    logging.info("\n--- Query 2: Trips by Region ---")
    with Session(tracker.engine) as session:
        from sqlalchemy.orm import aliased

        PersonRun = aliased(Run)
        HouseholdRun = aliased(Run)

        # Note: We use explicit Column objects in group_by/order_by
        # to avoid ambiguity between VPerson.consist_year and VHousehold.consist_year
        query = (
            select(
                PersonRun.parent_run_id.label("scenario"),
                VHousehold.region,
                VPerson.consist_year,
                func.sum(VPerson.number_of_trips).label("total_trips"),
            )
            .select_from(VPerson)
            .join(PersonRun, VPerson.consist_run_id == PersonRun.id)
            .join(VHousehold, VPerson.household_id == VHousehold.household_id)
            .join(HouseholdRun, VHousehold.consist_run_id == HouseholdRun.id)
            .where(PersonRun.parent_run_id == HouseholdRun.parent_run_id)
            .group_by(PersonRun.parent_run_id, VHousehold.region, VPerson.consist_year)
            .order_by(
                "scenario", VHousehold.region, VPerson.consist_year
            )  # <--- FIXED HERE
        )

        results = session.exec(query).all()
        assert len(results) > 0

    # Q3: Helper Method Usage
    # Find runs belonging to the baseline scenario header
    steps = tracker.find_runs(parent_id="scenario_baseline")
    assert len(steps) == 3

    # Q4: Snapshot Retrieval
    # We want the 2030 run. It is the most recent, so it is at index 0.
    target_run = steps[0]

    # Verify we got the right run
    assert target_run.year == 2030

    artifacts = tracker.get_artifacts_for_run(target_run.id)
    person_art = next(a for a, d in artifacts if a.key == "persons" and d == "output")

    df = consist.load(person_art, tracker=tracker)
    assert len(df) == 300
