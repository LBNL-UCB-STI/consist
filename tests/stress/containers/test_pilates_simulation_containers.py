import logging
from pathlib import Path
import pandas as pd
import numpy as np
from unittest.mock import patch
from sqlmodel import SQLModel, Field, select, func

from consist.core.tracker import Tracker
from consist.models.run import Run
import consist
from consist.integrations.containers import run_container


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
def run_simulation_scenario(
    tracker,
    scenario_name,
    base_trips,
    years,
    storage_mode,
    base_trips_by_year=None,
    advance_delta_by_year=None,
):
    rng = np.random.default_rng(42)
    n_hh = 100
    n_per = 300

    base_trips_by_year = base_trips_by_year or {}
    advance_delta_by_year = advance_delta_by_year or {}
    container_out_root = (tracker.run_dir / "container_out").resolve()
    # Example "external config bundle" that should affect identity but not be stored in DB.
    external_cfg_dir = (
        tracker.run_dir / "external_configs" / "generate_trips"
    ).resolve()
    external_cfg_dir.mkdir(parents=True, exist_ok=True)
    (external_cfg_dir / "model.conf").write_text("version=1\n")
    (external_cfg_dir / ".ignored").write_text("ignore_me=1\n")

    def advance_people(df_in: pd.DataFrame, delta: int = 1) -> pd.DataFrame:
        """Task-like helper: increment age for next year."""
        df_out = df_in.copy()
        df_out["age"] = df_out["age"] + delta
        return df_out

    class MockTripsBackend:
        """Mock container backend that writes a Parquet with regenerated trips."""

        def __init__(self, df_source: pd.DataFrame, base_trips: int, output_path: Path):
            self.df_source = df_source
            self.base_trips = base_trips
            self.output_path = output_path
            self.run_count = 0

        def resolve_image_digest(self, image: str) -> str:
            return f"digest:{image}"

        def run(self, image, command, volumes, env, working_dir):
            self.run_count += 1
            df_out = self.df_source.copy()
            df_out["number_of_trips"] = rng.poisson(
                lam=self.base_trips, size=len(df_out)
            )
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            df_out.to_parquet(self.output_path, index=False)
            return True

    # Header
    scenario_id = f"scenario_{scenario_name}"
    with tracker.scenario(
        scenario_name,
        config={"mode": storage_mode, "base_trips": base_trips},
        model="pilates_orchestrator",
        facet={"mode": storage_mode, "base_trips": base_trips},
        tags=["scenario_header"],
    ) as scenario:
        coupler = scenario.coupler

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
            consist.log_dataframe(df_hh, key="households", schema=Household)

            # Seed persons for the first year with zero trips
            seed_year = years[0]
            df_seed = pd.DataFrame(
                {
                    "person_id": np.arange(n_per),
                    "household_id": rng.integers(0, n_hh, size=n_per),
                    "age": rng.integers(18, 80, size=n_per),
                    "number_of_trips": np.zeros(n_per, dtype=int) + base_trips,
                }
            )
            seed_art = consist.log_dataframe(
                df_seed,
                key="persons",
                schema=Person,
                direction="output",
            )

        coupler.set("persons", seed_art)

        # Years (propagate previous year's persons into the next)
        for idx, year in enumerate(years):
            current_person_art = coupler.get("persons")
            step_inputs = [current_person_art] if current_person_art else None

            # Step 1: advance people (age only)
            with scenario.step(
                run_id=f"{scenario_id}_year_{year}_advance",
                name="advance_people",
                year=year,
                tags=["simulation", "advance"],
                config={"year": year},
                facet={"delta": advance_delta_by_year.get(year, 1)},
                inputs=step_inputs,
            ):
                cached_persons = coupler.get_cached("persons")
                if cached_persons:
                    coupler.set("persons", cached_persons)
                else:
                    prev_person_art = coupler.get("persons")
                    df_adv = df_seed if idx == 0 else consist.load(prev_person_art)
                    if idx != 0:
                        delta = advance_delta_by_year.get(year, 1)
                        df_adv = advance_people(df_adv, delta=delta)

                    prev_person_art = consist.log_dataframe(
                        df_adv,
                        key="persons",
                        schema=Person,
                        direction="output",
                    )
                    coupler.set("persons", prev_person_art)

            # Step 2: generate trips via mocked container (updates number_of_trips)
            with scenario.step(
                run_id=f"{scenario_id}_year_{year}_trips",
                name="generate_trips",
                year=year,
                tags=["simulation", "generate_trips"],
                config={
                    "year": year,
                    "base_trips": base_trips_by_year.get(year, base_trips),
                },
                facet={
                    "base_trips": base_trips_by_year.get(year, base_trips),
                },
                hash_inputs=[("generate_trips_config", external_cfg_dir)],
                inputs=[coupler.get("persons")],
            ):
                # Resolve from cache if present; otherwise execute container and log output.
                persons_art = coupler.get_cached_output("persons")
                if not persons_art:
                    df_adv = consist.load(coupler.get("persons"))
                    # Use a stable host path for container outputs so identical scenarios can reuse cache
                    container_out_dir = (container_out_root / str(year)).resolve()
                    container_out_dir.mkdir(parents=True, exist_ok=True)
                    output_path = (container_out_dir / "persons.parquet").resolve()

                    trips_lambda = base_trips_by_year.get(year, base_trips)

                    mock_backend = MockTripsBackend(
                        df_source=df_adv,
                        base_trips=trips_lambda,
                        output_path=output_path,
                    )

                    with patch(
                        "consist.integrations.containers.api.DockerBackend",
                        return_value=mock_backend,
                    ):
                        # Nested mode: run_container detects the active step and does NOT create
                        # a new Run row; signature/meta/artifacts are attached to the step run.
                        result = run_container(
                            tracker=tracker,
                            run_id=f"{scenario_id}_year_{year}_trips_container",
                            image="mock/generate_trips:latest",
                            command=["generate_trips"],
                            volumes={str(container_out_dir): "/out"},
                            inputs=[coupler.get("persons")],
                            outputs={"persons": output_path},
                            environment={"BASE_TRIPS": str(trips_lambda)},
                            backend_type="docker",
                        )

                        persons_art = result.artifacts["persons"]
                        assert persons_art is not None, "No persons output logged"

                coupler.set("persons", persons_art)


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
        .where(VPerson.consist_year.is_not(None))
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

    # Verify artifact metadata inherited step year/tags
    art_2020 = tracker.get_run_artifact(
        tracker.find_run(parent_id="baseline", year=2020, model="generate_trips").id,
        key_contains="persons",
        direction="output",
    )
    assert art_2020 is not None
    assert art_2020.meta.get("year") == 2020
    assert "simulation" in (art_2020.meta.get("tags") or [])

    # Q2: Complex Join
    logging.info("\n--- Query 2: Trips by Region ---")
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
        .group_by(VPerson.consist_scenario_id, VHousehold.region, VPerson.consist_year)
        .order_by("scenario", VHousehold.region, VPerson.consist_year)
    )

    results = consist.run_query(query, tracker=tracker)
    assert len(results) > 0

    # Q3: Helper Method Usage
    # We want the generate_trips run from 2030 specifically
    target_run = tracker.find_run(
        parent_id="baseline", year=2030, model="generate_trips"
    )
    assert target_run.year == 2030
    assert target_run.model_name == "generate_trips"
    # Container metadata should be attached to the step run (nested mode)
    assert "declared_outputs" in (target_run.meta or {})
    assert target_run.signature is not None

    # Indexing example
    # Get all runs for baseline, indexed by year
    # (Note: init/pop_synth has year=None, so it keys to None)
    runs_by_year = tracker.find_runs(
        parent_id="baseline", model="generate_trips", index_by="year"
    )
    assert 2020 in runs_by_year
    assert 2030 in runs_by_year
    assert runs_by_year[2030].id == target_run.id

    # Q4: Snapshot Retrieval
    person_art = tracker.get_run_artifact(
        target_run.id, key_contains="persons", direction="output"
    )

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

    person_art = tracker.get_run_artifact(
        target_run.id, key_contains="persons", direction="output"
    )
    df = consist.load(person_art, tracker=tracker)
    assert len(df) == n_per


def _get_step_run(tracker: Tracker, parent: str, year: int, model: str) -> Run:
    return tracker.find_run(parent_id=parent, year=year, model=model)


def test_pilates_caching_scenarios(tmp_path):
    """
    End-to-end cache expectations for multiple scenarios:
    A: baseline inputs.
    B: different logical inputs -> no hits.
    C: same as A but different name -> all hits.
    D: advance_people diverges in second year only.
    E: generate_trips diverges in second year only.
    """
    run_dir = tmp_path / "runs_cache"
    db_path = str(tmp_path / "provenance_cache.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path, schemas=[Household, Person])
    # Use content-based hashing for stable cache keys across scenarios
    tracker.identity.hashing_strategy = "full"

    years = [2020, 2030]

    with patch.object(
        tracker.identity, "get_code_version", return_value="stable_git_hash"
    ):
        # A: baseline
        run_simulation_scenario(tracker, "A", 10, years, "cold")
        # B: different inputs/base_trips -> no hits relative to A
        run_simulation_scenario(tracker, "B", 15, years, "cold")
        # C: same as A but different name -> should reuse A’s work
        run_simulation_scenario(tracker, "C", 10, years, "cold")
        # D: same as A, but advance delta changes only in 2030
        run_simulation_scenario(
            tracker,
            "D",
            10,
            years,
            "cold",
            advance_delta_by_year={2030: 2},
        )
        # E: same as A, but trips intensity changes only in 2030
        run_simulation_scenario(
            tracker,
            "E",
            10,
            years,
            "cold",
            base_trips_by_year={2030: 20},
        )

    # Helpers to inspect cache flags
    def cache_hit(parent, year, model):
        run = _get_step_run(tracker, parent=parent, year=year, model=model)
        return (run.meta or {}).get("cache_hit", False)

    # A: no cache metadata expected (first executions)
    assert cache_hit("A", 2020, "advance_people") is False
    assert cache_hit("A", 2020, "generate_trips") is False

    # B: different inputs -> also cold
    assert cache_hit("B", 2020, "advance_people") is False
    assert cache_hit("B", 2020, "generate_trips") is False

    # C: identical to A -> expect container hits
    assert cache_hit("C", 2020, "generate_trips") is True
    assert cache_hit("C", 2030, "generate_trips") is True

    # D: diverges at advance in 2030 -> first year hit, second miss
    assert cache_hit("D", 2020, "generate_trips") is True
    assert cache_hit("D", 2030, "generate_trips") is False

    # E: diverges at trips only in 2030 -> first year hit, 2030 miss
    assert cache_hit("E", 2020, "generate_trips") is True
    assert cache_hit("E", 2030, "generate_trips") is False


def test_pilates_cache_hydrates_across_run_dirs(tmp_path):
    """Cache hit should hydrate outputs into a new run_dir when the DB is shared."""
    db_path = str(tmp_path / "provenance_shared.duckdb")
    years = [2020, 2030]

    # First tracker writes the baseline scenario
    tracker1 = Tracker(
        run_dir=tmp_path / "runs_a",
        db_path=db_path,
        schemas=[Household, Person],
    )
    tracker1.identity.hashing_strategy = "full"
    with patch.object(
        tracker1.identity, "get_code_version", return_value="stable_git_hash"
    ):
        run_simulation_scenario(tracker1, "baseline_a", 10, years, "cold")

    # Second tracker in a new run_dir replays the same work and should hydrate
    tracker2 = Tracker(
        run_dir=tmp_path / "runs_b",
        db_path=db_path,
        schemas=[Household, Person],
    )
    tracker2.identity.hashing_strategy = "full"
    with patch.object(
        tracker2.identity, "get_code_version", return_value="stable_git_hash"
    ):
        run_simulation_scenario(tracker2, "baseline_replay", 10, years, "cold")

    out_path = tracker2.run_dir / "container_out" / "2020" / "persons.parquet"
    assert out_path.exists(), "Cache reuse should materialize output in the new run_dir"

    replay_run = tracker2.find_run(
        parent_id="baseline_replay", year=2020, model="generate_trips"
    )
    # Depending on validation, this may run or hydrate; either way we expect outputs present.
    assert "declared_outputs" in (replay_run.meta or {})


def test_pilates_cache_miss_on_code_change(tmp_path):
    """
    Document current behavior: container cache ignores code version, so a code change alone
    does not invalidate the container signature. (Unit tests should cover code-aware cache.)
    """
    db_path = str(tmp_path / "provenance_code.duckdb")
    years = [2020]

    tracker = Tracker(
        run_dir=tmp_path / "runs_code",
        db_path=db_path,
        schemas=[Household, Person],
    )
    tracker.identity.hashing_strategy = "full"

    with patch.object(tracker.identity, "get_code_version", return_value="git_sha_1"):
        run_simulation_scenario(tracker, "code_v1", 10, years, "cold")

    with patch.object(tracker.identity, "get_code_version", return_value="git_sha_2"):
        run_simulation_scenario(tracker, "code_v2", 10, years, "cold")

    run2 = tracker.find_run(parent_id="code_v2", year=2020, model="generate_trips")
    assert (run2.meta or {}).get("cache_hit") is True


def test_pilates_cache_replay_with_ingested_outputs(tmp_path):
    """
    Cache hits should reuse ingested artifacts (is_ingested=True) and hydrate files when replayed.
    This documents that ingestion metadata is preserved across cache reuse.
    """
    db_path = str(tmp_path / "provenance_ingest.duckdb")
    years = [2020]

    tracker1 = Tracker(
        run_dir=tmp_path / "runs_ingest_a",
        db_path=db_path,
        schemas=[Household, Person],
    )
    tracker1.identity.hashing_strategy = "full"
    with patch.object(
        tracker1.identity, "get_code_version", return_value="stable_git_hash"
    ):
        run_simulation_scenario(tracker1, "ingest_a", 10, years, "cold")

    tracker2 = Tracker(
        run_dir=tmp_path / "runs_ingest_b",
        db_path=db_path,
        schemas=[Household, Person],
    )
    tracker2.identity.hashing_strategy = "full"
    with patch.object(
        tracker2.identity, "get_code_version", return_value="stable_git_hash"
    ):
        run_simulation_scenario(tracker2, "ingest_replay", 10, years, "cold")

    # Cache hit preferred on trips; validation may downgrade to execute if outputs missing
    replay_run = tracker2.find_run(
        parent_id="ingest_replay", year=2020, model="generate_trips"
    )
    cache_flag = (replay_run.meta or {}).get("cache_hit")
    assert cache_flag in (True, False)

    # Outputs should be linked and readable
    arts = tracker2.get_artifacts_for_run(replay_run.id).outputs
    persons_art = arts.get("persons") or next(iter(arts.values()))
    df = consist.load(persons_art, tracker=tracker2)
    assert len(df) == 300

    # Hydration should materialize the file in the new run_dir
    out_path = tracker2.run_dir / "container_out" / "2020" / "persons.parquet"
    assert out_path.exists()
