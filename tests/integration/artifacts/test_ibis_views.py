from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlmodel import Field, SQLModel

pytest.importorskip("ibis")

from consist.core.tracker import Tracker
from consist.integrations.ibis import ibis_connection, ibis_grouped_view, ibis_view


class Person(SQLModel, table=True):
    __tablename__ = "ibis_person"

    person_id: int = Field(primary_key=True)
    home_zone_id: int
    age: int
    income: float


class FinalTrip(SQLModel, table=True):
    __tablename__ = "final_trips"

    trip_id: int = Field(primary_key=True)
    household_id: int
    trip_mode: str


class FinalHousehold(SQLModel, table=True):
    __tablename__ = "final_households"

    household_id: int = Field(primary_key=True)
    auto_ownership: int


def _make_tracker(tmp_path: Path) -> Tracker:
    run_dir = tmp_path / "runs"
    return Tracker(run_dir=run_dir, db_path=str(tmp_path / "provenance.duckdb"))


def test_ibis_view_queries_cold_parquet(tmp_path: Path) -> None:
    tracker = _make_tracker(tmp_path)
    cold_path = tmp_path / "people.parquet"
    pd.DataFrame(
        [
            {"person_id": 1, "home_zone_id": 10, "age": 21, "income": 100_000.0},
            {"person_id": 2, "home_zone_id": 10, "age": 34, "income": 120_000.0},
            {"person_id": 3, "home_zone_id": 11, "age": 15, "income": 50_000.0},
        ]
    ).to_parquet(cold_path)

    with tracker.start_run("run-cold", "model", year=2024, cache_mode="overwrite"):
        tracker.log_artifact(str(cold_path), key="ibis_person", driver="parquet")

    people = ibis_view(tracker, model=Person)

    raw = people.to_pandas().sort_values("person_id").reset_index(drop=True)
    assert list(raw["person_id"]) == [1, 2, 3]
    assert "consist_year" in raw.columns
    assert raw["consist_year"].tolist() == [2024, 2024, 2024]

    by_year = people.filter(people.consist_year == 2024).to_pandas()
    assert len(by_year) == 3

    adults = people.filter(people.age >= 18)
    summary = (
        adults.group_by("home_zone_id")
        .agg(avg_income=adults.income.mean(), n=adults.count())
        .to_pandas()
        .sort_values("home_zone_id")
        .reset_index(drop=True)
    )
    assert summary.to_dict("records") == [
        {"home_zone_id": 10, "avg_income": 110_000.0, "n": 2},
    ]


def test_ibis_view_queries_hot_rows_and_keeps_tracker_usable(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)

    with tracker.start_run("run-hot", "model", year=2025, cache_mode="overwrite"):
        tracker.ingest(
            artifact=tracker.log_artifact("dummy", "ibis_person"),
            data=[
                {
                    "person_id": 1,
                    "home_zone_id": 99,
                    "age": 42,
                    "income": 87_500.0,
                }
            ],
            schema=Person,
        )

    tracker.view(Person)

    backend = ibis_connection(tracker)
    table = backend.table("v_ibis_person")
    df = table.to_pandas()
    assert df["consist_year"].tolist() == [2025]
    assert df["person_id"].tolist() == [1]

    backend.disconnect()

    with tracker.engine.connect() as conn:
        count = conn.exec_driver_sql("SELECT COUNT(*) FROM v_ibis_person").scalar_one()
    assert count == 1


def test_ibis_connection_reports_open_duckdb_conflict(tmp_path: Path) -> None:
    tracker = _make_tracker(tmp_path)

    with tracker.engine.connect():
        with pytest.raises(RuntimeError, match="Close the active SQLAlchemy session"):
            ibis_connection(tracker)


def test_ibis_grouped_view_queries_cold_artifacts_and_keeps_tracker_usable(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)
    df1 = pd.DataFrame({"id": [1], "value": [10.0]})
    df2 = pd.DataFrame({"id": [2], "value": [20.0]})
    p1 = tmp_path / "linkstats_a.parquet"
    p2 = tmp_path / "linkstats_b.parquet"
    df1.to_parquet(p1)
    df2.to_parquet(p2)

    with tracker.start_run(
        "grouped-a", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        a1 = tracker.log_artifact(
            str(p1),
            key="linkstats_unmodified_parquet__iter1",
            driver="parquet",
            profile_file_schema=True,
            facet={
                "artifact_family": "linkstats_unmodified_phys_sim_iter_parquet",
                "year": 2018,
                "iteration": 0,
                "phys_sim_iteration": 9,
                "beam_sub_iteration": 1,
            },
            facet_index=True,
        )

    with tracker.start_run(
        "grouped-b", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        tracker.log_artifact(
            str(p2),
            key="linkstats_unmodified_phys_sim_iter_parquet_9_2",
            driver="parquet",
            profile_file_schema=True,
            facet={
                "artifact_family": "linkstats_unmodified_phys_sim_iter_parquet",
                "year": 2018,
                "iteration": 0,
                "phys_sim_iteration": 9,
                "beam_sub_iteration": 2,
            },
            facet_index=True,
        )

    with ibis_grouped_view(
        tracker,
        view_name="v_linkstats_all",
        artifact_id=a1.id,
        namespace="beam",
        params=[
            "artifact_family=linkstats_unmodified_phys_sim_iter_parquet",
            "year=2018",
            "iteration=0",
        ],
        drivers=["parquet"],
        attach_facets=[
            "artifact_family",
            "year",
            "iteration",
            "phys_sim_iteration",
            "beam_sub_iteration",
        ],
        mode="hybrid",
    ) as table:
        df = table.to_pandas().sort_values("id").reset_index(drop=True)
        assert df["id"].tolist() == [1, 2]
        assert df["value"].tolist() == [10.0, 20.0]
        assert df["consist_year"].tolist() == [2018, 2018]
        assert df["facet_beam_sub_iteration"].tolist() == [1, 2]

    with tracker.engine.connect() as conn:
        count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM v_linkstats_all"
        ).scalar_one()
    assert count == 2


def test_ibis_grouped_view_queries_profiled_dataframe_outputs_from_tracker_run(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)

    def step(setting_id: int) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [setting_id],
                "value": [float(setting_id)],
            }
        )

    result_a = tracker.run(
        fn=step,
        run_id="sweep-a",
        model="simulate",
        config={"setting_id": 1},
        facet={"setting_id": 1},
        outputs=["sweep"],
    )
    result_b = tracker.run(
        fn=step,
        run_id="sweep-b",
        model="simulate",
        config={"setting_id": 2},
        facet={"setting_id": 2},
        outputs=["sweep"],
    )

    artifact_a = result_a.outputs["sweep"]
    assert artifact_a.meta.get("schema_id") is not None
    assert result_b.outputs["sweep"].meta.get("schema_id") is not None

    with ibis_grouped_view(
        tracker,
        view_name="v_sweep_runs_profiled",
        artifact_id=artifact_a.id,
        model="simulate",
        attach_facets=["setting_id"],
        mode="hybrid",
    ) as table:
        df = table.to_pandas().sort_values("id").reset_index(drop=True)

    assert df["id"].tolist() == [1, 2]
    assert df["facet_setting_id"].tolist() == [1, 2]
    assert df["consist_year"].isna().tolist() == [True, True]


def test_ibis_grouped_view_queries_hot_rows_and_keeps_tracker_usable(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)

    with tracker.start_run("run-hot", "model", year=2025, cache_mode="overwrite"):
        artifact = tracker.log_artifact(
            "dummy",
            "ibis_person",
            facet={"home_zone_id": 99},
            facet_index=True,
        )
        tracker.ingest(
            artifact=artifact,
            data=[
                {
                    "person_id": 1,
                    "home_zone_id": 99,
                    "age": 42,
                    "income": 87_500.0,
                }
            ],
            schema=Person,
        )

    with ibis_grouped_view(
        tracker,
        view_name="v_people",
        artifact_id=artifact.id,
        attach_facets=["home_zone_id"],
        mode="hybrid",
    ) as table:
        df = table.to_pandas()
        assert df["consist_year"].tolist() == [2025]
        assert df["person_id"].tolist() == [1]
        assert df["facet_home_zone_id"].tolist() == [99]

    with tracker.engine.connect() as conn:
        count = conn.exec_driver_sql("SELECT COUNT(*) FROM v_people").scalar_one()
    assert count == 1


def test_ibis_grouped_view_attaches_run_facets_when_artifact_facets_are_absent(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)
    p1 = tmp_path / "sweep_a.parquet"
    p2 = tmp_path / "sweep_b.parquet"
    pd.DataFrame({"id": [1], "value": [10.0]}).to_parquet(p1)
    pd.DataFrame({"id": [2], "value": [20.0]}).to_parquet(p2)

    with tracker.start_run(
        "sweep-a",
        "simulate",
        year=2024,
        config={"setting_id": 1, "label": "a"},
        facet_from=["setting_id", "label"],
        cache_mode="overwrite",
    ):
        artifact_a = tracker.log_artifact(
            str(p1),
            key="ibis_sweep",
            driver="parquet",
            profile_file_schema=True,
        )

    with tracker.start_run(
        "sweep-b",
        "simulate",
        year=2024,
        config={"setting_id": 2, "label": "b"},
        facet_from=["setting_id", "label"],
        cache_mode="overwrite",
    ):
        tracker.log_artifact(
            str(p2),
            key="ibis_sweep",
            driver="parquet",
            profile_file_schema=True,
        )

    with ibis_grouped_view(
        tracker,
        view_name="v_sweep_runs",
        artifact_id=artifact_a.id,
        model="simulate",
        attach_facets=["setting_id", "label"],
        mode="hybrid",
    ) as table:
        df = table.to_pandas().sort_values("id").reset_index(drop=True)

    assert df["id"].tolist() == [1, 2]
    assert df["value"].tolist() == [10.0, 20.0]
    assert df["facet_setting_id"].tolist() == [1, 2]
    assert df["facet_label"].tolist() == ["a", "b"]
    assert df["consist_year"].tolist() == [2024, 2024]


def test_ibis_view_handles_missing_cold_file(tmp_path: Path) -> None:
    tracker = _make_tracker(tmp_path)
    cold_path = tmp_path / "missing_people.parquet"
    pd.DataFrame(
        [
            {"person_id": 1, "home_zone_id": 10, "age": 21, "income": 100_000.0},
        ]
    ).to_parquet(cold_path)

    with tracker.start_run("run-missing", "model", year=2026, cache_mode="overwrite"):
        tracker.log_artifact(str(cold_path), key="ibis_person", driver="parquet")

    cold_path.unlink()

    table = ibis_view(tracker, model=Person)
    df = table.to_pandas()
    assert df.empty
    assert "consist_year" in df.columns
    assert "person_id" in df.columns


def test_multiple_ibis_views_can_be_opened_and_joined_on_same_tracker(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)
    trips_path = tmp_path / "final_trips.parquet"
    households_path = tmp_path / "final_households.parquet"
    pd.DataFrame(
        [
            {"trip_id": 1, "household_id": 100, "trip_mode": "DRIVE"},
            {"trip_id": 2, "household_id": 100, "trip_mode": "WALK"},
            {"trip_id": 3, "household_id": 200, "trip_mode": "TRANSIT"},
        ]
    ).to_parquet(trips_path)
    pd.DataFrame(
        [
            {"household_id": 100, "auto_ownership": 2},
            {"household_id": 200, "auto_ownership": 0},
        ]
    ).to_parquet(households_path)

    with tracker.start_run(
        "run-multi", "activitysim", year=2025, cache_mode="overwrite"
    ):
        tracker.log_artifact(str(trips_path), key="final_trips", driver="parquet")
        tracker.log_artifact(
            str(households_path), key="final_households", driver="parquet"
        )

    trips = ibis_view(tracker, model=FinalTrip, key="final_trips")
    households = ibis_view(tracker, model=FinalHousehold, key="final_households")

    assert ibis_connection(tracker) is ibis_connection(tracker)

    # Tracker methods and new runs must remain usable while cached Ibis backend is open
    runs = tracker.find_runs()
    assert [r.id for r in runs] == ["run-multi"]
    assert tracker.get_artifact("final_trips", run_id="run-multi") is not None

    with tracker.start_run(
        "run-multi-followup", "activitysim", year=2025, cache_mode="overwrite"
    ):
        pass

    assert len(tracker.find_runs()) == 2
    assert len(trips.to_pandas()) == 3

    joined = (
        trips.join(households, ["consist_run_id", "household_id"])
        .select("trip_id", "household_id", "trip_mode", "auto_ownership")
        .order_by("trip_id")
        .to_pandas()
    )
    assert joined.to_dict("records") == [
        {
            "trip_id": 1,
            "household_id": 100,
            "trip_mode": "DRIVE",
            "auto_ownership": 2,
        },
        {
            "trip_id": 2,
            "household_id": 100,
            "trip_mode": "WALK",
            "auto_ownership": 2,
        },
        {
            "trip_id": 3,
            "household_id": 200,
            "trip_mode": "TRANSIT",
            "auto_ownership": 0,
        },
    ]


def test_ibis_view_remains_usable_after_ibis_grouped_view_exits(
    tmp_path: Path,
) -> None:
    tracker = _make_tracker(tmp_path)
    cold_path = tmp_path / "people_grouped_keepalive.parquet"
    pd.DataFrame(
        [
            {"person_id": 1, "home_zone_id": 10, "age": 21, "income": 100_000.0},
            {"person_id": 2, "home_zone_id": 10, "age": 34, "income": 120_000.0},
        ]
    ).to_parquet(cold_path)

    with tracker.start_run("run-keepalive", "model", year=2024, cache_mode="overwrite"):
        art = tracker.log_artifact(
            str(cold_path),
            key="ibis_person",
            driver="parquet",
            profile_file_schema=True,
            facet={"home_zone_id": 10},
            facet_index=True,
        )

    people = ibis_view(tracker, model=Person)
    assert len(people.to_pandas()) == 2

    with ibis_grouped_view(
        tracker,
        view_name="v_people_grouped_keepalive",
        artifact_id=art.id,
        attach_facets=["home_zone_id"],
        mode="hybrid",
    ) as grouped_table:
        assert len(grouped_table.to_pandas()) == 2

    # Shared cached backend must still be connected after exiting ibis_grouped_view
    df_after = people.to_pandas().sort_values("person_id").reset_index(drop=True)
    assert df_after["person_id"].tolist() == [1, 2]


def test_tracker_close_disconnects_and_evicts_cached_ibis_backend(
    tmp_path: Path,
) -> None:
    from consist.integrations.ibis import _CACHED_BACKENDS, _is_backend_connected

    tracker = _make_tracker(tmp_path)
    backend = ibis_connection(tracker)
    assert tracker in _CACHED_BACKENDS
    assert _is_backend_connected(backend) is True

    tracker.close()

    assert tracker not in _CACHED_BACKENDS
    assert _is_backend_connected(backend) is False
