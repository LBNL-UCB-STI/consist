from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlmodel import Field, SQLModel

pytest.importorskip("ibis")

from consist.core.tracker import Tracker
from consist.integrations.ibis import ibis_connection, ibis_view


class Person(SQLModel, table=True):
    __tablename__ = "ibis_person"

    person_id: int = Field(primary_key=True)
    home_zone_id: int
    age: int
    income: float


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
