from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel

from consist import run_query
from consist.integrations.activitysim import ActivitySimConfigAdapter
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimConfigIngestRunLink,
    ActivitySimConstantsCache,
)


def _ensure_activitysim_tables(engine) -> None:
    if engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        SQLModel.metadata.create_all(
            connection,
            tables=[
                ActivitySimCoefficientsCache.__table__,
                ActivitySimConstantsCache.__table__,
                ActivitySimConfigIngestRunLink.__table__,
            ],
        )


def _seed_activitysim_rows(session: Session) -> None:
    session.add(
        ActivitySimCoefficientsCache(
            content_hash="hash-coef",
            file_name="coefficients.csv",
            coefficient_name="time",
            segment="",
            source_type="coefficient",
            value_raw="1.2",
            value_num=1.2,
        )
    )
    session.add(
        ActivitySimCoefficientsCache(
            content_hash="hash-coef",
            file_name="coefficients.csv",
            coefficient_name="cost",
            segment="",
            source_type="coefficient",
            value_raw="-0.3",
            value_num=-0.3,
        )
    )
    session.add(
        ActivitySimCoefficientsCache(
            content_hash="hash-coef-2",
            file_name="coefficients.csv",
            coefficient_name="time",
            segment="",
            source_type="coefficient",
            value_raw="1.5",
            value_num=1.5,
        )
    )
    session.add(
        ActivitySimCoefficientsCache(
            content_hash="hash-coef-2",
            file_name="coefficients.csv",
            coefficient_name="cost",
            segment="",
            source_type="coefficient",
            value_raw="-0.2",
            value_num=-0.2,
        )
    )
    session.add(
        ActivitySimConstantsCache(
            content_hash="hash-const",
            file_name="settings.yaml",
            key="sample_rate",
            value_type="float",
            value_num=0.5,
        )
    )
    session.add_all(
        [
            ActivitySimConfigIngestRunLink(
                run_id="run_coeff",
                table_name="activitysim_coefficients_cache",
                content_hash="hash-coef",
                file_name="coefficients.csv",
            ),
            ActivitySimConfigIngestRunLink(
                run_id="run_coeff_b",
                table_name="activitysim_coefficients_cache",
                content_hash="hash-coef-2",
                file_name="coefficients.csv",
            ),
            ActivitySimConfigIngestRunLink(
                run_id="run_const",
                table_name="activitysim_constants_cache",
                content_hash="hash-const",
                file_name="settings.yaml",
            ),
        ]
    )
    session.commit()


def test_activitysim_adapter_coefficients_helpers(tracker):
    adapter = ActivitySimConfigAdapter()
    _ensure_activitysim_tables(tracker.engine)
    with Session(tracker.engine) as session:
        _seed_activitysim_rows(session)

    rows = adapter.coefficients_rows(coefficient="time", tracker=tracker)
    assert sorted(rows) == [
        ("run_coeff", pytest.approx(1.2)),
        ("run_coeff_b", pytest.approx(1.5)),
    ]

    by_run = adapter.coefficients_by_run(
        coefficient="time",
        collapse="first",
        tracker=tracker,
    )
    assert by_run == {
        "run_coeff": pytest.approx(1.2),
        "run_coeff_b": pytest.approx(1.5),
    }

    query = adapter.coefficients_query(coefficient="time")
    assert sorted(run_query(query, tracker=tracker)) == [
        ("run_coeff", pytest.approx(1.2)),
        ("run_coeff_b", pytest.approx(1.5)),
    ]

    combos = adapter.coefficients_combinations(
        coefficients=["time", "cost"],
        collapse="first",
        tracker=tracker,
    )
    assert len(combos) == 2
    for combo, runs in combos.items():
        if runs == ["run_coeff"]:
            assert combo[0] == pytest.approx(1.2)
            assert combo[1] == pytest.approx(-0.3)
        elif runs == ["run_coeff_b"]:
            assert combo[0] == pytest.approx(1.5)
            assert combo[1] == pytest.approx(-0.2)
        else:
            raise AssertionError(f"Unexpected combo mapping: {combo} -> {runs}")


def test_activitysim_adapter_constants_helpers(tracker):
    adapter = ActivitySimConfigAdapter()
    _ensure_activitysim_tables(tracker.engine)
    with Session(tracker.engine) as session:
        _seed_activitysim_rows(session)

    rows = adapter.constants_rows(key="sample_rate", tracker=tracker)
    assert rows == [("run_const", pytest.approx(0.5))]

    by_run = adapter.constants_by_run(
        key="sample_rate",
        collapse="first",
        tracker=tracker,
    )
    assert by_run == {"run_const": pytest.approx(0.5)}
