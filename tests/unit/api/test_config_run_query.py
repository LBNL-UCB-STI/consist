from __future__ import annotations

from sqlmodel import Session, SQLModel

from consist import config_run_query, config_run_rows, run_query
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimConfigIngestRunLink,
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
                ActivitySimConfigIngestRunLink.__table__,
            ],
        )


def _seed_coefficients(session: Session) -> None:
    session.add(
        ActivitySimCoefficientsCache(
            content_hash="hash-1",
            file_name="coefficients.csv",
            coefficient_name="time",
            segment="",
            source_type="coefficient",
            value_raw="1.0",
            value_num=1.0,
        )
    )
    session.add_all(
        [
            ActivitySimConfigIngestRunLink(
                run_id="run_1",
                table_name="activitysim_coefficients_cache",
                content_hash="hash-1",
                file_name="coefficients.csv",
            ),
            ActivitySimConfigIngestRunLink(
                run_id="run_2",
                table_name="activitysim_coefficients_cache",
                content_hash="hash-1",
                file_name="other.csv",
            ),
            ActivitySimConfigIngestRunLink(
                run_id="run_3",
                table_name="activitysim_constants_cache",
                content_hash="hash-1",
                file_name="coefficients.csv",
            ),
        ]
    )
    session.commit()


def test_config_run_rows_default_filters(tracker):
    _ensure_activitysim_tables(tracker.engine)
    with Session(tracker.engine) as session:
        _seed_coefficients(session)

    rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimCoefficientsCache.value_num,
        ],
        where=ActivitySimCoefficientsCache.coefficient_name == "time",
        tracker=tracker,
    )

    assert rows == [("run_1", 1.0)]


def test_config_run_rows_join_on_override(tracker):
    _ensure_activitysim_tables(tracker.engine)
    with Session(tracker.engine) as session:
        _seed_coefficients(session)

    rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[ActivitySimConfigIngestRunLink.run_id],
        where=ActivitySimCoefficientsCache.coefficient_name == "time",
        join_on=["content_hash"],
        tracker=tracker,
    )

    run_ids = {row if isinstance(row, str) else row[0] for row in rows}
    assert run_ids == {"run_1", "run_2"}


def test_config_run_query_default_columns(tracker):
    _ensure_activitysim_tables(tracker.engine)
    with Session(tracker.engine) as session:
        _seed_coefficients(session)

    query = config_run_query(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        where=ActivitySimCoefficientsCache.coefficient_name == "time",
    )
    rows = run_query(query, tracker=tracker)

    assert len(rows) == 1
    run_id, coefficient = rows[0]
    assert run_id == "run_1"
    assert coefficient.coefficient_name == "time"
