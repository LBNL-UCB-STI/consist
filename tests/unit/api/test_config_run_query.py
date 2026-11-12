from __future__ import annotations

from pydantic import BaseModel
import pytest

from consist import (
    ParameterizedSQL,
    config_run_query,
    config_run_rows,
    query_df,
    query_rows,
)
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimConfigIngestRunLink,
)


def _seed_coefficients(tracker) -> None:
    db = tracker.db
    assert db is not None
    with db.transaction():
        db.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        db.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_coefficients_cache ("
            "content_hash VARCHAR, file_name VARCHAR, coefficient_name VARCHAR, "
            "segment VARCHAR, source_type VARCHAR, value_raw VARCHAR, value_num DOUBLE, "
            "dims_json VARCHAR, constrain VARCHAR, is_constrained BOOLEAN"
            ")"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_config_ingest_run_link ("
            "run_id VARCHAR, table_name VARCHAR, content_hash VARCHAR, file_name VARCHAR"
            ")"
        )
        db.execute(
            "INSERT INTO global_tables.activitysim_coefficients_cache VALUES "
            "('hash-1', 'coefficients.csv', 'time', '', 'coefficient', '1.0', 1.0, NULL, NULL, NULL), "
            "('hash-1', 'coefficients.csv', 'unconstrained_item', '', 'coefficient', '', NULL, NULL, NULL, NULL)"
        )
        db.execute(
            "INSERT INTO global_tables.activitysim_config_ingest_run_link VALUES "
            "('run_1', 'activitysim_coefficients_cache', 'hash-1', 'coefficients.csv'), "
            "('run_2', 'activitysim_coefficients_cache', 'hash-1', 'other.csv'), "
            "('run_3', 'activitysim_constants_cache', 'hash-1', 'coefficients.csv')"
        )


def test_parameterized_sql_coerces_sequence_params_to_tuple() -> None:
    stmt = ParameterizedSQL("SELECT ? AS x, ? AS y", [1, "two"])  # type: ignore[arg-type]
    assert stmt.params == (1, "two")
    assert isinstance(stmt.params, tuple)


def test_config_run_rows_default_filters_and_where_shapes(tracker) -> None:
    _seed_coefficients(tracker)

    rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimCoefficientsCache.value_num,
        ],
        where={"coefficient_name": "time"},
        tracker=tracker,
    )
    assert rows == [("run_1", 1.0)]

    in_rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[ActivitySimConfigIngestRunLink.run_id],
        where={"coefficient_name": ["time", "missing"]},
        tracker=tracker,
    )
    assert in_rows == ["run_1"]

    null_rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[
            ActivitySimConfigIngestRunLink.run_id,
            ActivitySimCoefficientsCache.coefficient_name,
        ],
        where={"value_num": None},
        tracker=tracker,
    )
    assert null_rows == [("run_1", "unconstrained_item")]

    empty_seq_rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[ActivitySimConfigIngestRunLink.run_id],
        where={"coefficient_name": []},
        tracker=tracker,
    )
    assert empty_seq_rows == []

    with pytest.raises(TypeError, match="SQLAlchemy"):
        config_run_query(
            ActivitySimCoefficientsCache,
            link_table=ActivitySimConfigIngestRunLink,
            where=ActivitySimCoefficientsCache.coefficient_name == "time",
        )


def test_config_run_rows_join_on_override(tracker) -> None:
    _seed_coefficients(tracker)

    rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        columns=[ActivitySimConfigIngestRunLink.run_id],
        where=ParameterizedSQL('t."coefficient_name" = ?', ("time",)),
        join_on=["content_hash"],
        tracker=tracker,
    )

    run_ids = {row if isinstance(row, str) else row[0] for row in rows}
    assert run_ids == {"run_1", "run_2"}


def test_config_run_query_default_columns_and_query_helpers(tracker) -> None:
    _seed_coefficients(tracker)

    query = config_run_query(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        where=("t.coefficient_name = ?", ["time"]),
    )
    assert isinstance(query, ParameterizedSQL)

    rows = config_run_rows(
        ActivitySimCoefficientsCache,
        link_table=ActivitySimConfigIngestRunLink,
        where=("t.coefficient_name = ?", ["time"]),
        tracker=tracker,
    )
    assert len(rows) == 1
    run_id, coefficient = rows[0]
    assert run_id == "run_1"
    assert isinstance(coefficient, ActivitySimCoefficientsCache)
    assert coefficient.coefficient_name == "time"

    df = query_df(query, tracker=tracker)
    assert list(df["run_id"]) == ["run_1"]
    assert list(df["coefficient_name"]) == ["time"]

    class CoeffRow(BaseModel):
        run_id: str
        coefficient_name: str
        value_num: float | None

    typed_rows = query_rows(query, model=CoeffRow, tracker=tracker)
    assert typed_rows == [
        CoeffRow(run_id="run_1", coefficient_name="time", value_num=1.0)
    ]
