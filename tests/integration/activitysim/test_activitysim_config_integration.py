from __future__ import annotations

from pathlib import Path

from sqlmodel import Session, select

from consist.integrations.activitysim import ActivitySimConfigAdapter
from consist.models.activitysim import ActivitySimConstants
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs


def _log_mock_output(tracker, run_name: str, tmp_path: Path) -> None:
    output_path = tmp_path / f"{run_name}_summary.csv"
    output_path.write_text("a,b\n1,2\n", encoding="utf-8")
    tracker.log_artifact(
        output_path,
        key=f"outputs:{run_name}",
        direction="output",
    )


def test_activitysim_ingest_and_query_by_config_value(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()

    base_a, overlay_a = build_activitysim_test_configs(tmp_path, sample_rate=0.25)
    run_a = tracker.begin_run(
        "activitysim_run_a",
        "activitysim",
        config={"sample_rate": 0.25},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_a, base_a], ingest=True)
    _log_mock_output(tracker, "activitysim_run_a", tmp_path)
    tracker.end_run()

    base_b, overlay_b = build_activitysim_test_configs(tmp_path, sample_rate=0.5)
    run_b = tracker.begin_run(
        "activitysim_run_b",
        "activitysim",
        config={"sample_rate": 0.5},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_b, base_b], ingest=True)
    _log_mock_output(tracker, "activitysim_run_b", tmp_path)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with tracker.engine.begin() as connection:
        run_rows = connection.exec_driver_sql(
            "SELECT DISTINCT run_id FROM global_tables.activitysim_constants "
            "WHERE key = 'sample_rate' AND value_num = 0.5"
        ).fetchall()
        run_ids = [row[0] for row in run_rows]
        assert run_ids

        output_rows = connection.exec_driver_sql(
            "SELECT a.key FROM artifact a "
            "JOIN run_artifact_link r ON a.id = r.artifact_id "
            "WHERE r.direction = 'output' AND r.run_id IN (%s)"
            % ", ".join([f"'{run_id}'" for run_id in run_ids])
        ).fetchall()
        output_keys = [row[0] for row in output_rows]

    assert set(output_keys) == {"outputs:activitysim_run_b"}
    assert run_a.id != run_b.id


def test_activitysim_sqlmodel_query(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()

    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path, sample_rate=0.5)
    tracker.begin_run(
        "activitysim_sqlmodel_query",
        "activitysim",
        config={"sample_rate": 0.5},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstants)
            .where(ActivitySimConstants.key == "sample_rate")
            .where(ActivitySimConstants.value_num == 0.5)
        ).all()

    assert rows


def test_activitysim_query_coefficients(tracker, tmp_path: Path):
    """Demonstrate querying accessibility coefficients across runs."""
    from consist.models.activitysim import ActivitySimCoefficients

    adapter = ActivitySimConfigAdapter()

    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    tracker.begin_run(
        "activitysim_coeff_query",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        # Query: which runs have the time coefficient from accessibility_coefficients.csv?
        rows = session.exec(
            select(ActivitySimCoefficients)
            .where(ActivitySimCoefficients.coefficient_name == "time")
            .where(
                ActivitySimCoefficients.file_name == "accessibility_coefficients.csv"
            )
        ).all()

    assert len(rows) > 0, f"Expected rows, got: {rows}"
    # time coefficient should be 1.1 from the test data
    assert any(row.value_num == 1.1 for row in rows), (
        f"Expected value_num=1.1, got: {[row.value_num for row in rows]}"
    )

    from sqlalchemy import Float, cast

    with Session(tracker.engine) as session:
        numeric_rows = session.exec(
            select(ActivitySimCoefficients)
            .where(ActivitySimCoefficients.value_num.is_not(None))
            .where(cast(ActivitySimCoefficients.value_num, Float) > 1.0)
            .where(ActivitySimCoefficients.coefficient_name == "time")
        ).all()

    assert numeric_rows, (
        "Expected numeric comparison on value_num to work without casting."
    )
