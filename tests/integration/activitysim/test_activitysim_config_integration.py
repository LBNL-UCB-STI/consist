from __future__ import annotations

from pathlib import Path

from sqlmodel import Session, select

from consist.integrations.activitysim import ActivitySimConfigAdapter
from consist.models.activitysim import ActivitySimConstantsCache
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
            "SELECT DISTINCT l.run_id "
            "FROM global_tables.activitysim_constants_cache c "
            "JOIN global_tables.activitysim_config_ingest_run_link l "
            "ON l.content_hash = c.content_hash "
            "WHERE l.table_name = 'activitysim_constants_cache' "
            "AND c.key = 'sample_rate' AND c.value_num = 0.5"
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
            select(ActivitySimConstantsCache)
            .where(ActivitySimConstantsCache.key == "sample_rate")
            .where(ActivitySimConstantsCache.value_num == 0.5)
        ).all()

    assert rows


def test_activitysim_query_coefficients(tracker, tmp_path: Path):
    """Demonstrate querying accessibility coefficients across runs."""
    from consist.models.activitysim import ActivitySimCoefficientsCache

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

    rows = adapter.coefficients_rows(
        coefficient="time",
        file_name="accessibility_coefficients.csv",
        tracker=tracker,
    )

    assert len(rows) > 0, f"Expected rows, got: {rows}"
    # time coefficient should be 1.1 from the test data
    assert any(value == 1.1 for _, value in rows), (
        f"Expected value_num=1.1, got: {[value for _, value in rows]}"
    )
    assert {run_id for run_id, _ in rows} == {"activitysim_coeff_query"}

    from sqlalchemy import Float, cast

    with Session(tracker.engine) as session:
        numeric_rows = session.exec(
            select(ActivitySimCoefficientsCache)
            .where(ActivitySimCoefficientsCache.value_num.is_not(None))
            .where(cast(ActivitySimCoefficientsCache.value_num, Float) > 1.0)
            .where(ActivitySimCoefficientsCache.coefficient_name == "time")
        ).all()

    assert numeric_rows, "Expected numeric comparison on value_num to work."


def test_activitysim_config_plan_apply(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)

    facet_spec = {
        "yaml": {
            "accessibility.yaml": [
                {"key": "CONSTANTS.AUTO_TIME", "alias": "auto_time"}
            ],
        },
        "coefficients": {
            "accessibility_coefficients.csv": [
                {"key": "time", "alias": "access_time_coef"}
            ],
        },
    }
    plan = tracker.prepare_config(
        adapter,
        [overlay_dir, base_dir],
        strict=True,
        facet_spec=facet_spec,
        facet_schema_name="activitysim_config",
        facet_index=True,
    )

    tracker.begin_run("activitysim_plan_apply", "activitysim", cache_mode="overwrite")
    tracker.apply_config_plan(plan, ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstantsCache).where(
                ActivitySimConstantsCache.key == "sample_rate"
            )
        ).all()

    assert rows, "Expected constants to be ingested from config plan."

    matches = tracker.find_runs_by_facet_kv(
        namespace="activitysim",
        key="auto_time",
        value_num=1.5,
    )
    assert matches, "Expected facet lookup to find the run by auto_time."


def test_activitysim_config_plan_run_applies_ingest(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)

    plan = tracker.prepare_config(
        adapter,
        [overlay_dir, base_dir],
        strict=True,
    )

    tracker.run(
        fn=lambda: None,
        name="activitysim_plan_run",
        model="activitysim",
        config_plan=plan,
        cache_mode="overwrite",
    )

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstantsCache).where(
                ActivitySimConstantsCache.key == "sample_rate"
            )
        ).all()

    assert rows, "Expected config plan ingest during Tracker.run."


def test_activitysim_template_refs_ingested(tracker, tmp_path: Path):
    from consist.models.activitysim import ActivitySimCoefficientTemplateRefsCache

    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)

    tracker.begin_run(
        "activitysim_template_refs",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimCoefficientTemplateRefsCache).where(
                ActivitySimCoefficientTemplateRefsCache.file_name
                == "tour_mode_choice_coefficients_template.csv"
            )
        ).all()

    assert rows, "Expected template reference rows to be ingested."
