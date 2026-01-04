"""End-to-end test: canonicalize ActivitySim config across multiple scenarios and compare."""

from __future__ import annotations

from pathlib import Path

from sqlmodel import Session, select

from consist.integrations.activitysim import ActivitySimConfigAdapter
from consist.models.activitysim import ActivitySimCoefficients, ActivitySimConstants
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs


def test_activitysim_config_across_multiple_scenarios(tracker, tmp_path: Path):
    """
    End-to-end test: canonicalize ActivitySim config across multiple scenarios.

    This demonstrates the typical workflow for ActivitySim scenario analysis:
    1. Create two scenarios with different calibration parameters
    2. Canonicalize and ingest config for each
    3. Query and compare coefficients across scenarios
    """
    adapter = ActivitySimConfigAdapter()

    # Scenario A: baseline
    base_a, overlay_a = build_activitysim_test_configs(tmp_path / "scenario_a")
    run_a = tracker.begin_run("scenario_a_baseline", "activitysim", cache_mode="overwrite")
    tracker.canonicalize_config(adapter, [overlay_a, base_a], ingest=True)
    tracker.end_run()

    # Scenario B: different configuration (could have different coefficients in practice)
    base_b, overlay_b = build_activitysim_test_configs(tmp_path / "scenario_b")
    run_b = tracker.begin_run("scenario_b_adjusted", "activitysim", cache_mode="overwrite")
    tracker.canonicalize_config(adapter, [overlay_b, base_b], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    # Query 1: Compare coefficients across both scenarios
    with Session(tracker.engine) as session:
        # Get all accessibility coefficients from both runs
        coeff_rows = session.exec(
            select(ActivitySimCoefficients)
            .where(ActivitySimCoefficients.file_name == "accessibility_coefficients.csv")
        ).all()

    assert len(coeff_rows) >= 4, "Should have coefficients from both scenarios (2+ per scenario)"

    # Group by run to show comparison
    runs_with_coeffs = {}
    for row in coeff_rows:
        if row.run_id not in runs_with_coeffs:
            runs_with_coeffs[row.run_id] = {}
        runs_with_coeffs[row.run_id][row.coefficient_name] = float(row.value_num)

    # Both scenarios should have time and cost coefficients
    assert len(runs_with_coeffs) == 2, "Should have coefficients from 2 runs"
    for coeffs in runs_with_coeffs.values():
        assert "time" in coeffs, "Should have time coefficient"
        assert "cost" in coeffs, "Should have cost coefficient"
        assert coeffs["time"] == 1.1, "time coefficient should be 1.1"
        assert coeffs["cost"] == 2.2, "cost coefficient should be 2.2"

    # Query 2: Find runs with specific coefficient values
    with tracker.engine.begin() as connection:
        # In a real scenario, you might query like this:
        # "Find all runs where time coefficient > 1.0"
        high_time_coeff = connection.exec_driver_sql(
            """
            SELECT * FROM global_tables.activitysim_coefficients
            WHERE coefficient_name = 'time' AND CAST(value_num AS FLOAT) > 1.0
            """
        ).fetchall()

    assert len(high_time_coeff) >= 2, "Both scenarios should have time coefficient > 1.0"

    # Query 3: Count config entries across runs
    # This demonstrates how you'd aggregate config metadata for scenario comparison
    with tracker.engine.begin() as connection:
        # Count coefficient entries per run
        runs_coeff_count = connection.exec_driver_sql(
            """
            SELECT ac.run_id, COUNT(*) as coeff_count
            FROM global_tables.activitysim_coefficients ac
            WHERE ac.coefficient_name = 'time' AND CAST(ac.value_num AS FLOAT) = 1.1
            GROUP BY ac.run_id
            """
        ).fetchall()

    assert len(runs_coeff_count) == 2, "Should find both scenarios with time coefficient = 1.1"
    # Each scenario should have exactly 1 time coefficient entry
    counts = [row[1] for row in runs_coeff_count]
    assert all(c == 1 for c in counts), "Each scenario should have 1 time coefficient entry"
