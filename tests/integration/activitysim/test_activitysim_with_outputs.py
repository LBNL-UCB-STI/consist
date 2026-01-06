"""End-to-end test: canonicalize ActivitySim config across multiple scenarios and compare."""

from __future__ import annotations

from pathlib import Path

from consist.integrations.activitysim import ActivitySimConfigAdapter
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
    tracker.begin_run("scenario_a_baseline", "activitysim", cache_mode="overwrite")
    tracker.canonicalize_config(adapter, [overlay_a, base_a], ingest=True)
    tracker.end_run()

    # Scenario B: tweak a coefficient to simulate a calibration adjustment
    base_b, overlay_b = build_activitysim_test_configs(tmp_path / "scenario_b")
    coeffs_b = base_b / "accessibility_coefficients.csv"
    coeffs_b.write_text(
        "coefficient,value\ntime,1.3\ncost,2.2\n",
        encoding="utf-8",
    )
    tracker.begin_run("scenario_b_adjusted", "activitysim", cache_mode="overwrite")
    tracker.canonicalize_config(adapter, [overlay_b, base_b], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    # Query 1: Compare coefficients across both scenarios
    with tracker.engine.begin() as connection:
        coeff_rows = connection.exec_driver_sql(
            """
            SELECT l.run_id, c.coefficient_name, c.value_num
            FROM global_tables.activitysim_coefficients_cache c
            JOIN global_tables.activitysim_config_ingest_run_link l
              ON l.content_hash = c.content_hash
             AND l.table_name = 'activitysim_coefficients_cache'
            WHERE c.file_name = 'accessibility_coefficients.csv'
            """
        ).fetchall()

    assert len(coeff_rows) >= 4, (
        "Should have coefficients from both scenarios (2+ per scenario)"
    )

    # Group by run to show comparison
    runs_with_coeffs = {}
    for row in coeff_rows:
        run_id, coefficient_name, value_num = row
        runs_with_coeffs.setdefault(run_id, {})[coefficient_name] = float(value_num)

    # Both scenarios should have time and cost coefficients
    assert len(runs_with_coeffs) == 2, "Should have coefficients from 2 runs"
    time_values = set()
    for coeffs in runs_with_coeffs.values():
        assert "time" in coeffs, "Should have time coefficient"
        assert "cost" in coeffs, "Should have cost coefficient"
        time_values.add(coeffs["time"])
        assert coeffs["cost"] == 2.2, "cost coefficient should be 2.2"
    assert time_values == {1.1, 1.3}, (
        f"Expected time coefficients 1.1 and 1.3, got {time_values}"
    )

    # Query 2: Find runs with specific coefficient values
    with tracker.engine.begin() as connection:
        # In a real scenario, you might query like this:
        # "Find all runs where time coefficient > 1.0"
        high_time_coeff = connection.exec_driver_sql(
            """
            SELECT l.run_id, c.coefficient_name, c.value_num
            FROM global_tables.activitysim_coefficients_cache c
            JOIN global_tables.activitysim_config_ingest_run_link l
              ON l.content_hash = c.content_hash
             AND l.table_name = 'activitysim_coefficients_cache'
            WHERE c.coefficient_name = 'time' AND CAST(c.value_num AS FLOAT) > 1.0
            """
        ).fetchall()

    assert len(high_time_coeff) >= 2, (
        "Both scenarios should have time coefficient > 1.0"
    )

    # Query 3: Count config entries across runs
    # This demonstrates how you'd aggregate config metadata for scenario comparison
    with tracker.engine.begin() as connection:
        # Count coefficient entries per run
        runs_coeff_count = connection.exec_driver_sql(
            """
            SELECT l.run_id, COUNT(*) as coeff_count
            FROM global_tables.activitysim_coefficients_cache c
            JOIN global_tables.activitysim_config_ingest_run_link l
              ON l.content_hash = c.content_hash
             AND l.table_name = 'activitysim_coefficients_cache'
            WHERE c.coefficient_name = 'time' AND CAST(c.value_num AS FLOAT) IN (1.1, 1.3)
            GROUP BY l.run_id
            """
        ).fetchall()

    assert len(runs_coeff_count) == 2, (
        "Should find both scenarios with time coefficient = 1.1"
    )
    # Each scenario should have exactly 1 time coefficient entry
    counts = [row[1] for row in runs_coeff_count]
    assert all(c == 1 for c in counts), (
        "Each scenario should have 1 time coefficient entry"
    )
