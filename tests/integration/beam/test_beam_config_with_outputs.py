"""End-to-end test: canonicalize BEAM config across multiple scenarios and compare."""

from __future__ import annotations

from pathlib import Path

from consist.integrations.beam import BeamConfigAdapter
from tests.helpers.beam_fixtures import build_beam_test_configs


def _update_sample_fraction(path: Path, value: float) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    updated: list[str] = []
    found = False
    for line in lines:
        if line.strip().startswith(
            "beam.agentsim.agentSampleSizeAsFractionOfPopulation"
        ):
            updated.append(
                f"beam.agentsim.agentSampleSizeAsFractionOfPopulation = {value}"
            )
            found = True
        else:
            updated.append(line)
    if not found:
        updated.append(f"beam.agentsim.agentSampleSizeAsFractionOfPopulation = {value}")
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")


def test_beam_config_across_multiple_scenarios(tracker, tmp_path: Path):
    """
    End-to-end test: canonicalize BEAM config across multiple scenarios.

    This demonstrates:
    1. Two runs with different agent sample fractions
    2. Config ingestion for each run
    3. Querying runs by facet value
    """
    facet_spec = {
        "keys": [
            {
                "key": "beam.agentsim.agentSampleSizeAsFractionOfPopulation",
                "alias": "agent_sample_fraction",
            }
        ],
    }

    # Scenario A: baseline override
    case_a, overlay_a, _ = build_beam_test_configs(tmp_path / "scenario_a")
    _update_sample_fraction(overlay_a, 0.25)
    adapter_a = BeamConfigAdapter(primary_config=overlay_a)
    plan_a = tracker.prepare_config(
        adapter_a,
        [case_a],
        facet_spec=facet_spec,
        facet_schema_name="beam_config",
        facet_index=True,
    )
    tracker.begin_run("beam_scenario_a", "beam", cache_mode="overwrite")
    tracker.apply_config_plan(plan_a, ingest=True)
    tracker.end_run()

    # Scenario B: adjusted sample fraction
    case_b, overlay_b, _ = build_beam_test_configs(tmp_path / "scenario_b")
    _update_sample_fraction(overlay_b, 0.75)
    adapter_b = BeamConfigAdapter(primary_config=overlay_b)
    plan_b = tracker.prepare_config(
        adapter_b,
        [case_b],
        facet_spec=facet_spec,
        facet_schema_name="beam_config",
        facet_index=True,
    )
    tracker.begin_run("beam_scenario_b", "beam", cache_mode="overwrite")
    tracker.apply_config_plan(plan_b, ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with tracker.engine.begin() as connection:
        rows = connection.exec_driver_sql(
            """
            SELECT l.run_id, c.value_num
            FROM global_tables.beam_config_cache c
            JOIN global_tables.beam_config_ingest_run_link l
              ON l.content_hash = c.content_hash
             AND l.table_name = 'beam_config_cache'
            WHERE c.key = 'beam.agentsim.agentSampleSizeAsFractionOfPopulation'
            """
        ).fetchall()

    values_by_run = {row[0]: float(row[1]) for row in rows}
    assert values_by_run.get("beam_scenario_a") == 0.25
    assert values_by_run.get("beam_scenario_b") == 0.75

    matches_a = tracker.find_runs_by_facet_kv(
        namespace="beam",
        key="agent_sample_fraction",
        value_num=0.25,
    )
    matches_b = tracker.find_runs_by_facet_kv(
        namespace="beam",
        key="agent_sample_fraction",
        value_num=0.75,
    )

    ids_a = {run.id for run in matches_a}
    ids_b = {run.id for run in matches_b}
    assert "beam_scenario_a" in ids_a
    assert "beam_scenario_b" in ids_b
