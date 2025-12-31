import pytest
from pathlib import Path
from sqlmodel import Session, select

from consist.core.tracker import Tracker
from consist.models.run import RunArtifactLink


def test_scenario_records_child_artifacts_and_steps(tracker: Tracker, run_dir: Path):
    base_in = run_dir / "exogenous.csv"
    base_in.write_text("base")

    with tracker.scenario("scenario_merge", config={"global": 1}) as sc:
        sc.add_input(base_in, key="exog")

        with sc.trace("stage_one", config={"local": 2}) as t:
            step_in = run_dir / "stage_in.txt"
            step_in.write_text("in")
            t.log_artifact(step_in, key="stage_input", direction="input")

            step_out = run_dir / "stage_out.txt"
            step_out.write_text("out")
            t.log_artifact(step_out, key="stage_output", direction="output")

    parent_artifacts = tracker.get_artifacts_for_run("scenario_merge")
    assert {"exog", "stage_input"} <= set(parent_artifacts.inputs.keys())
    assert "stage_output" in parent_artifacts.outputs

    header = tracker.get_run("scenario_merge")
    assert header.status == "completed"
    assert tracker._last_consist.run.status == "completed"
    assert header.meta.get("steps")
    step_summary = header.meta["steps"][0]
    assert step_summary["id"].endswith("stage_one")
    assert "stage_output" in step_summary["outputs"].values()
    assert "stage_input" in step_summary["inputs"].values()


def test_scenario_links_child_artifacts_in_db(tracker: Tracker, run_dir: Path):
    if not tracker.db:
        pytest.skip("Database manager not configured")

    with tracker.scenario("scenario_db") as sc:
        with sc.trace("stage_db") as t:
            child_out = run_dir / "db_out.csv"
            child_out.write_text("data")
            t.log_artifact(child_out, key="db_out", direction="output")

            child_in = run_dir / "db_in.csv"
            child_in.write_text("data")
            t.log_artifact(child_in, key="db_in", direction="input")

    child_run_id = "scenario_db_stage_db"
    child_artifacts = tracker.get_artifacts_for_run(child_run_id)
    out_id = child_artifacts.outputs["db_out"].id
    in_id = child_artifacts.inputs["db_in"].id

    with Session(tracker.db.engine) as session:
        links = session.exec(
            select(RunArtifactLink).where(RunArtifactLink.run_id == "scenario_db")
        ).all()

    directions = {(link.artifact_id, link.direction) for link in links}
    assert (out_id, "output") in directions
    assert (in_id, "input") in directions
