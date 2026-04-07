import logging

from sqlmodel import Session, select

from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink


def test_run_artifact_link_direction_conflict_warns_and_preserves_first_link(
    tracker, tmp_path, caplog
):
    """
    A single Artifact cannot be linked to the same Run as both input and output.

    This test covers a common footgun: "pass-through" steps that attempt to re-log
    an input artifact as an output artifact within the same run. Consist should:
    - warn with a targeted message
    - preserve the original link direction (do not overwrite)
    """
    caplog.set_level(logging.WARNING)

    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2\n")

    with tracker.start_run(run_id="conflict_run", model="unit_test"):
        art = tracker.log_artifact(str(path), key="data.csv", direction="input")
        # Attempt to re-log the *same Artifact instance* as an output in the same run.
        tracker.log_artifact(art, direction="output")

    warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(
        "Ignoring attempt to link artifact_id" in msg
        and "already linked as 'input'" in msg
        for msg in warnings
    ), f"Expected conflict warning; got: {warnings}"

    with Session(tracker.engine) as session:
        links = session.exec(
            select(RunArtifactLink).where(RunArtifactLink.run_id == "conflict_run")
        ).all()

    assert len(links) == 1
    assert links[0].direction == "input"


def test_run_artifact_link_distinct_input_and_output_no_warning(
    tracker, tmp_path, caplog
):
    """
    Happy path: a run consumes an input artifact and produces a *different* output artifact.

    This demonstrates the correct pattern for "pass-through"-like steps: if you want
    an output edge in lineage, write a new file (or otherwise produce a distinct artifact)
    rather than trying to re-log the same artifact as both input and output.
    """
    caplog.set_level(logging.WARNING)

    input_path = tmp_path / "data.csv"
    input_path.write_text("a,b\n1,2\n")

    output_path = tmp_path / "data_processed.csv"
    output_path.write_text("a,b\n2,4\n")

    with tracker.start_run(run_id="happy_run", model="unit_test"):
        tracker.log_artifact(str(input_path), key="data.csv", direction="input")
        tracker.log_artifact(
            str(output_path), key="data_processed.csv", direction="output"
        )

    warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("Ignoring attempt to link artifact_id" in msg for msg in warnings), (
        f"Did not expect conflict warning; got: {warnings}"
    )

    with Session(tracker.engine) as session:
        links = session.exec(
            select(RunArtifactLink).where(RunArtifactLink.run_id == "happy_run")
        ).all()

    assert len(links) == 2
    assert sorted([link.direction for link in links]) == ["input", "output"]


def test_log_artifact_syncs_artifact_and_link_in_one_db_step(
    tracker, tmp_path, monkeypatch
):
    path = tmp_path / "single.csv"
    path.write_text("x,y\n1,2\n", encoding="utf-8")

    def _forbid_separate_link(*args, **kwargs):
        raise AssertionError("sync_artifact should not call link_artifact_to_run")

    monkeypatch.setattr(tracker.db, "link_artifact_to_run", _forbid_separate_link)

    with tracker.start_run(run_id="single_sync_run", model="unit_test"):
        artifact = tracker.log_artifact(path, key="single", direction="output")

    with Session(tracker.engine) as session:
        persisted_artifact = session.get(Artifact, artifact.id)
        links = session.exec(
            select(RunArtifactLink).where(
                RunArtifactLink.run_id == "single_sync_run",
                RunArtifactLink.artifact_id == artifact.id,
            )
        ).all()

    assert persisted_artifact is not None
    assert len(links) == 1
    assert links[0].direction == "output"
