import logging


def test_run_artifact_link_direction_conflict_warns_and_preserves_first_link(
    tracker, tmp_path, caplog
):
    path = tmp_path / "same_artifact.csv"
    path.write_text("x\n1\n")

    with caplog.at_level(logging.WARNING):
        with tracker.start_run(run_id="conflict_run", model="unit_test"):
            artifact = tracker.log_artifact(path, key="same", direction="input")
            tracker.log_artifact(artifact, direction="output")

    warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(
        "Ignoring duplicate RunArtifactLink" in msg
        or "already linked to run" in msg
        or "conflict" in msg.lower()
        for msg in warnings
    ), f"Expected conflict warning; got: {warnings}"

    links = tracker.db.fetchall(
        "SELECT direction FROM run_artifact_link WHERE run_id = ?",
        ["conflict_run"],
    )

    assert len(links) == 1
    assert links[0][0] == "input"


def test_run_artifact_link_distinct_input_and_output_no_warning(
    tracker, tmp_path, caplog
):
    in_path = tmp_path / "input.csv"
    out_path = tmp_path / "output.csv"
    in_path.write_text("x\n1\n")
    out_path.write_text("x\n2\n")

    with caplog.at_level(logging.WARNING):
        with tracker.start_run(run_id="happy_run", model="unit_test"):
            tracker.log_artifact(in_path, key="in", direction="input")
            tracker.log_artifact(out_path, key="out", direction="output")

    warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any(
        "RunArtifactLink" in msg or "already linked to run" in msg for msg in warnings
    ), f"Did not expect conflict warning; got: {warnings}"

    links = tracker.db.fetchall(
        "SELECT direction FROM run_artifact_link WHERE run_id = ?",
        ["happy_run"],
    )

    assert len(links) == 2
    assert sorted([row[0] for row in links]) == ["input", "output"]


def test_log_artifact_syncs_artifact_and_link_in_one_db_step(tracker, tmp_path):
    path = tmp_path / "single.csv"
    path.write_text("x\n1\n")

    with tracker.start_run(run_id="single_sync_run", model="unit_test"):
        artifact = tracker.log_artifact(path, key="single", direction="output")

    persisted_artifact = tracker.db.get_artifact(artifact.id)
    links = tracker.db.fetchall(
        "SELECT direction FROM run_artifact_link WHERE run_id = ? AND artifact_id = ?",
        ["single_sync_run", artifact.id],
    )

    assert persisted_artifact is not None
    assert len(links) == 1
    assert links[0][0] == "output"
