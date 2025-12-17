from pathlib import Path


from consist.core.materialize import materialize_artifacts
from consist.core.tracker import Tracker


def test_materialize_artifacts_copies_file(
    tracker: Tracker, run_dir: Path, tmp_path: Path
):
    src = run_dir / "src.txt"
    src.write_text("hello")

    with tracker.start_run("produce_1", model="producer", cache_mode="overwrite"):
        artifact = tracker.log_artifact(
            src, key="src", direction="output", driver="txt"
        )

    dest = tmp_path / "dest.txt"
    result = materialize_artifacts(
        tracker=tracker, items=[(artifact, dest)], on_missing="raise"
    )
    assert dest.exists()
    assert dest.read_text() == "hello"
    assert result["src"] == str(dest.resolve())


def test_materialize_artifacts_warns_on_missing_source(
    tracker: Tracker, tmp_path: Path
):
    missing = tmp_path / "missing.txt"
    artifact = tracker.artifacts.create_artifact(
        path=str(missing), run_id="r0", key="missing", direction="output", driver="txt"
    )
    dest = tmp_path / "dest.txt"
    result = materialize_artifacts(
        tracker=tracker, items=[(artifact, dest)], on_missing="warn"
    )
    assert result == {}


def test_tracker_cache_hit_can_materialize_all_outputs(
    tracker: Tracker, run_dir: Path, tmp_path: Path
):
    """
    Phase 1 behavior: on cache hits, core Consist can optionally *copy* cached outputs
    into a caller-specified directory (bytes-on-disk materialization).
    """
    src = run_dir / "cached.txt"
    src.write_text("payload")

    with tracker.start_run("r1", model="m", cache_mode="overwrite"):
        tracker.log_artifact(src, key="cached", direction="output", driver="txt")

    out_dir = tmp_path / "materialized"
    with tracker.start_run(
        "r2",
        model="m",
        materialize_cached_outputs="all",
        materialize_cached_outputs_dir=out_dir,
    ):
        pass

    copied = out_dir / "cached.txt"
    assert copied.exists()
    assert copied.read_text() == "payload"


def test_tracker_cache_hit_can_materialize_requested_outputs(
    tracker: Tracker, run_dir: Path, tmp_path: Path
):
    src_a = run_dir / "a.txt"
    src_b = run_dir / "b.txt"
    src_a.write_text("a")
    src_b.write_text("b")

    with tracker.start_run("r1_req", model="m_req", cache_mode="overwrite"):
        tracker.log_artifact(src_a, key="a", direction="output", driver="txt")
        tracker.log_artifact(src_b, key="b", direction="output", driver="txt")

    dest_a = tmp_path / "only_a.txt"
    with tracker.start_run(
        "r2_req",
        model="m_req",
        materialize_cached_outputs="requested",
        materialize_cached_output_paths={"a": dest_a},
    ):
        pass

    assert dest_a.exists()
    assert dest_a.read_text() == "a"
    assert not (tmp_path / "b.txt").exists()
