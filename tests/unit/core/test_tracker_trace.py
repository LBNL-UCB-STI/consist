import pytest


def test_tracker_trace_logs_output_paths_on_exception(tracker, tmp_path):
    output_path = tmp_path / "out.txt"

    with pytest.raises(RuntimeError, match="boom"):
        with tracker.trace(
            name="trace_failure",
            output_paths={"out": output_path},
        ):
            output_path.write_text("data")
            raise RuntimeError("boom")

    record = tracker.last_run
    assert record is not None
    assert any(artifact.key == "out" for artifact in record.outputs)
