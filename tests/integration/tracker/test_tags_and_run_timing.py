from consist import Tracker
from pathlib import Path
import time


def test_tags_and_run_timing(tracker: Tracker, tmp_path: Path):
    # Create a test file
    test_file = tmp_path / "test_data.csv"
    test_file.write_text("a,b,c\n1,2,3\n")

    # Run with new features
    with tracker.start_run(
        run_id="test_run_001",
        model="test_model",
        tags=["production", "test", "v1"],
        year=2024,
    ):
        tracker.log_artifact(str(test_file), key="test_data")
        time.sleep(0.1)  # Small delay to ensure duration is measurable

    # Check the run has our new fields
    run = tracker.last_run.run
    print(f"Run ID: {run.id}")
    print(f"Tags: {run.tags}")
    print(f"Started at: {run.started_at}")
    print(f"Ended at: {run.ended_at}")
    print(f"Duration: {run.duration_seconds:.3f} seconds")

    # Check artifact has hash
    output_art = (
        tracker.last_run.outputs[0]
        if tracker.last_run.outputs
        else tracker.last_run.inputs[0]
    )
    print(f"Artifact key: {output_art.key}")
    print(f"Artifact hash: {output_art.hash}")

    # Check history includes new fields
    df = tracker.history(limit=5)
    print(f"\\nHistory columns: {list(df.columns)}")
    print(f"Tags from history: {df.iloc[0]['tags']}")
    print(f"Duration from history: {df.iloc[0]['duration_seconds']:.3f} seconds")

    # Test tag filtering
    df_filtered = tracker.history(tags=["production"])
    tracker.history(tags=["production"])
    print(f"\\nFiltered by production tag: {len(df_filtered)} runs")
    assert len(df_filtered) == 1

    df_filtered2 = tracker.history(tags=["nonexistent"])
    print(f"Filtered by nonexistent tag: {len(df_filtered2)} runs")
    assert len(df_filtered2) == 0

    print("\\n✅ All new features working correctly!")
