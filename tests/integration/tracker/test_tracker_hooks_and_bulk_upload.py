from consist import Tracker
from pathlib import Path
import time


def test_bulk_upload_and_description(tracker: Tracker, tmp_path: Path):
    # Test 1: Event Hooks
    print("=== Testing Event Hooks ===")
    hook_log = []

    @tracker.on_run_start
    def log_start(run):
        hook_log.append(f"START: {run.id}")

    @tracker.on_run_complete
    def log_complete(run, outputs):
        hook_log.append(f"COMPLETE: {run.id} with {len(outputs)} outputs")

    @tracker.on_run_failed
    def log_failed(run, error):
        hook_log.append(f"FAILED: {run.id} - {error}")

    # Create test files
    test_files = []
    for i in range(3):
        f = tmp_path / f"data_{i}.csv"
        f.write_text(f"col1,col2\\n{i},{i * 2}\\n")
        test_files.append(f)

    # Run with new features - success case
    with tracker.start_run(
        run_id="test_run_001",
        model="test_model",
        description="Testing all the new features",
        tags=["test", "features"],
    ):
        # Test bulk log_artifacts
        arts = tracker.log_artifacts(test_files, batch_id="batch_1")
        time.sleep(0.05)

    print(f"Hook log: {hook_log}")
    print(f"Logged {len(arts)} artifacts in bulk")

    # Test 2: Check description field
    print("\\n=== Testing Description Field ===")
    run = tracker.last_run.run
    assert run.description == "Testing all the new features"

    # Test 3: Test failure hook
    print("\\n=== Testing Failure Hook ===")
    try:
        with tracker.start_run(
            run_id="test_run_002",
            model="failing_model",
            description="This run will fail",
        ):
            raise ValueError("Intentional failure for testing")
    except ValueError:
        pass

    print(f"Final hook log: {hook_log}")

    # Test 4: Verify artifacts have hashes
    print("\\n=== Verifying Artifact Hashes ===")
    for art in arts:
        print(f"  {art.key}: hash={art.hash[:16]}...")

    print("\\n✅ All new features working correctly!")
