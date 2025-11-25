# tests/unit/test_paths.py
from consist.core.tracker import Tracker

def test_virtualize_path_with_mounts(tmp_path):
    """
    Tests the path virtualization logic of the Tracker.

    This unit test focuses on the `_virtualize_path` internal method, ensuring it
    correctly converts absolute file system paths into portable, scheme-based URIs
    based on the configured mounts.

    What happens:
    1. A Tracker is initialized with a set of "mounts" mapping schemes like "inputs"
       and "outputs" to specific directory paths.
    2. The `_virtualize_path` method is called with a path that falls under one of
       the configured mounts.
    3. The method is called again with a path that is relative to the Tracker's `run_dir`.

    What's checked:
    - The path under the mount point is correctly converted to a scheme-based URI
      (e.g., "/mnt/data/file.csv" -> "inputs://file.csv").
    - The path relative to the `run_dir` is correctly converted to a relative URI
      (e.g., "/path/to/run_dir/file.log" -> "./file.log").
    """
    # Setup mounts
    mounts = {"inputs": "/mnt/data", "outputs": "/mnt/results"}
    tracker = Tracker(run_dir=tmp_path, mounts=mounts)

    # Test 1: Match Mount
    # Note: We don't need the file to actually exist for this logic test
    result = tracker._virtualize_path("/mnt/data/2020/households.csv")
    assert result == "inputs://2020/households.csv"

    # Test 2: Match Relative
    result = tracker._virtualize_path(str(tmp_path / "consist.json"))
    assert result == "./consist.json"