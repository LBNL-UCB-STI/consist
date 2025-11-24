# tests/unit/test_paths.py
from consist.core.tracker import Tracker

def test_virtualize_path_with_mounts(tmp_path):
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