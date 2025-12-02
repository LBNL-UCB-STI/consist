"""
Unit Tests for Consist's Path Virtualization and Resolution Logic

This module contains unit tests for Consist's path virtualization and resolution logic.
It specifically verifies the `_virtualize_path` and `resolve_uri` methods of the `Tracker`,
ensuring that file system paths are correctly converted to and from portable URIs
based on configured mounts and run directories. These functions are crucial for
enabling Consist's reproducibility features across different environments.
"""

# tests/unit/test_paths.py
from consist.core.tracker import Tracker


def test_virtualize_path_with_mounts(tmp_path):
    """
    Tests the `_virtualize_path` method of the `Tracker` to ensure it correctly
    converts absolute file system paths into portable, scheme-based URIs based on
    configured mounts.

    This unit test focuses on verifying the "Path Resolution & Mounts" architectural
    principle, where Consist stores abstract URIs instead of concrete file paths
    to enhance portability.

    What happens:
    1. A `Tracker` is initialized with a set of "mounts" mapping schemes like "inputs"
       and "outputs" to specific temporary directory paths.
    2. The `_virtualize_path` method is called with two types of paths:
       - A path that falls directly under one of the configured mounts (e.g., `/mnt/data/file.csv`).
       - A path that is relative to the `Tracker`'s `run_dir` (e.g., `tmp_path/consist.json`).

    What's checked:
    - For the path under a mount point, the method returns a correctly formatted,
      scheme-based URI (e.g., "inputs://2020/households.csv").
    - For the path relative to the `run_dir`, the method returns a correctly formatted,
      `./` relative URI (e.g., "./consist.json").
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
