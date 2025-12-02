# tests/unit/test_identity.py

"""
Unit Tests for Consist's IdentityManager

This module contains unit tests for the `IdentityManager` class within Consist's core,
focusing on the various hashing mechanisms that establish the unique identity of runs
and artifacts.

It verifies the correctness of:
-   **Configuration Hashing**: Ensuring canonicalization, proper handling of exclusions,
    and correct conversion of various Python and NumPy data types.
-   **Input Artifact Hashing**: Differentiating between provenance-based inputs
    (linked to previous runs) and raw file content, and applying the configured
    hashing strategy ("fast" vs. "full").
-   **Code Version Detection**: Accurately detecting the Git commit SHA and
    flagging "dirty" repository states to safeguard reproducibility.
"""
import pytest
import hashlib
import tempfile
import os
import time
from unittest.mock import MagicMock, patch
from pathlib import Path

# Import the class
from consist.core.identity import IdentityManager
from consist.models.artifact import Artifact

# Conditional numpy import for testing
try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class TestConfigHashing:
    """
    Tests the `compute_config_hash` method of `IdentityManager`.

    These tests ensure that configuration hashing is:
    -   **Deterministic**: Identical configurations produce identical hashes,
        regardless of dictionary key order.
    -   **Sensitive to relevant changes**: Changes in configuration values
        result in different hashes.
    -   **Robust to exclusions**: Specific keys can be excluded from hashing.
    -   **Type-agnostic**: Correctly handles various Python and NumPy types,
        converting them to a canonical, hashable representation.
    """

    def test_canonicalization(self):
        """
        Tests that `compute_config_hash` produces identical hashes for configurations
        that are logically the same but have different dictionary key orders.

        This verifies the "Canonical Config Hashing" principle, crucial for ensuring
        that minor variations in config definition do not lead to false cache misses.

        What happens:
        1. Two configuration dictionaries (`config_a` and `config_b`) are defined.
           They contain the same key-value pairs but with keys in a different order
           (including nested dictionaries).
        2. `compute_config_hash` is called for both configurations.

        What's checked:
        - The SHA256 hashes generated for `config_a` and `config_b` are identical.
        """
        im = IdentityManager()

        config_a = {"a": 1, "b": 2, "c": {"x": 10, "y": 20}}
        config_b = {"c": {"y": 20, "x": 10}, "b": 2, "a": 1}  # Mixed order

        hash_a = im.compute_config_hash(config_a)
        hash_b = im.compute_config_hash(config_b)

        assert hash_a == hash_b

    def test_exclusions(self):
        """
        Tests that `compute_config_hash` correctly excludes specified keys from the hashing process.

        This is vital for handling non-deterministic values (e.g., timestamps, temporary IDs)
        or sensitive information that should not impact the reproducibility hash.

        What happens:
        1. Two configuration dictionaries (`config_a` and `config_b`) are defined.
           They are identical except for a 'timestamp' key, which has different values.
        2. `compute_config_hash` is called for both configurations, with 'timestamp'
           specified in `exclude_keys`.
        3. A third hash (`hash_c`) is computed for `config_b` without any exclusions.

        What's checked:
        - When 'timestamp' is excluded, `hash_a` and `hash_b` are identical, proving
          that the exclusion mechanism works.
        - When 'timestamp' is *not* excluded, `hash_c` is different from `hash_a`,
          confirming that the exclusion correctly changes the hash.
        """
        im = IdentityManager()

        config_a = {"a": 1, "timestamp": 12345}
        config_b = {"a": 1, "timestamp": 67890}  # Different timestamp

        # If we exclude 'timestamp', hashes should match
        hash_a = im.compute_config_hash(config_a, exclude_keys=["timestamp"])
        hash_b = im.compute_config_hash(config_b, exclude_keys=["timestamp"])

        assert hash_a == hash_b

        # Verify inclusion changes hash
        hash_c = im.compute_config_hash(config_b, exclude_keys=[])
        assert hash_a != hash_c

    @pytest.mark.skipif(not HAS_NUMPY, reason="Numpy not installed")
    def test_numpy_cleaning(self):
        """
        Tests that `compute_config_hash` correctly converts NumPy data types
        to standard Python types before hashing.

        This addresses "The NumPy Problem" by ensuring that configurations
        using NumPy types (e.g., `np.int64`, `np.ndarray`) produce the same
        hash as their standard Python equivalents, preventing serialization
        errors and ensuring consistent hashing.

        What happens:
        1. A configuration dictionary (`config_std`) is defined using standard Python types.
        2. Another configuration dictionary (`config_np`) is defined with equivalent values
           but using NumPy types.
        3. `compute_config_hash` is called for both configurations.

        What's checked:
        - The SHA256 hashes generated for `config_std` and `config_np` are identical,
          proving that NumPy types are correctly converted and do not affect the final hash.
        """
        im = IdentityManager()

        # Config with standard types
        config_std = {"val": 10, "arr": [1, 2]}

        # Config with numpy types
        config_np = {"val": np.int64(10), "arr": np.array([1, 2])}

        hash_std = im.compute_config_hash(config_std)
        hash_np = im.compute_config_hash(config_np)

        assert hash_std == hash_np


class TestInputHashing:
    """
    Tests the `compute_input_hash` method of `IdentityManager`.

    These tests verify that input artifact hashing correctly:
    -   Handles provenance-based inputs (from previous runs) by using their `run_id`.
    -   Computes content or metadata hashes for raw file-based inputs.
    -   Ensures order-independence of input lists.
    -   Applies the configured hashing strategy ("fast" vs. "full") for files.
    """

    def test_provenance_based_inputs(self):
        """
        Tests that `compute_input_hash` correctly uses `run_id` for artifacts
        that originate from previous Consist runs and ensures order-independence.

        This verifies that inputs with known provenance contribute to the Merkle DAG
        identity based on their source run, and that the order in which they are
        provided does not affect the final hash.

        What happens:
        1. Two `Artifact` objects (`art1`, `art2`) are created, each with a distinct `run_id`.
        2. `compute_input_hash` is called with these artifacts in two different orders.
        3. A third `Artifact` (`art3`) is created, identical to `art1` but with a different `run_id`.
        4. `compute_input_hash` is called with `art3` and `art2`.

        What's checked:
        - The hash computed for `[art1, art2]` is identical to the hash for `[art2, art1]`,
          proving order-independence.
        - The hash computed with `art3` (different `run_id`) is different from the original,
          proving sensitivity to provenance changes.
        """
        im = IdentityManager()

        # Two artifacts from previous runs
        art1 = Artifact(key="a1", uri="x", driver="x", run_id="run_123")
        art2 = Artifact(key="a2", uri="y", driver="y", run_id="run_456")

        # Order shouldn't matter
        hash_a = im.compute_input_hash([art1, art2])
        hash_b = im.compute_input_hash([art2, art1])

        assert hash_a == hash_b

        # Changing source run_id should change hash
        art3 = Artifact(key="a1", uri="x", driver="x", run_id="run_999")
        hash_c = im.compute_input_hash([art3, art2])
        assert hash_a != hash_c

    def test_raw_file_inputs(self):
        """
        Tests `compute_input_hash` for raw file inputs (i.e., artifacts with `run_id=None`).

        This verifies that when an input is a direct file on the filesystem not
        produced by a previous Consist run, its content is correctly hashed to
        form part of the overall input identity.

        What happens:
        1. A temporary file is created with known content ("hello world").
        2. An `Artifact` object is created to represent this raw file, with `run_id=None`.
        3. A `path_resolver` function is provided to map the artifact's URI to the
           temporary file's path.
        4. `compute_input_hash` is called with this artifact and resolver.

        What's checked:
        - The computed input hash matches the expected SHA256 hash of the file's content,
          wrapped in the "file:<hash>" composite structure.
        """
        im = IdentityManager()

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"hello world")
            fname = f.name

        try:
            # Artifact representing the raw file (no run_id)
            art = Artifact(
                key="raw", uri="inputs://file.txt", driver="txt", run_id=None
            )
            resolver = lambda uri: fname

            hash_val = im.compute_input_hash([art], path_resolver=resolver)

            # Expected: SHA256 of "hello world"
            expected_file_hash = hashlib.sha256(b"hello world").hexdigest()
            expected_composite = hashlib.sha256(
                f"file:{expected_file_hash}".encode()
            ).hexdigest()

            assert hash_val == expected_composite

        finally:
            if os.path.exists(fname):
                os.remove(fname)

    def test_fast_vs_full_hashing(self):
        """
        Tests the difference between "full" (content-based) and "fast" (metadata-based)
        hashing strategies for raw files.

        This verifies that `full` hashing is sensitive only to content changes,
        while `fast` hashing is sensitive to metadata changes like modification time,
        even if content remains the same.

        What happens:
        1. A temporary file (`fname`) is created with initial content.
        2. `compute_input_hash` is called using both "full" and "fast" strategies,
           generating `hash_full_1` and `hash_fast_1`.
        3. The file's modification timestamp is updated using `Path(fname).touch()`,
           but its content remains unchanged.
        4. `compute_input_hash` is called again for both strategies, generating
           `hash_full_2` and `hash_fast_2`.

        What's checked:
        - `hash_full_1` is equal to `hash_full_2`, confirming that content hashing
          is unaffected by metadata changes.
        - `hash_fast_1` is *not* equal to `hash_fast_2`, confirming that fast hashing
          is sensitive to metadata changes (specifically modification time).
        """
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"hello")
            fname = f.name

        try:
            art = Artifact(key="raw", uri="inputs://x", driver="x", run_id=None)
            resolver = lambda uri: fname

            # 1. Full Mode
            im_full = IdentityManager(hashing_strategy="full")
            hash_full_1 = im_full.compute_input_hash([art], resolver)

            # 2. Fast Mode
            im_fast = IdentityManager(hashing_strategy="fast")
            hash_fast_1 = im_fast.compute_input_hash([art], resolver)

            # 3. Modify only metadata (touch)
            # Sleep to ensure mtime changes
            time.sleep(0.01)
            Path(fname).touch()

            hash_full_2 = im_full.compute_input_hash([art], resolver)
            hash_fast_2 = im_fast.compute_input_hash([art], resolver)

            # Full hash should be identical (content didn't change)
            assert hash_full_1 == hash_full_2

            # Fast hash should change (mtime changed)
            assert hash_fast_1 != hash_fast_2

        finally:
            os.remove(fname)


class TestCodeVersion:
    """
    Tests the `get_code_version` method of `IdentityManager`.

    These tests ensure that the Git code version (SHA) is correctly retrieved
    and that "dirty" repository states are appropriately flagged with a nonce,
    safeguarding reproducibility by preventing false cache hits for uncommitted changes.
    """

    @patch("consist.core.identity.git")
    def test_clean_repo(self, mock_git: MagicMock):
        """
        Tests `get_code_version` when the Git repository is in a clean state (no uncommitted changes).

        This verifies that for a clean repository, `get_code_version` returns the exact
        commit SHA, which is crucial for identifying reproducible code versions.

        What happens:
        1. The `git` module and its `Repo` class are mocked.
        2. The mock `Repo` is configured to report `is_dirty.return_value = False`
           and `head.object.hexsha = "abc12345"`.
        3. `get_code_version` is called.

        What's checked:
        - The returned code version is exactly the mocked Git commit SHA ("abc12345").
        """
        im = IdentityManager()

        # Mock Repo structure
        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = False
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        assert im.get_code_version() == "abc12345"

    @patch("consist.core.identity.git")
    def test_dirty_repo(self, mock_git: MagicMock):
        """
        Tests `get_code_version` when the Git repository is in a dirty state (has uncommitted changes).

        This verifies that for a dirty repository, `get_code_version` appends a
        timestamp/nonce to the commit SHA, ensuring that runs from an uncommitted
        state do not incorrectly collide with runs from a clean commit. This is
        crucial for safeguarding reproducibility.

        What happens:
        1. The `git` module and its `Repo` class are mocked.
        2. The mock `Repo` is configured to report `is_dirty.return_value = True`
           and `head.object.hexsha = "abc12345"`.
        3. `get_code_version` is called.

        What's checked:
        - The returned code version starts with the mocked Git commit SHA and
          is appended with "-dirty-".
        - The appended part contains a nonce (timestamp), ensuring uniqueness
          for different dirty states over time.
        """
        im = IdentityManager()

        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = True
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        version = im.get_code_version()

        assert version.startswith("abc12345-dirty-")
        # Ensure a nonce/timestamp was added
        assert len(version) > len("abc12345-dirty-")
