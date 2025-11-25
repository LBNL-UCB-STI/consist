# tests/unit/test_identity.py

import pytest
import json
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

    def test_canonicalization(self):
        """Test that dictionary key order does not affect the hash."""
        im = IdentityManager()

        config_a = {"a": 1, "b": 2, "c": {"x": 10, "y": 20}}
        config_b = {"c": {"y": 20, "x": 10}, "b": 2, "a": 1}  # Mixed order

        hash_a = im.compute_config_hash(config_a)
        hash_b = im.compute_config_hash(config_b)

        assert hash_a == hash_b

    def test_exclusions(self):
        """Test that excluded keys do not affect the hash."""
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
        """Test that numpy types are converted and result in consistent hashes."""
        im = IdentityManager()

        # Config with standard types
        config_std = {"val": 10, "arr": [1, 2]}

        # Config with numpy types
        config_np = {
            "val": np.int64(10),
            "arr": np.array([1, 2])
        }

        hash_std = im.compute_config_hash(config_std)
        hash_np = im.compute_config_hash(config_np)

        assert hash_std == hash_np


class TestInputHashing:

    def test_provenance_based_inputs(self):
        """Test hashing inputs that come from previous runs."""
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
        """Test hashing raw file inputs (no run_id)."""
        im = IdentityManager()

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"hello world")
            fname = f.name

        try:
            # Artifact representing the raw file (no run_id)
            art = Artifact(key="raw", uri="inputs://file.txt", driver="txt", run_id=None)
            resolver = lambda uri: fname

            hash_val = im.compute_input_hash([art], path_resolver=resolver)

            # Expected: SHA256 of "hello world"
            expected_file_hash = hashlib.sha256(b"hello world").hexdigest()
            expected_composite = hashlib.sha256(f"file:{expected_file_hash}".encode()).hexdigest()

            assert hash_val == expected_composite

        finally:
            if os.path.exists(fname):
                os.remove(fname)

    def test_fast_vs_full_hashing(self):
        """Test that fast mode uses metadata and detects mtime changes."""

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

    @patch("consist.core.identity.git")
    def test_clean_repo(self, mock_git):
        """Test behavior when git repo is clean."""
        im = IdentityManager()

        # Mock Repo structure
        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = False
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        assert im.get_code_version() == "abc12345"

    @patch("consist.core.identity.git")
    def test_dirty_repo(self, mock_git):
        """Test behavior when git repo is dirty."""
        im = IdentityManager()

        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = True
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        version = im.get_code_version()

        assert version.startswith("abc12345-dirty-")
        # Ensure a nonce/timestamp was added
        assert len(version) > len("abc12345-dirty-")