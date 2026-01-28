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
    (linked to previous runs) and raw file content.
-   **Callable/Code Hashing**: Verifying the "tiered" code identity strategies
    (Source vs. Module) and external dependency tracking.
-   **Code Version Detection**: Accurately detecting the Git commit SHA and
    flagging "dirty" repository states.
"""

import pytest
import hashlib
import tempfile
import os
import time
import importlib.util
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
    """Tests the `compute_config_hash` method of `IdentityManager`."""

    def test_canonicalization(self):
        """
        Tests that `compute_config_hash` produces identical hashes for configurations
        that are logically the same but have different dictionary key orders.
        """
        im = IdentityManager()

        config_a = {"a": 1, "b": 2, "c": {"x": 10, "y": 20}}
        config_b = {"c": {"y": 20, "x": 10}, "b": 2, "a": 1}  # Mixed order

        hash_a = im.compute_config_hash(config_a)
        hash_b = im.compute_config_hash(config_b)

        assert hash_a == hash_b

    def test_exclusions(self):
        """
        Tests that `compute_config_hash` correctly excludes specified keys.
        """
        im = IdentityManager()

        config_a = {"a": 1, "timestamp": 12345}
        config_b = {"a": 1, "timestamp": 67890}

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
        Tests that `compute_config_hash` correctly converts NumPy data types.
        """
        im = IdentityManager()
        config_std = {"val": 10, "arr": [1, 2]}
        config_np = {"val": np.int64(10), "arr": np.array([1, 2])}

        hash_std = im.compute_config_hash(config_std)
        hash_np = im.compute_config_hash(config_np)

        assert hash_std == hash_np


class TestInputHashing:
    """Tests the `compute_input_hash` method of `IdentityManager`."""

    def test_provenance_based_inputs(self):
        """
        Tests that inputs with known provenance (run_id) are hashed by ID,
        and that order does not matter.
        """
        im = IdentityManager()

        art1 = Artifact(key="a1", container_uri="x", driver="x", run_id="run_123")
        art2 = Artifact(key="a2", container_uri="y", driver="y", run_id="run_456")

        hash_a = im.compute_input_hash([art1, art2])
        hash_b = im.compute_input_hash([art2, art1])

        assert hash_a == hash_b

        # Changing source run_id should change hash
        art3 = Artifact(key="a1", container_uri="x", driver="x", run_id="run_999")
        hash_c = im.compute_input_hash([art3, art2])
        assert hash_a != hash_c

    def test_raw_file_inputs(self):
        """
        Tests hashing for raw file inputs (no run_id) using a path resolver.
        """
        im = IdentityManager()

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"hello world")
            fname = f.name

        try:
            art = Artifact(
                key="raw", container_uri="inputs://file.txt", driver="txt", run_id=None
            )

            def resolver(uri: str) -> str:
                return fname

            hash_val = im.compute_input_hash([art], path_resolver=resolver)

            # Expected: SHA256 of "hello world"
            expected_file_hash = hashlib.sha256(b"hello world").hexdigest()
            expected_composite = hashlib.sha256(
                f"driver:txt|container_uri:inputs://file.txt|file:{expected_file_hash}".encode()
            ).hexdigest()

            assert hash_val == expected_composite

        finally:
            if os.path.exists(fname):
                os.remove(fname)

    def test_table_path_affects_identity(self):
        im = IdentityManager()
        art_a = Artifact(
            key="a",
            container_uri="inputs://file.h5",
            driver="h5_table",
            run_id="run_shared",
            table_path="/tables/a",
        )
        art_b = Artifact(
            key="b",
            container_uri="inputs://file.h5",
            driver="h5_table",
            run_id="run_shared",
            table_path="/tables/b",
        )
        hash_a = im.compute_input_hash([art_a])
        hash_b = im.compute_input_hash([art_b])
        assert hash_a != hash_b

    def test_array_path_affects_identity(self):
        im = IdentityManager()
        art_a = Artifact(
            key="a",
            container_uri="inputs://array.zarr",
            driver="zarr",
            run_id="run_shared",
            array_path="/group/a",
        )
        art_b = Artifact(
            key="b",
            container_uri="inputs://array.zarr",
            driver="zarr",
            run_id="run_shared",
            array_path="/group/b",
        )
        hash_a = im.compute_input_hash([art_a])
        hash_b = im.compute_input_hash([art_b])
        assert hash_a != hash_b

    def test_run_id_inputs_ignore_container_uri(self):
        im = IdentityManager()
        art_a = Artifact(
            key="output_a",
            container_uri="inputs://a.parquet",
            driver="parquet",
            run_id="run_shared",
        )
        art_b = Artifact(
            key="output_a",
            container_uri="workspace://a.parquet",
            driver="parquet",
            run_id="run_shared",
        )
        hash_a = im.compute_input_hash([art_a])
        hash_b = im.compute_input_hash([art_b])
        assert hash_a == hash_b

    def test_fast_vs_full_hashing(self):
        """
        Tests the difference between "full" (content) and "fast" (metadata) hashing.
        """
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"hello")
            fname = f.name

        try:
            art = Artifact(
                key="raw", container_uri="inputs://x", driver="x", run_id=None
            )

            def resolver(uri: str) -> str:
                return fname

            im_full = IdentityManager(hashing_strategy="full")
            hash_full_1 = im_full.compute_input_hash([art], resolver)

            im_fast = IdentityManager(hashing_strategy="fast")
            hash_fast_1 = im_fast.compute_input_hash([art], resolver)

            # Modify only metadata (touch)
            time.sleep(0.01)
            Path(fname).touch()

            hash_full_2 = im_full.compute_input_hash([art], resolver)
            hash_fast_2 = im_fast.compute_input_hash([art], resolver)

            assert hash_full_1 == hash_full_2  # Content same
            assert hash_fast_1 != hash_fast_2  # Metadata changed

        finally:
            os.remove(fname)


class TestZarrHashing:
    """Tests Zarr-aware hashing behavior for directory inputs."""

    def test_fast_hash_ignores_chunk_content_when_metadata_stable(self, tmp_path):
        store = tmp_path / "skims.zarr"
        store.mkdir()
        metadata = store / ".zmetadata"
        metadata.write_text('{"zarr_consolidated_format":1}', encoding="utf-8")
        chunk = store / "0.0.0"
        chunk.write_bytes(b"aaaa")

        im_fast = IdentityManager(hashing_strategy="fast")
        im_full = IdentityManager(hashing_strategy="full")

        hash_fast_1 = im_fast.compute_file_checksum(store)
        hash_full_1 = im_full.compute_file_checksum(store)

        original_stat = chunk.stat()
        chunk.write_bytes(b"bbbb")
        os.utime(chunk, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

        hash_fast_2 = im_fast.compute_file_checksum(store)
        hash_full_2 = im_full.compute_file_checksum(store)

        assert hash_fast_1 == hash_fast_2
        assert hash_full_1 != hash_full_2

    def test_fast_hash_changes_on_metadata_update(self, tmp_path):
        store = tmp_path / "skims.zarr"
        store.mkdir()
        metadata = store / ".zmetadata"
        metadata.write_text('{"metadata":"v1"}', encoding="utf-8")
        chunk = store / "0.0.0"
        chunk.write_bytes(b"aaaa")

        im_fast = IdentityManager(hashing_strategy="fast")
        hash_fast_1 = im_fast.compute_file_checksum(store)

        metadata.write_text('{"metadata":"v2"}', encoding="utf-8")
        hash_fast_2 = im_fast.compute_file_checksum(store)

        assert hash_fast_1 != hash_fast_2


class TestCallableHashing:
    """
    Tests the new `compute_callable_hash` method in `IdentityManager`.

    Verifies the "Tiered Code Identity" approach:
    1. 'source': Hash only the function body.
    2. 'module': Hash the file containing the function.
    3. 'extra_deps': Hash external files declared as dependencies.
    """

    def test_strategy_source(self):
        """
        Tests 'source' strategy: Hashes rely on the function definition itself.
        """
        im = IdentityManager()

        def func_v1():
            return "version 1"

        def func_v2():
            return "version 2"

        # 1. Different bodies -> Different hashes
        h1 = im.compute_callable_hash(func_v1, strategy="source")
        h2 = im.compute_callable_hash(func_v2, strategy="source")
        assert h1 != h2

        # 2. Same body (logic) -> Same hash
        # Note: inspect.getsource includes the definition line (def x():).
        # To test 'same body' effectively with anonymous functions or redefinitions:
        def func_v1_redefined():
            return "version 1"

        # The function NAME is part of the source line "def func_name...",
        # so these will actually differ if the names differ.
        # Ideally, we verify that identical code produces identical hashes.
        h1_redux = im.compute_callable_hash(func_v1, strategy="source")
        assert h1 == h1_redux

    def test_strategy_module(self, tmp_path):
        """
        Tests 'module' strategy: Hashes the file on disk containing the function.

        This validates that:
        1. Changes to the file (even comments outside the function) change the hash.
        2. The hash reflects the file content, even if the in-memory object is stale.
        """
        im = IdentityManager(project_root=str(tmp_path))

        # 1. Create a python module file
        module_path = tmp_path / "my_module.py"
        module_path.write_text("def my_func():\n    return 42\n")

        # 2. Load the module dynamically
        spec = importlib.util.spec_from_file_location("my_module", module_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        func = module.my_func

        # 3. Compute initial hash
        hash_v1 = im.compute_callable_hash(func, strategy="module")

        # 4. Modify the file on disk (add a comment or constant)
        # Note: We do NOT need to reload the module. The strategy checks the file on disk.
        module_path.write_text("def my_func():\n    return 42\n\n# New Comment\n")

        # 5. Compute new hash
        hash_v2 = im.compute_callable_hash(func, strategy="module")

        assert hash_v1 != hash_v2

    def test_extra_deps(self, tmp_path):
        """
        Tests explicit dependency tracking via `extra_deps`.
        """
        im = IdentityManager(project_root=str(tmp_path))

        # Define a function (strategy doesn't matter for this test, use source)
        def my_func():
            pass

        # Create a config file
        config_path = tmp_path / "config.yaml"
        config_path.write_text("param: 100")

        # 1. Compute hash with dependency
        # extra_deps expects paths relative to project_root
        h1 = im.compute_callable_hash(
            my_func, strategy="source", extra_deps=["config.yaml"]
        )

        # 2. Modify dependency
        config_path.write_text("param: 200")

        # 3. Compute hash again
        h2 = im.compute_callable_hash(
            my_func, strategy="source", extra_deps=["config.yaml"]
        )

        assert h1 != h2

    def test_missing_extra_dep(self, tmp_path):
        """
        Tests that a missing dependency results in a distinct hash but doesn't crash.
        """
        im = IdentityManager(project_root=str(tmp_path))

        def my_func():
            pass

        # Dep doesn't exist
        h_missing = im.compute_callable_hash(my_func, extra_deps=["ghost.txt"])

        # Create it
        (tmp_path / "ghost.txt").write_text("boo")

        h_exists = im.compute_callable_hash(my_func, extra_deps=["ghost.txt"])

        assert h_missing != h_exists


class TestCodeVersion:
    """Tests the `get_code_version` method of `IdentityManager`."""

    @patch("consist.core.identity.git")
    def test_clean_repo(self, mock_git: MagicMock):
        """Verifies clean repo returns exact SHA."""
        im = IdentityManager()

        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = False
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        assert im.get_code_version() == "abc12345"

    @patch("consist.core.identity.git")
    def test_dirty_repo(self, mock_git: MagicMock):
        """Verifies dirty repo appends nonce."""
        im = IdentityManager()

        mock_repo = MagicMock()
        mock_repo.is_dirty.return_value = True
        mock_repo.head.object.hexsha = "abc12345"
        mock_git.Repo.return_value = mock_repo

        version = im.get_code_version()
        assert version.startswith("abc12345-dirty-")
        assert len(version) > len("abc12345-dirty-")

    @patch("consist.core.identity.git")
    def test_git_error_handling(self, mock_git: MagicMock):
        """
        Tests that get_code_version gracefully handles Git errors
        (e.g., inside a container or not a git repo).
        """
        im = IdentityManager()

        # Simulate InvalidGitRepositoryError
        # Note: We need to set the side_effect on the Repo constructor
        mock_git.InvalidGitRepositoryError = Exception  # mocking the exception class
        mock_git.NoSuchPathError = Exception

        mock_git.Repo.side_effect = mock_git.InvalidGitRepositoryError("Bad repo")

        assert im.get_code_version() == "unknown_code_version"

    def test_git_missing_module(self):
        """
        Tests behavior when 'git' python module is not installed.
        """
        # We need to simulate the module being missing.
        # This is hard if it's already imported.
        # We can check the logic by patching the module level variable in identity.py if possible,
        # or by checking if the real test environment has git.

        # Easier approach: Patch `consist.core.identity.git` to be None
        with patch("consist.core.identity.git", None):
            im = IdentityManager()
            assert im.get_code_version() == "no_git_module_found"
