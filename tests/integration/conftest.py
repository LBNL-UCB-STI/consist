import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def stable_identity():
    """
    Mocks the IdentityManager to return a stable git hash ONLY for integration tests.

    Integration tests often create files on disk during execution. If we check
    the real git status, these new files make the repo look "dirty", changing
    the code hash in the middle of a test and causing cache misses.

    We place this in tests/integration/conftest.py so it does NOT affect
    unit tests (which specifically test the logic of IdentityManager).
    """
    with patch("consist.core.identity.IdentityManager.get_code_version", return_value="static_test_hash"):
        yield