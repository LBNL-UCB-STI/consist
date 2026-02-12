"""
Tests for ArtifactKeyRegistry: validates key collection and enforcement.
Workflow helpers and docs rely on deterministic key lists and clear errors.
"""

from __future__ import annotations

import pytest

from consist.utils import ArtifactKeyRegistry


class BaseKeys(ArtifactKeyRegistry):
    RAW = "raw"


class Keys(BaseKeys):
    PREPROCESSED = "preprocessed"
    ANALYSIS = "analysis"
    IGNORED = 123
    _PRIVATE = "private"


def test_all_keys_collects_string_constants() -> None:
    assert Keys.all_keys() == ["raw", "preprocessed", "analysis"]


def test_validate_rejects_unknown_keys() -> None:
    with pytest.raises(ValueError, match="Unknown artifact keys"):
        Keys.validate(["raw", "missing"])


def test_validate_require_all_flags_missing() -> None:
    with pytest.raises(ValueError, match="Missing artifact keys"):
        Keys.validate(["raw"], require_all=True)


def test_validate_accepts_known_keys() -> None:
    Keys.validate(["raw", "analysis"])
