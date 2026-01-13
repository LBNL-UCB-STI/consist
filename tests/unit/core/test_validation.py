from __future__ import annotations

import pytest

from consist.core.validation import (
    MAX_ARTIFACT_KEY_LENGTH,
    MAX_CONFIG_KEY_LENGTH,
    MAX_CONFIG_VALUE_LENGTH,
    MAX_METADATA_VALUE_LENGTH,
    validate_artifact_key,
    validate_config_structure,
    validate_run_meta,
)


def test_validate_artifact_key_accepts_valid() -> None:
    valid = [
        "output",
        "synthetic_population",
        "zarr_skims",
        "zone_to_zone_travel_times",
        "data_v2.0",
        "result-final_2024",
        "_underscore_start",
        "config:overlay/settings.yaml",
    ]
    for key in valid:
        validate_artifact_key(key)


def test_validate_artifact_key_rejects_invalid() -> None:
    invalid = [
        "",
        "key with spaces",
        "key://uri",
        "config/../settings.yaml",
        "config//settings.yaml",
        "a" * (MAX_ARTIFACT_KEY_LENGTH + 1),
    ]
    for key in invalid:
        with pytest.raises(ValueError):
            validate_artifact_key(key)


def test_validate_config_structure_rejects_long_key() -> None:
    config = {"a" * (MAX_CONFIG_KEY_LENGTH + 1): "ok"}
    with pytest.raises(ValueError):
        validate_config_structure(config)


def test_validate_config_structure_rejects_long_value() -> None:
    config = {"key": "x" * (MAX_CONFIG_VALUE_LENGTH + 1)}
    with pytest.raises(ValueError):
        validate_config_structure(config)


def test_validate_run_meta_rejects_long_value() -> None:
    meta = {"note": "x" * (MAX_METADATA_VALUE_LENGTH + 1)}
    with pytest.raises(ValueError):
        validate_run_meta(meta)


def test_validate_run_meta_accepts_basic() -> None:
    meta = {"name": "run", "tags": ["a", "b"], "count": 3, "active": True}
    validate_run_meta(meta)
