from __future__ import annotations

import os
import re
from typing import Any, Mapping, Sequence

MAX_ARTIFACT_KEY_LENGTH = 256
VALID_ARTIFACT_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:/-]{0,255}$")

MAX_RUN_NAME_LENGTH = 256
MAX_DESCRIPTION_LENGTH = 4096
MAX_TAG_LENGTH = 256

MAX_CONFIG_KEY_LENGTH = 256
MAX_CONFIG_VALUE_LENGTH = 1_000_000


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


MAX_METADATA_ITEMS = _env_int("CONSIST_MAX_METADATA_ITEMS", 200)
MAX_METADATA_KEY_LENGTH = _env_int("CONSIST_MAX_METADATA_KEY_LENGTH", 256)
MAX_METADATA_VALUE_LENGTH = _env_int("CONSIST_MAX_METADATA_VALUE_LENGTH", 65_536)
ALLOWED_METADATA_TYPES = (str, int, float, bool, type(None), list, dict)


def validate_artifact_key(key: str) -> None:
    if not isinstance(key, str):
        raise TypeError(f"Artifact key must be a string, got {type(key)}")
    if not key:
        raise ValueError("Artifact key cannot be empty.")
    if len(key) > MAX_ARTIFACT_KEY_LENGTH:
        raise ValueError(
            f"Artifact key exceeds max length {MAX_ARTIFACT_KEY_LENGTH}: {key}"
        )
    if ".." in key or "//" in key:
        raise ValueError(
            f"Invalid artifact key '{key}'. Keys cannot contain '..' or '//'."
        )
    if not VALID_ARTIFACT_KEY_PATTERN.match(key):
        raise ValueError(
            f"Invalid artifact key '{key}'. Keys must start with a letter/number/underscore, "
            "contain only alphanumeric/underscore/dash/dot/colon/slash, and be <= "
            f"{MAX_ARTIFACT_KEY_LENGTH} characters."
        )


def validate_run_strings(
    *, model_name: str, description: str | None, tags: Sequence[str] | None
) -> None:
    if not isinstance(model_name, str):
        raise TypeError(f"model_name must be a string, got {type(model_name)}")
    if not model_name:
        raise ValueError("model_name cannot be empty.")
    if len(model_name) > MAX_RUN_NAME_LENGTH:
        raise ValueError(
            f"model_name exceeds max length {MAX_RUN_NAME_LENGTH}: {model_name}"
        )
    if description is not None:
        if not isinstance(description, str):
            raise TypeError(f"description must be a string, got {type(description)}")
        if len(description) > MAX_DESCRIPTION_LENGTH:
            raise ValueError(
                f"description exceeds max length {MAX_DESCRIPTION_LENGTH}."
            )
    if tags is not None:
        if isinstance(tags, (str, bytes)):
            raise TypeError("tags must be a sequence of strings, not a string.")
        for tag in tags:
            if not isinstance(tag, str):
                raise TypeError(f"tags must be strings, got {type(tag)}")
            if not tag:
                raise ValueError("tags cannot contain empty strings.")
            if len(tag) > MAX_TAG_LENGTH:
                raise ValueError(f"tag exceeds max length {MAX_TAG_LENGTH}: {tag}")


def validate_config_structure(config: Mapping[str, Any]) -> None:
    stack: list[Any] = [config]
    while stack:
        value = stack.pop()
        if isinstance(value, Mapping):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError(f"Config keys must be strings, got {type(key)}")
                if len(key) > MAX_CONFIG_KEY_LENGTH:
                    raise ValueError(
                        f"Config key exceeds max length {MAX_CONFIG_KEY_LENGTH}: {key}"
                    )
                stack.append(item)
        elif isinstance(value, list):
            stack.extend(value)
        elif isinstance(value, str):
            if len(value) > MAX_CONFIG_VALUE_LENGTH:
                raise ValueError(
                    f"Config value exceeds max length {MAX_CONFIG_VALUE_LENGTH}."
                )


def validate_run_meta(meta: Mapping[str, Any]) -> None:
    if not isinstance(meta, Mapping):
        raise TypeError(f"Run metadata must be a mapping, got {type(meta)}")
    if len(meta) > MAX_METADATA_ITEMS:
        raise ValueError(
            f"Run metadata exceeds {MAX_METADATA_ITEMS} items (got {len(meta)}). "
            "Set CONSIST_MAX_METADATA_ITEMS to override."
        )
    for key, value in meta.items():
        if not isinstance(key, str):
            raise TypeError(f"Metadata keys must be strings, got {type(key)}")
        if not key:
            raise ValueError("Metadata keys cannot be empty.")
        if len(key) > MAX_METADATA_KEY_LENGTH:
            raise ValueError(
                f"Metadata key exceeds {MAX_METADATA_KEY_LENGTH} chars: {key}. "
                "Set CONSIST_MAX_METADATA_KEY_LENGTH to override."
            )
        if not isinstance(value, ALLOWED_METADATA_TYPES):
            raise TypeError(
                f"Metadata value for '{key}' has unsupported type {type(value)}."
            )
        if isinstance(value, str) and len(value) > MAX_METADATA_VALUE_LENGTH:
            raise ValueError(
                f"Metadata value for '{key}' exceeds {MAX_METADATA_VALUE_LENGTH} chars. "
                "Set CONSIST_MAX_METADATA_VALUE_LENGTH to override."
            )
        if isinstance(value, (list, dict)):
            if len(str(value)) > MAX_METADATA_VALUE_LENGTH:
                raise ValueError(
                    f"Metadata value for '{key}' exceeds {MAX_METADATA_VALUE_LENGTH} chars. "
                    "Set CONSIST_MAX_METADATA_VALUE_LENGTH to override."
                )
