"""Shared building blocks for coupler-like mapping helpers.

This module centralizes behavior used by ``Coupler`` and ``NoopCoupler``.
When adding another coupler variant, prefer extending ``CouplerMapMixin`` and
overriding only the documented hook methods.
"""

from __future__ import annotations

import builtins
import warnings
from typing import Any, Callable, Generic, Mapping, MutableMapping, Protocol, TypeVar


class DeclaredOutputLike(Protocol):
    """Protocol for declared-output metadata records."""

    required: bool
    description: str | None


TDeclaredOutput = TypeVar("TDeclaredOutput", bound=DeclaredOutputLike)
TArtifact = TypeVar("TArtifact")


def should_warn_undefined_key(
    *,
    key: str,
    warn_undefined: bool,
    declared_outputs: Mapping[str, Any],
    undefined_keys: set[str],
) -> bool:
    """Return ``True`` when a new undefined key warning should be emitted.

    The function tracks first-seen undefined keys in ``undefined_keys`` so each
    key warns at most once.
    """
    if not warn_undefined:
        return False
    if key in declared_outputs or key in undefined_keys:
        return False
    undefined_keys.add(key)
    return True


def register_declared_outputs(
    names: tuple[str, ...],
    *,
    required: bool | Mapping[str, bool],
    description: Mapping[str, str] | None,
    declared_outputs: MutableMapping[str, TDeclaredOutput],
    output_descriptions: MutableMapping[str, str],
    create_declared_output: Callable[[], TDeclaredOutput],
    validate_key: Callable[[str], Any],
) -> None:
    """Register output declarations with sticky ``required`` semantics.

    ``required=True`` cannot be downgraded by later calls. Descriptions are
    updated when provided.
    """
    if isinstance(required, Mapping):
        required_map = {str(k): bool(v) for k, v in required.items()}
        default_required = False
    else:
        required_map = {}
        default_required = bool(required)
    description_map = {str(k): v for k, v in (description or {}).items()}

    for name in names:
        if not isinstance(name, str):
            raise TypeError("Coupler output names must be strings.")
        key = name.strip()
        if not key:
            raise ValueError("Coupler output names cannot be empty.")
        validate_key(key)
        entry = declared_outputs.get(key, create_declared_output())
        entry.required = entry.required or required_map.get(key, default_required)
        if key in description_map:
            entry.description = description_map[key]
            output_descriptions[key] = description_map[key]
        declared_outputs[key] = entry


def find_missing_required_outputs(
    declared_outputs: Mapping[str, DeclaredOutputLike],
    artifacts: Mapping[str, Any],
) -> list[str]:
    """Return sorted required output keys that are missing or ``None``."""
    return sorted(
        [
            key
            for key, entry in declared_outputs.items()
            if entry.required and (key not in artifacts or artifacts[key] is None)
        ]
    )


def collect_artifacts_by_keys(
    artifacts: Mapping[str, TArtifact],
    *,
    keys: tuple[str, ...],
    prefix: str,
    set_artifact: Callable[[str, TArtifact], Any],
    validate_key: Callable[[str], Any] | None = None,
) -> dict[str, TArtifact]:
    """Collect a key subset from ``artifacts`` into a coupler-like destination."""
    if not isinstance(artifacts, Mapping):
        raise TypeError("collect_by_keys expects a mapping of artifacts.")
    collected: dict[str, TArtifact] = {}
    for key in keys:
        if not isinstance(key, str):
            raise TypeError("collect_by_keys keys must be strings.")
        if validate_key is not None:
            validate_key(key)
        if key not in artifacts:
            raise KeyError(f"Missing artifact for key {key!r}.")
        coupler_key = f"{prefix}{key}"
        artifact = artifacts[key]
        set_artifact(coupler_key, artifact)
        collected[coupler_key] = artifact
    return collected


class CouplerMapMixin(Generic[TArtifact, TDeclaredOutput]):
    """Shared map-like behavior for coupler implementations.

    Extension points for subclasses:
    - ``_validate_artifact_key``: enforce key rules for reads/writes.
    - ``_create_declared_output``: construct declared-output records.
    - ``_collect_validate_key``: optional override to relax source-key checks.
    - ``set``: concrete storage API exposed to callers.
    """

    _artifacts: MutableMapping[str, TArtifact]
    _declared_outputs: MutableMapping[str, TDeclaredOutput]
    _warn_undefined: bool
    _undefined_keys: builtins.set[str]
    _output_descriptions: MutableMapping[str, str]

    def _validate_artifact_key(self, key: str) -> None:
        """Validate a coupler key or raise a key-related exception."""
        raise NotImplementedError

    def _create_declared_output(self) -> TDeclaredOutput:
        """Create an empty declared-output record for new keys."""
        raise NotImplementedError

    def _collect_validate_key(self) -> Callable[[str], Any] | None:
        """Validator used by ``collect_by_keys`` for requested source keys."""
        return self._validate_artifact_key

    def set(self, key: str, artifact: TArtifact) -> TArtifact:
        """Store an artifact under ``key`` (implemented by subclass)."""
        raise NotImplementedError

    def _store_artifact(
        self, key: str, artifact: TArtifact, *, validate_key: bool = True
    ) -> TArtifact:
        """Shared storage pipeline: optional validation + undefined-key warning."""
        if validate_key:
            self._validate_artifact_key(key)
        self._maybe_warn_undefined(key)
        self._artifacts[key] = artifact
        return artifact

    def _maybe_warn_undefined(self, key: str) -> None:
        """Emit a one-time undefined-key warning when warnings are enabled."""
        if should_warn_undefined_key(
            key=key,
            warn_undefined=self._warn_undefined,
            declared_outputs=self._declared_outputs,
            undefined_keys=self._undefined_keys,
        ):
            warnings.warn(
                f"Setting undefined coupler key '{key}'. "
                f"Declared outputs: {sorted(self._declared_outputs)}",
                UserWarning,
                stacklevel=2,
            )

    def update(
        self, artifacts: Mapping[str, TArtifact] | None = None, /, **kwargs: TArtifact
    ) -> None:
        """Bulk-store artifacts from a mapping and/or keyword arguments."""
        if artifacts:
            for key, artifact in artifacts.items():
                self.set(key, artifact)
        if kwargs:
            for key, artifact in kwargs.items():
                self.set(key, artifact)

    def get(self, key: str) -> TArtifact | None:
        """Return artifact for ``key`` or ``None`` if unset."""
        self._validate_artifact_key(key)
        return self._artifacts.get(key)

    def require(self, key: str) -> TArtifact:
        """Return artifact for ``key`` or raise a helpful ``KeyError``."""
        artifact = self.get(key)
        if artifact is None:
            available = ", ".join(sorted(self._artifacts.keys())) or "<none>"
            raise KeyError(
                f"Coupler missing key={key!r}. Available keys: {available}. "
                f"Did you forget to call coupler.set({key!r}, ...)?"
            )
        return artifact

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undefined: bool = False,
        description: Mapping[str, str] | None = None,
    ) -> None:
        """Declare expected output keys and optional metadata."""
        if not names:
            return
        if warn_undefined:
            self._warn_undefined = True
        register_declared_outputs(
            names,
            required=required,
            description=description,
            declared_outputs=self._declared_outputs,
            output_descriptions=self._output_descriptions,
            create_declared_output=self._create_declared_output,
            validate_key=self._validate_artifact_key,
        )

    def missing_declared_outputs(self) -> list[str]:
        """Return required declared outputs that are still unset."""
        return find_missing_required_outputs(
            self._declared_outputs,
            self._artifacts,
        )

    def require_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = True,
        warn_undefined: bool = False,
        description: Mapping[str, str] | None = None,
    ) -> None:
        """Convenience wrapper for declaring outputs as required."""
        self.declare_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, TArtifact], *keys: str, prefix: str = ""
    ) -> dict[str, TArtifact]:
        """Copy selected artifact keys into this coupler with an optional prefix."""
        return collect_artifacts_by_keys(
            artifacts,
            keys=keys,
            prefix=prefix,
            set_artifact=self.set,
            validate_key=self._collect_validate_key(),
        )

    def describe_outputs(self) -> dict[str, str]:
        """Return a copy of declared-output descriptions."""
        return dict(self._output_descriptions)
