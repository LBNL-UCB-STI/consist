from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, TYPE_CHECKING

from consist.models.artifact import Artifact
from consist.core.validation import validate_artifact_key

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class Coupler:
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.

    It does not log artifacts, infer inputs/outputs, or mutate Artifacts as a side effect
    of reads. Keep provenance operations on the Tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self.tracker = tracker
        self._artifacts: Dict[str, Artifact] = {}
        self._declared_outputs: Dict[str, DeclaredOutput] = {}
        self._warn_undocumented = False
        self._undocumented_keys: set[str] = set()
        self._output_descriptions: Dict[str, str] = {}

    def _maybe_warn_undocumented(self, key: str) -> None:
        if not self._warn_undocumented:
            return
        if key in self._declared_outputs or key in self._undocumented_keys:
            return
        self._undocumented_keys.add(key)
        warnings.warn(
            f"Setting undocumented coupler key '{key}'. "
            f"Declared outputs: {sorted(self._declared_outputs)}",
            UserWarning,
            stacklevel=2,
        )

    def set(self, key: str, artifact: Artifact) -> Artifact:
        """Store an artifact under a validated key."""
        validate_artifact_key(key)
        self._maybe_warn_undocumented(key)
        self._artifacts[key] = artifact
        return artifact

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Set an artifact, accepting both Artifact objects and artifact-like values.

        This method is useful when integrating with optional dependencies (like noop mode)
        where you may receive either:
        - A real Artifact (when tracking is enabled)
        - An artifact-like object with .path and .container_uri properties (noop mode)
        - A Path or string (fallback)

        All three forms are stored in the coupler and can be retrieved with get() or require().

        Parameters
        ----------
        key : str
            The coupler key (e.g., "persons", "skims"). Must follow artifact-key rules.
        value : Artifact or artifact-like or Path or str
            The value to store. Can be a real Artifact, artifact-like object with
            .path/.container_uri properties, a Path, or a string path.

        Returns
        -------
        Any
            The value that was stored.

        Examples
        --------
        Using with optional Consist dependency:
        ```python
        # Works whether log_output returns Artifact or NoopArtifact
        artifact = tracker.log_output(path, key="persons")
        coupler.set_from_artifact("persons", artifact)
        ```

        Mixed real and fallback artifacts:
        ```python
        artifact = log_output(path) or path  # Fallback to path string
        coupler.set_from_artifact("key", artifact)  # Handles both
        ```
        """
        validate_artifact_key(key)
        self._maybe_warn_undocumented(key)
        self._artifacts[key] = value
        return value

    def update(
        self, artifacts: Optional[Dict[str, Artifact]] = None, /, **kwargs: Artifact
    ) -> None:
        """
        Bulk-update the coupler mapping.

        Examples
        --------
        `coupler.update({"persons": art})` or `coupler.update(persons=art)`
        """
        if artifacts:
            for key, artifact in artifacts.items():
                self.set(key, artifact)
        if kwargs:
            for key, artifact in kwargs.items():
                self.set(key, artifact)

    def get(self, key: str) -> Optional[Artifact]:
        """Return the current artifact for `key`, or None if unset (key is validated)."""
        validate_artifact_key(key)
        return self._artifacts.get(key)

    def require(self, key: str) -> Artifact:
        """Return the artifact for `key`, raising a clear error if unset."""
        artifact = self.get(key)
        if artifact is None:
            available = ", ".join(sorted(self._artifacts.keys())) or "<none>"
            raise KeyError(
                f"Coupler missing key={key!r}. Available keys: {available}. "
                f"Did you forget to call coupler.set({key!r}, ...)?"
            )
        return artifact

    def keys(self) -> Iterable[str]:
        return self._artifacts.keys()

    def items(self) -> Iterable[tuple[str, Artifact]]:
        return self._artifacts.items()

    def values(self) -> Iterable[Artifact]:
        return self._artifacts.values()

    def __contains__(self, key: object) -> bool:
        return key in self._artifacts

    def __getitem__(self, key: str) -> Artifact:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Artifact) -> None:
        self.set(key, artifact)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        """
        Resolve an artifact's URI to an absolute host path.

        This does not mutate the Artifact; it only returns a resolved Path.
        """
        artifact = self.require(key) if required else self.get(key)
        if not artifact:
            return None
        if not self.tracker:
            raise RuntimeError(
                f"Cannot resolve path for {key!r}: no tracker attached to Coupler."
            )
        return Path(self.tracker.resolve_uri(artifact.container_uri))

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undocumented: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        Declare expected coupler outputs for runtime validation and documentation.

        This method enables early detection of missing outputs by validating at scenario
        exit time that all required outputs have been set. Can be used in conjunction
        with a schema, or independently for dynamic output declarations.

        Parameters
        ----------
        *names : str
            Output names to declare.
        required : bool or Mapping[str, bool], default False
            If bool: applies to all declared outputs.
            If Mapping: per-key required status (e.g., {"persons": True, "jobs": False}).
        warn_undocumented : bool, default False
            Warn when setting keys that were not declared.
        description : Mapping[str, str], optional
            Human-readable descriptions of outputs for documentation.

        Raises
        ------
        TypeError
            If any name is not a string.
        ValueError
            If any name is empty after stripping whitespace.

        Examples
        --------
        Declare all outputs as required:

        ```python
        coupler.declare_outputs("persons", "households", required=True)
        ```

        Mix required and optional:

        ```python
        coupler.declare_outputs(
            "persons", "households", "legacy_format",
            required={"persons": True, "households": True, "legacy_format": False}
        )
        ```

        With descriptions:

        ```python
        coupler.declare_outputs(
            "skims",
            description={"skims": "Zone-to-zone travel times in Zarr format"}
        )
        ```

        Notes
        -----
        Required status is "sticky": once an output is marked required, later
        declarations cannot downgrade it to optional.
        """
        if not names:
            return
        if warn_undocumented:
            self._warn_undocumented = True
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
            validate_artifact_key(key)
            entry = self._declared_outputs.get(key, DeclaredOutput())
            entry.required = entry.required or required_map.get(key, default_required)
            if key in description_map:
                entry.description = description_map[key]
                self._output_descriptions[key] = description_map[key]
            self._declared_outputs[key] = entry

    def missing_declared_outputs(self) -> list[str]:
        """
        Return required declared outputs that have not been set in the coupler.

        This checks only outputs declared via declare_outputs(), not schema keys.
        Use validate_all_schema_keys_set() to check schema-based validation.

        Returns
        -------
        list[str]
            Sorted list of required declared outputs that are missing.
        """
        return sorted(
            [
                key
                for key, entry in self._declared_outputs.items()
                if entry.required
                and (key not in self._artifacts or self._artifacts[key] is None)
            ]
        )

    def require_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = True,
        warn_undocumented: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        Declare required outputs with optional undocumented key warnings.
        """
        self.declare_outputs(
            *names,
            required=required,
            warn_undocumented=warn_undocumented,
            description=description,
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Artifact], *keys: str, prefix: str = ""
    ) -> Dict[str, Artifact]:
        """
        Collect explicit artifacts into the coupler by key.

        This method selectively ingests artifacts from a mapping, storing them under
        optionally-prefixed keys. Useful when a step returns many outputs but you
        only want specific ones, or when you need to namespace outputs by scenario/year.
        """
        if not isinstance(artifacts, Mapping):
            raise TypeError("collect_by_keys expects a mapping of artifacts.")
        collected: Dict[str, Artifact] = {}
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("collect_by_keys keys must be strings.")
            validate_artifact_key(key)
            if key not in artifacts:
                raise KeyError(f"Missing artifact for key {key!r}.")
            coupler_key = f"{prefix}{key}"
            artifact = artifacts[key]
            self.set(coupler_key, artifact)
            collected[coupler_key] = artifact
        return collected

    def describe_outputs(self) -> Dict[str, str]:
        """Return descriptions of declared outputs (for documentation/introspection)."""
        return dict(self._output_descriptions)


@dataclass
class DeclaredOutput:
    required: bool = False
    description: Optional[str] = None
