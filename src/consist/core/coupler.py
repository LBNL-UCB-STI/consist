from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, TYPE_CHECKING, TypeVar, cast

from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class Coupler:
    """
    Scenario-local helper to thread named artifacts between steps.

    Coupler is intentionally small:
    - It stores the "latest Artifact for a semantic key" in-memory.
    - It can "adopt" a cached output from the current step (when a cache hit occurred).

    It does not log artifacts, infer inputs/outputs, or mutate Artifacts as a side effect
    of reads. Keep provenance operations on the Tracker.
    """

    def __init__(self, tracker: Optional["Tracker"] = None) -> None:
        self.tracker = tracker
        self._artifacts: Dict[str, Artifact] = {}
        self._declared_outputs: Dict[str, DeclaredOutput] = {}

    def set(self, key: str, artifact: Artifact) -> Artifact:
        self._artifacts[key] = artifact
        return artifact

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
            self._artifacts.update(artifacts)
        if kwargs:
            self._artifacts.update(kwargs)

    def get(self, key: str) -> Optional[Artifact]:
        """Return the current artifact for `key`, or None if unset."""
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

    def pop(self, key: str, default: Optional[Artifact] = None) -> Optional[Artifact]:
        return self._artifacts.pop(key, default)

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
        return Path(self.tracker.resolve_uri(artifact.uri))

    def adopt_cached_output(self, key: Optional[str] = None) -> Optional[Artifact]:
        """
        If the *current step run* is a cache hit, adopt a hydrated cached output.

        This is a thin wrapper around `tracker.cached_output()`. When an artifact is found,
        it is stored into the Coupler under:
        - `key` if provided
        - otherwise `artifact.key`
        """
        if not self.tracker:
            return None
        artifact = self.tracker.cached_output(key=key)
        if artifact:
            self.set(key or artifact.key, artifact)
        return artifact

    def get_cached(self, key: Optional[str] = None) -> Optional[Artifact]:
        """Alias for `adopt_cached_output()` (kept for backwards compatibility)."""
        return self.adopt_cached_output(key=key)

    def get_cached_output(self, key: Optional[str] = None) -> Optional[Artifact]:
        """Alias for `adopt_cached_output()` (kept for backwards compatibility)."""
        return self.adopt_cached_output(key=key)

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        Declare expected coupler outputs for runtime validation and documentation.

        This method enables early detection of missing outputs by validating at scenario
        exit time that all required outputs have been set. Optional outputs are tracked
        but not validated.

        Parameters
        ----------
        *names : str
            Output names to declare.
        required : bool or Mapping[str, bool], default False
            If bool: applies to all declared outputs.
            If Mapping: per-key required status (e.g., {"persons": True, "jobs": False}).
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
        """
        if not names:
            return
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
            entry = self._declared_outputs.get(key, DeclaredOutput())
            entry.required = entry.required or required_map.get(key, default_required)
            if key in description_map:
                entry.description = description_map[key]
            self._declared_outputs[key] = entry

    def missing_declared_outputs(self) -> list[str]:
        """
        Return required declared outputs that have not been set in the coupler.
        """
        return sorted(
            [
                key
                for key, entry in self._declared_outputs.items()
                if entry.required and key not in self._artifacts
            ]
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Artifact], *keys: str, prefix: str = ""
    ) -> Dict[str, Artifact]:
        """
        Collect explicit artifacts into the coupler by key.

        This method selectively ingests artifacts from a mapping, storing them under
        optionally-prefixed keys. Useful when a step returns many outputs but you
        only want specific ones, or when you need to namespace outputs by scenario/year.

        Parameters
        ----------
        artifacts : Mapping[str, Artifact]
            Mapping of available artifacts (e.g., from a step's outputs dict).
        *keys : str
            Keys to collect from the mapping.
        prefix : str, default ""
            Prefix to prepend to coupler keys (e.g., "2030_" -> "2030_persons").

        Returns
        -------
        Dict[str, Artifact]
            Mapping of (prefixed) keys -> ingested artifacts.

        Raises
        ------
        TypeError
            If artifacts is not a mapping or if any key is not a string.
        KeyError
            If any requested key is not present in artifacts.

        Examples
        --------
        Collect specific outputs from a subprocess result:
        ```python
        result = sc.run("step", fn=some_func)  # returns RunResult with outputs dict
        sc.collect_by_keys(result.outputs, "persons", "households")
        # Now coupler.get("persons") and coupler.get("households") are available
        ```

        With year prefix (scenario/temporal namespacing):
        ```python
        for year in [2020, 2030, 2040]:
            result = sc.run(f"forecast_{year}", fn=forecast_fn, year=year)
            sc.collect_by_keys(result.outputs, "skims", "population", prefix=f"{year}_")
        # coupler contains: "2020_skims", "2020_population", "2030_skims", etc.
        ```
        """
        if not isinstance(artifacts, Mapping):
            raise TypeError("collect_by_keys expects a mapping of artifacts.")
        collected: Dict[str, Artifact] = {}
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("collect_by_keys keys must be strings.")
            if key not in artifacts:
                raise KeyError(f"Missing artifact for key {key!r}.")
            coupler_key = f"{prefix}{key}"
            artifact = artifacts[key]
            self.set(coupler_key, artifact)
            collected[coupler_key] = artifact
        return collected


@dataclass
class DeclaredOutput:
    required: bool = False
    description: Optional[str] = None


class CouplerSchemaBase:
    """
    Runtime wrapper that exposes a Coupler with attribute-style access.

    This base class is used by the `coupler_schema` decorator to provide typed,
    attribute-based access to coupler artifacts. It is not typically instantiated
    directly; instead, use the decorator on a class with type annotations.

    Attributes
    ----------
    coupler : Coupler
        The underlying Coupler instance (accessible via the `coupler` property).

    Methods
    -------
    get(key) : Optional[Artifact]
        Return artifact for key, or None if unset.
    require(key) : Artifact
        Return artifact for key, raising KeyError if unset.
    set(key, artifact) : Artifact
        Set artifact for key.
    update(artifacts, **kwargs)
        Bulk update the coupler.

    Examples
    --------
    Typically not used directly; instantiate decorated classes instead:
    ```python
    @coupler_schema
    class MyOutputs:
        data: Artifact

    schema = MyOutputs(coupler)
    schema.data = some_artifact  # Type-safe access
    ```
    """

    def __init__(self, coupler: Coupler) -> None:
        self._coupler = coupler

    @property
    def coupler(self) -> Coupler:
        """Access the underlying Coupler instance."""
        return self._coupler

    def get(self, key: str) -> Optional[Artifact]:
        """Return artifact for key, or None if unset."""
        return self._coupler.get(key)

    def require(self, key: str) -> Artifact:
        """Return artifact for key, raising KeyError if unset."""
        return self._coupler.require(key)

    def set(self, key: str, artifact: Artifact) -> Artifact:
        """Set artifact for key."""
        return self._coupler.set(key, artifact)

    def update(
        self, artifacts: Optional[Dict[str, Artifact]] = None, /, **kwargs: Artifact
    ) -> None:
        """Bulk update the coupler."""
        self._coupler.update(artifacts, **kwargs)


SchemaT = TypeVar("SchemaT", bound=CouplerSchemaBase)


def coupler_schema(cls: type[SchemaT]) -> type[SchemaT]:
    """
    Decorator that turns an annotated class into a typed Coupler view.

    Transforms an annotated class into a runtime wrapper that provides type-safe,
    attribute-style access to Coupler artifacts. Each annotation becomes a read/write
    property that calls `coupler.require()` on access (getter) and `coupler.set()`
    on assignment (setter).

    The decorator is opt-in and purely additive:
    - Does not affect existing Coupler behavior
    - No changes needed to internal caching or artifact tracking
    - IDE provides autocomplete and type hints for artifact access
    - Runtime errors are descriptive ("missing key X, available: Y, Z")

    Parameters
    ----------
    cls : type
        A class with type annotations. Each annotation name becomes a property.
        (Private fields starting with "_" are skipped.)

    Returns
    -------
    type
        A new class inheriting from CouplerSchemaBase with typed properties.

    Raises
    ------
    ValueError
        If the class has no annotations.

    Examples
    --------
    Basic usage with type-safe properties:
    ```python
    from consist import coupler_schema
    from consist.models.artifact import Artifact

    @coupler_schema
    class MyWorkflowOutputs:
        persons: Artifact
        households: Artifact
        jobs: Artifact

    # In a scenario context:
    typed = sc.coupler_schema(MyWorkflowOutputs)
    typed.persons = artifact1  # type-safe set
    retrieved = typed.persons  # type-safe get, raises KeyError if not set
    ```

    With docstrings for documentation:
    ```python
    @coupler_schema
    class ActivitySimOutputs:
        \"\"\"Outputs from ActivitySim model run.\"\"\"
        persons: Artifact  # Person records with activity schedules
        households: Artifact  # Household attributes and linkages
        skims: Artifact  # Zone-to-zone travel time skims in Zarr format

    typed = sc.coupler_schema(ActivitySimOutputs)
    # All properties enforce correct access via coupler.require(key)
    ```

    Attribute access automatically validates:
    ```python
    @coupler_schema
    class Outputs:
        data: Artifact

    typed = sc.coupler_schema(Outputs)
    # typed.data raises KeyError with helpful message if not set
    # KeyError: Coupler missing key='data'. Available keys: ...
    ```
    """
    annotations: Dict[str, Any] = dict(getattr(cls, "__annotations__", {}) or {})
    if not annotations:
        raise ValueError("coupler_schema requires annotated fields.")

    attrs: Dict[str, Any] = {}

    def _make_property(key: str):
        def getter(self) -> Artifact:
            return self._coupler.require(key)

        def setter(self, artifact: Artifact) -> None:
            self._coupler.set(key, artifact)

        return property(getter, setter)

    for key in annotations:
        if key.startswith("_"):
            continue
        attrs[key] = _make_property(key)

    def __init__(self, coupler: Coupler) -> None:
        CouplerSchemaBase.__init__(self, coupler)

    attrs["__init__"] = __init__
    attrs["__annotations__"] = annotations
    schema_cls = type(cls.__name__, (cls, CouplerSchemaBase), attrs)
    schema_cls.__doc__ = cls.__doc__
    return cast(type[SchemaT], schema_cls)
