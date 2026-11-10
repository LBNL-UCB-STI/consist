from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, replace
import os
from pathlib import Path
from types import MappingProxyType
import inspect
import logging
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    Tuple,
)
import uuid

from consist.artifact import (
    Artifact as V2Artifact,
    EMPTY_META,
    FrozenMeta,
    MissingOutputError,
    read_local_df,
)
from consist.core._coupler_shared import CouplerMapMixin
from consist.core.coupler import CouplerView, DeclaredOutput
from consist.core.decorators import (
    get_callable_name,
    get_required_runtime_kwargs,
    get_step_definition,
)
from consist.core.output_sets import (
    build_output_set_manifest,
    discover_output_set_members,
)
from consist.core.run_invocation import (
    DeclaredOutputSpec,
    infer_path_annotated_parameters,
    normalize_declared_outputs_mapping,
    prepare_declared_output_targets,
)
from consist.core.validation import validate_artifact_key
from consist.models.artifact import Artifact as LegacyArtifact
from consist.protocols import TrackerLike
from consist.types import (
    ExecutionOptions,
    InputBindingMode,
    OutputPathRef,
    OutputPolicyOptions,
    OutputSet,
    RunCompletionArchivePolicy,
)

from consist.models.run import Run, CouplerSnapshotPhase
from consist.core.workflow import RestoredCouplerSnapshotResult


def _infer_local_ref_driver(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".zarr":
        return "zarr"
    ext = suffix.lstrip(".")
    if ext == "pq":
        return "parquet"
    if ext in {"h5", "hdf5"}:
        return "h5"
    if ext in {"nc", "netcdf"}:
        return "netcdf"
    return ext if ext else "file"


@dataclass(frozen=True, slots=True)
class LocalRef:
    """
    Lightweight, zero-I/O local path wrapper for noop mode (``NoopCoupler[LocalRef]``).

    Provides structural parity with ``Artifact`` (``.path``, ``.get_path()``,
    ``.as_path()``, ``.locate()``, ``.as_df()``, ``.with_meta()``, ``os.PathLike``)
    without reading or hashing bytes on disk at construction time.
    """

    path: Path
    key: Optional[str] = None
    driver: Optional[str] = None
    container_uri: Optional[str] = None
    table_path: Optional[str] = None
    array_path: Optional[str] = None
    meta: Mapping[str, Any] = field(default_factory=lambda: EMPTY_META)
    run_id: Optional[str] = None
    id: Optional[str] = None

    def __post_init__(self) -> None:
        coerced_path = (
            self.path if isinstance(self.path, Path) else Path(os.fspath(self.path))
        )
        object.__setattr__(self, "path", coerced_path)
        frozen_meta = FrozenMeta(self.meta) if self.meta else EMPTY_META
        object.__setattr__(self, "meta", frozen_meta)
        if self.container_uri is None:
            object.__setattr__(self, "container_uri", str(coerced_path))
        if self.driver is None:
            object.__setattr__(self, "driver", _infer_local_ref_driver(coerced_path))

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def get_path(self) -> Path:
        return self.path

    def as_path(self, tracker: Any = None) -> Path:
        del tracker
        return self.path

    def locate(
        self,
        *,
        ledger: Any = None,
        mounts: Any = None,
        verify: str = "exists",
    ) -> Optional[Path]:
        del ledger, mounts, verify
        return self.path

    def with_meta(self, **changes: Any) -> LocalRef:
        merged = dict(self.meta)
        merged.update(changes)
        return replace(self, meta=FrozenMeta(merged))

    def as_df(self, **kwargs: Any) -> Any:
        resolved = self.path
        if not resolved.exists():
            raise FileNotFoundError(f"LocalRef file not found at {resolved}")
        return read_local_df(
            resolved,
            driver=self.driver,
            member=self.table_path,
            default_h5_key="data",
            **kwargs,
        )

    @property
    def abs_path(self) -> Optional[str]:
        return str(self.path) if self.path is not None else None

    @property
    def tracker(self) -> None:
        return None

    def ensure_local_path(
        self,
        *,
        tracker: Any = None,
        target_root: Optional[Path | str] = None,
        source_root: Optional[Path | str] = None,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[Path]:
        del tracker, target_root, source_root
        if self.path is not None and self.path.exists():
            return self.path
        if on_missing == "raise":
            raise FileNotFoundError(
                f"Cannot ensure local path for noop artifact {self.key!r}: {self.path!r} does not exist."
            )
        logging.warning(
            "[Consist] Cannot ensure local path for noop artifact %r: %r does not exist.",
            self.key,
            self.path,
        )
        return None


NoopArtifact = LocalRef


@dataclass
class NoopRunResult:
    """
    Minimal RunResult-compatible payload for noop mode.
    """

    outputs: Dict[str, NoopArtifact] = field(default_factory=dict)
    cache_hit: bool = False
    run: Optional[Run] = None

    @property
    def output(self) -> Optional[NoopArtifact]:
        if not self.outputs:
            return None
        return next(iter(self.outputs.values()))

    def as_dict(self) -> Dict[str, NoopArtifact]:
        return dict(self.outputs)


class NoopCoupler(CouplerMapMixin[Any, DeclaredOutput]):
    """
    Coupler-compatible helper for noop mode (``NoopCoupler[LocalRef]``).
    """

    def __init__(self) -> None:
        self._artifacts: MutableMapping[str, Any] = {}
        self._declared_outputs: MutableMapping[str, DeclaredOutput] = {}
        self._warn_undefined = False
        self._undefined_keys: set[str] = set()
        self._output_descriptions: MutableMapping[str, str] = {}
        self._history: Dict[str, list[Any]] = {}

    def _validate_artifact_key(self, key: str) -> None:
        validate_artifact_key(key)

    def _coerce_artifact(self, key: str, artifact: Any) -> Any:
        if artifact is None:
            return None
        if isinstance(artifact, LocalRef):
            return artifact if artifact.key == key else replace(artifact, key=key)
        if isinstance(artifact, (V2Artifact, LegacyArtifact)):
            return artifact
        if isinstance(artifact, (str, Path, os.PathLike)):
            return LocalRef(path=Path(os.fspath(artifact)), key=key)
        raise TypeError(
            f"NoopCoupler.set({key!r}, ...) expected a LocalRef, Artifact, or path, "
            f"got {type(artifact).__name__}: {artifact!r}"
        )

    def _create_declared_output(self) -> DeclaredOutput:
        return DeclaredOutput()

    def _collect_validate_key(self) -> None:
        return None

    def set(self, key: str, artifact: Any) -> Any:
        self._validate_artifact_key(key)
        coerced = self._coerce_artifact(key, artifact)
        stored = self._store_artifact(key, coerced, validate_key=False)
        if stored is not None:
            self._history.setdefault(key, []).append(stored)
        return stored

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Set an artifact, accepting both Artifact objects and artifact-like values.

        This method is useful when integrating with optional dependencies where you may
        receive either real Artifacts or artifact-like objects with .path/.container_uri properties.

        All forms are stored in the coupler and can be retrieved with get() or require().
        """
        stored = self._store_artifact(key, value, validate_key=False)
        if (
            isinstance(stored, (LocalRef, V2Artifact, LegacyArtifact))
            and isinstance(key, str)
            and bool(key)
        ):
            self._history.setdefault(key, []).append(stored)
        return stored

    def view(self, namespace: str) -> CouplerView:
        """
        Return a namespace-scoped coupler view.

        Noop mode uses the same namespacing behavior as real couplers so
        workflow code can remain identical across enabled/disabled tracking.
        """
        return CouplerView(self, namespace)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        artifact = self.require(key) if required else self.get(key)
        if artifact is None:
            return None
        if isinstance(artifact, LocalRef):
            return artifact.path
        if isinstance(artifact, V2Artifact):
            located = artifact.locate()
            return located if located is not None else artifact.path
        if isinstance(artifact, LegacyArtifact):
            return artifact.path
        if isinstance(artifact, (Path, str, os.PathLike)):
            return Path(os.fspath(artifact))
        try:
            return Path(artifact.get_path())
        except AttributeError:
            pass
        try:
            if artifact.path is not None:
                return Path(artifact.path)
        except AttributeError:
            pass
        try:
            if artifact.container_uri is not None:
                return Path(artifact.container_uri)
        except AttributeError:
            pass
        return None


class NoopRunContext:
    """
    Minimal run context for noop mode that returns artifact placeholders.
    """

    def __init__(self) -> None:
        self._artifacts: Dict[str, NoopArtifact] = {}

    def log_artifact(
        self,
        path: Any,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: str = "same_uri",
        facet: Optional[Any] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> NoopArtifact:
        _ = content_hash
        _ = force_hash_override
        _ = validate_content_hash
        _ = reuse_if_unchanged
        _ = reuse_scope
        _ = facet
        _ = facet_schema_version
        _ = facet_index
        if key is None:
            if isinstance(path, NoopArtifact) and path.key is not None:
                key = path.key
            else:
                key = Path(path).stem if path is not None else "artifact"
        artifact = _build_noop_artifact(path, key=key, meta=meta)
        self._artifacts[key] = artifact
        return artifact

    def log_input(
        self, path: Any, key: Optional[str] = None, **meta: Any
    ) -> NoopArtifact:
        return self.log_artifact(path, key=key, **meta)

    def log_output(
        self, path: Any, key: Optional[str] = None, **meta: Any
    ) -> NoopArtifact:
        return self.log_artifact(path, key=key, **meta)

    def log_artifacts(
        self,
        outputs: Mapping[str, Any],
        facets_by_key: Optional[Mapping[str, Any]] = None,
        facet_schema_versions_by_key: Optional[Mapping[str, Union[str, int]]] = None,
        facet_index: bool = False,
        **meta: Any,
    ) -> Dict[str, NoopArtifact]:
        _ = facet_index
        artifacts: Dict[str, NoopArtifact] = {}
        for key, value in outputs.items():
            artifacts[key] = self.log_artifact(
                value,
                key=key,
                facet=facets_by_key.get(key) if facets_by_key else None,
                facet_schema_version=(
                    facet_schema_versions_by_key.get(key)
                    if facet_schema_versions_by_key
                    else None
                ),
                **meta,
            )
        return artifacts

    def set_output_meta(
        self,
        key: str,
        meta: Mapping[str, Any],
        *,
        merge: bool = True,
    ) -> None:
        _ = (key, meta, merge)
        return None

    set_pending_output_meta = set_output_meta


class NoopScenarioContext:
    """
    Scenario-like context that executes without provenance tracking.
    """

    def __init__(
        self,
        name: str,
        *,
        step_run_completion_archive: Optional[
            Union[
                RunCompletionArchivePolicy,
                Any,
            ]
        ] = None,
        resume_from_run_id: Optional[str] = None,
        resume_phase: CouplerSnapshotPhase = "post",
        **kwargs: Any,
    ) -> None:
        if (
            step_run_completion_archive is not None
            and not isinstance(step_run_completion_archive, RunCompletionArchivePolicy)
            and not callable(step_run_completion_archive)
        ):
            raise TypeError(
                "step_run_completion_archive must be a RunCompletionArchivePolicy, "
                "a callable returning RunCompletionArchivePolicy | None, or None."
            )
        self.name = name
        self.step_run_completion_archive = step_run_completion_archive
        self.resume_from_run_id = resume_from_run_id
        self.resume_phase = resume_phase
        self.kwargs = kwargs
        self.coupler = NoopCoupler()
        self._inputs: Dict[str, NoopArtifact] = {}
        self._output_base_dir = Path(kwargs.get("output_base_dir") or Path.cwd())

    @property
    def run_id(self) -> str:
        return self.name

    @property
    def inputs(self) -> Mapping[str, NoopArtifact]:
        return dict(self._inputs)

    def add_input(self, path: Any, key: str, **kwargs: Any) -> NoopArtifact:
        artifact = _build_noop_artifact(path, key=key, meta=kwargs)
        self._inputs[key] = artifact
        self.coupler.set(key, artifact)
        return artifact

    @property
    def meta(self) -> Mapping[str, Any]:
        return MappingProxyType({})

    def log_meta(self, **metadata: Any) -> None:
        _ = metadata
        return None

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.coupler.declare_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def require_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = True,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.coupler.require_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Any], *keys: str, prefix: str = ""
    ) -> Dict[str, Any]:
        return self.coupler.collect_by_keys(artifacts, *keys, prefix=prefix)

    def run(
        self,
        fn: Optional[Any] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]] = None,
        outputs: Optional[Union[Sequence[str], Mapping[str, OutputPathRef]]] = None,
        output_paths: Optional[Mapping[str, Any]] = None,
        output_sets: Optional[Mapping[str, OutputSet]] = None,
        output_policy: Optional[OutputPolicyOptions] = None,
        execution_options: Optional[ExecutionOptions] = None,
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str | None = None,
        **kwargs: Any,
    ) -> NoopRunResult:
        resolved_runtime_kwargs_mapping = (
            runtime_kwargs
            if runtime_kwargs is not None
            else (
                execution_options.runtime_kwargs
                if execution_options is not None
                else None
            )
        )
        resolved_runtime_kwargs = (
            dict(resolved_runtime_kwargs_mapping)
            if resolved_runtime_kwargs_mapping is not None
            else None
        )
        resolved_inject_context_value = (
            inject_context
            if inject_context is not None
            else (
                execution_options.inject_context
                if execution_options is not None
                else False
            )
        )
        resolved_inject_context: bool | str = (
            resolved_inject_context_value
            if resolved_inject_context_value is not None
            else False
        )
        step_def = get_step_definition(fn)
        effective_inputs = inputs if inputs is not None else step_def.inputs
        resolved_inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]] = (
            effective_inputs
        )
        if isinstance(effective_inputs, Mapping):
            resolved_map: Dict[str, Any] = {}
            for k, v in effective_inputs.items():
                if isinstance(v, str) and v in self.coupler:
                    resolved_map[str(k)] = self.coupler.require(v)
                elif isinstance(v, str) and v in self._inputs:
                    resolved_map[str(k)] = self._inputs[v]
                else:
                    resolved_map[str(k)] = v
            resolved_inputs = resolved_map
        elif isinstance(effective_inputs, Iterable) and not isinstance(
            effective_inputs, (str, bytes)
        ):
            items = list(effective_inputs)
            if items and all(isinstance(item, str) for item in items):
                promoted: Dict[str, Any] = {}
                for item in items:
                    key_str = str(item)
                    leaf = key_str.rsplit("/", 1)[-1]
                    if key_str in self.coupler:
                        promoted[leaf] = self.coupler.require(key_str)
                    elif key_str in self._inputs:
                        promoted[leaf] = self._inputs[key_str]
                    else:
                        promoted[leaf] = Path(key_str)
                resolved_inputs = promoted
        return _run_noop_step(
            fn=fn,
            name=name,
            run_id=run_id,
            config=config,
            inputs=resolved_inputs,
            outputs=outputs,
            output_paths=output_paths,
            output_sets=output_sets,
            output_policy=output_policy,
            execution_options=execution_options,
            runtime_kwargs=resolved_runtime_kwargs,
            inject_context=resolved_inject_context,
            default_name=self.name,
            on_outputs=self.coupler.update,
            output_base_dir=self._output_base_dir,
        )

    @contextmanager
    def trace(self, name: str, **kwargs: Any) -> Iterator[NoopRunContext]:
        yield NoopRunContext()

    def capture_coupler_snapshot(
        self,
        phase: CouplerSnapshotPhase = "post",
        run_id: Optional[str] = None,
        step_ordinal: Optional[int] = None,
    ) -> None:
        """Noop implementation of capture_coupler_snapshot."""
        return None

    def restore_coupler_snapshot(
        self,
        run_id: Optional[str] = None,
        *,
        scenario_run_id: Optional[str] = None,
        phase: CouplerSnapshotPhase = "post",
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
        clear_existing: bool = False,
        fallback_to_completed_outputs: bool = True,
        target_root: Optional[Union[str, Path]] = None,
        source_root: Optional[Union[str, Path]] = None,
        remappable_schemes: Optional[
            Union[Iterable[str], Mapping[str, Union[str, Path]]]
        ] = None,
        prune_unrecoverable: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "never",
    ) -> RestoredCouplerSnapshotResult:
        """Noop implementation of restore_coupler_snapshot."""
        _ = (
            run_id,
            scenario_run_id,
            phase,
            year,
            iteration,
            stage,
            model_name,
            before_step_ordinal,
            fallback_to_completed_outputs,
            target_root,
            source_root,
            remappable_schemes,
            prune_unrecoverable,
            validate_content_hash,
        )
        if clear_existing:
            self.coupler.clear()
        return RestoredCouplerSnapshotResult()

    def find_latest_coupler_snapshot(
        self,
        *,
        scenario_run_id: Optional[str] = None,
        phase: Optional[CouplerSnapshotPhase] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
    ) -> Optional[Tuple[str, int, Dict[str, Any]]]:
        """Noop implementation of find_latest_coupler_snapshot."""
        return None


class NoopTracker(TrackerLike):
    """
    Tracker-compatible noop implementation with parity-focused validation.
    """

    def __init__(self, *, output_base_dir: Optional[Path] = None) -> None:
        self._warned = False
        self._output_base_dir = output_base_dir or Path.cwd()

    def _warn_disabled(self) -> None:
        if not self._warned:
            logging.warning(
                "[Consist] Provenance tracking disabled; using noop tracker."
            )
            self._warned = True

    def log_output(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> NoopArtifact:
        self._warn_disabled()
        return NoopRunContext().log_output(path, key=key, **metadata)

    def log_input(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> NoopArtifact:
        self._warn_disabled()
        return NoopRunContext().log_input(path, key=key, **metadata)

    def log_artifacts(
        self, outputs: Mapping[str, Any], **metadata: Any
    ) -> Dict[str, NoopArtifact]:
        self._warn_disabled()
        return NoopRunContext().log_artifacts(outputs, **metadata)

    def set_output_meta(
        self,
        key: str,
        meta: Mapping[str, Any],
        *,
        merge: bool = True,
    ) -> None:
        self._warn_disabled()
        _ = (key, meta, merge)
        return None

    set_pending_output_meta = set_output_meta

    @contextmanager
    def scenario(self, name: str, **kwargs: Any) -> Iterator[NoopScenarioContext]:
        self._warn_disabled()
        yield NoopScenarioContext(name, **kwargs)

    def run(
        self,
        fn: Optional[Any] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]] = None,
        outputs: Optional[Union[Sequence[str], Mapping[str, OutputPathRef]]] = None,
        output_paths: Optional[Mapping[str, Any]] = None,
        output_sets: Optional[Mapping[str, OutputSet]] = None,
        output_policy: Optional[OutputPolicyOptions] = None,
        execution_options: Optional[ExecutionOptions] = None,
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str | None = None,
        **kwargs: Any,
    ) -> NoopRunResult:
        self._warn_disabled()
        resolved_runtime_kwargs_mapping = (
            runtime_kwargs
            if runtime_kwargs is not None
            else (
                execution_options.runtime_kwargs
                if execution_options is not None
                else None
            )
        )
        resolved_runtime_kwargs = (
            dict(resolved_runtime_kwargs_mapping)
            if resolved_runtime_kwargs_mapping is not None
            else None
        )
        resolved_inject_context_value = (
            inject_context
            if inject_context is not None
            else (
                execution_options.inject_context
                if execution_options is not None
                else False
            )
        )
        resolved_inject_context: bool | str = (
            resolved_inject_context_value
            if resolved_inject_context_value is not None
            else False
        )
        return _run_noop_step(
            fn=fn,
            name=name,
            run_id=run_id,
            config=config,
            inputs=inputs,
            outputs=outputs,
            output_paths=output_paths,
            output_sets=output_sets,
            output_policy=output_policy,
            execution_options=execution_options,
            runtime_kwargs=resolved_runtime_kwargs,
            inject_context=resolved_inject_context,
            output_base_dir=self._output_base_dir,
        )

    def get_coupler_snapshot(
        self,
        run_id: str,
        phase: CouplerSnapshotPhase = "post",
        *,
        must_exist: bool = False,
    ) -> Dict[str, Any]:
        self._warn_disabled()
        if must_exist:
            raise KeyError(f"No snapshot found for run {run_id}")
        return {}

    def diff_coupler_snapshots(
        self,
        left_run_id: str,
        right_run_id: str,
        *,
        left_phase: CouplerSnapshotPhase = "post",
        right_phase: CouplerSnapshotPhase = "post",
    ) -> Any:
        self._warn_disabled()
        import pandas as pd

        return pd.DataFrame(
            columns=[
                "role_key",
                "status",
                "left_artifact_id",
                "right_artifact_id",
                "left_hash",
                "right_hash",
                "left_producer_run_id",
                "right_producer_run_id",
            ]
        )


def _build_noop_artifact(
    value: Any, *, key: str, meta: Optional[Dict[str, Any]] = None
) -> NoopArtifact:
    validate_artifact_key(key)
    if isinstance(value, LocalRef):
        if value.key == key and not meta:
            return value
        merged_meta = dict(value.meta)
        if meta:
            merged_meta.update(meta)
        return replace(value, key=key, meta=merged_meta)
    if isinstance(value, LegacyArtifact):
        path = value.path
        return LocalRef(
            key=key,
            path=path,
            container_uri=str(path),
            meta=dict(meta or {}),
            driver=value.driver,
            table_path=value.table_path,
            array_path=value.array_path,
        )
    if isinstance(value, V2Artifact):
        located = value.locate()
        path = located if located is not None else value.path
        return LocalRef(
            key=key,
            path=path,
            container_uri=str(path),
            meta=dict(meta or {}),
            driver=value.driver,
        )
    table_path: Optional[str] = None
    array_path: Optional[str] = None
    path_value: Any = None
    try:
        table_path = value.table_path
    except AttributeError:
        pass
    try:
        array_path = value.array_path
    except AttributeError:
        pass
    try:
        path_value = value.path
    except AttributeError:
        pass
    if path_value is not None:
        try:
            path = Path(path_value)
        except Exception:
            path = None
        if path is not None:
            return LocalRef(
                key=key,
                path=path,
                container_uri=str(path),
                meta=dict(meta or {}),
                table_path=table_path,
                array_path=array_path,
            )
    path = Path(value) if value is not None else Path(key)
    return LocalRef(
        key=key,
        path=path,
        container_uri=str(path),
        meta=dict(meta or {}),
        table_path=table_path,
        array_path=array_path,
    )


def _extract_input_path(value: Any) -> Path:
    if isinstance(value, LocalRef):
        return value.path
    if isinstance(value, V2Artifact):
        located = value.locate()
        return located if located is not None else value.path
    if isinstance(value, LegacyArtifact):
        return value.path
    if isinstance(value, (Path, str, os.PathLike)):
        return Path(os.fspath(value))
    try:
        return Path(value.get_path())
    except AttributeError:
        pass
    try:
        return Path(value.path)
    except AttributeError as exc:
        raise TypeError(
            f"Cannot resolve input path from {type(value).__name__}: {value!r}"
        ) from exc


def _load_noop_input_value(value: Any) -> Any:
    if isinstance(value, (LocalRef, V2Artifact, LegacyArtifact)):
        return value.as_df()
    if isinstance(value, (Path, str, os.PathLike)):
        return LocalRef(path=Path(os.fspath(value))).as_df()
    return value


def _run_noop_step(
    *,
    fn: Optional[Any],
    name: Optional[str],
    run_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]] = None,
    outputs: Optional[Union[Sequence[str], Mapping[str, OutputPathRef]]] = None,
    output_paths: Optional[Mapping[str, Any]] = None,
    output_sets: Optional[Mapping[str, OutputSet]] = None,
    output_policy: Optional[OutputPolicyOptions] = None,
    execution_options: Optional[ExecutionOptions] = None,
    runtime_kwargs: Optional[Dict[str, Any]] = None,
    inject_context: bool | str = False,
    default_name: Optional[str] = None,
    on_outputs: Optional[Any] = None,
    output_base_dir: Optional[Path] = None,
) -> NoopRunResult:
    if fn is None and name is None:
        raise ValueError("Noop run requires name when fn is None.")
    fn_name = get_callable_name(fn)
    step_def = get_step_definition(fn)

    resolved_name = name or fn_name or "run"
    base = default_name or resolved_name
    resolved_run_id = run_id or f"{base}_{resolved_name}_{uuid.uuid4().hex[:8]}"
    base_dir = output_base_dir or Path.cwd()

    effective_outputs: Any = outputs if outputs is not None else step_def.outputs
    effective_output_paths: Optional[Mapping[str, Any]] = (
        output_paths if output_paths is not None else step_def.output_paths
    )
    effective_inputs: Any = inputs if inputs is not None else step_def.inputs
    effective_config: Dict[str, Any] = dict(
        config if config is not None else (step_def.config or {})
    )

    declared_output_specs: Optional[Dict[str, DeclaredOutputSpec]] = None
    outputs_list: Optional[List[str]] = None
    if isinstance(effective_outputs, Mapping):
        if effective_output_paths is not None:
            raise ValueError(
                "Cannot specify both outputs={...} mapping and output_paths={...}."
            )
        declared_output_specs = normalize_declared_outputs_mapping(effective_outputs)
        outputs_list = list(declared_output_specs.keys())
    elif effective_outputs is not None:
        outputs_list = list(effective_outputs)

    effective_output_missing: str = (
        output_policy.output_missing
        if output_policy is not None and output_policy.output_missing is not None
        else ("error" if declared_output_specs is not None else "warn")
    )

    explicit_input_binding: Optional[InputBindingMode] = (
        execution_options.input_binding
        if execution_options is not None and execution_options.input_binding is not None
        else step_def.input_binding
    )
    explicit_load_inputs: Optional[bool] = (
        execution_options.load_inputs
        if execution_options is not None and execution_options.load_inputs is not None
        else step_def.load_inputs
    )
    explicit_binding_requested = (
        explicit_input_binding is not None or explicit_load_inputs is not None
    )
    effective_input_binding: InputBindingMode
    if explicit_input_binding is not None:
        effective_input_binding = explicit_input_binding
    elif explicit_load_inputs is True:
        effective_input_binding = "loaded"
    elif explicit_load_inputs is False:
        effective_input_binding = "none"
    elif isinstance(effective_inputs, Mapping):
        effective_input_binding = "loaded"
    else:
        effective_input_binding = "none"

    call_kwargs: Dict[str, Any] = dict(runtime_kwargs or {})
    required_runtime: Tuple[str, ...] = get_required_runtime_kwargs(fn)
    if required_runtime:
        missing = [
            req_name for req_name in required_runtime if req_name not in call_kwargs
        ]
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(
                f"Missing runtime_kwargs for {resolved_name!r}: {missing_list}. "
                "Provide them via execution_options=ExecutionOptions(runtime_kwargs={...}) "
                "or runtime_kwargs={...}, or remove "
                "@consist.require_runtime_kwargs."
            )

    declared_target_paths: Dict[str, Path] = (
        prepare_declared_output_targets(
            declared_output_specs,
            default_base_dir=base_dir,
        )
        if declared_output_specs is not None
        else {}
    )

    if fn is not None:
        sig = inspect.signature(fn)
        params = sig.parameters
        has_var_kw = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
        path_annotated_params = infer_path_annotated_parameters(fn)

        if "config" in params and "config" not in call_kwargs and effective_config:
            call_kwargs["config"] = effective_config

        for param_name, param in params.items():
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            if param_name in call_kwargs:
                continue
            if (
                effective_input_binding != "none"
                and isinstance(effective_inputs, Mapping)
                and param_name in effective_inputs
            ):
                raw_in = effective_inputs[param_name]
                use_paths = effective_input_binding == "paths" or (
                    not explicit_binding_requested
                    and param_name in path_annotated_params
                )
                if use_paths:
                    in_path = _extract_input_path(raw_in)
                    if not in_path.exists():
                        raise ValueError(
                            f"input_binding='paths' requires a materialized local path for input {param_name!r}."
                        )
                    call_kwargs[param_name] = in_path
                else:
                    call_kwargs[param_name] = _load_noop_input_value(raw_in)
                continue
            if param_name in declared_target_paths:
                call_kwargs[param_name] = declared_target_paths[param_name]
                continue
            if param_name in effective_config:
                call_kwargs[param_name] = effective_config[param_name]

        if has_var_kw and declared_target_paths:
            for role, target_path in declared_target_paths.items():
                if role not in call_kwargs:
                    call_kwargs[role] = target_path

        if inject_context:
            ctx_name = (
                inject_context if isinstance(inject_context, str) else "_consist_ctx"
            )
            if ctx_name in params or has_var_kw:
                call_kwargs[ctx_name] = NoopRunContext()
            else:
                raise ValueError(
                    f"inject_context requested '{ctx_name}', but fn does not accept it."
                )
        try:
            sig.bind_partial(**call_kwargs)
        except TypeError as exc:
            raise TypeError(
                f"Noop run could not bind arguments for {resolved_name!r}: {exc}"
            ) from exc

    result = None
    if fn is not None:
        result = fn(**call_kwargs)

    if declared_output_specs is not None and result is not None:
        raise ValueError(
            f"Run {resolved_name!r} declared outputs={{role: path}} mapping and must return None, "
            f"got {type(result).__name__}."
        )

    outputs_map: Dict[str, NoopArtifact] = {}
    if declared_output_specs is not None:
        for role, spec in declared_output_specs.items():
            target_path = declared_target_paths[role]
            if not target_path.exists():
                if effective_output_missing == "error":
                    raise MissingOutputError(role=role, path=target_path)
                if effective_output_missing == "warn":
                    logging.warning(
                        "[Consist] Declared output %r was not produced at %s.",
                        role,
                        target_path,
                    )
                continue
            outputs_map[role] = _build_noop_artifact(target_path, key=role)

    if effective_output_paths:
        for key, ref in effective_output_paths.items():
            outputs_map[str(key)] = _build_noop_artifact(ref, key=str(key))

    if outputs_list and declared_output_specs is None and result is not None:
        if isinstance(result, Mapping):
            for key, value in result.items():
                outputs_map[str(key)] = _build_noop_artifact(value, key=str(key))
        elif isinstance(result, (list, tuple)) and len(result) == len(outputs_list):
            for key, value in zip(outputs_list, result):
                outputs_map[str(key)] = _build_noop_artifact(value, key=str(key))
        else:
            if len(outputs_list) == 1:
                outputs_map[str(outputs_list[0])] = _build_noop_artifact(
                    result, key=str(outputs_list[0])
                )
    elif (
        outputs_list is None
        and declared_output_specs is None
        and effective_output_paths is None
        and result is not None
    ):
        default_key = (
            fn_name
            if isinstance(fn_name, str) and fn_name and fn_name != "<lambda>"
            else resolved_name
        )
        if isinstance(result, (NoopArtifact, Path)):
            outputs_map[default_key] = _build_noop_artifact(result, key=default_key)
        elif isinstance(result, Mapping):
            inferred: Dict[str, NoopArtifact] = {}
            for key, value in result.items():
                if not isinstance(key, str):
                    inferred = {}
                    break
                if not isinstance(value, (NoopArtifact, Path)):
                    inferred = {}
                    break
                inferred[key] = _build_noop_artifact(value, key=key)
            outputs_map.update(inferred)

    if output_sets:
        for key, output_set in output_sets.items():
            root_path = _resolve_noop_output_path(output_set.root, base_dir)
            resolved_output_set = replace(output_set, root=root_path)
            members = discover_output_set_members(resolved_output_set)
            outputs_map[str(key)] = _build_noop_artifact(
                root_path,
                key=str(key),
                meta={
                    "artifact_set": True,
                    "output_set_key": str(key),
                    "output_set_manifest": build_output_set_manifest(
                        key=str(key),
                        output_set=resolved_output_set,
                        members=members,
                        config={},
                        logged_members=[],
                    ),
                },
            )

    if outputs_map and on_outputs is not None:
        on_outputs(outputs_map)

    return NoopRunResult(
        run=Run(
            id=resolved_run_id,
            model_name=resolved_name,
            config_hash=None,
            git_hash=None,
            status="completed",
        ),
        outputs=outputs_map,
        cache_hit=False,
    )


def _resolve_noop_output_path(ref: Any, base_dir: Path) -> Path:
    if isinstance(ref, LocalRef):
        return ref.path
    if isinstance(ref, LegacyArtifact):
        return ref.path
    if isinstance(ref, V2Artifact):
        located = ref.locate()
        return located if located is not None else ref.path
    try:
        ref_path = ref.path
    except AttributeError:
        ref_path = None
    if ref_path is not None:
        return Path(ref_path)
    ref_str = str(ref)
    if isinstance(ref, str) and "://" in ref_str:
        return Path(ref_str)
    candidate = Path(ref_str)
    if not candidate.is_absolute():
        return base_dir / candidate
    return candidate


__all__ = [
    "LocalRef",
    "NoopArtifact",
    "NoopCoupler",
    "NoopRunContext",
    "NoopRunResult",
    "NoopScenarioContext",
    "NoopTracker",
]
