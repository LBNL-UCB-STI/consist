"""Internal orchestration for tracker function-style execution paths.

This module centralizes ``Tracker.run`` and ``Tracker.trace`` control flow:
option normalization, metadata resolution, run context setup, callable/container
execution, output handling, and cache-hit behavior. Public API remains on
``Tracker``; this module exists to reduce tracker-file complexity.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping as MappingABC
from contextlib import contextmanager
from dataclasses import dataclass
import inspect
import logging
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Union,
    cast,
)
import uuid
import warnings

import pandas as pd
from pydantic import BaseModel

from consist.core.error_messages import format_problem_cause_fix
from consist.core.run_invocation import resolve_run_invocation
from consist.core.run_options import resolve_runtime_kwargs_alias
from consist.core.workflow import RunContext
from consist.models.artifact import Artifact, get_tracker_ref, set_tracker_ref
from consist.models.run import RunResult
from consist.types import (
    ArtifactRef,
    CacheOptions,
    CodeIdentityMode,
    DriverType,
    ExecutionOptions,
    FacetLike,
    InputBindingMode,
    IdentityInputs,
    OutputPolicyOptions,
    RunInputRef,
)

if TYPE_CHECKING:
    from consist.core.config_canonicalization import ConfigAdapter, ConfigPlan
    from consist.core.run_invocation import ResolvedRunInvocation
    from consist.core.tracker import Tracker


class ResolveInputRefsFn(Protocol):
    def __call__(
        self,
        tracker: "Tracker",
        inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
        depends_on: Optional[List[RunInputRef]],
        *,
        include_keyed_artifacts: bool,
    ) -> tuple[List[ArtifactRef], Dict[str, Artifact]]: ...


class PreviewRunArtifactDirFn(Protocol):
    def __call__(
        self,
        tracker: "Tracker",
        *,
        run_id: str,
        model: str,
        description: Optional[str],
        year: Optional[int],
        iteration: Optional[int],
        parent_run_id: Optional[str],
        tags: Optional[List[str]],
    ) -> Path: ...


class ResolveOutputPathFn(Protocol):
    def __call__(
        self, tracker: "Tracker", ref: ArtifactRef, base_dir: Path
    ) -> Path: ...


@dataclass(frozen=True)
class RunTraceHelpers:
    """Injected helper surface used by ``RunTraceCoordinator``.

    Parameters
    ----------
    resolve_input_refs : ResolveInputRefsFn
        Shared input/dependency resolution routine.
    preview_run_artifact_dir : PreviewRunArtifactDirFn
        Resolver for deterministic run output base directory previews.
    resolve_output_path : ResolveOutputPathFn
        Output path normalization routine.
    is_xarray_dataset : Callable[[Any], bool]
        Runtime type guard for xarray dataset outputs.
    write_xarray_dataset : Callable[[Any, Path], None]
        Serializer for xarray dataset outputs.

    Notes
    -----
    Helper injection avoids re-implementing tracker-local behavior in this
    module and provides a narrow seam for focused testing/mocking.
    """

    resolve_input_refs: ResolveInputRefsFn
    preview_run_artifact_dir: PreviewRunArtifactDirFn
    resolve_output_path: ResolveOutputPathFn
    is_xarray_dataset: Callable[[Any], bool]
    write_xarray_dataset: Callable[[Any, Path], None]


@dataclass(frozen=True)
class RunInvocationContext:
    resolved_name: str
    config: Optional[Union[Dict[str, Any], BaseModel]]
    config_plan: Optional["ConfigPlan"]
    outputs: Optional[List[str]]
    output_paths: Optional[Mapping[str, ArtifactRef]]
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]
    output_mismatch: Literal["warn", "error", "ignore"]
    output_missing: Literal["warn", "error", "ignore"]
    executor: Literal["python", "container"]
    container: Any
    runtime_kwargs: Optional[Mapping[str, Any]]
    inject_context: Optional[Union[bool, str]]
    input_binding: InputBindingMode
    resolved_inputs: List[ArtifactRef]
    input_artifacts_by_key: Dict[str, Artifact]
    run_id: str
    start_kwargs: Dict[str, Any]


class RunTraceCoordinator:
    """Coordinate internal ``Tracker.run`` and ``Tracker.trace`` workflows."""

    def __init__(self, tracker: "Tracker", helpers: RunTraceHelpers) -> None:
        self._tracker = tracker
        self._helpers = helpers

    @staticmethod
    def _raise_unexpected_kwargs(kwargs: Mapping[str, Any]) -> None:
        names = sorted(kwargs.keys())
        if not names:
            return
        if len(names) == 1:
            raise TypeError(f"unexpected keyword argument '{names[0]}'")
        joined = "', '".join(names)
        raise TypeError(f"unexpected keyword arguments '{joined}'")

    def _resolve_adapter_config_dirs(self, adapter: "ConfigAdapter") -> list[Any]:
        def _coerce_roots(value: Any) -> Optional[list[Any]]:
            if value is None:
                return None
            if isinstance(value, (str, Path)):
                return [value]
            try:
                values = list(value)
            except TypeError as exc:
                raise TypeError(
                    format_problem_cause_fix(
                        problem=(
                            "adapter root_dirs/config_dirs must be a path or iterable "
                            "of paths."
                        ),
                        cause=(
                            "The adapter did not provide usable config roots for "
                            "identity resolution."
                        ),
                        fix=(
                            "Expose adapter.root_dirs or adapter.config_dirs as "
                            "Path/str values (or iterables of them)."
                        ),
                    )
                ) from exc
            return values or None

        root_dirs = _coerce_roots(getattr(adapter, "root_dirs", None))
        if root_dirs is None:
            root_dirs = _coerce_roots(getattr(adapter, "config_dirs", None))
        if root_dirs is None:
            primary_config = getattr(adapter, "primary_config", None)
            if primary_config is not None:
                root_dirs = [Path(primary_config).expanduser().resolve().parent]
        if root_dirs is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "adapter= requires config roots via "
                        "adapter.root_dirs/config_dirs or adapter.primary_config."
                    ),
                    cause="The adapter cannot locate configuration files to hash.",
                    fix=(
                        "Use the recommended path and provide adapter.root_dirs "
                        "(or config_dirs), or set adapter.primary_config."
                    ),
                )
            )
        return root_dirs

    def _prepare_config_plan(
        self,
        *,
        adapter: Optional["ConfigAdapter"],
    ) -> Optional["ConfigPlan"]:
        if adapter is None:
            return None
        tracker = self._tracker
        return tracker.prepare_config(
            adapter=adapter,
            config_dirs=self._resolve_adapter_config_dirs(adapter),
        )

    @staticmethod
    def _build_config_for_run(
        *,
        config: Optional[Union[Dict[str, Any], BaseModel]],
        config_plan: Optional["ConfigPlan"],
        run_id: str,
    ) -> Optional[Union[Dict[str, Any], BaseModel]]:
        """Attach config-plan metadata to config when a plan is present."""
        config_for_run = config
        if config_plan is None:
            return config_for_run
        if config is None:
            config_for_run = {}
        elif isinstance(config, BaseModel):
            config_for_run = config.model_dump()
        else:
            config_for_run = dict(config)
        if "__consist_config_plan__" in config_for_run:
            logging.warning(
                "[Consist] Overwriting user-provided '__consist_config_plan__' in config for run %s.",
                run_id,
            )
        config_for_run["__consist_config_plan__"] = {
            "adapter": config_plan.adapter_name,
            "hash": config_plan.identity_hash,
            "adapter_version": config_plan.adapter_version,
        }
        return config_for_run

    @staticmethod
    def _build_start_kwargs(
        *,
        invocation: "ResolvedRunInvocation",
        run_id: str,
        config_for_run: Optional[Union[Dict[str, Any], BaseModel]],
        resolved_inputs: List[ArtifactRef],
        cache_mode: str,
        cache_hydration: Optional[str],
        resolved_cache_epoch: int,
        year: Optional[int],
        iteration: Optional[int],
        parent_run_id: Optional[str],
        phase: Optional[str] = None,
        stage: Optional[str] = None,
        code_identity_callable: Optional[Callable[..., Any]] = None,
        materialize_cached_output_paths: Optional[Dict[str, Path]] = None,
        materialize_cached_outputs_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Build the kwargs payload passed into ``tracker.start_run``."""
        start_kwargs: Dict[str, Any] = {
            "run_id": run_id,
            "model": invocation.model,
            "config": config_for_run,
            "inputs": resolved_inputs or None,
            "tags": invocation.tags,
            "description": invocation.description,
            "cache_mode": cache_mode,
            "facet": invocation.facet,
            "facet_from": invocation.facet_from,
            "hash_inputs": invocation.identity_inputs,
            "year": year,
            "iteration": iteration,
            "parent_run_id": parent_run_id,
            "validate_cached_outputs": invocation.validate_cached_outputs,
            "cache_epoch": resolved_cache_epoch,
        }
        if invocation.code_identity is not None:
            start_kwargs["code_identity"] = invocation.code_identity
        if invocation.code_identity_extra_deps is not None:
            start_kwargs["code_identity_extra_deps"] = list(
                invocation.code_identity_extra_deps
            )
        if code_identity_callable is not None:
            start_kwargs["_consist_code_identity_callable"] = code_identity_callable
        optional_values: Dict[str, Any] = {
            "cache_version": invocation.cache_version,
            "phase": phase,
            "stage": stage,
            "facet_schema_version": invocation.facet_schema_version,
            "facet_index": invocation.facet_index,
            "cache_hydration": cache_hydration,
            "materialize_cached_output_paths": materialize_cached_output_paths,
            "materialize_cached_outputs_dir": materialize_cached_outputs_dir,
        }
        for key, value in optional_values.items():
            if value is not None:
                start_kwargs[key] = value
        return start_kwargs

    def _resolve_cache_hydration_targets(
        self,
        *,
        tracker: "Tracker",
        cache_hydration: Optional[str],
        output_paths: Optional[Mapping[str, ArtifactRef]],
        run_id: str,
        model: str,
        description: Optional[str],
        year: Optional[int],
        iteration: Optional[int],
        parent_run_id: Optional[str],
        tags: Optional[List[str]],
    ) -> tuple[Optional[Dict[str, Path]], Optional[Path]]:
        """Resolve cache-hydration output materialization targets, if requested."""
        materialize_cached_output_paths: Optional[Dict[str, Path]] = None
        materialize_cached_outputs_dir: Optional[Path] = None
        if cache_hydration == "outputs-requested":
            if output_paths is None:
                raise ValueError(
                    format_problem_cause_fix(
                        problem=(
                            "cache_hydration='outputs-requested' requires output_paths."
                        ),
                        cause=(
                            "Requested-output hydration needs explicit destination "
                            "paths."
                        ),
                        fix=(
                            "Declare output_paths={key: path} when using "
                            "cache_hydration='outputs-requested'."
                        ),
                    )
                )
            output_base_dir = self._helpers.preview_run_artifact_dir(
                tracker,
                run_id=run_id,
                model=model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )
            materialize_cached_output_paths = {
                str(output_key): self._helpers.resolve_output_path(
                    tracker,
                    ref,
                    output_base_dir,
                )
                for output_key, ref in output_paths.items()
            }
        elif cache_hydration == "outputs-all":
            materialize_cached_outputs_dir = self._helpers.preview_run_artifact_dir(
                tracker,
                run_id=run_id,
                model=model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )

        return materialize_cached_output_paths, materialize_cached_outputs_dir

    def _prepare_run_invocation_context(
        self,
        *,
        fn: Optional[Callable[..., Any]],
        name: Optional[str],
        run_id: Optional[str],
        model: Optional[str],
        description: Optional[str],
        config: Optional[Dict[str, Any]],
        adapter: Optional["ConfigAdapter"],
        inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
        input_keys: Optional[Iterable[str] | str],
        optional_input_keys: Optional[Iterable[str] | str],
        depends_on: Optional[List[RunInputRef]],
        tags: Optional[List[str]],
        facet: Optional[FacetLike],
        facet_from: Optional[List[str]],
        facet_schema_version: Optional[Union[str, int]],
        facet_index: Optional[bool],
        identity_inputs: IdentityInputs,
        year: Optional[int],
        iteration: Optional[int],
        phase: Optional[str],
        stage: Optional[str],
        parent_run_id: Optional[str],
        outputs: Optional[List[str]],
        output_paths: Optional[Mapping[str, ArtifactRef]],
        cache_options: Optional[CacheOptions],
        output_policy: Optional[OutputPolicyOptions],
        execution_options: Optional[ExecutionOptions],
        runtime_kwargs: Optional[Mapping[str, Any]],
    ) -> RunInvocationContext:
        tracker = self._tracker
        execution_options = resolve_runtime_kwargs_alias(
            api_name="Tracker.run",
            execution_options=execution_options,
            runtime_kwargs=runtime_kwargs,
        )

        invocation = resolve_run_invocation(
            fn=fn,
            name=name,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            identity_inputs=identity_inputs,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            tags=tags,
            facet=facet,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            year=year,
            iteration=iteration,
            phase=phase,
            stage=stage,
            outputs=outputs,
            output_paths=output_paths,
            cache_options=cache_options,
            output_policy=output_policy,
            execution_options=execution_options,
            default_name_template=None,
            allow_template=None,
            apply_step_defaults=None,
            consist_settings=tracker.settings,
            consist_workspace=tracker.run_dir,
            consist_state=tracker.current_consist,
            missing_name_error="Tracker.run requires a run name.",
            python_missing_fn_error="Tracker.run requires a callable fn.",
        )

        resolved_name = invocation.name
        config_plan = self._prepare_config_plan(
            adapter=invocation.adapter,
        )
        input_binding = invocation.input_binding
        if (
            input_binding != "none"
            and invocation.inputs is not None
            and not isinstance(invocation.inputs, Mapping)
        ):
            problem = (
                "load_inputs=True requires inputs to be a dict."
                if invocation.load_inputs is True and input_binding == "loaded"
                else f"input_binding={input_binding!r} requires inputs to be a dict."
            )
            fix = (
                "Pass inputs as a mapping (for example {'data': path}) or set "
                "ExecutionOptions(load_inputs=False)."
                if invocation.load_inputs is True and input_binding == "loaded"
                else "Pass inputs as a mapping (for example {'data': path}) or use "
                "input_binding='none' when inputs are identity-only."
            )
            raise ValueError(
                format_problem_cause_fix(
                    problem=problem,
                    cause=(
                        "Automatic input binding needs named inputs so Consist can "
                        "match function parameters."
                    ),
                    fix=fix,
                )
            )

        cache_hydration = invocation.cache_hydration
        if cache_hydration is None and input_binding != "none":
            cache_hydration = "inputs-missing"

        if (
            invocation.input_keys is not None
            or invocation.optional_input_keys is not None
        ):
            warnings.warn(
                "Tracker.run ignores input_keys/optional_input_keys; use inputs mapping instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        resolved_inputs, input_artifacts_by_key = self._helpers.resolve_input_refs(
            tracker,
            invocation.inputs,
            depends_on,
            include_keyed_artifacts=invocation.executor == "python",
        )

        if run_id is None:
            run_id = f"{resolved_name}_{uuid.uuid4().hex[:8]}"

        (
            materialize_cached_output_paths,
            materialize_cached_outputs_dir,
        ) = self._resolve_cache_hydration_targets(
            tracker=tracker,
            cache_hydration=cache_hydration,
            output_paths=invocation.output_paths,
            run_id=run_id,
            model=invocation.model,
            description=invocation.description,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            tags=invocation.tags,
        )

        cache_mode = invocation.cache_mode
        if invocation.executor == "container" and cache_mode != "overwrite":
            logging.warning(
                "[Consist] executor='container' uses container-level caching; forcing cache_mode='overwrite'."
            )
            cache_mode = "overwrite"

        resolved_cache_epoch = (
            tracker._cache_epoch
            if invocation.cache_epoch is None
            else invocation.cache_epoch
        )
        config_for_run = self._build_config_for_run(
            config=invocation.config,
            config_plan=config_plan,
            run_id=run_id,
        )
        start_kwargs = self._build_start_kwargs(
            invocation=invocation,
            run_id=run_id,
            config_for_run=config_for_run,
            resolved_inputs=resolved_inputs,
            cache_mode=cache_mode,
            cache_hydration=cache_hydration,
            resolved_cache_epoch=resolved_cache_epoch,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            phase=phase,
            stage=stage,
            code_identity_callable=(
                fn if invocation.executor == "python" and fn is not None else None
            ),
            materialize_cached_output_paths=materialize_cached_output_paths,
            materialize_cached_outputs_dir=materialize_cached_outputs_dir,
        )

        return RunInvocationContext(
            resolved_name=resolved_name,
            config=invocation.config,
            config_plan=config_plan,
            outputs=invocation.outputs,
            output_paths=invocation.output_paths,
            inputs=invocation.inputs,
            output_mismatch=invocation.output_mismatch,
            output_missing=invocation.output_missing,
            executor=invocation.executor,
            container=invocation.container,
            runtime_kwargs=invocation.runtime_kwargs,
            inject_context=invocation.inject_context,
            input_binding=input_binding,
            resolved_inputs=resolved_inputs,
            input_artifacts_by_key=input_artifacts_by_key,
            run_id=run_id,
            start_kwargs=start_kwargs,
        )

    @staticmethod
    def _apply_missing_output_policy(
        label: str,
        missing: List[str],
        *,
        policy: Literal["warn", "error", "ignore"],
    ) -> None:
        if not missing:
            return
        msg = f"{label} missing outputs: {missing}"
        if policy == "error":
            raise RuntimeError(msg)
        if policy == "warn":
            logging.warning("[Consist] %s", msg)

    @staticmethod
    def _apply_output_mismatch_policy(
        msg: str,
        *,
        policy: Literal["warn", "error", "ignore"],
    ) -> bool:
        if policy == "error":
            raise RuntimeError(msg)
        if policy == "warn":
            logging.warning("[Consist] %s", msg)
        return False

    @staticmethod
    def _log_output_value(
        tracker: "Tracker",
        output_key: str,
        output_value: Optional[ArtifactRef],
    ) -> Optional[Artifact]:
        if output_value is None:
            return None
        return tracker.log_artifact(
            output_value,
            key=output_key,
            direction="output",
        )

    @staticmethod
    def _default_output_key(
        *,
        fn: Optional[Callable[..., Any]],
        resolved_name: str,
    ) -> str:
        fn_name = getattr(fn, "__name__", None) if fn is not None else None
        if isinstance(fn_name, str) and fn_name and fn_name != "<lambda>":
            return fn_name
        return resolved_name

    @staticmethod
    def _try_infer_artifact_outputs(
        *,
        tracker: "Tracker",
        fn: Optional[Callable[..., Any]],
        resolved_name: str,
        result: Any,
    ) -> Optional[Dict[str, Artifact]]:
        if isinstance(result, (Artifact, Path)):
            output_key = RunTraceCoordinator._default_output_key(
                fn=fn,
                resolved_name=resolved_name,
            )
            logged = RunTraceCoordinator._log_output_value(
                tracker,
                output_key,
                cast(Optional[ArtifactRef], result),
            )
            if logged is None:
                return {}
            return {output_key: logged}

        if isinstance(result, dict):
            inferred: Dict[str, Artifact] = {}
            for output_key, output_value in result.items():
                if not isinstance(output_key, str):
                    return None
                if not isinstance(output_value, (Artifact, Path)):
                    return None
                logged = RunTraceCoordinator._log_output_value(
                    tracker,
                    output_key,
                    cast(Optional[ArtifactRef], output_value),
                )
                if logged is not None:
                    inferred[output_key] = logged
            return inferred

        return None

    def _log_declared_output_paths(
        self,
        *,
        tracker: "Tracker",
        resolved_name: str,
        output_paths: Optional[Mapping[str, ArtifactRef]],
        output_base_dir: Path,
        on_missing_outputs: Callable[[str, List[str]], None],
    ) -> Dict[str, Artifact]:
        outputs_map: Dict[str, Artifact] = {}
        if output_paths is None:
            return outputs_map

        for output_key, ref in output_paths.items():
            ref_path = self._helpers.resolve_output_path(tracker, ref, output_base_dir)
            if not ref_path.exists():
                on_missing_outputs(f"Run {resolved_name!r}", [str(output_key)])
                continue
            if isinstance(ref, Artifact):
                outputs_map[output_key] = tracker.log_artifact(
                    ref,
                    key=output_key,
                    direction="output",
                )
            else:
                outputs_map[output_key] = tracker.log_artifact(
                    ref_path,
                    key=output_key,
                    direction="output",
                )
        return outputs_map

    def _normalize_run_outputs(
        self,
        *,
        tracker: "Tracker",
        current_consist: Any,
        fn: Optional[Callable[..., Any]],
        resolved_name: str,
        outputs: Optional[List[str]],
        output_paths: Optional[Mapping[str, ArtifactRef]],
        result: Any,
        captured_outputs: Mapping[str, Artifact],
        on_missing_outputs: Callable[[str, List[str]], None],
        on_output_mismatch: Callable[[str], bool],
    ) -> Dict[str, Artifact]:
        output_base_dir = tracker.run_artifact_dir()
        outputs_map = self._log_declared_output_paths(
            tracker=tracker,
            resolved_name=resolved_name,
            output_paths=output_paths,
            output_base_dir=output_base_dir,
            on_missing_outputs=on_missing_outputs,
        )

        if outputs:
            if result is None:
                pass
            elif isinstance(result, dict):
                for output_key, output_value in result.items():
                    logged = self._log_output_value(
                        tracker,
                        str(output_key),
                        cast(Optional[ArtifactRef], output_value),
                    )
                    if logged is not None:
                        outputs_map[str(output_key)] = logged
            elif isinstance(result, (list, tuple)):
                if len(result) != len(outputs):
                    on_output_mismatch(
                        "Output list length does not match declared outputs."
                    )
                else:
                    for output_key, output_value in zip(outputs, result):
                        logged = self._log_output_value(
                            tracker,
                            output_key,
                            cast(Optional[ArtifactRef], output_value),
                        )
                        if logged is not None:
                            outputs_map[output_key] = logged
            elif isinstance(result, pd.DataFrame):
                if len(outputs) != 1:
                    on_output_mismatch(
                        "Single return value does not match declared outputs."
                    )
                else:
                    outputs_map[outputs[0]] = tracker.log_dataframe(
                        result,
                        key=outputs[0],
                    )
            elif isinstance(result, pd.Series):
                if len(outputs) != 1:
                    on_output_mismatch(
                        "Single return value does not match declared outputs."
                    )
                else:
                    outputs_map[outputs[0]] = tracker.log_dataframe(
                        result.to_frame(name=outputs[0]),
                        key=outputs[0],
                    )
            elif self._helpers.is_xarray_dataset(result):
                if len(outputs) != 1:
                    on_output_mismatch(
                        "Single return value does not match declared outputs."
                    )
                else:
                    key_name = outputs[0]
                    output_path = output_base_dir / f"{key_name}.zarr"
                    self._helpers.write_xarray_dataset(result, output_path)
                    outputs_map[key_name] = tracker.log_artifact(
                        output_path,
                        key=key_name,
                        direction="output",
                        driver="zarr",
                    )
            elif isinstance(result, (Artifact, str, Path)):
                if len(outputs) != 1:
                    on_output_mismatch(
                        "Single return value does not match declared outputs."
                    )
                else:
                    logged = self._log_output_value(
                        tracker,
                        outputs[0],
                        cast(Optional[ArtifactRef], result),
                    )
                    if logged is not None:
                        outputs_map[outputs[0]] = logged
            else:
                raise TypeError(f"Run returned unsupported type {type(result)}")
        elif result is not None:
            inferred_outputs = None
            if output_paths is None:
                inferred_outputs = self._try_infer_artifact_outputs(
                    tracker=tracker,
                    fn=fn,
                    resolved_name=resolved_name,
                    result=result,
                )
            if inferred_outputs is not None:
                outputs_map.update(inferred_outputs)
            else:
                logging.warning(
                    "[Consist] Run %r returned a value but no outputs were declared; ignoring return value.",
                    resolved_name,
                )

        if captured_outputs:
            for output_key, artifact in captured_outputs.items():
                outputs_map.setdefault(output_key, artifact)

        logged_outputs = {
            artifact.key: artifact for artifact in current_consist.outputs
        }
        if outputs:
            missing_keys = [
                output_key
                for output_key in outputs
                if output_key not in outputs_map and output_key not in logged_outputs
            ]
            on_missing_outputs(f"Run {resolved_name!r}", missing_keys)
            for output_key in outputs:
                if output_key not in outputs_map and output_key in logged_outputs:
                    outputs_map[output_key] = logged_outputs[output_key]
        elif logged_outputs:
            for output_key, artifact in logged_outputs.items():
                outputs_map.setdefault(output_key, artifact)

        return outputs_map

    def _execute_container_run(
        self,
        *,
        tracker: "Tracker",
        run_id: str,
        run: Any,
        resolved_name: str,
        output_paths: Optional[Mapping[str, ArtifactRef]],
        container: Any,
        input_binding: InputBindingMode,
        resolved_inputs: List[ArtifactRef],
        on_missing_outputs: Callable[[str, List[str]], None],
    ) -> RunResult:
        if container is None or output_paths is None:
            raise RuntimeError(
                "Container execution requires container and output_paths."
            )
        if input_binding != "none":
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "executor='container' does not support automatic input binding."
                    ),
                    cause=(
                        "Input binding is only supported for Python callable "
                        "execution."
                    ),
                    fix=(
                        "Use input_binding='none' (or legacy load_inputs=False) for "
                        "container runs and pass inputs through inputs=... only for "
                        "identity and lineage."
                    ),
                )
            )
        if not isinstance(container, MappingABC):
            raise TypeError(
                format_problem_cause_fix(
                    problem="container must be a mapping of run_container arguments.",
                    cause="The provided container spec is not a mapping object.",
                    fix=(
                        "Pass execution_options=ExecutionOptions(container={...}) "
                        "with key/value options."
                    ),
                )
            )

        from consist.integrations.containers import run_container

        container_args = dict(container)
        image = container_args.pop("image", None)
        command = container_args.pop("command", None)
        backend_type = container_args.pop("backend", None) or container_args.pop(
            "backend_type", None
        )
        backend_type = backend_type or "docker"
        environment = container_args.pop("environment", None) or {}
        working_dir = container_args.pop("working_dir", None)
        volumes = container_args.pop("volumes", None) or {}
        pull_latest = bool(container_args.pop("pull_latest", False))
        lineage_mode = container_args.pop("lineage_mode", "full")

        if container_args:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        "Unknown container options were provided: "
                        f"{sorted(container_args.keys())}."
                    ),
                    cause=(
                        "The container spec includes keys that are not accepted "
                        "by the run_container integration."
                    ),
                    fix=(
                        "Remove unknown keys and keep only supported fields "
                        "(for example image, command, backend, environment, "
                        "volumes, working_dir, pull_latest, lineage_mode)."
                    ),
                )
            )
        if image is None or command is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="container spec must include image and command.",
                    cause=(
                        "Container execution cannot start without an image and command."
                    ),
                    fix=(
                        "Set container={'image': '...', 'command': [...]} in "
                        "ExecutionOptions."
                    ),
                )
            )

        output_base_dir = tracker.run_artifact_dir()
        resolved_output_paths = {
            str(key): self._helpers.resolve_output_path(tracker, ref, output_base_dir)
            for key, ref in output_paths.items()
        }

        result = run_container(
            tracker=tracker,
            run_id=run_id,
            image=image,
            command=command,
            volumes=volumes,
            inputs=resolved_inputs,
            outputs=resolved_output_paths,
            environment=environment,
            working_dir=working_dir,
            backend_type=backend_type,
            pull_latest=pull_latest,
            lineage_mode=lineage_mode,
        )

        outputs_map = dict(result.artifacts)
        missing = [key for key in output_paths.keys() if key not in outputs_map]
        on_missing_outputs(f"Run {resolved_name!r}", missing)

        return RunResult(
            run=run,
            outputs=outputs_map,
            cache_hit=result.cache_hit,
        )

    def _execute_python_run(
        self,
        *,
        tracker: "Tracker",
        active_tracker: "Tracker",
        fn: Optional[Callable[..., Any]],
        resolved_name: str,
        config: Optional[Union[Dict[str, Any], BaseModel]],
        inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
        runtime_kwargs_dict: Optional[Mapping[str, Any]],
        inject_context: Optional[Union[bool, str]],
        input_binding: InputBindingMode,
        input_artifacts_by_key: Dict[str, Artifact],
        capture_dir: Optional[Path],
        capture_pattern: str,
    ) -> tuple[Any, Dict[str, Artifact]]:
        runtime_kwargs = dict(runtime_kwargs_dict or {})
        required_runtime = getattr(fn, "__consist_runtime_required__", ())
        if required_runtime:
            missing = [
                required_name
                for required_name in required_runtime
                if required_name not in runtime_kwargs
            ]
            if missing:
                missing_list = ", ".join(sorted(missing))
                raise ValueError(
                    format_problem_cause_fix(
                        problem=(
                            f"Missing runtime_kwargs for {resolved_name!r}: "
                            f"{missing_list}."
                        ),
                        cause=(
                            "The step requires runtime-only parameters declared via "
                            "@consist.require_runtime_kwargs."
                        ),
                        fix=(
                            "Provide runtime_kwargs={...} (or "
                            "execution_options=ExecutionOptions("
                            "runtime_kwargs={...})) with those keys, or remove "
                            "@consist.require_runtime_kwargs."
                        ),
                    )
                )
        config_dict: Dict[str, Any] = {}
        if config is None:
            config_dict = {}
        elif isinstance(config, BaseModel):
            config_dict = config.model_dump()
        else:
            config_dict = dict(config)

        if fn is None:
            raise ValueError(
                format_problem_cause_fix(
                    problem="run() requires a callable `fn` to execute.",
                    cause="The Python executor was selected without a function.",
                    fix=(
                        "Provide fn=callable, or use "
                        "ExecutionOptions(executor='container') with container "
                        "settings."
                    ),
                )
            )
        fn_callable = fn
        sig = inspect.signature(fn_callable)
        params = sig.parameters
        has_var_kw = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
        call_kwargs: Dict[str, Any] = {}

        if "config" in params and "config" not in runtime_kwargs and config is not None:
            call_kwargs["config"] = (
                config if isinstance(config, BaseModel) else config_dict
            )

        for param_name, param in params.items():
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            if param_name in call_kwargs:
                continue
            if param_name in runtime_kwargs:
                call_kwargs[param_name] = runtime_kwargs[param_name]
                continue
            if input_binding != "none" and isinstance(inputs, Mapping):
                if param_name in input_artifacts_by_key:
                    if param_name in config_dict:
                        logging.warning(
                            "[Consist] Ambiguous param %r present in inputs and config; preferring inputs.",
                            param_name,
                        )
                    artifact = input_artifacts_by_key[param_name]
                    if get_tracker_ref(artifact) is None:
                        set_tracker_ref(artifact, tracker)
                    if input_binding == "paths":
                        artifact_path = artifact.as_path(tracker=tracker)
                        if not artifact_path.exists():
                            raise ValueError(
                                format_problem_cause_fix(
                                    problem=(
                                        "input_binding='paths' requires a materialized "
                                        f"local path for input {param_name!r}."
                                    ),
                                    cause=(
                                        "The bound input artifact does not currently "
                                        "exist at a local filesystem path."
                                    ),
                                    fix=(
                                        "Use cache_hydration='inputs-missing', ensure "
                                        "the source path is accessible on this host, or "
                                        "switch to input_binding='loaded'."
                                    ),
                                )
                            )
                        call_kwargs[param_name] = artifact_path
                    else:
                        if artifact.driver in DriverType.tabular_drivers():
                            from consist.api import load_df

                            call_kwargs[param_name] = load_df(
                                artifact, tracker=tracker
                            )
                        else:
                            call_kwargs[param_name] = tracker.load(artifact)
                    continue
            if param_name in config_dict:
                call_kwargs[param_name] = config_dict[param_name]

        for runtime_key, runtime_value in runtime_kwargs.items():
            if runtime_key not in call_kwargs:
                call_kwargs[runtime_key] = runtime_value

        if inject_context:
            ctx_name = (
                inject_context if isinstance(inject_context, str) else "_consist_ctx"
            )
            if ctx_name not in call_kwargs:
                if ctx_name in params or has_var_kw:
                    call_kwargs[ctx_name] = RunContext(tracker)
                else:
                    raise ValueError(
                        format_problem_cause_fix(
                            problem=(
                                f"inject_context requested '{ctx_name}', but fn does "
                                "not accept it."
                            ),
                            cause=(
                                "Context injection was enabled, but the function "
                                "signature has no matching parameter."
                            ),
                            fix=(
                                f"Add a '{ctx_name}' parameter to fn, set "
                                "inject_context to a parameter name that exists, "
                                "or disable inject_context."
                            ),
                        )
                    )

        try:
            sig.bind_partial(**call_kwargs)
        except TypeError as exc:
            raise TypeError(
                f"Tracker.run could not bind arguments for {resolved_name!r}: {exc}"
            ) from exc

        captured_outputs: Dict[str, Artifact] = {}
        if capture_dir is not None:
            with active_tracker.capture_outputs(
                capture_dir, pattern=capture_pattern
            ) as cap:
                result = fn_callable(**call_kwargs)
            if result is not None:
                raise ValueError(
                    "capture_dir requires the run function to return None. "
                    "Use inject_context to log outputs manually if you need a return value."
                )
            captured_outputs = {a.key: a for a in cap.artifacts}
        else:
            result = fn_callable(**call_kwargs)

        return result, captured_outputs

    def run(
        self,
        fn: Optional[Callable[..., Any]] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        model: Optional[str] = None,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        adapter: Optional["ConfigAdapter"] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
        inputs: Optional[
            Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[RunInputRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        identity_inputs: IdentityInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        phase: Optional[str] = None,
        stage: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_options: Optional[CacheOptions] = None,
        output_policy: Optional[OutputPolicyOptions] = None,
        execution_options: Optional[ExecutionOptions] = None,
        runtime_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> RunResult:
        """Execute function/container run flow with tracker-level orchestration.

        Parameters
        ----------
        fn : Callable[..., Any] | None, optional
            Python callable for executor='python'. May be ``None`` for
            executor='container' when ``name`` is provided.
        name : str | None, optional
            Run name override.
        run_id : str | None, optional
            Explicit run id override.
        model : str | None, optional
            Model/component namespace.
        description : str | None, optional
            Human-readable run description.
        config : dict[str, Any] | None, optional
            Run config payload.
        adapter : ConfigAdapter | None, optional
            Config adapter used to derive a config plan before execution.
        config_plan_ingest : bool, default True
            Whether config-plan ingestables are ingested.
        config_plan_profile_schema : bool, default False
            Whether config-plan ingestables profile schema.
        inputs : Mapping[str, RunInputRef] | Iterable[RunInputRef] | None, optional
            Inputs and/or input mapping.
        input_keys : Iterable[str] | str | None, optional
            Deprecated key-based input API.
        optional_input_keys : Iterable[str] | str | None, optional
            Deprecated key-based optional input API.
        depends_on : list[RunInputRef] | None, optional
            Extra hash-only dependencies.
        tags : list[str] | None, optional
            Run tags.
        facet : FacetLike | None, optional
            Optional run facet payload.
        facet_from : list[str] | None, optional
            Optional config keys projected into facet payload.
        facet_schema_version : str | int | None, optional
            Optional run facet schema version.
        facet_index : bool | None, optional
            Whether run facet key/values are indexed.
        identity_inputs : IdentityInputs, optional
            Additional identity hash inputs.
        year : int | None, optional
            Run year metadata.
        iteration : int | None, optional
            Run iteration metadata.
        phase : str | None, optional
            Run phase metadata.
        stage : str | None, optional
            Run stage metadata.
        parent_run_id : str | None, optional
            Parent run id for lineage/scenario linkage.
        outputs : list[str] | None, optional
            Declared output keys.
        output_paths : Mapping[str, ArtifactRef] | None, optional
            Explicit output path mapping.
        capture_dir : Path | None, optional
            Output-capture directory.
        capture_pattern : str, default "*"
            Capture glob for capture mode.
        cache_options : CacheOptions | None, optional
            Grouped cache controls.
        output_policy : OutputPolicyOptions | None, optional
            Grouped output mismatch/missing policy controls.
        execution_options : ExecutionOptions | None, optional
            Grouped execution controls.
        runtime_kwargs : Mapping[str, Any] | None, optional
            Top-level alias for ``execution_options.runtime_kwargs``.
            This is mutually exclusive with
            ``execution_options=ExecutionOptions(runtime_kwargs=...)``.

        Returns
        -------
        RunResult
            Run metadata plus resolved output artifacts.

        Notes
        -----
        This method currently performs both metadata resolution and execution-time
        validation. Future cleanup should split those responsibilities into
        independent phases/components.

        """
        tracker = self._tracker
        context = self._prepare_run_invocation_context(
            fn=fn,
            name=name,
            run_id=run_id,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            depends_on=depends_on,
            tags=tags,
            facet=facet,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            identity_inputs=identity_inputs,
            year=year,
            iteration=iteration,
            phase=phase,
            stage=stage,
            parent_run_id=parent_run_id,
            outputs=outputs,
            output_paths=output_paths,
            cache_options=cache_options,
            output_policy=output_policy,
            execution_options=execution_options,
            runtime_kwargs=runtime_kwargs,
        )
        resolved_name = context.resolved_name
        config_plan = context.config_plan
        outputs = context.outputs
        output_paths = context.output_paths
        inputs = context.inputs
        output_mismatch = context.output_mismatch
        output_missing = context.output_missing
        executor = context.executor
        container = context.container
        runtime_kwargs_dict = context.runtime_kwargs
        inject_context = context.inject_context
        input_binding = context.input_binding
        resolved_inputs = context.resolved_inputs
        input_artifacts_by_key = context.input_artifacts_by_key
        run_id = context.run_id
        start_kwargs = context.start_kwargs

        def _handle_missing_outputs(label: str, missing: List[str]) -> None:
            self._apply_missing_output_policy(
                label,
                missing,
                policy=output_missing,
            )

        def _handle_output_mismatch(msg: str) -> bool:
            return self._apply_output_mismatch_policy(
                msg,
                policy=output_mismatch,
            )

        with tracker.start_run(**start_kwargs) as active_tracker:
            current_consist = active_tracker.current_consist
            if current_consist is None:
                raise RuntimeError("No active run context is available.")

            if active_tracker.is_cached:
                cached_outputs = {a.key: a for a in current_consist.outputs}
                expected_keys = set(outputs or [])
                if output_paths:
                    expected_keys.update(output_paths.keys())
                missing = [k for k in expected_keys if k not in cached_outputs]
                _handle_missing_outputs(f"Cache hit for run {resolved_name!r}", missing)
                if expected_keys:
                    outputs_map = {
                        k: cached_outputs[k]
                        for k in expected_keys
                        if k in cached_outputs
                    }
                else:
                    outputs_map = cached_outputs
                return RunResult(
                    run=current_consist.run,
                    outputs=outputs_map,
                    cache_hit=True,
                )

            if config_plan is not None:
                tracker.apply_config_plan(
                    config_plan,
                    run=current_consist.run,
                    ingest=config_plan_ingest,
                    profile_schema=config_plan_profile_schema,
                )

            if executor == "container":
                return self._execute_container_run(
                    tracker=tracker,
                    run_id=run_id,
                    run=current_consist.run,
                    resolved_name=resolved_name,
                    output_paths=output_paths,
                    container=container,
                    input_binding=input_binding,
                    resolved_inputs=resolved_inputs,
                    on_missing_outputs=_handle_missing_outputs,
                )

            result, captured_outputs = self._execute_python_run(
                tracker=tracker,
                active_tracker=active_tracker,
                fn=fn,
                resolved_name=resolved_name,
                config=context.config,
                inputs=inputs,
                runtime_kwargs_dict=runtime_kwargs_dict,
                inject_context=inject_context,
                input_binding=input_binding,
                input_artifacts_by_key=input_artifacts_by_key,
                capture_dir=capture_dir,
                capture_pattern=capture_pattern,
            )

            outputs_map = self._normalize_run_outputs(
                tracker=tracker,
                current_consist=current_consist,
                fn=fn,
                resolved_name=resolved_name,
                outputs=outputs,
                output_paths=output_paths,
                result=result,
                captured_outputs=captured_outputs,
                on_missing_outputs=_handle_missing_outputs,
                on_output_mismatch=_handle_output_mismatch,
            )

            return RunResult(
                run=current_consist.run,
                outputs=outputs_map,
                cache_hit=False,
            )

    @contextmanager
    def trace(
        self,
        name: str,
        *,
        run_id: Optional[str] = None,
        model: Optional[str] = None,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        adapter: Optional["ConfigAdapter"] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
        inputs: Optional[
            Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[RunInputRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        identity_inputs: IdentityInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_mode: str = "reuse",
        cache_hydration: Optional[str] = None,
        cache_version: Optional[int] = None,
        cache_epoch: Optional[int] = None,
        validate_cached_outputs: str = "lazy",
        code_identity: Optional[CodeIdentityMode] = None,
        code_identity_extra_deps: Optional[List[str]] = None,
        output_mismatch: str = "warn",
        output_missing: str = "warn",
    ) -> Iterator["Tracker"]:
        """Execute context-managed trace flow with tracker-level orchestration.

        Parameters
        ----------
        name : str
            Trace/run name.
        run_id : str | None, optional
            Explicit run id override.
        model : str | None, optional
            Model/component namespace.
        description : str | None, optional
            Human-readable run description.
        config : dict[str, Any] | None, optional
            Run config payload.
        adapter : ConfigAdapter | None, optional
            Config adapter used to derive a config plan before execution.
        config_plan_ingest : bool, default True
            Whether config-plan ingestables are ingested.
        config_plan_profile_schema : bool, default False
            Whether config-plan ingestables profile schema.
        inputs : Mapping[str, RunInputRef] | Iterable[RunInputRef] | None, optional
            Inputs and/or input mapping.
        input_keys : Iterable[str] | str | None, optional
            Deprecated key-based input API.
        optional_input_keys : Iterable[str] | str | None, optional
            Deprecated key-based optional input API.
        depends_on : list[RunInputRef] | None, optional
            Extra hash-only dependencies.
        tags : list[str] | None, optional
            Run tags.
        facet : FacetLike | None, optional
            Optional run facet payload.
        facet_from : list[str] | None, optional
            Optional config keys projected into facet payload.
        facet_schema_version : str | int | None, optional
            Optional run facet schema version.
        facet_index : bool | None, optional
            Whether run facet key/values are indexed.
        identity_inputs : IdentityInputs, optional
            Additional identity hash inputs.
        year : int | None, optional
            Run year metadata.
        iteration : int | None, optional
            Run iteration metadata.
        parent_run_id : str | None, optional
            Parent run id for lineage/scenario linkage.
        outputs : list[str] | None, optional
            Declared output keys validated at scope exit.
        output_paths : Mapping[str, ArtifactRef] | None, optional
            Explicit output path mapping validated/logged at scope exit.
        capture_dir : Path | None, optional
            Output-capture directory.
        capture_pattern : str, default "*"
            Capture glob for capture mode.
        cache_mode : str, default "reuse"
            Cache mode for the trace run.
        cache_hydration : str | None, optional
            Cache materialization policy.
        cache_version : int | None, optional
            Cache-version discriminator folded into run identity.
        cache_epoch : int | None, optional
            Cache-epoch discriminator folded into run identity.
        validate_cached_outputs : str, default "lazy"
            Cached-output validation policy.
        code_identity : CodeIdentityMode | None, optional
            Code identity mode override for hash computation.
        code_identity_extra_deps : list[str] | None, optional
            Extra dependency paths included in code identity hashing.
        output_mismatch : str, default "warn"
            Output mismatch policy.
        output_missing : str, default "warn"
            Missing-output policy.

        Yields
        ------
        Tracker
            Active tracker within trace run scope.

        """
        tracker = self._tracker
        output_mismatch_policy = cast(
            Literal["warn", "error", "ignore"], output_mismatch
        )
        output_missing_policy = cast(Literal["warn", "error", "ignore"], output_missing)

        resolved_invocation = resolve_run_invocation(
            fn=None,
            name=name,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            identity_inputs=identity_inputs,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            tags=tags,
            facet=facet,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            year=year,
            iteration=iteration,
            phase=None,
            stage=None,
            outputs=outputs,
            output_paths=output_paths,
            cache_options=CacheOptions(
                cache_mode=cache_mode,
                cache_hydration=cache_hydration,
                cache_version=cache_version,
                cache_epoch=cache_epoch,
                validate_cached_outputs=validate_cached_outputs,
                code_identity=code_identity,
                code_identity_extra_deps=code_identity_extra_deps,
            ),
            output_policy=OutputPolicyOptions(
                output_mismatch=output_mismatch_policy,
                output_missing=output_missing_policy,
            ),
            execution_options=None,
            default_name_template=None,
            allow_template=None,
            apply_step_defaults=None,
            consist_settings=tracker.settings,
            consist_workspace=tracker.run_dir,
            consist_state=tracker.current_consist,
            missing_name_error="Tracker.trace requires a run name.",
            python_missing_fn_error="Tracker.trace does not execute callable functions.",
            allow_python_without_fn=True,
        )

        resolved_name = resolved_invocation.name
        config_plan = self._prepare_config_plan(
            adapter=resolved_invocation.adapter,
        )
        cache_hydration = resolved_invocation.cache_hydration
        output_missing = resolved_invocation.output_missing

        if (
            resolved_invocation.input_keys is not None
            or resolved_invocation.optional_input_keys is not None
        ):
            warnings.warn(
                "Tracker.trace ignores input_keys/optional_input_keys; use inputs mapping instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        resolved_inputs, _ = self._helpers.resolve_input_refs(
            tracker,
            resolved_invocation.inputs,
            depends_on,
            include_keyed_artifacts=False,
        )

        if run_id is None:
            run_id = f"{resolved_name}_{uuid.uuid4().hex[:8]}"

        (
            materialize_cached_output_paths,
            materialize_cached_outputs_dir,
        ) = self._resolve_cache_hydration_targets(
            tracker=tracker,
            cache_hydration=cache_hydration,
            output_paths=resolved_invocation.output_paths,
            run_id=run_id,
            model=resolved_invocation.model,
            description=resolved_invocation.description,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            tags=resolved_invocation.tags,
        )

        resolved_cache_epoch = (
            tracker._cache_epoch
            if resolved_invocation.cache_epoch is None
            else resolved_invocation.cache_epoch
        )
        config_for_run = self._build_config_for_run(
            config=resolved_invocation.config,
            config_plan=config_plan,
            run_id=run_id,
        )
        start_kwargs = self._build_start_kwargs(
            invocation=resolved_invocation,
            run_id=run_id,
            config_for_run=config_for_run,
            resolved_inputs=resolved_inputs,
            cache_mode=resolved_invocation.cache_mode,
            cache_hydration=cache_hydration,
            resolved_cache_epoch=resolved_cache_epoch,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            materialize_cached_output_paths=materialize_cached_output_paths,
            materialize_cached_outputs_dir=materialize_cached_outputs_dir,
        )

        def _handle_missing_outputs(label: str, missing: List[str]) -> None:
            self._apply_missing_output_policy(
                label,
                missing,
                policy=output_missing,
            )

        with tracker.start_run(**start_kwargs) as active_tracker:
            current_consist = active_tracker.current_consist
            if current_consist is None:
                raise RuntimeError("No active run context is available.")

            output_base_dir = tracker.run_artifact_dir()
            try:
                if config_plan is not None and not active_tracker.is_cached:
                    tracker.apply_config_plan(
                        config_plan,
                        run=current_consist.run,
                        ingest=config_plan_ingest,
                        profile_schema=config_plan_profile_schema,
                    )
                if capture_dir is not None:
                    with active_tracker.capture_outputs(
                        capture_dir,
                        pattern=capture_pattern,
                    ):
                        yield active_tracker
                else:
                    yield active_tracker
            finally:
                self._log_declared_output_paths(
                    tracker=tracker,
                    resolved_name=resolved_name,
                    output_paths=resolved_invocation.output_paths,
                    output_base_dir=output_base_dir,
                    on_missing_outputs=_handle_missing_outputs,
                )

                if resolved_invocation.outputs:
                    logged_outputs = {
                        artifact.key: artifact for artifact in current_consist.outputs
                    }
                    missing_keys = [
                        output_key
                        for output_key in resolved_invocation.outputs
                        if output_key not in logged_outputs
                    ]
                    _handle_missing_outputs(f"Run {resolved_name!r}", missing_keys)
