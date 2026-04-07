from __future__ import annotations

from contextlib import contextmanager
import uuid
from collections.abc import Iterable, Mapping as MappingABC
from types import MappingProxyType, TracebackType
from typing import (
    List,
    Optional,
    Dict,
    Any,
    Callable,
    Literal,
    Mapping,
    Iterator,
    Union,
    cast,
)

from consist import Artifact
from consist.models.run import ConsistRecord, RunResult
from typing import TYPE_CHECKING
from consist.core.coupler import Coupler
from consist.core.error_messages import format_problem_cause_fix
from consist.core.input_utils import coerce_input_map
from consist.core.run_invocation import resolve_run_invocation
from consist.types import (
    ArtifactRef,
    BindingResult,
    CacheOptions,
    CodeIdentityMode,
    ExecutionOptions,
    FacetLike,
    InputBindingMode,
    IdentityInputs,
    OutputPolicyOptions,
    RunInputRef,
)
from pathlib import Path

if TYPE_CHECKING:
    from consist.core.config_canonicalization import ConfigAdapter
    from consist.core.tracker import Tracker


def _raise_unexpected_kwargs(kwargs: Mapping[str, Any]) -> None:
    names = sorted(kwargs.keys())
    if not names:
        return
    if len(names) == 1:
        raise TypeError(f"unexpected keyword argument '{names[0]}'")
    joined = "', '".join(names)
    raise TypeError(f"unexpected keyword arguments '{joined}'")


class OutputCapture:
    """
    Holder for artifacts collected inside a ``capture_outputs`` context.

    The tracker yields this object so callers can inspect which artifacts were
    automatically logged once the context exits.
    """

    def __init__(self) -> None:
        """
        Initialize an empty artifact buffer.
        """
        self.artifacts: List[Artifact] = []


class RunContext:
    """
    A lightweight helper object injected into user functions.
    When you execute a run with `inject_context=True`, Consist passes a `RunContext`
    to your function. This allows you to access run-aware helpers—like the run's
    dedicated artifact directory and artifact logging methods—without needing to
    reference a global tracker instance directly.

    Examples
    --------
    ```python
    def my_step(ctx: RunContext):
        # Access the run's dedicated directory
        output_path = ctx.run_dir / "results.csv"
        # ... generate file ...
        ctx.log_artifact(output_path, "results")
    ```
    """

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    @property
    def run_dir(self) -> Path:
        """
        Run-specific output directory for the active step.

        Returns
        -------
        Path
            The directory where this step should write outputs by default. This
            value is derived from the active run and respects any per-run
            artifact directory overrides.
        """
        path = self._tracker.run_artifact_dir()
        path.mkdir(parents=True, exist_ok=True)
        return path

    def output_dir(self, namespace: Optional[str] = None) -> Path:
        """
        Resolve the managed output directory for the active run.

        Parameters
        ----------
        namespace : Optional[str], optional
            Optional relative subdirectory under the managed run output directory.

        Returns
        -------
        Path
            Absolute directory path for managed outputs. The directory is created
            if it does not exist.
        """
        base_dir = self.run_dir.resolve()
        if namespace is None:
            return base_dir

        if not isinstance(namespace, str):
            raise TypeError("namespace must be a string or None.")

        normalized = namespace.strip()
        if not normalized:
            return base_dir

        namespace_path = Path(normalized)
        if namespace_path.is_absolute():
            raise ValueError("namespace must be a relative path.")

        resolved = (base_dir / namespace_path).resolve()
        try:
            resolved.relative_to(base_dir)
        except ValueError as exc:
            raise ValueError(
                f"namespace must resolve under {base_dir}; got {resolved}"
            ) from exc
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    def output_path(self, key: str, ext: str = "parquet") -> Path:
        """
        Resolve a deterministic managed output path for the active run.

        Parameters
        ----------
        key : str
            Artifact key used as the output filename stem. Relative path segments
            are allowed to organize outputs into subdirectories.
        ext : str, default "parquet"
            File extension to append. Leading dots are ignored and the extension
            is normalized to lowercase.

        Returns
        -------
        Path
            Absolute managed output path. Parent directories are created if needed.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string.")
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError("key must be a non-empty string.")

        if not isinstance(ext, str):
            raise TypeError("ext must be a string.")
        normalized_ext = ext.strip().lstrip(".").lower()
        if not normalized_ext:
            raise ValueError("ext must be a non-empty string.")

        base_dir = self.output_dir().resolve()
        resolved = (base_dir / f"{normalized_key}.{normalized_ext}").resolve()
        try:
            resolved.relative_to(base_dir)
        except ValueError as exc:
            raise ValueError(
                f"output key must resolve under {base_dir}; got {resolved}"
            ) from exc
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved

    def log_artifact(self, *args: Any, **kwargs: Any) -> Artifact:
        """
        Log an artifact within the active run.

        This is a thin wrapper around ``Tracker.log_artifact``.

        Parameters
        ----------
        *args : Any
            Positional arguments forwarded to ``Tracker.log_artifact``.
        **kwargs : Any
            Keyword arguments forwarded to ``Tracker.log_artifact``.

        Returns
        -------
        Artifact
            The logged artifact.
        """
        return self._tracker.log_artifact(*args, **kwargs)

    def log_input(self, *args: Any, **kwargs: Any) -> Artifact:
        """
        Log an input artifact within the active run.

        This is a thin wrapper around ``Tracker.log_input``.

        Parameters
        ----------
        *args : Any
            Positional arguments forwarded to ``Tracker.log_input``.
        **kwargs : Any
            Keyword arguments forwarded to ``Tracker.log_input``.

        Returns
        -------
        Artifact
            The logged input artifact.
        """
        return self._tracker.log_input(*args, **kwargs)

    def log_output(self, *args: Any, **kwargs: Any) -> Artifact:
        """
        Log an output artifact within the active run.

        This is a thin wrapper around ``Tracker.log_output``.

        Parameters
        ----------
        *args : Any
            Positional arguments forwarded to ``Tracker.log_output``.
        **kwargs : Any
            Keyword arguments forwarded to ``Tracker.log_output``.

        Returns
        -------
        Artifact
            The logged output artifact.
        """
        return self._tracker.log_output(*args, **kwargs)

    def log_artifacts(self, *args: Any, **kwargs: Any) -> Dict[str, Artifact]:
        """
        Log multiple artifacts within the active run.

        This is a thin wrapper around ``Tracker.log_artifacts``.

        Parameters
        ----------
        *args : Any
            Positional arguments forwarded to ``Tracker.log_artifacts``.
        **kwargs : Any
            Keyword arguments forwarded to ``Tracker.log_artifacts``.

        Returns
        -------
        Dict[str, Artifact]
            Mapping of artifact keys to logged artifacts.
        """
        return self._tracker.log_artifacts(*args, **kwargs)

    def log_meta(self, **kwargs: Any) -> None:
        """
        Update metadata for the active run.

        This is a thin wrapper around ``Tracker.log_meta``.

        Parameters
        ----------
        **kwargs : Any
            Metadata key/value pairs to merge into the run record.
        """
        self._tracker.log_meta(**kwargs)

    @property
    def inputs(self) -> Dict[str, Artifact]:
        """
        Mapping of input artifact keys to artifacts for the active step.

        Returns
        -------
        Dict[str, Artifact]
            Dictionary of input artifacts keyed by their semantic keys. Raises a
            ``RuntimeError`` if accessed outside an active run.
        """
        current_consist = self._tracker.current_consist
        if current_consist is None:
            raise RuntimeError("No active run context is available.")
        return {a.key: a for a in current_consist.inputs}

    def load(self, key_or_artifact: Union[str, Artifact]) -> Any:
        """
        Load data from an input artifact by key or from an Artifact instance.

        Parameters
        ----------
        key_or_artifact : Union[str, Artifact]
            Input artifact key from ``inputs`` or an Artifact object.

        Returns
        -------
        Any
            Loaded data (driver-dependent).
        """
        if isinstance(key_or_artifact, str):
            key_or_artifact = self.inputs[key_or_artifact]
        return self._tracker.load(key_or_artifact)

    @contextmanager
    def capture_outputs(
        self, directory: Path, pattern: str = "*"
    ) -> Iterator[OutputCapture]:
        """
        Capture files written under ``directory`` and log them as outputs on exit.

        Parameters
        ----------
        directory : Path
            Directory to monitor for new or modified files.
        pattern : str, default "*"
            Glob pattern for files to capture.

        Yields
        ------
        OutputCapture
            Container listing artifacts that were logged during the context.
        """
        with self._tracker.capture_outputs(directory, pattern=pattern) as cap:
            yield cap


class ScenarioContext:
    """
    A context manager for grouping multiple steps into a single "scenario".

    A scenario creates a parent run (the "header") that aggregates the
    results, metadata, and lineage of all steps executed within its block.
    It provides a `coupler` to pass artifacts between steps, making it
    ideal for multi-stage simulation workflows.

    Attributes
    ----------
    coupler : Coupler
        Scenario-local artifact registry for passing outputs between steps.
        Supports runtime-declared output validation.

    Examples
    --------
    ```python
    with tracker.scenario("base_case") as sc:
        # Step 1: Pre-process
        sc.run(preprocess_fn, inputs={"raw": "data.csv"}, outputs=["clean"])
        # Step 2: Model (reads "clean" from the coupler automatically)
        sc.run(model_fn, input_keys=["clean"], outputs=["results"])
    ```
    """

    def __init__(
        self,
        tracker: "Tracker",
        name: str,
        config: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        model: str = "scenario",
        step_cache_hydration: Optional[str] = None,
        name_template: Optional[str] = None,
        cache_epoch: Optional[int] = None,
        coupler: Optional[Coupler] = None,
        require_outputs: Optional[Iterable[str]] = None,
        **kwargs: Any,
    ):
        self.tracker = tracker
        self.name = name
        self.model = model
        self.config_arg = config or {}
        self.tags = tags or []
        self._step_tags = self._coerce_step_tags(kwargs.pop("step_tags", None))
        self._step_facet = kwargs.pop("step_facet", None)
        self.kwargs = kwargs
        self.step_cache_hydration = step_cache_hydration
        self.name_template = name_template
        self.cache_epoch = cache_epoch

        # Internal State
        self._header_record: Optional[ConsistRecord] = None
        self._suspended_cache_mode: Optional[str] = None
        self._inputs: Dict[str, Artifact] = {}
        self._first_step_started: bool = False
        self._last_step_name: Optional[str] = None

        # ARTIFACT STORAGE ARCHITECTURE
        # ==============================
        # Artifacts flow through multiple storage locations:
        #
        # 1. ConsistRecord.run.outputs (in-memory list, authoritative during run)
        #    - Populated by hydrate_cache_hit_outputs() on cache hit
        #    - Populated by tracker.log_artifact() when step produces outputs
        #    - Each artifact is independent (deep-cloned on cache hit)
        #
        # 2. coupler._artifacts (dict, convenience access for scenarios)
        #    - Populated from ConsistRecord.run.outputs via coupler.update()
        #    - Allows step code to access outputs by key: coupler.get("key")
        #    - Automatically synced after each tracker.run() call
        #
        # 3. Tracker._artifact_cache (internal, for runtime tracking)
        #    - Derived from ConsistRecord.run.outputs
        #    - Used for convenience lookups within tracker
        #
        # SYNCHRONIZATION POINTS:
        # - After cache hit: hydrate_cache_hit_outputs() → record.outputs → coupler.update()
        # - After user log: log_artifact() → record.outputs → coupler.update()
        # - Consistency: Artifacts in coupler are independent copies (no cross-run mutations)
        #
        self.coupler = coupler or Coupler(tracker=tracker)
        if require_outputs:
            self.coupler.require_outputs(*require_outputs)

    @property
    def run_id(self) -> str:
        """
        Run ID of the scenario header.

        Returns
        -------
        str
            The run ID for the scenario header (or the scenario name if the
            header has not been created yet).
        """
        return self._header_record.run.id if self._header_record else self.name

    @property
    def config(self) -> MappingProxyType:
        """
        Read-only view of the scenario configuration.

        Returns
        -------
        MappingProxyType
            Immutable mapping of configuration values for the scenario. Updates
            are applied by changing inputs to the scenario, not by mutating this
            mapping.
        """
        if self._header_record:
            return MappingProxyType(self._header_record.config)
        return MappingProxyType(self.config_arg)

    @property
    def inputs(self) -> MappingProxyType:
        """
        Read-only view of registered exogenous inputs.

        Returns
        -------
        MappingProxyType
            Immutable mapping of input keys to artifacts added via ``add_input``.
            This reflects only scenario-level inputs, not step inputs.
        """
        return MappingProxyType(self._inputs)

    def add_input(self, path: ArtifactRef, key: str, **kwargs) -> Artifact:
        """
        Log an external input artifact to the scenario header run.

        Parameters
        ----------
        path : ArtifactRef
            Path (or prebuilt ``Artifact``) representing the input.
        key : str
            Semantic key for the artifact.
        **kwargs : Any
            Additional metadata forwarded to ``Tracker.log_artifact``.

        Returns
        -------
        Artifact
            Logged artifact associated with the scenario.

        Raises
        ------
        RuntimeError
            If a step has already started or the scenario context is inactive.
        """
        if self._first_step_started:
            raise RuntimeError(
                "Cannot add scenario inputs after first step has started. "
                "Register all inputs before calling scenario.trace()."
            )

        if not self._header_record:
            raise RuntimeError("Scenario not active. Use within 'with' block.")

        # Temporarily restore header context to log artifact
        # This allows us to use standard log_artifact logic without keeping run active
        prev_consist = self.tracker.current_consist
        self.tracker.current_consist = self._header_record
        try:
            artifact = self.tracker.log_artifact(
                path, key=key, direction="input", **kwargs
            )
        finally:
            self.tracker.current_consist = prev_consist

        self._inputs[key] = artifact
        return artifact

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        warn_undefined: bool = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        Declare outputs that should be present in the scenario coupler.

        Parameters
        ----------
        *names : str
            Output keys to declare.
        required : bool | Mapping[str, bool], default False
            Whether declared outputs are required. A mapping allows per-key
            overrides.
        warn_undefined : bool, default False
            If True, warn when outputs are logged that were not declared.
        description : Optional[Mapping[str, str]], optional
            Human-readable descriptions for declared outputs.
        """
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
        """
        Declare required outputs that must be present at scenario exit.

        This is a convenience wrapper around ``declare_outputs`` that defaults
        ``required=True``.

        Parameters
        ----------
        *names : str
            Output keys to require.
        required : bool | Mapping[str, bool], default True
            Whether required outputs are enforced. A mapping allows per-key
            overrides.
        warn_undefined : bool, default False
            If True, warn when outputs are logged that were not declared.
        description : Optional[Mapping[str, str]], optional
            Human-readable descriptions for required outputs.
        """
        self.coupler.require_outputs(
            *names,
            required=required,
            warn_undefined=warn_undefined,
            description=description,
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Artifact], *keys: str, prefix: str = ""
    ) -> Dict[str, Artifact]:
        """
        Collect explicit artifacts into the scenario coupler by key.

        Parameters
        ----------
        artifacts : Mapping[str, Artifact]
            Source artifacts mapping (usually outputs from a step).
        *keys : str
            Keys to collect from the mapping.
        prefix : str, default ""
            Optional prefix to apply to collected keys in the coupler.

        Returns
        -------
        Dict[str, Artifact]
            The collected artifacts keyed by their (possibly prefixed) names.
        """
        return self.coupler.collect_by_keys(artifacts, *keys, prefix=prefix)

    def _coerce_keys(self, value: Optional[Iterable[str] | str]) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    def _coerce_step_tags(self, value: Optional[Iterable[str]]) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raise TypeError("step_tags must be an iterable of strings, not a str.")
        return list(value)

    def _merge_step_tags(self, tags: Optional[List[str]]) -> Optional[List[str]]:
        merged: list[str] = []
        seen: set[str] = set()

        for source in (tags, self._step_tags):
            if not source:
                continue
            for tag in source:
                if tag in seen:
                    continue
                seen.add(tag)
                merged.append(tag)

        return merged or None

    def _merge_step_facet(self, facet: Optional[FacetLike]) -> Optional[FacetLike]:
        if self._step_facet is None and facet is None:
            return None

        merged: Dict[str, Any] = {}
        if self._step_facet is not None:
            merged.update(self._coerce_mapping(self._step_facet, "step_facet"))
        if facet is not None:
            merged.update(self._coerce_mapping(facet, "facet"))
        return self.tracker.identity.normalize_json(merged)

    def _resolve_input_value(self, value: RunInputRef) -> ArtifactRef:
        def _resolve_coupler_ref(ref: str) -> Optional[ArtifactRef]:
            if ref in self.coupler:
                return self.coupler.require(ref)
            return None

        return self.tracker._resolve_input_reference(
            value,
            type_label="Scenario inputs",
            missing_path_error=(
                "Problem: Scenario input path does not exist: {path!s}\n"
                "Cause: The provided scenario input path is missing or not accessible.\n"
                "Fix: Use an existing path, or pass a Coupler key from a prior step. "
                "For the recommended path, use consist.refs(...) between steps."
            ),
            missing_string_error=(
                "Problem: Scenario input string did not resolve: {value!r} "
                "(checked path: {path!s}).\n"
                "Cause: The string is neither a Coupler key nor an existing path.\n"
                "Fix: Use a Coupler key produced earlier in the scenario, or provide "
                "a valid file path. For the recommended path, use consist.refs(...)."
            ),
            string_ref_resolver=_resolve_coupler_ref,
        )

    def _resolve_inputs(
        self,
        inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
        input_keys: Optional[Iterable[str] | str],
        optional_input_keys: Optional[Iterable[str] | str],
    ) -> Optional[Union[Dict[str, ArtifactRef], List[ArtifactRef]]]:
        resolved_inputs: Optional[Union[Dict[str, ArtifactRef], List[ArtifactRef]]] = (
            None
        )
        if inputs is not None:
            if isinstance(inputs, MappingABC):
                resolved_dict: Dict[str, ArtifactRef] = {}
                typed_inputs = cast(Mapping[str, RunInputRef], inputs)
                for key, value in typed_inputs.items():
                    if not isinstance(key, str):
                        raise TypeError(
                            f"inputs mapping keys must be str (got {type(key)})."
                        )
                    resolved_dict[key] = self._resolve_input_value(value)
                resolved_inputs = resolved_dict
            else:
                resolved_inputs = [self._resolve_input_value(v) for v in list(inputs)]

        for key in self._coerce_keys(input_keys):
            artifact = self.coupler.require(key)
            if resolved_inputs is None:
                resolved_inputs = {key: artifact}
            elif isinstance(resolved_inputs, dict):
                resolved_inputs.setdefault(key, artifact)
            else:
                resolved_inputs.append(artifact)

        for key in self._coerce_keys(optional_input_keys):
            artifact = self.coupler.get(key)
            if artifact is None:
                continue
            if resolved_inputs is None:
                resolved_inputs = {key: artifact}
            elif isinstance(resolved_inputs, dict):
                resolved_inputs.setdefault(key, artifact)
            else:
                resolved_inputs.append(artifact)

        return resolved_inputs

    def _promote_inputs_for_binding(
        self,
        inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
        input_binding: InputBindingMode,
    ) -> Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]]:
        if input_binding == "none" or inputs is None:
            return inputs
        if isinstance(inputs, (list, tuple)):
            if all(isinstance(value, str) for value in inputs) and all(
                value in self.coupler for value in inputs
            ):
                return {value: value for value in inputs}
        return inputs

    def _resolve_output_paths(
        self, output_paths: Optional[Mapping[str, ArtifactRef]]
    ) -> Optional[Dict[str, ArtifactRef]]:
        if output_paths is None:
            return None
        resolved_output_paths: Dict[str, ArtifactRef] = {}
        for key, ref in coerce_input_map(output_paths).items():
            if isinstance(ref, str) and ref in self.coupler:
                path = self.coupler.path(ref)
                if path is None:
                    raise RuntimeError(
                        format_problem_cause_fix(
                            problem=(
                                f"Coupler key {ref!r} has no path to use for "
                                f"output_paths[{key!r}]."
                            ),
                            cause=(
                                "The referenced Coupler artifact has no materialized "
                                "path."
                            ),
                            fix=(
                                "Log the artifact with a real path before reusing it in "
                                "output_paths, or provide an explicit filesystem path."
                            ),
                        )
                    )
                resolved_output_paths[str(key)] = path
            else:
                resolved_output_paths[str(key)] = ref
        return resolved_output_paths

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
        binding: Optional[BindingResult] = None,
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
    ) -> RunResult:
        """
        Execute a cached scenario step and update the Coupler with outputs.

        This method wraps ``Tracker.run`` while ensuring the scenario header
        is updated with step metadata and artifacts.
        Use ``execution_options.runtime_kwargs`` for runtime-only inputs and
        `consist.require_runtime_kwargs` to validate required keys.
        For direct workflow code, prefer primitive `inputs=` kwargs and, when
        needed, the direct `input_keys=` / `optional_input_keys=` compatibility
        surfaces. For complex or externally orchestrated workflows that already
        resolved the binding plan, pass ``binding=BindingResult(...)`` instead;
        `binding` is an execution envelope and is mutually exclusive with the
        primitive input kwargs.
        Pass policy controls via ``cache_options``, ``output_policy``,
        and ``execution_options``.

        ``adapter`` and ``identity_inputs`` are the public identity-related
        kwargs.

        Examples
        --------
        Direct workflow code:

        ```python
        sc.run(
            fn=step,
            inputs={"raw": raw_path},
            execution_options=ExecutionOptions(input_binding="paths"),
        )

        sc.run(
            fn=step,
            inputs={"raw": consist.ref(previous_result, key="raw")},
            execution_options=ExecutionOptions(input_binding="loaded"),
        )
        ```

        Orchestrator-facing execution envelope:

        ```python
        sc.run(
            fn=step,
            binding=BindingResult(
                inputs={"raw": raw_path},
                input_keys=["data"],
                optional_input_keys=["maybe"],
            ),
            execution_options=ExecutionOptions(input_binding="paths"),
        )
        ```

        `binding` cannot be combined with primitive `inputs`, `input_keys`, or
        `optional_input_keys`.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active. Use within 'with' block.")

        func_name = getattr(fn, "__name__", None) if fn is not None else None
        if fn is not None and func_name is None and name is None:
            raise ValueError("ScenarioContext.run requires a run name.")
        if fn is None and name is None:
            raise ValueError("ScenarioContext.run requires name when fn is None.")

        if binding is not None:
            if (
                inputs is not None
                or input_keys is not None
                or optional_input_keys is not None
            ):
                raise ValueError(
                    format_problem_cause_fix(
                        problem=(
                            "ScenarioContext.run received both binding and primitive "
                            "input kwargs."
                        ),
                        cause=(
                            "binding=... is a complete execution envelope for "
                            "scenario inputs, so it cannot be combined with "
                            "inputs=..., input_keys=..., or optional_input_keys=...."
                        ),
                        fix=(
                            "Pass either binding=BindingResult(...) or the primitive "
                            "input kwargs, but not both."
                        ),
                    )
                )
            invocation_inputs = binding.inputs
            input_keys = binding.input_keys
            optional_input_keys = binding.optional_input_keys
        else:
            invocation_inputs = inputs

        requested_input_binding = (
            execution_options.input_binding if execution_options is not None else None
        )
        if (
            requested_input_binding is None
            and execution_options is not None
            and execution_options.load_inputs is True
        ):
            requested_input_binding = "loaded"
        if requested_input_binding in {"loaded", "paths"}:
            invocation_inputs = self._promote_inputs_for_binding(
                invocation_inputs,
                requested_input_binding,
            )

        resolved_invocation = resolve_run_invocation(
            fn=fn,
            name=name,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            identity_inputs=identity_inputs,
            inputs=invocation_inputs,
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
            default_name_template=self.name_template,
            allow_template=True,
            apply_step_defaults=True,
            consist_settings=self.tracker.settings,
            consist_workspace=self.tracker.run_dir,
            consist_state=self._header_record,
            missing_name_error="ScenarioContext.run requires a run name.",
            python_missing_fn_error="Tracker.run requires a callable fn.",
        )

        resolved_name = resolved_invocation.name
        resolved_model = resolved_invocation.model
        resolved_description = resolved_invocation.description
        resolved_config = resolved_invocation.config
        resolved_adapter = resolved_invocation.adapter
        resolved_identity_inputs = resolved_invocation.identity_inputs
        resolved_tags = resolved_invocation.tags
        resolved_facet = resolved_invocation.facet
        resolved_facet_index = resolved_invocation.facet_index
        resolved_outputs = resolved_invocation.outputs
        resolved_output_paths = resolved_invocation.output_paths
        resolved_inputs = resolved_invocation.inputs
        resolved_input_keys = resolved_invocation.input_keys
        resolved_optional_input_keys = resolved_invocation.optional_input_keys
        resolved_facet_from = resolved_invocation.facet_from
        resolved_facet_schema_version = resolved_invocation.facet_schema_version
        resolved_cache_mode = resolved_invocation.cache_mode
        resolved_cache_hydration = resolved_invocation.cache_hydration
        resolved_cache_version = resolved_invocation.cache_version
        resolved_validate_cached_outputs = resolved_invocation.validate_cached_outputs
        resolved_input_binding = resolved_invocation.input_binding
        resolved_output_mismatch = resolved_invocation.output_mismatch
        resolved_output_missing = resolved_invocation.output_missing
        resolved_executor = resolved_invocation.executor
        resolved_container = resolved_invocation.container
        runtime_kwargs_dict = resolved_invocation.runtime_kwargs
        resolved_inject_context = resolved_invocation.inject_context
        cache_epoch = resolved_invocation.cache_epoch
        resolved_tags = self._merge_step_tags(resolved_tags)
        resolved_facet = self._merge_step_facet(resolved_facet)

        if run_id is None:
            run_id = f"{self.run_id}_{resolved_name}_{uuid.uuid4().hex[:8]}"
        if parent_run_id is None:
            parent_run_id = self.run_id

        self._first_step_started = True
        self._last_step_name = resolved_name

        effective_cache_hydration = (
            resolved_cache_hydration
            if resolved_cache_hydration is not None
            else self.step_cache_hydration
        )

        promoted_inputs = self._promote_inputs_for_binding(
            resolved_inputs, resolved_input_binding
        )
        resolved_inputs = self._resolve_inputs(
            promoted_inputs, resolved_input_keys, resolved_optional_input_keys
        )
        resolved_output_paths = self._resolve_output_paths(resolved_output_paths)

        resolved_cache_epoch = (
            cache_epoch
            if cache_epoch is not None
            else (
                self.cache_epoch
                if self.cache_epoch is not None
                else getattr(self.tracker, "_cache_epoch", None)
            )
        )

        result = self.tracker.run(
            fn=fn,
            name=resolved_name,
            run_id=run_id,
            model=resolved_model,
            description=resolved_description,
            config=resolved_config,
            adapter=resolved_adapter,
            config_plan_ingest=config_plan_ingest,
            config_plan_profile_schema=config_plan_profile_schema,
            inputs=resolved_inputs,
            input_keys=None,
            optional_input_keys=None,
            depends_on=depends_on,
            tags=resolved_tags,
            facet=resolved_facet,
            facet_from=resolved_facet_from,
            facet_schema_version=resolved_facet_schema_version,
            facet_index=resolved_facet_index,
            identity_inputs=resolved_identity_inputs,
            year=year,
            iteration=iteration,
            phase=phase,
            stage=stage,
            parent_run_id=parent_run_id,
            outputs=resolved_outputs,
            output_paths=resolved_output_paths,
            capture_dir=capture_dir,
            capture_pattern=capture_pattern,
            cache_options=CacheOptions(
                cache_mode=resolved_cache_mode,
                cache_hydration=effective_cache_hydration,
                cache_version=resolved_cache_version,
                cache_epoch=resolved_cache_epoch,
                validate_cached_outputs=resolved_validate_cached_outputs,
                code_identity=resolved_invocation.code_identity,
                code_identity_extra_deps=resolved_invocation.code_identity_extra_deps,
            ),
            output_policy=OutputPolicyOptions(
                output_mismatch=resolved_output_mismatch,
                output_missing=resolved_output_missing,
            ),
            execution_options=ExecutionOptions(
                input_binding=resolved_input_binding,
                executor=resolved_executor,
                container=resolved_container,
                runtime_kwargs=runtime_kwargs_dict,
                inject_context=resolved_inject_context,
            ),
        )

        if result.outputs:
            self.coupler.update(result.outputs)

        record = self.tracker.last_run
        if record and self._header_record:
            self._record_step_in_parent(record.run, record.inputs, record.outputs)

        return result

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
    ):
        """
        Manual tracing context manager for scenario steps.

        This wraps ``Tracker.trace`` to log a step while allowing inline code
        blocks. Use ``ScenarioContext.run`` when you want function execution
        to be skipped on cache hits.

        ``adapter`` and ``identity_inputs`` are the public identity-related
        kwargs.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active. Use within 'with' block.")

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
            default_name_template=self.name_template,
            allow_template=True,
            apply_step_defaults=True,
            consist_settings=self.tracker.settings,
            consist_workspace=self.tracker.run_dir,
            consist_state=self._header_record,
            missing_name_error="ScenarioContext.trace requires a step name.",
            python_missing_fn_error="Tracker.trace requires a callable fn.",
            allow_python_without_fn=True,
        )

        resolved_name = resolved_invocation.name
        resolved_model = resolved_invocation.model
        resolved_description = resolved_invocation.description
        resolved_config = resolved_invocation.config
        resolved_adapter = resolved_invocation.adapter
        resolved_identity_inputs = resolved_invocation.identity_inputs
        resolved_tags = self._merge_step_tags(resolved_invocation.tags)
        resolved_facet = self._merge_step_facet(resolved_invocation.facet)
        resolved_outputs = resolved_invocation.outputs
        resolved_output_paths = self._resolve_output_paths(
            resolved_invocation.output_paths
        )
        resolved_inputs = self._resolve_inputs(
            resolved_invocation.inputs,
            resolved_invocation.input_keys,
            resolved_invocation.optional_input_keys,
        )
        resolved_facet_from = resolved_invocation.facet_from
        resolved_facet_schema_version = resolved_invocation.facet_schema_version
        resolved_cache_mode = resolved_invocation.cache_mode
        resolved_cache_hydration = resolved_invocation.cache_hydration
        resolved_cache_version = resolved_invocation.cache_version
        resolved_validate_cached_outputs = resolved_invocation.validate_cached_outputs
        resolved_cache_epoch = resolved_invocation.cache_epoch

        if run_id is None:
            run_id = f"{self.run_id}_{resolved_name}"
        if parent_run_id is None:
            parent_run_id = self.run_id

        self._first_step_started = True
        self._last_step_name = resolved_name

        effective_cache_hydration = (
            resolved_cache_hydration or self.step_cache_hydration
        )

        try:
            self.tracker._active_coupler = self.coupler
            with self.tracker.trace(
                name=resolved_name,
                run_id=run_id,
                model=resolved_model,
                description=resolved_description,
                config=resolved_config,
                adapter=resolved_adapter,
                config_plan_ingest=config_plan_ingest,
                config_plan_profile_schema=config_plan_profile_schema,
                inputs=resolved_inputs,
                input_keys=None,
                optional_input_keys=None,
                depends_on=depends_on,
                tags=resolved_tags,
                facet=resolved_facet,
                facet_from=resolved_facet_from,
                facet_schema_version=resolved_facet_schema_version,
                facet_index=resolved_invocation.facet_index,
                identity_inputs=resolved_identity_inputs,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                outputs=resolved_outputs,
                output_paths=resolved_output_paths,
                capture_dir=capture_dir,
                capture_pattern=capture_pattern,
                cache_mode=resolved_cache_mode,
                cache_hydration=effective_cache_hydration,
                cache_version=resolved_cache_version,
                cache_epoch=resolved_cache_epoch,
                validate_cached_outputs=resolved_validate_cached_outputs,
                code_identity=resolved_invocation.code_identity,
                code_identity_extra_deps=resolved_invocation.code_identity_extra_deps,
                output_mismatch=resolved_invocation.output_mismatch,
                output_missing=resolved_invocation.output_missing,
            ) as t:
                if t.is_cached:
                    current_consist = t.current_consist
                    if current_consist and current_consist.outputs:
                        for artifact in current_consist.outputs:
                            if artifact.key:
                                self.coupler.set(artifact.key, artifact)
                yield t
        finally:
            self.tracker._active_coupler = None
            record = self.tracker.last_run
            if record and self._header_record:
                if record.outputs:
                    self.coupler.update({a.key: a for a in record.outputs})
                self._record_step_in_parent(record.run, record.inputs, record.outputs)

    def _coerce_mapping(self, obj: Any, label: str) -> Dict[str, Any]:
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        if hasattr(obj, "dict") and hasattr(obj, "json"):
            return obj.dict()
        if isinstance(obj, Mapping):
            return dict(obj)
        raise ValueError(
            f"ScenarioContext {label} must be a mapping or Pydantic model."
        )

    def _as_artifact_list(self, value: Any) -> List[Artifact]:
        if value is None:
            return []
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Artifact)]
        if isinstance(value, tuple):
            return [item for item in value if isinstance(item, Artifact)]
        if isinstance(value, Mapping):
            return [item for item in value.values() if isinstance(item, Artifact)]
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return [item for item in value if isinstance(item, Artifact)]
        return []

    def _extract_facet_from_config(
        self, config: Optional[Any], facet_from: List[str]
    ) -> Dict[str, Any]:
        if config is None:
            raise ValueError("facet_from requires a config to extract from.")
        config_dict = self._coerce_mapping(config, "config")
        missing = [key for key in facet_from if key not in config_dict]
        if missing:
            raise KeyError(f"facet_from keys not found in config: {missing}")
        return {key: config_dict[key] for key in facet_from}

    def _record_step_in_parent(self, child_run, child_inputs, child_outputs):
        # Resolve Parent Lists (assuming header_record is the wrapper)
        # If header_record is just a Run, you need to capture its lists in __enter__ too.
        if hasattr(self._header_record, "inputs"):
            parent_run = self._header_record.run
            parent_inputs_list = self._header_record.inputs
            parent_outputs_list = self._header_record.outputs
        else:
            # Fallback if header is weird, but ideally this shouldn't happen with the fix above
            parent_run = self._header_record
            parent_inputs_list = []
            parent_outputs_list = []
        if parent_run is None:
            return

        # --- Smart Merge Logic ---
        parent_output_ids = {a.id for a in parent_outputs_list}
        parent_input_ids = {a.id for a in parent_inputs_list}

        # Merge Outputs
        for artifact in child_outputs:
            if artifact.id not in parent_output_ids:
                parent_outputs_list.append(artifact)
                parent_output_ids.add(artifact.id)

        # Merge Inputs
        for artifact in child_inputs:
            if (
                artifact.id not in parent_input_ids
                and artifact.id not in parent_output_ids
            ):
                parent_inputs_list.append(artifact)
                parent_input_ids.add(artifact.id)

        # --- Record Metadata ---
        summary = {
            "id": child_run.id,
            "model": child_run.model_name,
            "status": child_run.status,
            "description": child_run.description,
            "started_at": (
                child_run.started_at.isoformat() if child_run.started_at else None
            ),
            "ended_at": child_run.ended_at.isoformat() if child_run.ended_at else None,
            "duration_seconds": child_run.duration_seconds,
            "inputs": {str(a.id): a.key for a in child_inputs},
            "outputs": {str(a.id): a.key for a in child_outputs},
        }

        if "steps" not in parent_run.meta:
            parent_run.meta["steps"] = []

        parent_run.meta["steps"].append(summary)

        # --- Force Flush ---
        current_state = self.tracker.current_consist
        self.tracker.current_consist = self._header_record

        self.tracker.persistence.flush_json()
        self.tracker.persistence.sync_run(parent_run)

        # --- NEW: Create database links for parent scenario ---
        if self.tracker.db:
            # Link ALL child artifacts to parent, regardless of deduplication
            # The database merge() handles duplicate links gracefully
            for artifact in child_outputs:
                self.tracker.db.link_artifact_to_run(
                    artifact_id=artifact.id, run_id=parent_run.id, direction="output"
                )

            for artifact in child_inputs:
                # Only link as input if not already an output
                if artifact.id not in parent_output_ids:
                    self.tracker.db.link_artifact_to_run(
                        artifact_id=artifact.id, run_id=parent_run.id, direction="input"
                    )

        self.tracker.current_consist = current_state

    def __enter__(self) -> "ScenarioContext":
        # Enforce No Nesting
        if self.tracker.current_consist is not None:
            raise RuntimeError(
                "Cannot start scenario: another run or scenario is active. "
                "Nested scenarios are not supported."
            )

        # Ensure tag exists
        if "scenario_header" not in self.tags:
            self.tags.append("scenario_header")

        # 1. Start Header Run
        # We use begin_run directly to initialize state
        run_id = self.kwargs.pop("run_id", self.name)
        # Coupler is a runtime-only object and should not be serialized into run meta.
        self.kwargs.pop("coupler", None)
        self.tracker.begin_run(
            run_id=run_id,
            model=self.model,
            config=self.config_arg,
            tags=self.tags,
            **self.kwargs,
        )

        # 2. Capture & Suspend
        # Save the record and clear the tracker's active state
        self._header_record = self.tracker.current_consist
        self._suspended_cache_options = self.tracker.suspend_cache_options()
        self.tracker.current_consist = None

        # Note: We leave the tracker pushed to the global context stack.
        # This ensures calls to `consist.log_artifact()` fail with our custom error
        # rather than "No active tracker".

        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        missing_error: Optional[RuntimeError] = None
        missing_outputs: list[str] = []
        if exc_type is None:
            missing_outputs = self.coupler.missing_declared_outputs()
            if missing_outputs:
                missing_error = RuntimeError(
                    f"Scenario missing declared outputs: {', '.join(missing_outputs)}."
                )

        # 1. Restore Header Context
        self.tracker.current_consist = self._header_record
        if self._suspended_cache_options is not None:
            self.tracker.restore_cache_options(self._suspended_cache_options)
        if self._header_record is None:
            raise RuntimeError("Scenario header record was not captured.")

        # 2. Handle Status
        status = "failed" if exc_type or missing_error else "completed"
        error_for_end_run: Optional[Exception] = None
        if missing_error is not None:
            error_for_end_run = missing_error
        elif isinstance(exc_val, Exception):
            error_for_end_run = exc_val
        if exc_type or missing_error:
            # Enrich metadata with failure context
            self._header_record.run.meta["failed_with"] = str(exc_val or missing_error)
            if self._last_step_name:
                self._header_record.run.meta["failed_step"] = self._last_step_name
            if missing_outputs:
                self._header_record.run.meta["missing_outputs"] = missing_outputs

        # 3. End Run
        # This handles DB sync, JSON flush, and event emission
        import logging

        logging.debug(
            f"[ScenarioContext] Ending header {self.run_id} with status={status}"
        )
        self.tracker.end_run(status=status, error=error_for_end_run)

        # Defensive: ensure header status/meta are persisted even if future end_run
        # behavior changes. We temporarily restore the header to flush/sync explicitly.
        if self._header_record:
            # Force the run object to reflect the final status before syncing.
            self._header_record.run.status = status
            self.tracker.current_consist = self._header_record
            logging.debug(
                "[ScenarioContext] Syncing header after end_run: "
                f"id={self._header_record.run.id}, status={self._header_record.run.status}"
            )
            self.tracker.persistence.flush_json()
            # Use a fresh Run clone to avoid any ORM identity/cache oddities
            try:
                from consist.models.run import Run

                cloned = Run(**self._header_record.run.model_dump())
                logging.debug(
                    f"[ScenarioContext] Syncing cloned header run id={cloned.id} status={cloned.status}"
                )
                self.tracker.persistence.sync_run(cloned)
            except Exception:
                # Fallback to direct sync on the original object
                logging.debug(
                    f"[ScenarioContext] Syncing original header run id={self._header_record.run.id} status={self._header_record.run.status}"
                )
                self.tracker.persistence.sync_run(self._header_record.run)

        # 4. Final Cleanup
        # end_run sets current_consist to None, but we ensure it matches expected state
        self.tracker.current_consist = None
        self._header_record = None
        self._suspended_cache_mode = None

        if missing_error:
            raise missing_error

        return False  # Propagate exceptions
