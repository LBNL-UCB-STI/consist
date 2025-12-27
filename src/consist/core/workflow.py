from contextlib import contextmanager
from collections.abc import Iterable
from types import MappingProxyType
from typing import List, Optional, Dict, Any, Callable, Mapping

from consist import Artifact
from consist.models.run import ConsistRecord
from typing import TYPE_CHECKING
from consist.core.coupler import Coupler
from consist.types import ArtifactRef, FacetLike
from pathlib import Path

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


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


class ScenarioContext:
    """
    Manage a scenario header run and its child steps.

    The context exposes ``step()`` helpers that suspend the parent header run,
    execute child runs sequentially, and aggregate artifacts/metadata back into the
    header record.
    """

    def __init__(
        self,
        tracker: "Tracker",
        name: str,
        config: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        model: str = "scenario",
        step_cache_hydration: Optional[str] = None,
        **kwargs: Any,
    ):
        self.tracker = tracker
        self.name = name
        self.model = model
        self.config_arg = config or {}
        self.tags = tags or []
        self.kwargs = kwargs
        self.step_cache_hydration = step_cache_hydration

        # Internal State
        self._header_record: Optional[ConsistRecord] = None
        self._suspended_cache_mode: Optional[str] = None
        self._inputs: Dict[str, Artifact] = {}
        self._first_step_started: bool = False
        self._last_step_name: Optional[str] = None
        self.coupler = Coupler(tracker)

    @property
    def run_id(self) -> str:
        """The Run ID of the scenario header."""
        return self._header_record.run.id if self._header_record else self.name

    @property
    def config(self) -> MappingProxyType:
        """Read-only view of the scenario configuration."""
        if self._header_record:
            return MappingProxyType(self._header_record.config)
        return MappingProxyType(self.config_arg)

    @property
    def inputs(self) -> MappingProxyType:
        """Read-only view of registered exogenous inputs."""
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
                "Register all inputs before calling scenario.step()."
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

    def run_step(
        self,
        name: str,
        fn: Callable[..., Any],
        *fn_args: Any,
        run_id: Optional[str] = None,
        model: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        input_keys: Optional[object] = None,
        optional_input_keys: Optional[object] = None,
        inputs: Optional[List[ArtifactRef]] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        cache_hydration: Optional[str] = None,
        materialize_cached_output_paths: Optional[Mapping[str, Path]] = None,
        materialize_cached_outputs_dir: Optional[Path] = None,
        validate_cached_outputs: str = "lazy",
        iteration: Optional[int] = None,
        year: Optional[int] = None,
        **fn_kwargs: Any,
    ) -> Dict[str, Artifact]:
        """
        Run a function-shaped scenario step that can be skipped on cache hits.

        This complements the imperative `scenario.step(...)` context manager.
        When Consist finds a cache hit for this step, the callable is NOT executed;
        instead, cached outputs are hydrated and placed into the scenario Coupler.

        Parameters
        ----------
        name : str
            Step name. Used to derive the default child run id (scenario_id + "_" + name).
        fn : Callable[..., Any]
            Callable to execute on cache misses. Can be an imported function or a bound
            method like `beamPreprocessor.run`.
        *fn_args, **fn_kwargs
            Arguments forwarded to `fn(...)` on cache misses.
        facet : Optional[FacetLike]
            Explicit facet payload to persist with the run.
        facet_from : Optional[List[str]]
            List of config keys to extract into facets (merged with explicit `facet`).
        input_keys : Optional[object]
            A string or list of strings naming Coupler keys to declare as step inputs.
            Inputs are resolved before the run starts (so caching and `db_fallback="inputs-only"`
            behave correctly).
        optional_input_keys : Optional[object]
            A string or list of strings naming Coupler keys to declare as inputs only
            when present (useful for warm-starting iterative runs).
        inputs : Optional[List[ArtifactRef]]
            Additional explicit inputs to declare for caching/provenance.
        outputs / output_paths
            Output contract for this step. Exactly one of these must be provided:
            - `outputs=[...]`: assert that `fn(...)` logs these output keys during execution.
            - `output_paths={key: ref, ...}`: after `fn(...)` returns, log these paths as outputs.
        output_paths resolution rules
            For `output_paths`, each value is interpreted as:
            - relative path → relative to the step run directory (`t.run_dir`)
            - URI-like string (contains `"://"`) → resolved via mounts (`tracker.resolve_uri`)
            - absolute path → used as-is
        cache_hydration : Optional[str]
            Controls whether cached bytes are materialized for inputs/outputs. If
            omitted, the scenario default (if any) is used.
            See `docs/caching-and-hydration.md` for the supported policies.

        Returns
        -------
        Dict[str, Artifact]
            Mapping of declared output key to hydrated `Artifact` for that key.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active. Use within 'with' block.")

        if (outputs is None) == (output_paths is None):
            raise ValueError(
                "ScenarioContext.run_step requires exactly one of `outputs=[...]` or `output_paths={...}`."
            )

        declared_output_keys: List[str]
        if output_paths is not None:
            declared_output_keys = list(output_paths.keys())
        else:
            declared_output_keys = list(outputs or [])

        if not declared_output_keys:
            raise ValueError(
                "ScenarioContext.run_step requires at least one output key."
            )

        resolved_inputs: List[ArtifactRef] = []
        if input_keys:
            keys_list: List[str]
            if isinstance(input_keys, str):
                keys_list = [input_keys]
            else:
                keys_list = list(input_keys)  # type: ignore[arg-type]
            resolved_inputs.extend([self.coupler.require(k) for k in keys_list])
        if optional_input_keys:
            keys_list: List[str]
            if isinstance(optional_input_keys, str):
                keys_list = [optional_input_keys]
            else:
                keys_list = list(optional_input_keys)  # type: ignore[arg-type]
            for key in keys_list:
                artifact = self.coupler.get(key)
                if artifact is not None:
                    resolved_inputs.append(artifact)
        if inputs:
            resolved_inputs.extend(list(inputs))

        def _resolve_output_ref(t: "Tracker", ref: ArtifactRef) -> ArtifactRef:
            if isinstance(ref, Artifact):
                return ref
            ref_str = str(ref)
            if "://" in ref_str:
                scheme = ref_str.split("://", 1)[0]
                if scheme != "file" and scheme not in t.mounts:
                    raise ValueError(
                        f"output_paths for key must use a mounted scheme or file:// (got {ref_str!r}). "
                        f"Known schemes: {sorted(t.mounts.keys())}"
                    )
                return Path(t.resolve_uri(ref_str))
            ref_path = Path(ref_str)
            if not ref_path.is_absolute():
                return t.run_dir / ref_path
            return ref_path

        step_kwargs: Dict[str, Any] = {
            "cache_mode": cache_mode,
        }
        effective_cache_hydration = cache_hydration or self.step_cache_hydration
        if effective_cache_hydration is not None:
            step_kwargs["cache_hydration"] = effective_cache_hydration
        step_kwargs["materialize_cached_output_paths"] = (
            dict(materialize_cached_output_paths)
            if materialize_cached_output_paths
            else None
        )
        step_kwargs["materialize_cached_outputs_dir"] = materialize_cached_outputs_dir
        step_kwargs["validate_cached_outputs"] = validate_cached_outputs
        if resolved_inputs:
            step_kwargs["inputs"] = resolved_inputs
        if run_id:
            step_kwargs["run_id"] = run_id
        if model:
            step_kwargs["model"] = model
        if config is not None:
            step_kwargs["config"] = config
        if facet is not None:
            step_kwargs["facet"] = facet
        if facet_from is not None:
            step_kwargs["facet_from"] = facet_from
        if tags is not None:
            step_kwargs["tags"] = tags
        if description is not None:
            step_kwargs["description"] = description
        if iteration is not None:
            step_kwargs["iteration"] = iteration
        if year is not None:
            step_kwargs["year"] = year

        with self.step(name, **step_kwargs) as t:
            if t.is_cached:
                hydrated: Dict[str, Artifact] = {}
                for k in declared_output_keys:
                    art = t.cached_output(key=k)
                    if art is None:
                        raise RuntimeError(
                            f"Cache hit for step {name!r} but missing cached output key={k!r}. "
                            "This cache entry does not satisfy the step output contract."
                        )
                    self.coupler.set(k, art)
                    hydrated[k] = art
                return hydrated

            fn(*fn_args, **fn_kwargs)

            if output_paths is not None:
                produced: Dict[str, Artifact] = {}
                for k, ref in output_paths.items():
                    resolved_ref = _resolve_output_ref(t, ref)
                    produced[k] = t.log_artifact(
                        resolved_ref, key=k, direction="output"
                    )
                    self.coupler.set(k, produced[k])
                return produced

            # outputs-based contract: ensure the callable logged the declared keys.
            by_key: Dict[str, Artifact] = {a.key: a for a in t.current_consist.outputs}
            missing = [k for k in declared_output_keys if k not in by_key]
            if missing:
                raise RuntimeError(
                    f"Step {name!r} did not produce declared outputs: {missing}. "
                    "Either log them via tracker.log_artifact(..., key=...), or use output_paths={...}."
                )
            for k in declared_output_keys:
                self.coupler.set(k, by_key[k])
            return {k: by_key[k] for k in declared_output_keys}

    @contextmanager
    def step(self, name: str, **kwargs):
        """
        Execute a child run as part of this scenario.

        Parameters
        ----------
        name : str
            Name of the step, used to derive the run ID and default model.
        **kwargs : Any
            Arguments forwarded to ``Tracker.start_run`` such as ``config``, ``facet_from``,
            or ``tags``.

        Yields
        ------
        Tracker
            Active tracker for the step run.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active.")

        self._first_step_started = True
        self._last_step_name = name

        # Apply scenario default cache hydration for steps unless overridden.
        if "cache_hydration" not in kwargs and self.step_cache_hydration is not None:
            kwargs["cache_hydration"] = self.step_cache_hydration

        # Ergonomic helper: allow declaring step inputs by Coupler key, while still
        # ensuring inputs are registered before begin_run() computes the cache signature.
        #
        # Example:
        #   with sc.step("transform", input_keys=["raw"]):
        #       raw = sc.coupler.require("raw")
        #       ...
        input_keys = kwargs.pop("input_keys", None)
        optional_input_keys = kwargs.pop("optional_input_keys", None)
        if input_keys or optional_input_keys:
            inputs = list(kwargs.get("inputs") or [])
            if input_keys:
                if isinstance(input_keys, str):
                    input_keys = [input_keys]
                inputs.extend(self.coupler.require(k) for k in input_keys)
            if optional_input_keys:
                if isinstance(optional_input_keys, str):
                    optional_input_keys = [optional_input_keys]
                for key in optional_input_keys:
                    artifact = self.coupler.get(key)
                    if artifact is not None:
                        inputs.append(artifact)
            if inputs:
                kwargs["inputs"] = inputs

        facet_from = kwargs.pop("facet_from", None)
        if facet_from:
            if isinstance(facet_from, str):
                raise ValueError("facet_from must be a list of config keys.")
            facet_keys = list(facet_from)
            config = kwargs.get("config")
            derived = self._extract_facet_from_config(config, facet_keys)
            facet = kwargs.get("facet")
            if facet is not None:
                facet_dict = self._coerce_mapping(facet, "facet")
                merged = dict(derived)
                merged.update(facet_dict)
                kwargs["facet"] = self.tracker.identity.normalize_json(merged)
            else:
                kwargs["facet"] = self.tracker.identity.normalize_json(derived)

        # 1. Construct Hierarchical ID & Enforce Linkage
        # Default ID: scenario_name + "_" + step_name
        run_id = kwargs.pop("run_id", f"{self.run_id}_{name}")
        kwargs["parent_run_id"] = self.run_id

        # Default model name to step name if not provided
        if "model" not in kwargs:
            kwargs["model"] = name

        # Containers for the captured state
        child_run = None
        child_inputs = []
        child_outputs = []

        try:
            # We open the tracker context...
            with self.tracker.start_run(run_id=run_id, **kwargs) as t:
                try:
                    # 1. Yield to let the user code run (This populates the artifacts)
                    yield t
                finally:
                    # 2. Capture State NOW (After code ran, but before tracker closes)
                    # We wrap in finally to ensure we capture partial state even if the step fails

                    # Resolve the context object (Run wrapper)
                    ctx = t.current_consist

                    if ctx:
                        # Capture Run Object
                        child_run = getattr(ctx, "run", ctx)

                        # Capture Artifacts (Copy to list so they survive context exit)
                        # Check 'current_inputs' (tracker attr) or 'inputs' (context attr)
                        if hasattr(t, "current_inputs"):
                            child_inputs = self._as_artifact_list(
                                getattr(t, "current_inputs")
                            )
                        elif hasattr(ctx, "inputs"):
                            child_inputs = self._as_artifact_list(ctx.inputs)

                        if hasattr(t, "current_outputs"):
                            child_outputs = self._as_artifact_list(
                                getattr(t, "current_outputs")
                            )
                        elif hasattr(ctx, "outputs"):
                            child_outputs = self._as_artifact_list(ctx.outputs)

        finally:
            # 3. Bubble Up (Tracker is now closed, but we have our copies)
            if child_run and self._header_record:
                self._record_step_in_parent(child_run, child_inputs, child_outputs)

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
            return list(value)
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, Mapping):
            return list(value.values())
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return list(value)
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

    def __enter__(self):
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. Restore Header Context
        self.tracker.current_consist = self._header_record
        if self._suspended_cache_options is not None:
            self.tracker.restore_cache_options(self._suspended_cache_options)
        if self._header_record is None:
            raise RuntimeError("Scenario header record was not captured.")

        # 2. Handle Status
        status = "failed" if exc_type else "completed"
        if exc_type:
            # Enrich metadata with failure context
            self._header_record.run.meta["failed_with"] = str(exc_val)
            if self._last_step_name:
                self._header_record.run.meta["failed_step"] = self._last_step_name

        # 3. End Run
        # This handles DB sync, JSON flush, and event emission
        import logging

        logging.debug(
            f"[ScenarioContext] Ending header {self.run_id} with status={status}"
        )
        self.tracker.end_run(status=status)

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

        return False  # Propagate exceptions
