from contextlib import contextmanager
from types import MappingProxyType
from typing import List, Optional, Dict, Any

from consist import Artifact
from consist.models.run import ConsistRecord
from typing import TYPE_CHECKING
from consist.core.coupler import Coupler
from consist.types import ArtifactRef

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
        **kwargs: Any,
    ):
        self.tracker = tracker
        self.name = name
        self.model = model
        self.config_arg = config or {}
        self.tags = tags or []
        self.kwargs = kwargs

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

    @contextmanager
    def step(self, name: str, **kwargs):
        """
        Execute a child run as part of this scenario.

        Parameters
        ----------
        name : str
            Name of the step, used to derive the run ID and default model.
        **kwargs : Any
            Arguments forwarded to ``Tracker.start_run`` such as ``config`` or ``tags``.

        Yields
        ------
        Tracker
            Active tracker for the step run.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active.")

        self._first_step_started = True
        self._last_step_name = name

        # Ergonomic helper: allow declaring step inputs by Coupler key, while still
        # ensuring inputs are registered before begin_run() computes the cache signature.
        #
        # Example:
        #   with sc.step("transform", input_keys=["raw"]):
        #       raw = sc.coupler.require("raw")
        #       ...
        input_keys = kwargs.pop("input_keys", None)
        if input_keys:
            if isinstance(input_keys, str):
                input_keys = [input_keys]
            coupler_inputs = [self.coupler.require(k) for k in input_keys]
            existing_inputs = kwargs.get("inputs")
            if existing_inputs:
                kwargs["inputs"] = list(existing_inputs) + coupler_inputs
            else:
                kwargs["inputs"] = coupler_inputs

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
                            child_inputs = list(t.current_inputs)
                        elif hasattr(ctx, "inputs"):
                            child_inputs = list(ctx.inputs)

                        if hasattr(t, "current_outputs"):
                            child_outputs = list(t.current_outputs)
                        elif hasattr(ctx, "outputs"):
                            child_outputs = list(ctx.outputs)

        finally:
            # 3. Bubble Up (Tracker is now closed, but we have our copies)
            if child_run and self._header_record:
                self._record_step_in_parent(child_run, child_inputs, child_outputs)

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

        self.tracker._flush_json()
        self.tracker._sync_run_to_db(parent_run)

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
        self._suspended_cache_mode = self.tracker._active_run_cache_mode
        self.tracker.current_consist = None
        self.tracker._active_run_cache_mode = None

        # Note: We leave the tracker pushed to the global context stack.
        # This ensures calls to `consist.log_artifact()` fail with our custom error
        # rather than "No active tracker".

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. Restore Header Context
        self.tracker.current_consist = self._header_record
        self.tracker._active_run_cache_mode = self._suspended_cache_mode

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
            self.tracker._flush_json()
            # Use a fresh Run clone to avoid any ORM identity/cache oddities
            try:
                from consist.models.run import Run

                cloned = Run(**self._header_record.run.model_dump())
                logging.debug(
                    f"[ScenarioContext] Syncing cloned header run id={cloned.id} status={cloned.status}"
                )
                self.tracker._sync_run_to_db(cloned)
            except Exception:
                # Fallback to direct sync on the original object
                logging.debug(
                    f"[ScenarioContext] Syncing original header run id={self._header_record.run.id} status={self._header_record.run.status}"
                )
                self.tracker._sync_run_to_db(self._header_record.run)

        # 4. Final Cleanup
        # end_run sets current_consist to None, but we ensure it matches expected state
        self.tracker.current_consist = None
        self._header_record = None
        self._suspended_cache_mode = None

        return False  # Propagate exceptions
