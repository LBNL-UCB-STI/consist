from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import List, Optional, Dict, Any, Union

from consist import Artifact
from consist.models.run import ConsistRecord
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class OutputCapture:
    """
    A helper object to temporarily hold artifacts captured during a `capture_outputs` block.

    This class provides a convenient way for users to access the artifacts that were
    automatically logged by the system immediately after a `capture_outputs` context manager
    finishes execution.

    Attributes:
        artifacts (List[Artifact]): A list of `Artifact` objects that were
                                    captured and logged during the `capture_outputs` block.
    """

    def __init__(self) -> None:
        """
        Initializes the OutputCapture object.

        This constructor sets up an empty list to store `Artifact` objects that
        will be captured during a `capture_outputs` context.
        """
        self.artifacts: List[Artifact] = []


class ScenarioContext:
    """
    Context manager for managing a Scenario Header Run and its constituent steps.

    This class is returned by `tracker.scenario()` and manages the lifecycle of a parent
    "header" run. It temporarily suspends the header run to allow child "step" runs
    to execute sequentially without violating the tracker's single-active-run constraint.
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

    def add_input(self, path: Union[str, Path], key: str, **kwargs) -> Artifact:
        """
        Register an exogenous input to the scenario header.

        Must be called before any steps are started. These inputs are logged to the
        header run to represent data entering the workflow.

        Args:
            path: File path to the input.
            key: Semantic key for the input (e.g., "population").
            **kwargs: Additional metadata for the artifact.

        Returns:
            Artifact: The logged input artifact.

        Raises:
            RuntimeError: If called after `step()` has been used.
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
        Execute a step run within the scenario.

        Wraps `tracker.start_run` with logic to:
        1. Auto-generate Run ID: `{scenario_id}_{step_name}`
        2. Link parent_run_id to the scenario.

        Args:
            name (str): Name of the step.
            **kwargs: Arguments passed to `tracker.start_run` (model, config, etc).

        Yields:
            Tracker: The tracker instance.
        """
        if not self._header_record:
            raise RuntimeError("Scenario not active.")

        self._first_step_started = True
        self._last_step_name = name

        # 1. Construct Hierarchical ID & Enforce Linkage
        # Default ID: scenario_name + "_" + step_name
        run_id = kwargs.pop("run_id", f"{self.run_id}_{name}")
        kwargs["parent_run_id"] = self.run_id

        # Default model name to step name if not provided
        if "model" not in kwargs:
            kwargs["model"] = name

        # 2. Delegate to standard start_run
        # The tracker is currently suspended (current_consist is None), so this is allowed.
        with self.tracker.start_run(run_id=run_id, **kwargs) as t:
            yield t

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
        self.tracker.end_run(status=status)

        # 4. Final Cleanup
        # end_run sets current_consist to None, but we ensure it matches expected state
        self.tracker.current_consist = None
        self._header_record = None
        self._suspended_cache_mode = None

        return False  # Propagate exceptions