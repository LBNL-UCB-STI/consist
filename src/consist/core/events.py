import logging
from typing import Callable, List
from consist.models.run import Run
from consist.models.artifact import Artifact


class EventManager:
    """Service responsible for managing Run Lifecycle Hooks."""

    def __init__(self):
        self._on_start: List[Callable[[Run], None]] = []
        self._on_complete: List[Callable[[Run, List[Artifact]], None]] = []
        self._on_failed: List[Callable[[Run, Exception], None]] = []

    # --- Registration ---

    def on_run_start(self, callback: Callable[[Run], None]):
        """
        Register a callback to be invoked when a run starts.

        Parameters
        ----------
        callback : Callable[[Run], None]
            Function to be called with the ``Run`` instance when a run starts.

        Returns
        -------
        Callable[[Run], None]
            The same callback, allowing decorator usage.
        """
        self._on_start.append(callback)
        return callback

    def on_run_complete(self, callback: Callable[[Run, List[Artifact]], None]):
        """
        Register a callback to be invoked when a run completes.

        Parameters
        ----------
        callback : Callable[[Run, List[Artifact]], None]
            Function called with the ``Run`` instance and a list of output ``Artifact`` objects.

        Returns
        -------
        Callable[[Run, List[Artifact]], None]
            The same callback, allowing decorator usage.
        """
        self._on_complete.append(callback)
        return callback

    def on_run_failed(self, callback: Callable[[Run, Exception], None]):
        """
        Register a callback to be invoked when a run fails.

        Parameters
        ----------
        callback : Callable[[Run, Exception], None]
            Function called with the ``Run`` instance and the exception that caused the failure.

        Returns
        -------
        Callable[[Run, Exception], None]
            The same callback, allowing decorator usage.
        """
        self._on_failed.append(callback)
        return callback

    # --- Emission ---

    def emit_start(self, run: Run):
        """
        Emit the start event to all registered ``on_run_start`` callbacks.

        Parameters
        ----------
        run : Run
            The ``Run`` instance that has started.
        """
        for hook in self._on_start:
            try:
                hook(run)
            except Exception as e:
                logging.warning(f"on_run_start hook failed: {e}")

    def emit_complete(self, run: Run, outputs: List[Artifact]):
        """
        Emit the completion event to all registered ``on_run_complete`` callbacks.

        Parameters
        ----------
        run : Run
            The ``Run`` instance that has completed.
        outputs : List[Artifact]
            List of output ``Artifact`` objects produced by the run.
        """
        for hook in self._on_complete:
            try:
                hook(run, outputs)
            except Exception as e:
                logging.warning(f"on_run_complete hook failed: {e}")

    def emit_failed(self, run: Run, error: Exception):
        """
        Emit the failure event to all registered ``on_run_failed`` callbacks.

        Parameters
        ----------
        run : Run
            The ``Run`` instance that failed.
        error : Exception
            The exception that caused the failure.
        """
        for hook in self._on_failed:
            try:
                hook(run, error)
            except Exception as e:
                logging.warning(f"on_run_failed hook failed: {e}")
