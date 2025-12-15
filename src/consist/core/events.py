import logging
from typing import Callable, List
from consist.models.run import Run
from consist.models.artifact import Artifact


class EventManager:
    """Manage run lifecycle hooks and emit lifecycle events."""

    def __init__(self):
        self._on_start: List[Callable[[Run], None]] = []
        self._on_complete: List[Callable[[Run, List[Artifact]], None]] = []
        self._on_failed: List[Callable[[Run, Exception], None]] = []

    # --- Registration ---

    def on_run_start(self, callback: Callable[[Run], None]):
        """
        Register a callback executed when a run starts.

        Parameters
        ----------
        callback : Callable[[Run], None]
            Handler invoked with the ``Run`` instance.

        Returns
        -------
        Callable[[Run], None]
            The registered callback, supporting decorator syntax.
        """
        self._on_start.append(callback)
        return callback

    def on_run_complete(self, callback: Callable[[Run, List[Artifact]], None]):
        """
        Register a callback executed when a run finishes successfully.

        Parameters
        ----------
        callback : Callable[[Run, List[Artifact]], None]
            Handler invoked with the completed ``Run`` and its output ``Artifact`` list.

        Returns
        -------
        Callable[[Run, List[Artifact]], None]
            The registered callback for decorator usage.
        """
        self._on_complete.append(callback)
        return callback

    def on_run_failed(self, callback: Callable[[Run, Exception], None]):
        """
        Register a callback executed when a run fails.

        Parameters
        ----------
        callback : Callable[[Run, Exception], None]
            Handler invoked with the failed ``Run`` and the raised exception.

        Returns
        -------
        Callable[[Run, Exception], None]
            The registered callback for decorator usage.
        """
        self._on_failed.append(callback)
        return callback

    # --- Emission ---

    def emit_start(self, run: Run):
        """
        Invoke start hooks for a run.

        Parameters
        ----------
        run : Run
            Run that just began.
        """
        for hook in self._on_start:
            try:
                hook(run)
            except Exception as e:
                logging.warning(f"on_run_start hook failed: {e}")

    def emit_complete(self, run: Run, outputs: List[Artifact]):
        """
        Invoke completion hooks for a run.

        Parameters
        ----------
        run : Run
            Completed run instance.
        outputs : List[Artifact]
            Artifacts produced by the run.
        """
        for hook in self._on_complete:
            try:
                hook(run, outputs)
            except Exception as e:
                logging.warning(f"on_run_complete hook failed: {e}")

    def emit_failed(self, run: Run, error: Exception):
        """
        Invoke failure hooks for a run.

        Parameters
        ----------
        run : Run
            Run that threw an exception.
        error : Exception
            Exception raised during execution.
        """
        for hook in self._on_failed:
            try:
                hook(run, error)
            except Exception as e:
                logging.warning(f"on_run_failed hook failed: {e}")
