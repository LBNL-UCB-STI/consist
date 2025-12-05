import logging
from typing import Callable, List, Optional
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
        self._on_start.append(callback)
        return callback

    def on_run_complete(self, callback: Callable[[Run, List[Artifact]], None]):
        self._on_complete.append(callback)
        return callback

    def on_run_failed(self, callback: Callable[[Run, Exception], None]):
        self._on_failed.append(callback)
        return callback

    # --- Emission ---

    def emit_start(self, run: Run):
        for hook in self._on_start:
            try:
                hook(run)
            except Exception as e:
                logging.warning(f"on_run_start hook failed: {e}")

    def emit_complete(self, run: Run, outputs: List[Artifact]):
        for hook in self._on_complete:
            try:
                hook(run, outputs)
            except Exception as e:
                logging.warning(f"on_run_complete hook failed: {e}")

    def emit_failed(self, run: Run, error: Exception):
        for hook in self._on_failed:
            try:
                hook(run, error)
            except Exception as e:
                logging.warning(f"on_run_failed hook failed: {e}")