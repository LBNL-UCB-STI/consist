"""Internal phase attribution hooks for benchmark-only profiling."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import ContextManager, Iterator, Protocol


class PhaseProfiler(Protocol):
    """Minimal protocol shared by benchmark attribution timers."""

    def track(self, label: str) -> ContextManager[None]:
        """Return a context manager that records elapsed time for ``label``."""


_BEGIN_RUN_PHASE_PROFILER: ContextVar[PhaseProfiler | None] = ContextVar(
    "_BEGIN_RUN_PHASE_PROFILER",
    default=None,
)


@contextmanager
def begin_run_phase_profiler(profiler: PhaseProfiler) -> Iterator[None]:
    """Temporarily enable internal begin-run phase attribution."""
    token = _BEGIN_RUN_PHASE_PROFILER.set(profiler)
    try:
        yield
    finally:
        _BEGIN_RUN_PHASE_PROFILER.reset(token)


@contextmanager
def track_begin_run_phase(label: str) -> Iterator[None]:
    """Record a begin-run phase when benchmark attribution is active."""
    profiler = _BEGIN_RUN_PHASE_PROFILER.get()
    if profiler is None:
        yield
        return

    with profiler.track(label):
        yield
