from __future__ import annotations

import atexit
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Iterator


@dataclass
class _DBProfileEntry:
    count: int = 0
    error_count: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0


class _DBCallProfiler:
    """Internal opt-in DB profiling helper for Consist maintainers.

    Enabled only when the ``CONSIST_DB_PROFILE_PATH`` environment variable is set.
    """

    def __init__(self) -> None:
        self.output_path = os.environ.get("CONSIST_DB_PROFILE_PATH")
        self.enabled = bool(self.output_path)
        self._lock = threading.Lock()
        self._registered = False
        self._calls: dict[str, _DBProfileEntry] = {}

    def _ensure_registered(self) -> None:
        if not self.enabled or self._registered:
            return
        atexit.register(self.dump)
        self._registered = True

    @contextmanager
    def track(self, name: str) -> Iterator[None]:
        if not self.enabled:
            yield
            return
        self._ensure_registered()
        started = time.perf_counter()
        failed = False
        try:
            yield
        except Exception:
            failed = True
            raise
        finally:
            elapsed = time.perf_counter() - started
            with self._lock:
                entry = self._calls.setdefault(name, _DBProfileEntry())
                entry.count += 1
                entry.total_seconds += elapsed
                entry.max_seconds = max(entry.max_seconds, elapsed)
                if failed:
                    entry.error_count += 1

    def dump(self) -> None:
        if not self.enabled or not self.output_path:
            return
        with self._lock:
            calls = {
                name: {
                    "count": entry.count,
                    "error_count": entry.error_count,
                    "total_seconds": round(entry.total_seconds, 6),
                    "avg_milliseconds": round(
                        (entry.total_seconds / entry.count) * 1000, 3
                    )
                    if entry.count
                    else 0.0,
                    "max_milliseconds": round(entry.max_seconds * 1000, 3),
                }
                for name, entry in sorted(
                    self._calls.items(),
                    key=lambda item: item[1].total_seconds,
                    reverse=True,
                )
            }
            payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "calls": calls,
            }
        try:
            output = Path(self.output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
        except Exception as exc:
            logging.warning(
                "Failed to write Consist DB profile to %s: %s", self.output_path, exc
            )


DB_CALL_PROFILER = _DBCallProfiler()
