from __future__ import annotations

import atexit
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional

from consist.store.detect import (
    ConsistError,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
)

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

RETRYABLE_DB_ERROR_MARKERS = (
    "database is locked",
    "database is busy",
    "io error",
    "lock",
    "already active",
    "already open",
    "another connection",
    "another process",
)


@dataclass
class _DBProfileEntry:
    count: int = 0
    error_count: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0


class _DBCallProfiler:
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


NON_RETRYABLE_DB_ERROR_MARKERS = (
    "database does not exist",
    "no such file or directory",
)


def is_retryable_db_error(message: str) -> bool:
    normalized = message.lower()
    if any(marker in normalized for marker in NON_RETRYABLE_DB_ERROR_MARKERS):
        return False
    return any(marker in normalized for marker in RETRYABLE_DB_ERROR_MARKERS)


class DatabaseRuntimeOps:
    """
    Runtime-only database operations for ``DatabaseManager`` over ``StoreLedger``.

    Provides bounded exponential-backoff retry behavior and optional DB call profiling.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def execute_with_retry(
        self,
        func: Callable[[], Any],
        operation_name: str = "db_op",
        retries: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        import duckdb

        default_retries = self._db._lock_retries
        base_sleep_seconds = self._db._lock_base_sleep_seconds
        max_sleep_seconds = self._db._lock_max_sleep_seconds
        retry_count = default_retries if retries is None else max(1, int(retries))
        for i in range(retry_count):
            try:
                with DB_CALL_PROFILER.track(operation_name):
                    return func()
            except (duckdb.Error, OSError, ConsistError) as exc:
                if isinstance(
                    exc, (DatabaseMigrationRequired, IncompatibleSchemaError)
                ):
                    raise
                if is_retryable_db_error(str(exc)):
                    if i == retry_count - 1:
                        raise
                    sleep_time = min(
                        (base_sleep_seconds * (1.5**i)) + random.uniform(0.05, 0.2),
                        max_sleep_seconds,
                    )
                    time.sleep(sleep_time)
                else:
                    raise
        raise RuntimeError(f"Concurrency retry limit exceeded in {operation_name}")
