from __future__ import annotations

import atexit
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm.exc import ConcurrentModificationError
from sqlmodel import Session

from consist.core._db_ops_base import _DatabaseOpsBase

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
        self._session_open_count = 0
        self._session_reuse_count = 0
        self._session_close_count = 0
        self._session_live_total_seconds = 0.0
        self._session_live_max_seconds = 0.0

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

    def note_session_open(self) -> None:
        if not self.enabled:
            return
        self._ensure_registered()
        with self._lock:
            self._session_open_count += 1

    def note_session_reuse(self) -> None:
        if not self.enabled:
            return
        self._ensure_registered()
        with self._lock:
            self._session_reuse_count += 1

    def note_session_close(self, *, live_seconds: float) -> None:
        if not self.enabled:
            return
        self._ensure_registered()
        with self._lock:
            self._session_close_count += 1
            self._session_live_total_seconds += live_seconds
            self._session_live_max_seconds = max(
                self._session_live_max_seconds, live_seconds
            )

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
                "session_scope": {
                    "outer_session_open_count": self._session_open_count,
                    "shared_session_reuse_count": self._session_reuse_count,
                    "outer_session_close_count": self._session_close_count,
                    "total_live_seconds": round(self._session_live_total_seconds, 6),
                    "avg_live_milliseconds": round(
                        (
                            self._session_live_total_seconds
                            / self._session_close_count
                            * 1000
                        ),
                        3,
                    )
                    if self._session_close_count
                    else 0.0,
                    "max_live_milliseconds": round(
                        self._session_live_max_seconds * 1000, 3
                    ),
                },
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


def is_retryable_db_error(message: str) -> bool:
    normalized = message.lower()
    return any(marker in normalized for marker in RETRYABLE_DB_ERROR_MARKERS)


class DatabaseRuntimeOps(_DatabaseOpsBase):
    """
    Runtime-only database operations extracted from ``DatabaseManager``.

    This service currently owns retry behavior, shared session scoping, and
    optional DB profiling. It still operates over the concrete
    ``DatabaseManager`` via ``_DatabaseOpsBase`` while the refactor remains in a
    transitional state.
    """

    def _rollback_session(self, session: Session) -> None:
        try:
            session.rollback()
        except Exception:
            return

    def execute_with_retry(
        self,
        func: Callable,
        operation_name: str = "db_op",
        retries: Optional[int] = None,
        **kwargs,
    ) -> Any:
        default_retries = getattr(self, "_lock_retries", 20)
        base_sleep_seconds = getattr(self, "_lock_base_sleep_seconds", 0.1)
        max_sleep_seconds = getattr(self, "_lock_max_sleep_seconds", 2.0)
        retry_count = default_retries if retries is None else max(1, int(retries))
        for i in range(retry_count):
            try:
                return func()
            except (OperationalError, DatabaseError) as e:
                if is_retryable_db_error(str(e)):
                    if i == retry_count - 1:
                        raise e
                    sleep_time = min(
                        (base_sleep_seconds * (1.5**i)) + random.uniform(0.05, 0.2),
                        max_sleep_seconds,
                    )
                    time.sleep(sleep_time)
                else:
                    raise e
        raise ConcurrentModificationError(f"Concurrency problem in {operation_name}")

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self._session_ctx.get()
        if session is not None:
            DB_CALL_PROFILER.note_session_reuse()
            try:
                yield session
            except Exception:
                self._rollback_session(session)
                raise
            return
        session = Session(self.engine)
        token = self._session_ctx.set(session)
        DB_CALL_PROFILER.note_session_open()
        started = time.perf_counter()
        try:
            yield session
        finally:
            session.close()
            DB_CALL_PROFILER.note_session_close(
                live_seconds=time.perf_counter() - started
            )
            self._session_ctx.reset(token)
