"""
Startup schema detection (`detect_schema`), `SchemaState`, `_resolve_auto_migrate`,
and `DatabaseMigrationRequired` error formatting.
"""

from __future__ import annotations

from datetime import date, datetime
import enum
import os
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from consist.location import lookup_live_ledger_by_path

if TYPE_CHECKING:
    import duckdb

V1_SENTINEL_TABLES: frozenset[str] = frozenset(
    {
        "artifact_content",
        "config_facet",
        "artifact_facet",
        "run_artifact_dir_entry",
    }
)


class ConsistError(RuntimeError):
    """Base exception class for Consist user-facing runtime and store errors."""


class IncompatibleSchemaError(ConsistError):
    """Raised when a Consist database uses an incompatible or newer schema version."""


class SchemaState(enum.Enum):
    """Startup schema state of a Consist DuckDB database path."""

    ABSENT = "absent"
    V1 = "v1"
    V2 = "v2"
    FUTURE = "future"
    INTERRUPTED = "interrupted"
    LOCKED = "locked"
    UNREADABLE = "unreadable"
    UNKNOWN = "unknown"


class DatabaseMigrationRequired(ConsistError):
    """Raised when opening a Consist v1 database without `auto_migrate` enabled."""

    def __init__(
        self,
        db_path: Path | str,
        *,
        run_count: int = 0,
        artifact_count: int = 0,
        last_written: str | None = None,
        message: str | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.run_count = int(run_count)
        self.artifact_count = int(artifact_count)
        self.last_written = last_written
        last_suffix = f" (last written {last_written})" if last_written else ""
        super().__init__(
            message
            if message is not None
            else (
                "Consist 0.5 can't open this database yet. It was created by Consist 0.4\n"
                "and needs a one-time upgrade.\n"
                "\n"
                f"  Database:  {self.db_path}\n"
                f"  Contents:  {self.run_count:,} runs, {self.artifact_count:,} artifacts{last_suffix}\n"
                "\n"
                "Nothing has been changed. Pick one:\n"
                "\n"
                "  1. Upgrade now (recommended). Your original file is kept as a backup:\n"
                f"       consist migrate {self.db_path}\n"
                "\n"
                "  2. Upgrade automatically the next time a tracker opens it:\n"
                "       Tracker(..., auto_migrate=True)    or    export CONSIST_AUTO_MIGRATE=1\n"
                "\n"
                "  3. Keep using the old version for now:\n"
                '       pip install "consist<0.5"'
            )
        )


def _migration_workdir(db_path: Path | str) -> Path:
    """Return the sidecar working directory used by `consist migrate`."""
    path = Path(db_path)
    return path.with_name(f"{path.name}.migrate.d")


def _resolve_auto_migrate(
    arg: bool | str | None = None,
) -> Literal["off", "full", "quick"]:
    """Resolve `auto_migrate` argument or `CONSIST_AUTO_MIGRATE` env var."""
    raw = arg if arg is not None else os.environ.get("CONSIST_AUTO_MIGRATE", "0")
    val = str(raw).strip().lower()
    if val in ("1", "true", "yes", "full"):
        return "full"
    if val == "quick":
        return "quick"
    if val in ("0", "false", "no", "off", ""):
        return "off"
    raise ValueError(f"CONSIST_AUTO_MIGRATE={raw!r}: expected 1, quick, or 0")


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        str(r[0])
        for r in con.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'main' AND table_name = ?",
            [table],
        ).fetchall()
    }


_LOCK_ERROR_MARKERS: tuple[str, ...] = (
    "lock",
    "conflicting",
    "already open",
    "different configuration",
    "database is busy",
)

_UNREADABLE_ERROR_MARKERS: tuple[str, ...] = (
    "serialization",
    "storage version",
    "backward",
    "not a valid duckdb",
    "corrupt",
    "magic bytes",
    "header",
    "unsupported version",
)


def detect_schema(db_path: Path | str) -> SchemaState:
    """Inspect `db_path` read-only and classify its schema state without mutating disk."""
    import duckdb

    path = Path(db_path)
    if _migration_workdir(path).exists():
        return SchemaState.INTERRUPTED
    if not path.exists():
        return SchemaState.ABSENT

    # Check in-process open ledgers first so opening a 2nd Tracker in the same process
    # does not trigger a DuckDB connection-config conflict on read_only=True connect.
    if (live := lookup_live_ledger_by_path(path)) is not None:
        return live.schema_state

    try:
        con = duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        msg = str(exc).lower()
        if any(marker in msg for marker in _LOCK_ERROR_MARKERS):
            return SchemaState.LOCKED
        if isinstance(exc, duckdb.SerializationException) or any(
            marker in msg for marker in _UNREADABLE_ERROR_MARKERS
        ):
            return SchemaState.UNREADABLE
        raise

    try:
        tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "consist_schema" in tables:
            row = con.execute("SELECT max(version) FROM consist_schema").fetchone()
            v = int(row[0]) if row is not None and row[0] is not None else None
            if v == 2:
                return SchemaState.V2
            if v is not None and v > 2:
                return SchemaState.FUTURE
            if v == 1:
                return SchemaState.V1

        if tables & V1_SENTINEL_TABLES:
            return SchemaState.V1

        cols = _table_columns(con, "artifact")
        if {"container_uri", "hash"} <= cols or {"uri", "hash"} <= cols:
            return SchemaState.V1
        return SchemaState.UNKNOWN
    finally:
        con.close()


def _format_last_written(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    return raw[:10] if len(raw) >= 10 else (raw or None)


def inspect_v1_summary(db_path: Path | str) -> DatabaseMigrationRequired:
    """Query `COUNT(*)` and `MAX(created_at)` from a v1 database and build `DatabaseMigrationRequired`."""
    import duckdb

    path = Path(db_path)
    run_count = 0
    artifact_count = 0
    last_written: str | None = None

    with duckdb.connect(str(path), read_only=True) as con:
        tables = {
            str(r[0])
            for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }
        if "run" in tables:
            r_cnt = con.execute('SELECT COUNT(*) FROM main."run"').fetchone()
            run_count = (
                int(r_cnt[0]) if r_cnt is not None and r_cnt[0] is not None else 0
            )

            run_cols = _table_columns(con, "run")
            ts_col = (
                "created_at"
                if "created_at" in run_cols
                else ("started_at" if "started_at" in run_cols else None)
            )
            if ts_col is not None:
                max_row = con.execute(
                    f'SELECT MAX("{ts_col}") FROM main."run"'
                ).fetchone()
                if max_row is not None:
                    last_written = _format_last_written(max_row[0])

        if "artifact" in tables:
            a_cnt = con.execute('SELECT COUNT(*) FROM main."artifact"').fetchone()
            artifact_count = (
                int(a_cnt[0]) if a_cnt is not None and a_cnt[0] is not None else 0
            )

    return DatabaseMigrationRequired(
        db_path=path,
        run_count=run_count,
        artifact_count=artifact_count,
        last_written=last_written,
    )


__all__ = [
    "ConsistError",
    "DatabaseMigrationRequired",
    "IncompatibleSchemaError",
    "SchemaState",
    "V1_SENTINEL_TABLES",
    "_migration_workdir",
    "_resolve_auto_migrate",
    "detect_schema",
    "inspect_v1_summary",
]
