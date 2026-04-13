"""
Consist Command Line Interface (CLI)

This module provides command-line utilities for interacting with Consist,
primarily for inspecting provenance data stored in the DuckDB database.
It offers functions like listing recent runs, providing a quick overview
of the execution history and status of various models and workflows.
"""

import os
import sys

# When invoked as a script (python consist/cli.py), the script directory can
# shadow stdlib modules like "types". Ensure imports resolve from the package root.
if __package__ is None and __spec__ is None:
    script_dir = os.path.dirname(__file__)
    package_root = os.path.dirname(script_dir)
    script_dir_real = os.path.realpath(script_dir)
    sys.path = [path for path in sys.path if os.path.realpath(path) != script_dir_real]
    if package_root not in sys.path:
        sys.path.insert(0, package_root)

import cmd
from contextlib import contextmanager
from dataclasses import asdict
import importlib
from importlib.metadata import PackageNotFoundError, version as package_version
import re
import shlex
import json
import uuid
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Mapping,
    Tuple,
    Literal,
    Sequence,
    cast,
)

import click
import duckdb
import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from sqlalchemy import and_, or_, select as sa_select
from sqlmodel import Session, col, select

from consist import Tracker
from consist.core.maintenance import DatabaseMaintenance
from consist.core.persistence import DatabaseManager
from consist.core.run_ordering import recent_run_order_by
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaField
from consist.tools import queries

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.models.artifact import Artifact
    from consist.models.run import Run

_READLINE: ModuleType | None = None
try:
    _READLINE = importlib.import_module("readline")
except ImportError:  # pragma: no cover - platform dependent
    _READLINE = None

app = typer.Typer(rich_markup_mode="markdown")
schema_app = typer.Typer(rich_markup_mode="markdown")
views_app = typer.Typer(rich_markup_mode="markdown")
db_app = typer.Typer(rich_markup_mode="markdown")
console = Console()

app.add_typer(
    schema_app,
    name="schema",
    help="Schema inspection, profiling, and code-generation commands.",
)
app.add_typer(
    views_app,
    name="views",
    help="Materialized and hybrid SQL view management commands.",
)
app.add_typer(
    db_app,
    name="db",
    help="Database maintenance and recovery commands.",
)

MAX_CLI_LIMIT = 1_000_000
MAX_PREVIEW_ROWS = 1_000_000
MAX_SEARCH_QUERY_LENGTH = 256
LIKE_ESCAPE_CHAR = "!"
CLI_EXIT_SUCCESS = 0
CLI_EXIT_RUNTIME_ERROR = 1
CLI_EXIT_USAGE_ERROR = 2
CLI_EXIT_INTERRUPTED = 130


def _resolve_cli_version() -> str:
    try:
        return package_version("consist")
    except PackageNotFoundError:
        return "unknown"


def _version_callback(value: bool) -> None:
    if not value:
        return
    console.print(f"consist {_resolve_cli_version()}")
    raise typer.Exit(CLI_EXIT_SUCCESS)


def _is_interactive_tty() -> bool:
    """Return True when stdin/stdout are both TTYs (safe for interactive prompts)."""
    stdin_isatty = getattr(sys.stdin, "isatty", None)
    stdout_isatty = getattr(sys.stdout, "isatty", None)
    return bool(
        callable(stdin_isatty)
        and callable(stdout_isatty)
        and stdin_isatty()
        and stdout_isatty()
    )


@app.callback()
def cli_root(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Show Consist CLI version and exit.",
    ),
) -> None:
    """Consist CLI root options."""
    del version


def _optional_xarray() -> Any | None:
    try:
        return importlib.import_module("xarray")
    except ImportError:
        return None


def output_json(data: Any) -> None:
    """Helper to output data as JSON."""
    import json

    print(json.dumps(data, default=str, indent=2))


def _parse_bounded_int(value: str, *, name: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {value!r}).") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(
            f"{name} must be between {minimum} and {maximum} (got {parsed})."
        )
    return parsed


def _parse_mount_overrides(values: Optional[List[str]]) -> Dict[str, str]:
    """Parse repeatable --mount NAME=PATH options into a resolved mount mapping."""
    if not values:
        return {}

    mounts: Dict[str, str] = {}
    for raw_value in values:
        spec = raw_value.strip()
        if "=" not in spec:
            raise ValueError(
                f"Invalid --mount value {raw_value!r}; expected NAME=PATH."
            )
        name, path_value = spec.split("=", 1)
        name = name.strip()
        path_value = path_value.strip()

        if not name:
            raise ValueError(
                f"Invalid --mount value {raw_value!r}; mount name cannot be empty."
            )
        if not path_value:
            raise ValueError(
                f"Invalid --mount value {raw_value!r}; mount path cannot be empty."
            )
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", name):
            raise ValueError(
                f"Invalid mount name {name!r}; use letters, numbers, '_' or '-' and start with a letter."
            )

        mounts[name] = str(_resolve_cli_path(path_value))
    return mounts


def _resolve_mount_overrides_or_exit(values: Optional[List[str]]) -> Dict[str, str]:
    try:
        return _parse_mount_overrides(values)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_USAGE_ERROR) from exc


def _escape_like_pattern(value: str) -> str:
    value = value.replace(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR * 2)
    value = value.replace("%", f"{LIKE_ESCAPE_CHAR}%")
    value = value.replace("_", f"{LIKE_ESCAPE_CHAR}_")
    return value


def find_db_path(explicit_path: Optional[str] = None) -> str:
    """Find the provenance database, checking common locations."""
    if explicit_path:
        return explicit_path

    # Check environment variable
    import os

    if env_path := os.getenv("CONSIST_DB"):
        return env_path

    # Check current directory
    if Path("provenance.duckdb").exists():
        return "provenance.duckdb"

    # Check common subdirectories
    for subdir in [".", "data", "db", ".consist"]:
        candidate = Path(subdir) / "provenance.duckdb"
        if candidate.exists():
            return str(candidate)

    # Fall back to default
    return "provenance.duckdb"


# Then update get_tracker:
def get_tracker(
    db_path: Optional[str] = None,
    *,
    run_dir: Optional[Path | str] = None,
    mounts: Optional[Dict[str, str]] = None,
) -> Tracker:
    """Initializes and returns a Tracker instance."""
    resolved_path = find_db_path(db_path)
    if not Path(resolved_path).exists():
        console.print(f"[red]Database not found at {resolved_path}[/red]")
        console.print(
            "[yellow]Hint: Use --db-path to specify location or set CONSIST_DB environment variable[/yellow]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    tracker_run_dir = Path(run_dir) if run_dir is not None else Path(".")
    return Tracker(run_dir=tracker_run_dir, db_path=resolved_path, mounts=mounts)


@contextmanager
def _tracker_session(tracker: Tracker) -> Iterator[Session]:
    db = getattr(tracker, "db", None)
    if not isinstance(db, DatabaseManager):
        with Session(tracker.engine) as session:
            yield session
        return
    with db.session_scope() as session:
        yield session


def _maintenance_service(db_path: Optional[str]) -> DatabaseMaintenance:
    tracker = get_tracker(db_path)
    db = getattr(tracker, "db", None)
    if not isinstance(db, DatabaseManager):
        console.print(
            "[red]Runtime error: tracker has no configured database manager.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    return DatabaseMaintenance(db=db, run_dir=Path(tracker.run_dir))


def _resolve_snapshot_sidecar_path(db: DatabaseManager, snapshot_path: Path) -> Path:
    sidecar_path_fn = getattr(db, "_snapshot_sidecar_path", None)
    if callable(sidecar_path_fn):
        return sidecar_path_fn(snapshot_path)
    base_name = snapshot_path.stem if snapshot_path.suffix else snapshot_path.name
    return snapshot_path.with_name(f"{base_name}.snapshot_meta.json")


@db_app.command("inspect")
def db_inspect(
    json_output: bool = typer.Option(
        False, "--json", help="Output maintenance report as JSON."
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Inspect database state and snapshot parity."""
    report = _maintenance_service(db_path).inspect()
    if json_output:
        output_json(asdict(report))
        return

    summary = Table(title="Database Inspect")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Total runs", str(report.total_runs))
    summary.add_row(
        "Runs by status",
        ", ".join(
            f"{status}={count}" for status, count in report.runs_by_status.items()
        )
        or "-",
    )
    summary.add_row("Total artifacts", str(report.total_artifacts))
    summary.add_row("Orphaned artifacts", str(report.orphaned_artifact_count))
    summary.add_row("Zombie runs", ", ".join(report.zombie_run_ids) or "-")
    summary.add_row("DB file size (MB)", f"{report.db_file_size_mb:.3f}")
    summary.add_row("JSON snapshots", str(report.json_snapshot_count))
    summary.add_row("JSON/DB parity", str(report.json_db_parity))
    console.print(summary)

    global_sizes = Table(title="Global Table Sizes")
    global_sizes.add_column("Table", style="cyan")
    global_sizes.add_column("Rows", style="magenta")
    if report.global_table_sizes:
        for table_name, row_count in report.global_table_sizes.items():
            global_sizes.add_row(table_name, str(row_count))
    else:
        global_sizes.add_row("-", "0")
    console.print(global_sizes)


@db_app.command("doctor")
def db_doctor(
    json_output: bool = typer.Option(
        False, "--json", help="Output maintenance diagnostics as JSON."
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Run read-only integrity diagnostics."""
    report = _maintenance_service(db_path).doctor()
    if json_output:
        output_json(asdict(report))
        return

    diagnostics = Table(title="Database Doctor")
    diagnostics.add_column("Check", style="cyan")
    diagnostics.add_column("Details", style="green")
    diagnostics.add_row("Zombie runs", ", ".join(report.zombie_run_ids) or "-")
    diagnostics.add_row(
        "Completed/failed with null ended_at",
        ", ".join(report.completed_without_end_time) or "-",
    )
    diagnostics.add_row(
        "Dangling parent run IDs",
        ", ".join(report.dangling_parent_run_ids) or "-",
    )
    diagnostics.add_row(
        "Artifacts with missing producing run",
        ", ".join(str(value) for value in report.artifacts_with_missing_producing_run)
        or "-",
    )
    diagnostics.add_row(
        "Global table schema drift",
        json.dumps(report.global_table_schema_drift, sort_keys=True) or "{}",
    )
    console.print(diagnostics)


@db_app.command("snapshot")
def db_snapshot(
    out_path: Path = typer.Option(
        ...,
        "--out",
        help="Destination path for the snapshot DuckDB file.",
    ),
    no_checkpoint: bool = typer.Option(
        False,
        "--no-checkpoint",
        help="Skip CHECKPOINT before copying the database.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Output snapshot metadata as JSON."
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Create a database snapshot and metadata sidecar."""
    maintenance = _maintenance_service(db_path)
    checkpoint = not no_checkpoint
    snapshot_path = maintenance.snapshot(out_path, checkpoint=checkpoint)
    sidecar_path = _resolve_snapshot_sidecar_path(maintenance.db, snapshot_path)

    if json_output:
        output_json(
            {
                "snapshot_path": str(snapshot_path),
                "checkpoint": checkpoint,
                "sidecar_path": str(sidecar_path),
            }
        )
        return

    console.print(f"Snapshot path: {snapshot_path}")
    console.print(f"Sidecar path: {sidecar_path}")


@db_app.command("rebuild")
def db_rebuild(
    json_dir: Path = typer.Option(
        Path("consist_runs"),
        "--json-dir",
        help="Directory containing per-run JSON snapshots to rebuild from.",
    ),
    mode: Literal["minimal", "full"] = typer.Option(
        "minimal",
        "--mode",
        help="Rebuild mode: minimal restores runs/artifacts/links; full also restores optional facet/schema indexes.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Parse and count rebuild candidates without writing to the DB.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output rebuild result as JSON."
    ),
) -> None:
    """Rebuild DB run/artifact/link rows from JSON snapshots."""
    maintenance = _maintenance_service(db_path)
    result = maintenance.rebuild_from_json(json_dir, dry_run=dry_run, mode=mode)
    if json_output:
        output_json(asdict(result))
        return

    summary = Table(title="Database Rebuild")
    summary.add_column("Field", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Mode", mode)
    summary.add_row("Dry run", str(result.dry_run))
    summary.add_row("JSON files scanned", str(result.json_files_scanned))
    summary.add_row("Runs inserted", str(result.runs_inserted))
    summary.add_row("Runs already present", str(result.runs_already_present))
    summary.add_row("Artifacts inserted", str(result.artifacts_inserted))
    summary.add_row("Errors", str(len(result.errors)))
    console.print(summary)

    if result.errors:
        error_table = Table(title="Rebuild Errors")
        error_table.add_column("Error", style="red", overflow="fold")
        for error in result.errors:
            error_table.add_row(error)
        console.print(error_table)


@db_app.command("compact")
def db_compact(
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output compact result as JSON."
    ),
) -> None:
    """Run VACUUM on the database."""
    maintenance = _maintenance_service(db_path)
    maintenance.compact()
    if json_output:
        output_json({"compacted": True})
        return
    console.print("Database compacted.")


@db_app.command("export")
def db_export(
    run_id: str = typer.Argument(..., help="Root run ID to export."),
    out_path: Path = typer.Option(
        ...,
        "--out",
        help="Destination shard database path (e.g., shard.duckdb).",
    ),
    no_children: bool = typer.Option(
        False,
        "--no-children",
        help="Export only this exact run (exclude descendants).",
    ),
    include_data: bool = typer.Option(
        False,
        "--include-data",
        help="Include filtered global_tables rows in the shard.",
    ),
    include_snapshots: bool = typer.Option(
        False,
        "--include-snapshots",
        help="Copy JSON snapshots into shard_snapshots.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview export without writing shard DB or files.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output export result as JSON."
    ),
) -> None:
    """Export run lineage into a standalone shard database."""
    maintenance = _maintenance_service(db_path)
    result = maintenance.export(
        run_id,
        out_path,
        include_data=include_data,
        include_snapshots=include_snapshots,
        include_children=not no_children,
        dry_run=dry_run,
    )
    if json_output:
        output_json(asdict(result))
        return

    summary = Table(title="Database Export")
    summary.add_column("Field", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Out path", str(result.out_path))
    summary.add_row("Dry run", str(dry_run))
    summary.add_row("Runs exported", str(len(result.run_ids)))
    summary.add_row("Artifacts exported", str(result.artifact_count))
    summary.add_row("Ingested tables exported", str(len(result.ingested_rows)))
    summary.add_row("Snapshots copied", str(result.snapshots_copied))
    console.print(summary)
    if result.unscoped_cache_tables_skipped:
        console.print(
            "Warning: skipped unscoped cache table(s) during export; rows were not "
            f"exported: {', '.join(result.unscoped_cache_tables_skipped)}"
        )


@db_app.command("merge")
def db_merge(
    shard_path: Path = typer.Argument(..., help="Path to shard database to merge."),
    conflict: Literal["error", "skip"] = typer.Option(
        "error",
        "--conflict",
        help="Conflict policy for run ID collisions.",
    ),
    include_snapshots: bool = typer.Option(
        False,
        "--include-snapshots",
        help="Copy JSON snapshots from shard_snapshots into canonical paths.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview merge without writing database or files.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output merge result as JSON."
    ),
) -> None:
    """Merge a shard database into the current provenance database."""
    maintenance = _maintenance_service(db_path)
    try:
        result = maintenance.merge(
            shard_path,
            conflict=conflict,
            include_snapshots=include_snapshots,
            dry_run=dry_run,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_USAGE_ERROR) from exc

    if json_output:
        output_json(asdict(result))
        return

    summary = Table(title="Database Merge")
    summary.add_column("Field", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Shard path", str(result.shard_path))
    summary.add_row("Dry run", str(dry_run))
    summary.add_row("Runs merged", str(len(result.runs_merged)))
    summary.add_row("Runs skipped", str(len(result.runs_skipped)))
    summary.add_row("Artifacts merged", str(result.artifacts_merged))
    summary.add_row("Global tables merged", str(len(result.ingested_tables_merged)))
    summary.add_row("Snapshots merged", str(result.snapshots_merged))
    console.print(summary)
    if result.unscoped_cache_tables_skipped:
        console.print(
            "Warning: skipped unscoped cache table(s) during merge; rows were not "
            f"merged: {', '.join(result.unscoped_cache_tables_skipped)}"
        )
    if result.incompatible_global_tables_skipped:
        details = "; ".join(
            f"{table}: {reason}"
            for table, reason in sorted(
                result.incompatible_global_tables_skipped.items()
            )
        )
        console.print(
            "Warning: skipped incompatible global table(s) during merge; "
            f"rows were not merged: {details}"
        )


@db_app.command("purge")
def db_purge(
    run_id: str = typer.Argument(..., help="Run ID to purge."),
    no_children: bool = typer.Option(
        False,
        "--no-children",
        help="Do not include descendant runs.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview purge plan without deleting anything.",
    ),
    delete_ingested_data: bool = typer.Option(
        False,
        "--delete-ingested-data",
        help="Delete run-scoped/link-scoped rows from global_tables schema.",
    ),
    prune_cache: bool = typer.Option(
        False,
        "--prune-cache",
        help=(
            "Prune unscoped cache rows no longer referenced by surviving run-link rows. "
            "Only applies when --delete-ingested-data is set."
        ),
    ),
    delete_files: bool = typer.Option(
        False,
        "--delete-files",
        help="Delete artifact disk files listed in purge plan.",
    ),
    unsafe_delete_targets: str = typer.Option(
        "fail",
        "--unsafe-delete-targets",
        help="How to handle deletion targets outside the maintenance run_dir: fail or skip.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt for non-dry-run purges.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output purge result as JSON."
    ),
) -> None:
    """Purge run records, snapshots, and optional ingested/global data."""
    if unsafe_delete_targets not in {"fail", "skip"}:
        console.print(
            "[red]--unsafe-delete-targets must be either 'fail' or 'skip'[/red]"
        )
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)

    maintenance = _maintenance_service(db_path)
    include_children = not no_children

    if not dry_run and not yes:
        preview = maintenance.plan_purge(run_id, include_children=include_children)
        scoped_ingested_rows = sum(
            count
            for table, count in preview.ingested_data.items()
            if preview.ingested_table_modes.get(table) in {"run_scoped", "run_link"}
        )
        scope_fields = [
            f"runs={len(preview.run_ids)}",
            f"snapshots={len(preview.json_files)}",
            f"orphaned_artifacts={len(preview.orphaned_artifact_ids)}",
            (
                f"disk_files=enabled({len(preview.disk_files)})"
                if delete_files
                else "disk_files=disabled"
            ),
            (
                f"unsafe_targets={len(preview.unsafe_json_files) + len(preview.unsafe_disk_files)}"
            ),
            (
                f"ingested_global=enabled(rows~{scoped_ingested_rows})"
                if delete_ingested_data
                else "ingested_global=preserve"
            ),
            (
                "prune_cache=enabled"
                if prune_cache and delete_ingested_data
                else "prune_cache=noop"
                if prune_cache and not delete_ingested_data
                else "prune_cache=disabled"
            ),
        ]
        confirmed = typer.confirm(
            "Confirm purge scope: " + ", ".join(scope_fields) + "?",
            default=False,
        )
        if not confirmed:
            console.print("Purge cancelled.")
            raise typer.Exit(CLI_EXIT_SUCCESS)

    result = maintenance.purge(
        run_id,
        include_children=include_children,
        delete_files=delete_files,
        delete_ingested_data=delete_ingested_data,
        prune_cache=prune_cache,
        dry_run=dry_run,
        unsafe_delete_targets=cast(Any, unsafe_delete_targets),
    )
    if json_output:
        output_json(asdict(result))
        return

    summary = Table(title="Database Purge")
    summary.add_column("Field", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Executed", str(result.executed))
    summary.add_row("Run IDs", ", ".join(result.plan.run_ids) or "-")
    summary.add_row("Child run IDs", ", ".join(result.plan.child_run_ids) or "-")
    summary.add_row(
        "Orphaned artifact IDs",
        ", ".join(str(value) for value in result.plan.orphaned_artifact_ids) or "-",
    )
    summary.add_row("Snapshot JSON files", str(len(result.plan.json_files)))
    summary.add_row("Disk files", str(len(result.plan.disk_files)))
    summary.add_row("Unsafe snapshot targets", str(len(result.plan.unsafe_json_files)))
    summary.add_row("Unsafe disk targets", str(len(result.plan.unsafe_disk_files)))
    summary.add_row(
        "Skipped unsafe targets",
        str(
            len(result.skipped_unsafe_json_files)
            + len(result.skipped_unsafe_disk_files)
        ),
    )
    summary.add_row("Ingested data skipped", str(result.ingested_data_skipped))
    console.print(summary)

    if result.skipped_unsafe_json_files or result.skipped_unsafe_disk_files:
        console.print(
            "[yellow]Skipped unsafe filesystem deletion target(s) outside the maintenance run_dir.[/yellow]"
        )


@db_app.command("fix-status")
def db_fix_status(
    run_id: str = typer.Argument(..., help="Run ID to update."),
    status: str = typer.Argument(..., help="New status: running|completed|failed."),
    reason: Optional[str] = typer.Option(
        None,
        "--reason",
        help="Optional reason recorded in run metadata.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Required when transitioning from completed/failed back to running.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    json_output: bool = typer.Option(
        False, "--json", help="Output updated run fields as JSON."
    ),
) -> None:
    """Fix an incorrect run status in-place."""
    maintenance = _maintenance_service(db_path)
    try:
        updated_run = maintenance.fix_status(run_id, status, reason=reason, force=force)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_USAGE_ERROR) from exc

    payload = {
        "id": updated_run.id,
        "status": updated_run.status,
        "ended_at": updated_run.ended_at,
        "updated_at": updated_run.updated_at,
        "meta": updated_run.meta,
    }
    if json_output:
        output_json(payload)
        return

    summary = Table(title="Run Status Updated")
    summary.add_column("Field", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Run ID", str(payload["id"]))
    summary.add_row("Status", str(payload["status"]))
    summary.add_row("Ended At", str(payload["ended_at"] or "-"))
    summary.add_row("Updated At", str(payload["updated_at"]))
    summary.add_row(
        "Meta", json.dumps(payload["meta"] or {}, default=str, sort_keys=True)
    )
    console.print(summary)


def _render_schema_profile(
    schema: "ArtifactSchema", fields: List["ArtifactSchemaField"]
) -> None:
    summary = getattr(schema, "summary_json", None) or {}
    table = Table()
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Nullable", style="green")

    for field in fields:
        nullable = "yes" if getattr(field, "nullable", True) else "no"
        table.add_row(str(field.name), str(field.logical_type), nullable)

    console.print(table)

    if summary:
        summary_parts = []
        for key in (
            "row_count",
            "num_rows",
            "rows",
            "column_count",
            "num_columns",
            "columns",
        ):
            value = summary.get(key)
            if isinstance(value, (int, float)) and value:
                summary_parts.append(f"{key}={value}")
        if summary_parts:
            console.print(f"[dim]Summary: {', '.join(summary_parts)}[/dim]")


def _coerce_mounts(value: Any) -> Dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    mounts: Dict[str, str] = {}
    for key, root in value.items():
        if isinstance(key, str) and isinstance(root, str) and root:
            mounts[key] = root
    return mounts


def _apply_inferred_mounts(tracker: Tracker, mounts: Mapping[str, str]) -> None:
    if not mounts:
        return
    merged = dict(mounts)
    merged.update(tracker.mounts)
    tracker.fs.mounts = merged
    tracker.mounts = tracker.fs.mounts


def _ensure_tracker_mounts_for_artifact(
    tracker: Tracker, artifact: "Artifact", *, trust_db: bool
) -> None:
    if not trust_db:
        return
    from consist.tools.mount_diagnostics import parse_mount_uri

    parsed = parse_mount_uri(artifact.container_uri)
    if parsed is None:
        return

    scheme, _ = parsed
    if scheme in tracker.mounts:
        return

    inferred: Dict[str, str] = {}
    run = tracker.get_run(artifact.run_id) if artifact.run_id else None
    if run and isinstance(run.meta, dict):
        inferred.update(_coerce_mounts(run.meta.get("mounts")))
        run_dir = run.meta.get("_physical_run_dir")
        if (
            scheme == "workspace"
            and scheme not in inferred
            and isinstance(run_dir, str)
            and run_dir
        ):
            inferred[scheme] = run_dir

    if scheme not in inferred:
        meta = artifact.meta or {}
        mount_root = meta.get("mount_root")
        if isinstance(mount_root, str) and mount_root:
            inferred[scheme] = mount_root

    _apply_inferred_mounts(tracker, inferred)


def _resolve_schema_capture_path(
    *,
    tracker: Tracker,
    artifact: "Artifact",
    run: "Run",
    override_path: Optional[Path],
) -> Path:
    if override_path is not None:
        return override_path.expanduser().resolve()

    candidates: List[Path] = []

    def _append_candidate(candidate_factory) -> None:
        try:
            candidates.append(candidate_factory())
        except (
            AttributeError,
            NotImplementedError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            return

    _append_candidate(lambda: tracker.resolve_historical_path(artifact, run))
    _append_candidate(
        lambda: Path(tracker.resolve_uri(artifact.container_uri)).resolve()
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    if candidates:
        rendered = ", ".join(str(path) for path in candidates)
        raise FileNotFoundError(
            "Could not resolve an existing artifact file path. "
            f"Tried: {rendered}. Provide --path to override."
        )

    raise FileNotFoundError(
        "Could not resolve artifact path from URI. Provide --path to override."
    )


@schema_app.command("capture-file")
def schema_capture_file(
    artifact_key: Optional[str] = typer.Option(
        None,
        "--artifact-key",
        "--table-key",
        help="Artifact key to profile.",
    ),
    artifact_id: Optional[str] = typer.Option(
        None,
        "--artifact-id",
        help="Artifact UUID to profile.",
    ),
    run_id: Optional[str] = typer.Option(
        None,
        "--run-id",
        help="Optional run scope for --artifact-key lookups.",
    ),
    path: Optional[Path] = typer.Option(
        None,
        "--path",
        help="Override on-disk file path to profile.",
    ),
    sample_rows: int = typer.Option(
        1000,
        "--sample-rows",
        min=1,
        max=MAX_CLI_LIMIT,
        help="Maximum rows sampled during schema inference.",
    ),
    if_changed: bool = typer.Option(
        False,
        "--if-changed",
        help=(
            "Reuse an existing schema observation when the file hash is unchanged. "
            "This affects schema capture only, not artifact-row reuse."
        ),
    ),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow metadata-based mount inference when resolving artifact paths.",
    ),
    mount: Optional[List[str]] = typer.Option(
        None,
        "--mount",
        help=(
            "Mount override mapping (repeatable): NAME=PATH. "
            "Example: --mount workspace=/path/to/archive"
        ),
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """
    Capture file schema metadata for an existing artifact record.

    This is useful for artifacts logged before file schema profiling was enabled.
    After capture, ``schema export`` and shell ``schema_stub`` can generate stubs
    from the newly persisted schema profile.
    """
    selectors = [artifact_key, artifact_id]
    if sum(1 for selector in selectors if selector is not None) != 1:
        console.print(
            "[red]Provide exactly one of --artifact-key or --artifact-id[/red]"
        )
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)
    if run_id is not None and artifact_id is not None:
        console.print(
            "[red]--run-id can only be used with --artifact-key selection.[/red]"
        )
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)
    if artifact_id is not None:
        try:
            uuid.UUID(artifact_id)
        except ValueError:
            console.print("[red]--artifact-id must be a UUID[/red]")
            raise typer.Exit(CLI_EXIT_USAGE_ERROR)

    resolved_db_path = find_db_path(db_path)
    mount_overrides = _resolve_mount_overrides_or_exit(mount)
    tracker = get_tracker(resolved_db_path, mounts=mount_overrides or None)
    if artifact_id is not None:
        artifact = tracker.get_artifact(artifact_id)
    else:
        if artifact_key is None:
            console.print(
                "[red]Internal selector validation failed: missing artifact key.[/red]"
            )
            raise typer.Exit(CLI_EXIT_USAGE_ERROR)
        artifact = tracker.get_artifact(artifact_key, run_id=run_id)
    if artifact is None:
        if artifact_key is not None and run_id is not None:
            console.print(
                f"[red]Artifact '{artifact_key}' not found for run '{run_id}'.[/red]"
            )
        elif artifact_key is not None:
            console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        else:
            console.print(f"[red]Artifact '{artifact_id}' not found.[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    driver = artifact.driver
    if driver not in ("csv", "parquet", "h5_table"):
        console.print(
            "[red]Only csv/parquet/h5_table artifacts support file schema capture; "
            f"got driver '{driver}'.[/red]"
        )
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)

    if artifact.run_id is None:
        console.print(
            "[red]Artifact has no producing run_id; cannot attach schema observation.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    run = tracker.get_run(artifact.run_id)
    if run is None:
        console.print(
            f"[red]Producing run '{artifact.run_id}' not found for artifact.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    _ensure_tracker_mounts_for_artifact(tracker, artifact, trust_db=trust_db)
    try:
        resolved_path = _resolve_schema_capture_path(
            tracker=tracker,
            artifact=artifact,
            run=run,
            override_path=path,
        )
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR) from exc

    if not resolved_path.exists():
        console.print(
            f"[red]Artifact file does not exist at resolved path: {resolved_path}[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    tracker.artifact_schemas.profile_file_artifact(
        artifact=artifact,
        run=run,
        resolved_path=str(resolved_path),
        driver=cast(Literal["csv", "parquet", "h5_table"], driver),
        sample_rows=sample_rows,
        source="file",
        reuse_if_unchanged=if_changed,
    )

    if not tracker.db:
        console.print("[red]Tracker database not initialized.[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    fetched = tracker.db.get_artifact_schema_for_artifact(artifact_id=artifact.id)
    if fetched is None:
        console.print(
            "[red]Schema capture did not produce a stored schema profile.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    schema, fields = fetched
    console.print(
        "[green]Captured file schema for artifact "
        f"'{artifact.key}' (schema_id={schema.id}, columns={len(fields)}).[/green]"
    )
    console.print(f"[dim]Profiled path: {resolved_path}[/dim]")


@schema_app.command("export")
def schema_export(
    schema_id: Optional[str] = typer.Option(
        None, "--schema-id", help="Artifact schema id (hash) to export."
    ),
    artifact_key: Optional[str] = typer.Option(
        None,
        "--artifact-key",
        "--table-key",
        help="Artifact key to export the associated captured schema.",
    ),
    artifact_id: Optional[str] = typer.Option(
        None,
        "--artifact-id",
        help="Artifact UUID to export the associated captured schema.",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Write stub to this path (prints to stdout if omitted)."
    ),
    class_name: Optional[str] = typer.Option(
        None, "--class-name", help="Override generated SQLModel class name."
    ),
    table_name: Optional[str] = typer.Option(
        None, "--table-name", help="Override generated __tablename__."
    ),
    include_system_cols: bool = typer.Option(
        False,
        "--include-system-cols",
        help="Include system/ingestion columns like consist_* and _dlt_*.",
    ),
    include_stats_comments: bool = typer.Option(
        True,
        "--stats-comments/--no-stats-comments",
        help="Include stats/enum hints as comments when available.",
    ),
    abstract: bool = typer.Option(
        True,
        "--abstract/--concrete",
        help="Export as an abstract SQLModel class (importable without defining a primary key).",
    ),
    prefer_source: Optional[str] = typer.Option(
        None,
        "--prefer-source",
        help="Preference hint for when user_provided schema does not exist: 'file' (original CSV/Parquet dtypes) or 'duckdb' (post-ingestion schema). User-provided schemas are ALWAYS preferred if they exist and cannot be overridden.",
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Export a captured artifact schema as an editable SQLModel stub.

    When an artifact has multiple schema profiles (file, duckdb, user_provided):
    - User-provided schemas are ALWAYS preferred (they represent manual curation)
    - If no user_provided schema exists, file schema is preferred by default
      (preserves richer type information like pandas category)
    - Use --prefer-source to specify a preference when user_provided is unavailable.
    """
    selectors = [schema_id, artifact_id, artifact_key]
    if sum(1 for selector in selectors if selector is not None) != 1:
        console.print(
            "[red]Provide exactly one of --schema-id, --artifact-id, or --artifact-key[/red]"
        )
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)
    if artifact_id is not None:
        try:
            uuid.UUID(artifact_id)
        except ValueError:
            console.print("[red]--artifact-id must be a UUID[/red]")
            raise typer.Exit(CLI_EXIT_USAGE_ERROR)

    # Validate prefer_source if provided
    if prefer_source is not None and prefer_source not in ("file", "duckdb"):
        console.print("[red]--prefer-source must be either 'file' or 'duckdb'[/red]")
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)
    resolved_prefer_source: Optional[Literal["file", "duckdb"]] = None
    if prefer_source is not None:
        resolved_prefer_source = cast(Literal["file", "duckdb"], prefer_source)

    tracker = get_tracker(db_path)
    if artifact_key is not None:
        artifact = tracker.get_artifact(artifact_key)
        if artifact is None:
            console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
            raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
        artifact_id = str(artifact.id)
    try:
        code = tracker.export_schema_sqlmodel(
            schema_id=schema_id,
            artifact_id=artifact_id,
            out_path=out,
            table_name=table_name,
            class_name=class_name,
            abstract=abstract,
            include_system_cols=include_system_cols,
            include_stats_comments=include_stats_comments,
            prefer_source=resolved_prefer_source,
        )
    except KeyError:
        console.print("[red]Captured schema not found for the provided selector.[/red]")
        if artifact_id is not None:
            console.print(
                "[yellow]Try `consist schema capture-file --artifact-id "
                f"{artifact_id}` to persist a file schema first.[/yellow]"
            )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(CLI_EXIT_USAGE_ERROR)

    if out is None:
        print(code)
    else:
        console.print(f"[green]Wrote SQLModel stub to {out}[/green]")


@schema_app.command("apply-fks")
def schema_apply_fks(
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Best-effort application of physical foreign key constraints."""
    tracker = get_tracker(db_path)
    if not tracker.db:
        console.print("[red]Tracker database not initialized.[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
    applied = tracker.db.apply_physical_fks()
    console.print(
        f"[green]Applied {applied} foreign key constraint(s) (best-effort).[/green]"
    )


@views_app.command("create")
def views_create(
    view_name: str = typer.Argument(..., help="Name of the SQL view to create."),
    schema_id: str = typer.Option(
        ..., "--schema-id", help="Schema id used to select artifacts."
    ),
    namespace: Optional[str] = typer.Option(
        None, "--namespace", help="Default artifact facet namespace."
    ),
    param: Optional[List[str]] = typer.Option(
        None,
        "--param",
        help=(
            "Facet predicate (repeatable): key=value, key>=value, key<=value. "
            "Example: --param beam.year=2018"
        ),
    ),
    attach_facet: Optional[List[str]] = typer.Option(
        None,
        "--attach-facet",
        help="Facet key to project as a typed column (repeatable).",
    ),
    driver: Optional[List[str]] = typer.Option(
        None,
        "--driver",
        help="Artifact driver filter (repeatable). Example: --driver parquet",
    ),
    include_system_columns: bool = typer.Option(
        True,
        "--include-system-columns/--no-system-columns",
        help="Include Consist system columns in the created view.",
    ),
    mode: Literal["hybrid", "hot_only", "cold_only"] = typer.Option(
        "hybrid",
        "--mode",
        help="Which storage tier(s) to include.",
    ),
    if_exists: Literal["replace", "error"] = typer.Option(
        "replace",
        "--if-exists",
        help="Behavior when the target view already exists.",
    ),
    missing_files: Literal["warn", "error", "skip_silent"] = typer.Option(
        "warn",
        "--missing-files",
        help="Behavior when selected cold files are missing.",
    ),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Filter by run id."),
    parent_run_id: Optional[str] = typer.Option(
        None, "--parent-run-id", help="Filter by parent/scenario run id."
    ),
    model: Optional[str] = typer.Option(
        None, "--model", help="Filter by run model name."
    ),
    status: Optional[str] = typer.Option(
        None, "--status", help="Filter by run status."
    ),
    year: Optional[int] = typer.Option(None, "--year", help="Filter by run year."),
    iteration: Optional[int] = typer.Option(
        None, "--iteration", help="Filter by run iteration."
    ),
    schema_compatible: bool = typer.Option(
        False,
        "--schema-compatible",
        help="Include subset/superset schema variants by field names.",
    ),
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Create a selector-driven grouped hybrid view across many artifacts."""
    tracker = get_tracker(db_path)
    try:
        tracker.create_grouped_view(
            view_name=view_name,
            schema_id=schema_id,
            namespace=namespace,
            params=param or [],
            drivers=driver,
            attach_facets=attach_facet or [],
            include_system_columns=include_system_columns,
            mode=mode,
            if_exists=if_exists,
            missing_files=missing_files,
            run_id=run_id,
            parent_run_id=parent_run_id,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
            schema_compatible=schema_compatible,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR) from exc

    console.print(f"[green]Created grouped view '{view_name}'.[/green]")


def _render_runs_table(
    tracker: Tracker,
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
) -> None:
    """Shared logic for displaying runs."""
    with _tracker_session(tracker) as session:
        results = queries.get_runs(
            session, limit=limit, model_name=model_name, tags=tags, status=status
        )

        title_parts = ["Recent Runs"]
        if model_name:
            title_parts.append(f"for model [green]{model_name}[/green]")
        if tags:
            title_parts.append(f"with tags [yellow]{', '.join(tags)}[/yellow]")
        if status:
            title_parts.append(f"with status [bold]{status}[/bold]")

        table = Table(title=" ".join(title_parts))
        table.add_column("ID", style="cyan")
        table.add_column("Model", style="green")
        table.add_column("Status")
        table.add_column("Tags", style="yellow")
        table.add_column("Created", style="dim")
        table.add_column("Duration (s)", style="magenta")

        for run in results:
            status_style = "green" if run.status == "completed" else "red"
            duration = f"{run.duration_seconds:.2f}" if run.duration_seconds else "N/A"
            tags_str = ", ".join(run.tags) if run.tags else ""
            table.add_row(
                run.id,
                run.model_name,
                f"[{status_style}]{run.status}[/]",
                tags_str,
                run.created_at.strftime("%Y-%m-%d %H:%M"),
                duration,
            )

        console.print(table)


def _render_artifacts_table(tracker: Tracker, run_id: str) -> List["Artifact"]:
    """Shared logic for displaying artifacts for a run."""
    run_artifacts = tracker.get_artifacts_for_run(run_id)
    rendered: List["Artifact"] = []

    table = Table(title=f"Artifacts for Run [cyan]{run_id}[/cyan]")
    table.add_column("Ref", style="dim", justify="right")
    table.add_column("Direction", style="yellow")
    table.add_column("Key", style="green", overflow="fold")
    table.add_column("Artifact ID", style="cyan", overflow="fold")
    table.add_column("URI", style="dim", overflow="fold")
    table.add_column("Driver")
    table.add_column("Hash", style="magenta")

    inputs = sorted(run_artifacts.inputs.values(), key=lambda x: x.key)
    outputs = sorted(run_artifacts.outputs.values(), key=lambda x: x.key)
    ref_index = 1

    for artifact in inputs:
        rendered.append(artifact)
        table.add_row(
            str(ref_index),
            "[blue]Input[/blue]",
            artifact.key,
            str(artifact.id),
            artifact.container_uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )
        ref_index += 1

    if inputs and outputs:
        table.add_section()

    for artifact in outputs:
        rendered.append(artifact)
        table.add_row(
            str(ref_index),
            "[green]Output[/green]",
            artifact.key,
            str(artifact.id),
            artifact.container_uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )
        ref_index += 1

    console.print(table)
    if rendered:
        console.print("[dim]Artifact refs:[/dim]")
        max_refs_to_show = 20
        for index, artifact in enumerate(rendered[:max_refs_to_show], start=1):
            console.print(f"[dim]  @{index}: {artifact.key} (id={artifact.id})[/dim]")
        if len(rendered) > max_refs_to_show:
            console.print(
                f"[dim]  ... and {len(rendered) - max_refs_to_show} more artifact refs[/dim]"
            )
    return rendered


def _render_artifact_query_table(results: List[Dict[str, Any]]) -> None:
    """Render artifact search results from facet-param queries."""
    table = Table(title="Artifact Query Results")
    table.add_column("Key", style="green", overflow="fold")
    table.add_column("Run", style="cyan", overflow="fold")
    table.add_column("Driver")
    table.add_column("Facet Namespace", style="yellow")
    table.add_column("Facet Schema Version", style="magenta")
    table.add_column("URI", style="dim")

    for row in results:
        table.add_row(
            str(row.get("key") or ""),
            str(row.get("run_id") or "-"),
            str(row.get("driver") or "-"),
            str(row.get("facet_namespace") or "-"),
            str(row.get("facet_schema_version") or "-"),
            str(row.get("container_uri") or ""),
        )

    console.print(table)


def _render_run_details(run: "Run") -> None:
    """Shared logic for displaying run details, config, and metadata."""
    info = Table.grid(padding=(0, 2))
    info.add_column(style="bold cyan")
    info.add_column()

    run_id = getattr(run, "id", None) or "[dim]unknown[/dim]"
    model_name = getattr(run, "model_name", None) or "[dim]unknown[/dim]"
    status_value = getattr(run, "status", None) or "unknown"
    status_style = (
        "green"
        if status_value == "completed"
        else "red"
        if status_value == "failed"
        else "yellow"
    )

    info.add_row("ID", run_id)
    info.add_row("Model", model_name)
    info.add_row("Status", f"[{status_style}]{status_value}[/]")

    parent_id = getattr(run, "parent_run_id", None)
    if parent_id:
        info.add_row("Parent", parent_id)

    scenario_value = getattr(run, "parent_run_id", None)
    if scenario_value:
        info.add_row("Scenario", scenario_value)

    if getattr(run, "year", None):
        info.add_row("Year", str(run.year))

    created_at = getattr(run, "created_at", None)
    if created_at:
        info.add_row("Created", created_at.strftime("%Y-%m-%d %H:%M:%S"))

    if getattr(run, "duration_seconds", None):
        info.add_row("Duration", f"{run.duration_seconds:.2f}s")

    if getattr(run, "tags", None):
        info.add_row("Tags", ", ".join(run.tags))

    signature = getattr(run, "signature", None)
    if signature:
        info.add_row("Signature", signature[:16] + "...")

    console.print(Panel(info, title="Run Details", border_style="cyan"))

    config_data = getattr(run, "config", None)
    if config_data:
        import json

        console.print("\n[bold]Configuration:[/]")
        console.print(Panel(json.dumps(config_data, indent=2), border_style="dim"))

    if getattr(run, "meta", None):
        console.print("\n[bold]Metadata:[/]")
        meta_table = Table(show_header=False, box=None)
        meta_table.add_column(style="yellow")
        meta_table.add_column()
        for key, value in run.meta.items():
            meta_table.add_row(f"  {key}", str(value))
        console.print(meta_table)


def _render_summary(summary_data: Dict[str, Any]) -> None:
    """Shared logic for displaying database summary and model breakdown."""
    if summary_data["total_runs"] == 0:
        console.print(
            Panel("[yellow]No runs found in the database.[/yellow]", title="Summary")
        )
        return

    panel_content = (
        f"[bold]Runs[/]: {summary_data['total_runs']} "
        f"([green]{summary_data['completed_runs']} completed[/], [red]{summary_data['failed_runs']} failed[/])\n"
        f"[bold]Artifacts[/]: {summary_data['total_artifacts']}\n"
        f"[bold]Date Range[/]: {summary_data['first_run_at'].strftime('%Y-%m-%d')} to {summary_data['last_run_at'].strftime('%Y-%m-%d')}"
    )
    console.print(Panel(panel_content, title="Database Summary", expand=False))

    model_table = Table(title="Runs per Model")
    model_table.add_column("Model Name", style="green")
    model_table.add_column("Run Count", style="magenta")
    for model, count in summary_data["models_distribution"]:
        model_table.add_row(model, str(count))

    console.print(model_table)


def _iter_artifact_rows(
    session: Session, *, batch_size: int
) -> Iterable[Tuple[uuid.UUID, str, str, Optional[str]]]:
    from consist.models.artifact import Artifact

    last_created = None
    last_id = None
    while True:
        stmt = (
            sa_select(
                col(Artifact.id),
                col(Artifact.key),
                col(Artifact.container_uri),
                col(Artifact.run_id),
                col(Artifact.created_at),
            )
            .order_by(col(Artifact.created_at), col(Artifact.id))
            .limit(batch_size)
        )
        if last_created is not None and last_id is not None:
            stmt = stmt.where(
                or_(
                    col(Artifact.created_at) > last_created,
                    and_(
                        col(Artifact.created_at) == last_created,
                        col(Artifact.id) > last_id,
                    ),
                )
            )

        batch = session.exec(cast(Any, stmt)).all()
        if not batch:
            break
        for art_id, key, uri, run_id, created_at in batch:
            yield art_id, key, uri, run_id
        last_id = batch[-1][0]
        last_created = batch[-1][4]


def _render_scenarios(tracker: Tracker, limit: int = 20) -> None:
    """Shared logic for displaying scenario overview (scenarios are parent runs)."""
    with _tracker_session(tracker) as session:
        from consist.models.run import Run
        from sqlmodel import func

        # Find parent runs by looking for distinct parent_run_ids
        query = (
            select(
                col(Run.parent_run_id).label("scenario_id"),  # ← Fixed: was Run.id
                func.count(col(Run.id)).label("run_count"),
                func.min(col(Run.created_at)).label("first_run"),
                func.max(col(Run.created_at)).label("last_run"),
            )
            .where(col(Run.parent_run_id).is_not(None))
            .group_by(col(Run.parent_run_id))
            .order_by(func.max(col(Run.created_at)).desc())
            .limit(limit)
        )

        results = session.exec(query).all()

        if not results:
            console.print("[yellow]No scenarios found.[/yellow]")
            return

    table = Table(title="Scenarios")
    table.add_column("Scenario ID", style="cyan")
    table.add_column("Runs", style="magenta")
    table.add_column("First Run", style="dim")
    table.add_column("Last Run", style="green")

    for row in results:
        table.add_row(
            row[0],  # scenario_id (parent_run_id)
            str(row[1]),  # run_count
            row[2].strftime("%Y-%m-%d %H:%M"),  # first_run
            row[3].strftime("%Y-%m-%d %H:%M"),  # last_run
        )

    console.print(table)


@app.command()
def search(
    query: str = typer.Argument(
        ..., help="Search term (searches run IDs, model names, tags)."
    ),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    limit: int = typer.Option(20, help="Maximum results."),
) -> None:
    """Search for runs by ID, model name, or tags (query length is capped)."""
    tracker = get_tracker(db_path)
    if not query or len(query) > MAX_SEARCH_QUERY_LENGTH:
        raise typer.BadParameter(
            f"Query must be 1-{MAX_SEARCH_QUERY_LENGTH} characters."
        )
    escaped_query = _escape_like_pattern(query)

    with _tracker_session(tracker) as session:
        from consist.models.run import Run
        from sqlmodel import select, or_, col

        # Search in multiple fields
        search_query = (
            select(Run)
            .where(
                or_(
                    col(Run.id).contains(escaped_query, escape=LIKE_ESCAPE_CHAR),
                    col(Run.model_name).contains(
                        escaped_query, escape=LIKE_ESCAPE_CHAR
                    ),
                    col(Run.tags).contains(escaped_query, escape=LIKE_ESCAPE_CHAR),
                    col(Run.parent_run_id).contains(
                        escaped_query, escape=LIKE_ESCAPE_CHAR
                    )
                    if escaped_query
                    else False,
                )
            )
            .order_by(*recent_run_order_by())
            .limit(limit)
        )

        results = session.exec(search_query).all()

        if not results:
            console.print(f"[yellow]No runs found matching '{query}'[/yellow]")
            return

        table = Table(title=f"Search Results: '{query}'")
        table.add_column("ID", style="cyan")
        table.add_column("Model", style="green")
        table.add_column("Scenario", style="yellow")
        table.add_column("Status")
        table.add_column("Created", style="dim")

        for run in results:
            status_style = "green" if run.status == "completed" else "red"
            table.add_row(
                run.id,
                run.model_name,
                run.parent_run_id or "-",
                f"[{status_style}]{run.status}[/]",
                run.created_at.strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)


@app.command()
def validate(
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    run_dir: Optional[str] = typer.Option(
        None,
        "--run-dir",
        help="Base directory for resolving relative artifact paths like ./outputs/...",
    ),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow metadata-based run-dir inference for relative artifact validation.",
    ),
    fix: bool = typer.Option(
        False, help="Attempt to fix issues (mark artifacts as missing)."
    ),
) -> None:
    """Check that artifacts referenced in DB actually exist on disk."""
    resolved_db_path = find_db_path(db_path)
    tracker = get_tracker(resolved_db_path, run_dir=run_dir)
    from consist.models.artifact import Artifact

    with _tracker_session(tracker) as session:
        missing = []
        missing_ids: List[uuid.UUID] = []
        batch_size = 1000
        if env_batch_size := os.getenv("CONSIST_VALIDATE_BATCH_SIZE"):
            try:
                batch_size = max(1, int(env_batch_size))
            except ValueError:
                batch_size = 1000

        for artifact_id, key, uri, run_id in _iter_artifact_rows(
            session, batch_size=batch_size
        ):
            if uri.startswith("./"):
                resolution_bases, _ = _build_relative_resolution_bases_for_uri(
                    tracker,
                    container_uri=uri,
                    run_id=run_id,
                    db_path=resolved_db_path,
                    run_dir=run_dir,
                    trust_db=trust_db,
                )
                if resolution_bases:
                    found = False
                    for _, base_dir in resolution_bases:
                        candidate = (base_dir / uri[2:]).resolve()
                        if candidate.exists():
                            found = True
                            break
                    if not found:
                        missing.append((key, uri, run_id))
                        missing_ids.append(artifact_id)
                    continue
            try:
                abs_path = tracker.resolve_uri(uri)
                if not Path(abs_path).exists():
                    missing.append((key, uri, run_id))
                    missing_ids.append(artifact_id)
            except (
                AttributeError,
                NotImplementedError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ):
                missing.append((key, uri, run_id))
                missing_ids.append(artifact_id)

        if not missing:
            console.print("[green]✓ All artifacts validated successfully[/green]")
            return

        if fix and missing_ids:
            updates = (
                select(Artifact)
                .where(col(Artifact.id).in_(set(missing_ids)))
                .order_by(col(Artifact.created_at))
            )
            for artifact in session.exec(cast(Any, updates)).all():
                current_meta = dict(artifact.meta or {})
                current_meta["missing_on_disk"] = True
                artifact.meta = current_meta
                session.add(artifact)
            session.commit()

        console.print(f"[yellow]⚠ Found {len(missing)} missing artifacts:[/yellow]\n")

        table = Table()
        table.add_column("Key", style="yellow")
        table.add_column("URI", style="dim")
        table.add_column("Run ID", style="cyan")

        for key, uri, run_id in missing[:50]:  # Limit display
            table.add_row(key, uri, run_id or "-")

        console.print(table)

        if len(missing) > 50:
            console.print(f"\n[dim]... and {len(missing) - 50} more[/dim]")


@app.command()
def scenarios(
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
    limit: int = typer.Option(20, help="Maximum scenarios to display."),
) -> None:
    """List all scenarios and their run counts."""
    tracker = get_tracker(db_path)
    _render_scenarios(tracker, limit)


@app.command()
def scenario(
    scenario_id: str = typer.Argument(..., help="The scenario ID to inspect."),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Show all runs in a scenario."""
    tracker = get_tracker(db_path)

    with _tracker_session(tracker) as session:
        from consist.models.run import Run

        query = (
            select(Run)
            .where(col(Run.parent_run_id) == scenario_id)
            .order_by(col(Run.created_at))
        )
        results = session.exec(query).all()

        if not results:
            console.print(f"[red]No runs found for scenario '{scenario_id}'[/red]")
            raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

        table = Table(title=f"Runs in Scenario: [cyan]{scenario_id}[/cyan]")
        table.add_column("Model", style="green")
        table.add_column("Year", style="yellow")
        table.add_column("Status")
        table.add_column("Created", style="dim")

        for run in results:
            status_style = "green" if run.status == "completed" else "red"
            table.add_row(
                run.model_name,
                str(run.year) if run.year else "-",
                f"[{status_style}]{run.status}[/]",
                run.created_at.strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)


@app.command()
def runs(
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
    limit: int = typer.Option(10, help="Maximum number of recent runs to display."),
    json_output: bool = typer.Option(
        False, "--json", help="Output runs as JSON for scripting/testing."
    ),
    model_name: Optional[str] = typer.Option(
        None, "--model", help="Filter by model name."
    ),
    tag: Optional[List[str]] = typer.Option(
        None,
        "--tag",
        help="Filter by runs containing a tag. Can be used multiple times.",
    ),
    status: Optional[str] = typer.Option(
        None, "--status", help="Filter by run status (e.g., 'completed', 'failed')."
    ),
) -> None:
    """
    Lists the most recent Consist runs recorded in the provenance database.
    """
    tracker = get_tracker(db_path)
    if json_output:
        with _tracker_session(tracker) as session:
            results = queries.get_runs(
                session, limit=limit, model_name=model_name, tags=tag, status=status
            )
            output = []
            for r in results:
                created_at = getattr(r, "created_at", None)
                output.append(
                    {
                        "id": getattr(r, "id", None),
                        "model": getattr(r, "model_name", None),
                        "status": getattr(r, "status", None),
                        "scenario_id": getattr(r, "parent_run_id", None),
                        "year": getattr(r, "year", None),
                        "created_at": created_at.isoformat() if created_at else None,
                        "duration_seconds": getattr(r, "duration_seconds", None),
                        "tags": getattr(r, "tags", None),
                        "meta": getattr(r, "meta", None),
                    }
                )
            output_json(output)
            return

    _render_runs_table(tracker, limit, model_name, tag, status)


@app.command()
def artifacts(
    run_id: Optional[str] = typer.Argument(
        None,
        help="Run ID to inspect. Omit when using --param/--key-prefix/--family-prefix.",
    ),
    param: Optional[List[str]] = typer.Option(
        None,
        "--param",
        help=(
            "Artifact facet predicate (repeatable): key=value, key>=value, key<=value. "
            "Example: --param beam.phys_sim_iteration=2"
        ),
    ),
    namespace: Optional[str] = typer.Option(
        None,
        "--namespace",
        help="Default facet namespace when predicates omit one.",
    ),
    key_prefix: Optional[str] = typer.Option(
        None,
        "--key-prefix",
        help="Filter by artifact key prefix.",
    ),
    family_prefix: Optional[str] = typer.Option(
        None,
        "--family-prefix",
        help="Filter by artifact_family facet prefix.",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        help="Maximum number of artifact query results.",
    ),
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Display run artifacts or query artifacts by indexed facet parameters."""
    tracker = get_tracker(db_path)
    query_mode = bool(param or key_prefix or family_prefix)
    if not query_mode:
        if not run_id:
            console.print(
                "[red]Provide a run_id or use query options like --param.[/red]"
            )
            raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
        run = tracker.get_run(run_id)
        if not run:
            console.print(f"[red]Run with ID '{run_id}' not found.[/red]")
            raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)
        _render_artifacts_table(tracker, run_id)
        return

    if run_id is not None:
        console.print(
            "[red]run_id cannot be combined with --param/--namespace/--key-prefix/"
            "--family-prefix.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    try:
        results = queries.find_artifacts_by_params(
            tracker,
            params=param or [],
            namespace=namespace,
            key_prefix=key_prefix,
            artifact_family_prefix=family_prefix,
            limit=limit,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR) from exc

    if not results:
        console.print("[yellow]No artifacts matched the provided filters.[/yellow]")
        raise typer.Exit()
    _render_artifact_query_table(results)


def _build_lineage_tree(
    tree_node: Tree, lineage_info: Dict[str, Any], visited_runs: set
) -> None:
    """Helper function to recursively build a rich.tree.Tree for artifact lineage."""
    run_info = lineage_info.get("producing_run")
    if not run_info:
        tree_node.add("[dim]No producing run found[/dim]")
        return

    run = run_info["run"]
    if run.id in visited_runs:
        tree_node.add(f"🔄 [dim]Recursive dependency on Run: {run.id}[/dim]")
        return

    visited_runs.add(run.id)
    run_branch = tree_node.add(
        f"🏃‍♂️ [bold green]Run:[/] [cyan]{run.id}[/] [dim]({run.model_name})[/dim]"
    )

    for i, input_lineage in enumerate(run_info.get("inputs", [])):
        artifact = input_lineage["artifact"]
        is_last = i == len(run_info["inputs"]) - 1
        prefix = "└──" if is_last else "├──"
        child_branch = run_branch.add(
            f"{prefix} 📄 [bold]Input:[/] [yellow]{artifact.key}[/yellow] [dim]({artifact.driver})[/dim]"
        )
        _build_lineage_tree(child_branch, input_lineage, visited_runs.copy())


def _resolve_cli_path(path: Path | str) -> Path:
    """Resolve a CLI path against cwd, preserving absolute inputs."""
    path_obj = Path(path).expanduser()
    if not path_obj.is_absolute():
        path_obj = Path.cwd() / path_obj
    return path_obj.resolve()


def _set_tracker_run_dir(tracker: Tracker, run_dir: Path) -> None:
    """Update tracker and file-system manager run_dir in sync."""
    tracker.run_dir = run_dir
    tracker.fs.run_dir = run_dir


def _build_relative_resolution_bases(
    tracker: Tracker,
    artifact: "Artifact",
    *,
    db_path: Optional[str],
    run_dir: Optional[str],
    trust_db: bool,
) -> tuple[List[Tuple[str, Path]], Optional[Path]]:
    return _build_relative_resolution_bases_for_uri(
        tracker,
        container_uri=artifact.container_uri,
        run_id=artifact.run_id,
        db_path=db_path,
        run_dir=run_dir,
        trust_db=trust_db,
    )


def _build_relative_resolution_bases_for_uri(
    tracker: Tracker,
    *,
    container_uri: str,
    run_id: Optional[str],
    db_path: Optional[str],
    run_dir: Optional[str],
    trust_db: bool,
) -> tuple[List[Tuple[str, Path]], Optional[Path]]:
    """Build ordered base directories for resolving relative artifact URIs."""
    if not container_uri.startswith("./"):
        return [], None

    bases: List[Tuple[str, Path]] = []
    seen: set[Path] = set()

    def _append(label: str, base: Path | str) -> None:
        resolved = _resolve_cli_path(base)
        if resolved in seen:
            return
        seen.add(resolved)
        bases.append((label, resolved))

    if run_dir:
        _append("explicit --run-dir", run_dir)

    if db_path:
        _append("db-path parent", _resolve_cli_path(db_path).parent)

    _append("current working directory", Path.cwd())

    db_metadata_run_dir: Optional[Path] = None
    run = tracker.get_run(run_id) if run_id else None
    if run and isinstance(run.meta, dict):
        candidate = run.meta.get("_physical_run_dir")
        if isinstance(candidate, str) and candidate:
            db_metadata_run_dir = _resolve_cli_path(candidate)
            if trust_db:
                _append(
                    "trusted run metadata (_physical_run_dir)",
                    db_metadata_run_dir,
                )

    return bases, db_metadata_run_dir


def _print_missing_artifact_file_help(
    tracker: Tracker,
    artifact: "Artifact",
    *,
    attempted_paths: Optional[List[Tuple[str, Path]]] = None,
    db_metadata_run_dir: Optional[Path] = None,
    trust_db: bool = False,
) -> None:
    abs_path = tracker.resolve_uri(artifact.container_uri)
    from consist.tools.mount_diagnostics import (
        build_mount_resolution_hint,
        format_missing_artifact_mount_help,
    )

    hint = build_mount_resolution_hint(
        artifact.container_uri, artifact_meta=artifact.meta, mounts=tracker.mounts
    )
    help_text = (
        format_missing_artifact_mount_help(hint, resolved_path=abs_path)
        if hint
        else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
    )
    extra_lines: List[str] = []
    if attempted_paths:
        extra_lines.append("Attempted resolution bases:")
        for label, path in attempted_paths:
            extra_lines.append(f"- {label}: {path}")
    if db_metadata_run_dir is not None and not trust_db:
        extra_lines.append(f"Database metadata suggests run_dir: {db_metadata_run_dir}")
        extra_lines.append(f"Try: --run-dir {db_metadata_run_dir}")
        extra_lines.append(
            "Or re-run with --trust-db to allow _physical_run_dir fallback."
        )
    if extra_lines:
        help_text = f"{help_text}\n" + "\n".join(extra_lines)

    console.print(
        f"[red]Artifact file not found at: {artifact.container_uri}[/red]\n{help_text}"
    )


def _print_optional_dependency_hint(driver: str) -> None:
    extras_map = {
        "zarr": "zarr",
        "h5": "hdf5",
        "hdf5": "hdf5",
        "h5_table": "hdf5",
        "geojson": "spatial",
        "shapefile": "spatial",
        "geopackage": "spatial",
    }
    extra = extras_map.get(driver)
    if extra is None:
        return
    console.print(
        f"Hint: install optional support with pip install -e '.[{extra}]'",
        markup=False,
    )


def _load_artifact_with_diagnostics(
    tracker: Tracker,
    artifact: "Artifact",
    *,
    n_rows: int | None = None,
    resolution_bases: Optional[List[Tuple[str, Path]]] = None,
    db_metadata_run_dir: Optional[Path] = None,
    trust_db: bool = False,
) -> Any | None:
    attempted_paths: List[Tuple[str, Path]] = []

    def _load_once() -> Any:
        import consist

        load_kwargs: Dict[str, Any] = {}
        if artifact.driver == "csv" and n_rows is not None:
            # Avoid loading the full file when we only need a small preview.
            load_kwargs["nrows"] = n_rows
        return consist.load(
            artifact, tracker=tracker, db_fallback="always", **load_kwargs
        )

    def _load_from_base(
        base_dir: Path, label: str, original_run_dir: Path
    ) -> Any | None:
        try:
            _set_tracker_run_dir(tracker, base_dir)
            return _load_once()
        except FileNotFoundError:
            attempted_paths.append(
                (label, (base_dir / artifact.container_uri[2:]).resolve())
            )
            return None
        finally:
            _set_tracker_run_dir(tracker, original_run_dir)

    try:
        if artifact.container_uri.startswith("./") and resolution_bases:
            original_run_dir = _resolve_cli_path(tracker.run_dir)
            for label, base_dir in resolution_bases:
                loaded = _load_from_base(base_dir, label, original_run_dir)
                if loaded is not None:
                    return loaded

            _print_missing_artifact_file_help(
                tracker,
                artifact,
                attempted_paths=attempted_paths,
                db_metadata_run_dir=db_metadata_run_dir,
                trust_db=trust_db,
            )
            return None

        return _load_once()
    except FileNotFoundError:
        _print_missing_artifact_file_help(
            tracker,
            artifact,
            attempted_paths=attempted_paths or None,
            db_metadata_run_dir=db_metadata_run_dir,
            trust_db=trust_db,
        )
        return None
    except ImportError as e:
        console.print(
            f"[red]Missing optional dependency while loading artifact: {e}[/red]"
        )
        _print_optional_dependency_hint(str(artifact.driver))
        return None
    except ValueError as e:
        console.print(
            f"[red]Unsupported artifact driver '{artifact.driver}': {e}[/red]"
        )
        return None
    except Exception as e:
        console.print(f"[red]Error loading artifact: {e}[/red]")
        return None


@app.command()
def lineage(
    artifact_key: str = typer.Argument(
        ..., help="The key or ID of the artifact to trace."
    ),
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Traces and displays the full lineage of an artifact."""
    tracker = get_tracker(db_path)
    lineage_data = tracker.get_artifact_lineage(artifact_key)

    if not lineage_data:
        console.print(
            f"[red]Could not find artifact with key or ID '{artifact_key}'.[/red]"
        )
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    start_artifact = lineage_data["artifact"]
    tree = Tree(
        f"🌳 [bold]Lineage for Artifact:[/] [yellow]{start_artifact.key}[/yellow] ([cyan]{start_artifact.id}[/cyan])"
    )

    if not lineage_data.get("producing_run"):
        tree.add("[dim]No producing run found. This may be a primary input.[/dim]")
    else:
        _build_lineage_tree(tree, lineage_data, set())

    console.print(tree)


@app.command()
def summary(
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Displays a high-level summary of the provenance database."""
    tracker = get_tracker(db_path)
    with _tracker_session(tracker) as session:
        summary_data = queries.get_summary(session)

    _render_summary(summary_data)
    if summary_data["total_runs"] == 0:
        raise typer.Exit()


@app.command()
def preview(
    artifact_key: str = typer.Argument(
        ..., help="The key or ID of the artifact to preview."
    ),
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
    run_dir: Optional[str] = typer.Option(
        None,
        "--run-dir",
        help="Base directory for resolving relative artifact paths like ./outputs/...",
    ),
    n_rows: int = typer.Option(5, "--rows", "-n", help="Number of rows to display."),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow metadata-based mount and run-dir inference for artifact resolution.",
    ),
    mount: Optional[List[str]] = typer.Option(
        None,
        "--mount",
        help=(
            "Mount override mapping (repeatable): NAME=PATH. "
            "Example: --mount workspace=/path/to/archive"
        ),
    ),
) -> None:
    """Shows a small preview of an artifact (tabular or array-like when supported)."""
    resolved_db_path = find_db_path(db_path)
    mount_overrides = _resolve_mount_overrides_or_exit(mount)
    tracker = get_tracker(
        resolved_db_path,
        run_dir=run_dir,
        mounts=mount_overrides or None,
    )

    # Fetch artifact first to give a precise message (unsupported driver vs missing)
    artifact = tracker.get_artifact(artifact_key)
    if not artifact:
        console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    _ensure_tracker_mounts_for_artifact(tracker, artifact, trust_db=trust_db)
    resolution_bases, db_metadata_run_dir = _build_relative_resolution_bases(
        tracker,
        artifact,
        db_path=resolved_db_path,
        run_dir=run_dir,
        trust_db=trust_db,
    )
    data = _load_artifact_with_diagnostics(
        tracker,
        artifact,
        n_rows=n_rows,
        resolution_bases=resolution_bases,
        db_metadata_run_dir=db_metadata_run_dir,
        trust_db=trust_db,
    )
    if data is None:
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    console.print(f"Preview: {artifact_key} [dim]({artifact.driver})[/dim]")

    if isinstance(data, duckdb.DuckDBPyRelation):
        df = data.limit(n_rows).df()
    elif isinstance(data, pd.DataFrame):
        df = data.head(n_rows)
    else:
        df = None

    if df is not None:
        table = Table()

        # Use pandas types to set column styles
        for col_name in df.columns:
            style = "cyan"
            if pd.api.types.is_numeric_dtype(df[col_name]):
                style = "magenta"
            elif pd.api.types.is_datetime64_any_dtype(df[col_name]):
                style = "green"
            table.add_column(str(col_name), style=style)

        for _, row in df.iterrows():
            table.add_row(*[str(item) for item in row])

        console.print(table)
        return

    xr = _optional_xarray()
    if xr is not None and isinstance(data, (xr.Dataset, xr.DataArray)):
        ds: xr.Dataset
        if isinstance(data, xr.DataArray):
            ds = data.to_dataset(name=getattr(data, "name", None) or "data")
        else:
            ds = data

        dims_table = Table(title="Dimensions")
        dims_table.add_column("Dim", style="cyan")
        dims_table.add_column("Size", style="magenta")
        for dim_name, dim_size in ds.sizes.items():
            dims_table.add_row(str(dim_name), str(dim_size))
        console.print(dims_table)

        if not ds.data_vars:
            console.print("[yellow]No data variables found.[/yellow]")
            return

        vars_table = Table(title="Data Variables")
        vars_table.add_column("Name", style="cyan")
        vars_table.add_column("Dtype", style="magenta")
        vars_table.add_column("Dims", style="green")
        vars_table.add_column("Shape", style="yellow")

        for var_name, da in ds.data_vars.items():
            vars_table.add_row(
                str(var_name),
                str(da.dtype),
                ", ".join(map(str, da.dims)),
                str(tuple(int(x) for x in da.shape)),
            )
        console.print(vars_table)

        # Best-effort sample values from the first variable.
        first_name = next(iter(ds.data_vars))
        da0 = ds[first_name]
        indexers: Dict[str, Any] = {}
        for i, dim in enumerate(da0.dims):
            if i == 0:
                indexers[dim] = slice(0, min(n_rows, int(da0.sizes[dim])))
            else:
                indexers[dim] = 0
        try:
            sample = da0.isel(indexers).values
            console.print(
                Panel(
                    str(sample),
                    title=f"Sample: {first_name} (isel {indexers})",
                    border_style="dim",
                )
            )
        except Exception:
            console.print(
                f"[yellow]Could not materialize sample values for '{first_name}'.[/yellow]"
            )
        return

    console.print(
        f"[yellow]Preview not implemented for loaded type: {type(data).__name__}[/yellow]"
    )
    console.print(
        "[dim]Hint: load programmatically via `consist.load(artifact, tracker=...)`[/dim]"
    )


class ConsistShell(cmd.Cmd):
    """Interactive shell for exploring Consist provenance."""

    intro = "Welcome to Consist Shell. Type help or ? to list commands.\n"
    prompt = "(consist) "
    _RUN_COMPLETION_FLAGS: Tuple[str, ...] = (
        "--limit",
        "--model",
        "--status",
        "--tag",
        "--json",
    )
    _ARTIFACT_COMPLETION_FLAGS: Tuple[str, ...] = (
        "--param",
        "--namespace",
        "--key-prefix",
        "--family-prefix",
        "--limit",
    )
    _PREVIEW_COMPLETION_FLAGS: Tuple[str, ...] = ("--rows", "-n", "--hash")
    _SCENARIOS_COMPLETION_FLAGS: Tuple[str, ...] = ("--limit",)
    _DB_SUBCOMMANDS: Tuple[str, ...] = (
        "inspect",
        "doctor",
        "snapshot",
        "rebuild",
        "compact",
        "export",
        "merge",
        "purge",
        "fix-status",
    )
    _SCHEMA_SUBCOMMANDS: Tuple[str, ...] = ("capture-file", "export", "apply-fks")
    _VIEWS_SUBCOMMANDS: Tuple[str, ...] = ("create",)
    _CLI_ROOT_COMMANDS: Tuple[str, ...] = (
        "search",
        "validate",
        "scenarios",
        "scenario",
        "runs",
        "artifacts",
        "lineage",
        "summary",
        "preview",
        "show",
        "schema",
        "views",
        "db",
    )
    _PASSTHROUGH_COMMANDS: Tuple[str, ...] = (
        "search",
        "validate",
        "scenario",
        "lineage",
        "schema",
        "views",
        "db",
    )
    _SCHEMA_STUB_COMPLETION_FLAGS: Tuple[str, ...] = (
        "--artifact-id",
        "--hash",
        "--run-id",
        "--source",
        "--class-name",
        "--table-name",
        "--include-system-cols",
        "--no-stats-comments",
        "--concrete",
    )
    _SCHEMA_STUB_SOURCES: Tuple[str, ...] = ("file", "duckdb", "user_provided")
    _PICKER_LIMIT = 20

    def __init__(
        self,
        tracker: Tracker,
        *,
        trust_db: bool = False,
        db_path: Optional[str] = None,
        run_dir: Optional[str] = None,
        mount_overrides: Optional[Mapping[str, str]] = None,
    ):
        super().__init__()
        self.tracker = tracker
        self.trust_db = trust_db
        self.db_path = db_path
        self.run_dir = run_dir
        self.mount_overrides = dict(mount_overrides or {})
        self._last_run_ids: List[str] = []
        self._last_artifact_ids: List[str] = []
        self._last_artifact_run_id: Optional[str] = None
        self._history_path = Path.home() / ".consist" / "shell_history"
        self._history_saved = False
        self._load_history()

    def _safe_split(self, value: str) -> List[str]:
        try:
            return shlex.split(value)
        except ValueError:
            return value.split()

    @staticmethod
    def _is_tty() -> bool:
        return _is_interactive_tty()

    @staticmethod
    def _complete_choices(text: str, options: Iterable[str]) -> List[str]:
        return [option for option in options if option.startswith(text)]

    @staticmethod
    def _unique_completion_options(options: Iterable[str]) -> List[str]:
        seen: set[str] = set()
        ordered: List[str] = []
        for option in options:
            if option not in seen:
                seen.add(option)
                ordered.append(option)
        return ordered

    def _is_first_argument(self, line: str, begidx: int) -> bool:
        return len(self._safe_split(line[:begidx])) <= 1

    @staticmethod
    def _has_option(args: Sequence[str], option_name: str) -> bool:
        return any(
            token == option_name or token.startswith(f"{option_name}=")
            for token in args
        )

    def _cached_run_ref_tokens(self) -> List[str]:
        return [f"#{index}" for index in range(1, len(self._last_run_ids) + 1)]

    def _cached_artifact_ref_tokens(self) -> List[str]:
        return [f"@{index}" for index in range(1, len(self._last_artifact_ids) + 1)]

    def _artifact_ref_population_hint(self) -> str:
        if self._last_run_ids:
            return (
                "Run `artifacts <run_id>` or `artifacts #<n>` first to populate @refs."
            )
        return "Run `artifacts <run_id>` first to populate @refs."

    @staticmethod
    def _cli_command_path(args: Sequence[str]) -> tuple[str, ...]:
        if not args:
            return ()
        if args[0] in {"db", "schema", "views"}:
            if len(args) > 1 and not args[1].startswith("-"):
                return args[0], args[1]
            return (args[0],)
        return (args[0],)

    def _prepare_cli_args(self, args: List[str]) -> List[str]:
        """Inject shell-level defaults when routed through Typer."""
        prepared = list(args)
        command_path = self._cli_command_path(prepared)
        group_root_without_subcommand = command_path in {
            ("db",),
            ("schema",),
            ("views",),
        }
        root_option_invocation = bool(prepared and prepared[0].startswith("-"))

        if (
            self.db_path
            and not root_option_invocation
            and not group_root_without_subcommand
            and not self._has_option(prepared, "--db-path")
        ):
            prepared.extend(["--db-path", self.db_path])

        if (
            command_path in {("preview",), ("validate",)}
            and self.run_dir
            and not self._has_option(prepared, "--run-dir")
        ):
            prepared.extend(["--run-dir", self.run_dir])

        if (
            command_path
            in {
                ("preview",),
                ("validate",),
                ("schema", "capture-file"),
            }
            and self.trust_db
            and not self._has_option(prepared, "--trust-db")
        ):
            prepared.append("--trust-db")

        if (
            command_path in {("preview",), ("schema", "capture-file")}
            and self.mount_overrides
            and not self._has_option(prepared, "--mount")
        ):
            for name, root in self.mount_overrides.items():
                prepared.extend(["--mount", f"{name}={root}"])

        return prepared

    def _invoke_cli(self, args: List[str]) -> None:
        if not args:
            return
        if args[0] == "shell":
            console.print("[red]Error: already inside Consist shell.[/red]")
            return
        try:
            prepared = self._prepare_cli_args(args)
            app(args=prepared, prog_name="consist", standalone_mode=False)
        except typer.Exit:
            return
        except click.ClickException as exc:
            console.print(f"[red]Error: {exc.format_message()}[/red]")
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def _invoke_cli_command(self, command_name: str, arg: str) -> None:
        command_args = [command_name, *self._safe_split(arg)]
        self._invoke_cli(command_args)

    def _normalize_schema_capture_file_shell_args(
        self, args: Sequence[str]
    ) -> Optional[List[str]]:
        if not args or args[0] != "capture-file":
            return list(args)

        normalized = [args[0]]
        i = 1
        positional_consumed = False
        while i < len(args):
            token = args[i]
            if token == "--artifact-ref":
                if i + 1 >= len(args):
                    raise ValueError("--artifact-ref requires a value")
                raw_ref = args[i + 1]
                if not raw_ref.startswith("@"):
                    raise ValueError("--artifact-ref must look like @1")
                artifact_id = self._resolve_artifact_ref(raw_ref)
                if artifact_id is None:
                    return None
                normalized.extend(["--artifact-id", artifact_id])
                i += 2
                continue
            if token.startswith("--artifact-ref="):
                raw_ref = token.split("=", 1)[1]
                if not raw_ref.startswith("@"):
                    raise ValueError("--artifact-ref must look like @1")
                artifact_id = self._resolve_artifact_ref(raw_ref)
                if artifact_id is None:
                    return None
                normalized.extend(["--artifact-id", artifact_id])
                i += 1
                continue
            if not positional_consumed and not token.startswith("-"):
                positional_consumed = True
                if token.startswith("@"):
                    artifact_id = self._resolve_artifact_ref(token)
                    if artifact_id is None:
                        return None
                    normalized.extend(["--artifact-id", artifact_id])
                else:
                    normalized.append(token)
                i += 1
                continue
            normalized.append(token)
            i += 1
        return normalized

    def _recent_run_ids(self, limit: int = _PICKER_LIMIT) -> List[str]:
        try:
            from consist.models.run import Run

            with _tracker_session(self.tracker) as session:
                statement = select(Run.id).order_by(*recent_run_order_by()).limit(limit)
                return [
                    str(run_id) for run_id in session.exec(statement).all() if run_id
                ]
        except Exception:
            return []

    def _recent_artifact_keys(self, limit: int = _PICKER_LIMIT) -> List[str]:
        try:
            from consist.models.artifact import Artifact

            with _tracker_session(self.tracker) as session:
                statement = (
                    select(Artifact.key)
                    .order_by(col(Artifact.created_at).desc())
                    .limit(limit)
                )
                seen: set[str] = set()
                keys: List[str] = []
                for key in session.exec(statement).all():
                    if isinstance(key, str) and key and key not in seen:
                        seen.add(key)
                        keys.append(key)
                return keys
        except Exception:
            return []

    def _load_history(self) -> None:
        if _READLINE is None:
            return
        try:
            if self._history_path.exists():
                _READLINE.read_history_file(str(self._history_path))
        except Exception:
            return

    def _save_history_once(self) -> None:
        if self._history_saved or _READLINE is None:
            return
        try:
            self._history_path.parent.mkdir(parents=True, exist_ok=True)
            _READLINE.write_history_file(str(self._history_path))
        except Exception:
            return
        self._history_saved = True

    @staticmethod
    def _select_from_list(
        title: str,
        choices: List[str],
        *,
        missing_message: str,
        prompt: str,
    ) -> Optional[str]:
        if not choices:
            console.print(missing_message)
            return None

        console.print(title)
        for index, value in enumerate(choices, start=1):
            console.print(f"  {index}. {value}")

        while True:
            raw_choice = input(prompt).strip()
            if not raw_choice:
                return None
            try:
                selected_index = int(raw_choice)
            except ValueError:
                console.print(
                    "[red]Error: enter a number or press Enter to cancel[/red]"
                )
                continue
            if 1 <= selected_index <= len(choices):
                return choices[selected_index - 1]
            console.print(
                f"[red]Error: selection must be between 1 and {len(choices)}[/red]"
            )

    def _resolve_run_id(self, arg: str, *, command_name: str) -> Optional[str]:
        run_id = arg.strip()
        if run_id:
            if run_id.startswith("#"):
                if not self._last_run_ids:
                    console.print(
                        "[red]Error: no cached run refs. Run `runs` first.[/red]"
                    )
                    return None
                raw_index = run_id[1:]
                if not raw_index:
                    console.print("[red]Error: run ref must look like #1[/red]")
                    return None
                try:
                    index = int(raw_index)
                except ValueError:
                    console.print("[red]Error: run ref must look like #1[/red]")
                    return None
                if index < 1 or index > len(self._last_run_ids):
                    console.print(
                        "[red]Error: run ref out of range "
                        f"(1-{len(self._last_run_ids)}).[/red]"
                    )
                    return None
                return self._last_run_ids[index - 1]
            return run_id
        if not self._is_tty():
            console.print(
                "[red]Error: run_id required. Pass a run_id or #<n>, or run `runs` "
                "first to populate shortcuts.[/red]"
            )
            return None
        return self._select_from_list(
            f"Recent runs for [cyan]{command_name}[/cyan]:",
            self._recent_run_ids(),
            missing_message=(
                "[yellow]No runs available. Pass an explicit run_id or use "
                "`context` to inspect shell defaults.[/yellow]"
            ),
            prompt="Choose run number (Enter to cancel): ",
        )

    def _resolve_artifact_key(self, args: List[str]) -> Optional[str]:
        if args:
            return self._resolve_artifact_ref(args[0])
        if not self._is_tty():
            console.print(
                "[red]Error: artifact_key required. Pass a key or @<n>. "
                f"{self._artifact_ref_population_hint()} Use `context` to inspect "
                "shell defaults.[/red]"
            )
            return None
        return self._select_from_list(
            "Recent artifacts for [cyan]preview[/cyan]:",
            self._recent_artifact_keys(),
            missing_message=(
                "[yellow]No artifacts available. "
                f"{self._artifact_ref_population_hint()}[/yellow]"
            ),
            prompt="Choose artifact number (Enter to cancel): ",
        )

    def _resolve_artifact_ref(self, value: str) -> Optional[str]:
        token = value.strip()
        if not token.startswith("@"):
            return token
        if not self._last_artifact_ids:
            console.print(
                "[red]Error: no cached artifact refs. "
                f"{self._artifact_ref_population_hint()}[/red]"
            )
            return None
        raw_index = token[1:]
        if not raw_index:
            console.print("[red]Error: artifact ref must look like @1[/red]")
            return None
        try:
            index = int(raw_index)
        except ValueError:
            console.print("[red]Error: artifact ref must look like @1[/red]")
            return None
        if index < 1 or index > len(self._last_artifact_ids):
            console.print(
                "[red]Error: artifact ref out of range "
                f"(1-{len(self._last_artifact_ids)}).[/red]"
            )
            return None
        return self._last_artifact_ids[index - 1]

    def _lookup_artifact_by_hash_prefix(
        self, hash_prefix: str, *, command_name: str, run_id: Optional[str] = None
    ) -> Optional["Artifact"]:
        candidate = hash_prefix.strip()
        if not candidate:
            console.print("[red]Error: --hash requires a non-empty prefix.[/red]")
            return None

        try:
            from consist.models.artifact import Artifact

            with _tracker_session(self.tracker) as session:
                # Intentional hash usage: this command is explicitly a hash-prefix
                # lookup UX, so ``content_id`` would be the wrong identifier here.
                statement = select(Artifact).where(
                    col(Artifact.hash).is_not(None),
                    col(Artifact.hash).startswith(candidate),
                )
                if run_id is not None:
                    statement = statement.where(col(Artifact.run_id) == run_id)
                statement = statement.order_by(col(Artifact.created_at).desc()).limit(6)
                matches = session.exec(statement).all()
        except Exception as exc:
            console.print(f"[red]Error: failed hash lookup: {exc}[/red]")
            return None

        if not matches:
            scope_text = f" in run '{run_id}'" if run_id else ""
            console.print(
                f"[red]No artifact found for hash prefix '{candidate}'{scope_text} "
                f"while running {command_name}.[/red]"
            )
            return None

        if len(matches) > 1:
            scope_text = f" in run '{run_id}'" if run_id else ""
            console.print(
                f"[red]Hash prefix '{candidate}' is ambiguous{scope_text}. "
                f"Use a longer prefix for {command_name}.[/red]"
            )
            for artifact in matches[:5]:
                console.print(
                    "[dim]  "
                    f"{artifact.hash} (key={artifact.key}, id={artifact.id}, run={artifact.run_id})"
                    "[/dim]"
                )
            return None

        return matches[0]

    def _parse_schema_stub_args(self, arg: str) -> Dict[str, Any]:
        args = self._safe_split(arg)
        parsed: Dict[str, Any] = {
            "artifact_key": None,
            "artifact_id": None,
            "hash_prefix": None,
            "run_id": None,
            "source": None,
            "class_name": None,
            "table_name": None,
            "include_system_cols": False,
            "include_stats_comments": True,
            "abstract": True,
        }

        i = 0
        while i < len(args):
            token = args[i]
            if token == "--class-name":
                if i + 1 >= len(args):
                    raise ValueError("--class-name requires a value")
                parsed["class_name"] = args[i + 1]
                i += 2
                continue
            if token == "--table-name":
                if i + 1 >= len(args):
                    raise ValueError("--table-name requires a value")
                parsed["table_name"] = args[i + 1]
                i += 2
                continue
            if token == "--artifact-id":
                if i + 1 >= len(args):
                    raise ValueError("--artifact-id requires a UUID value")
                parsed["artifact_id"] = args[i + 1]
                i += 2
                continue
            if token == "--hash":
                if i + 1 >= len(args):
                    raise ValueError("--hash requires a prefix value")
                parsed["hash_prefix"] = args[i + 1]
                i += 2
                continue
            if token.startswith("--hash="):
                parsed["hash_prefix"] = token.split("=", 1)[1]
                i += 1
                continue
            if token == "--run-id":
                if i + 1 >= len(args):
                    raise ValueError("--run-id requires a value")
                parsed["run_id"] = args[i + 1]
                i += 2
                continue
            if token == "--source":
                if i + 1 >= len(args):
                    raise ValueError(
                        "--source requires a value (file|duckdb|user_provided)"
                    )
                parsed["source"] = args[i + 1]
                i += 2
                continue
            if token == "--include-system-cols":
                parsed["include_system_cols"] = True
                i += 1
                continue
            if token == "--no-stats-comments":
                parsed["include_stats_comments"] = False
                i += 1
                continue
            if token == "--concrete":
                parsed["abstract"] = False
                i += 1
                continue
            if token.startswith("--"):
                raise ValueError(f"Unknown option: {token}")
            if parsed["artifact_key"] is not None:
                raise ValueError(
                    "schema_stub accepts at most one positional artifact_key."
                )
            parsed["artifact_key"] = token
            i += 1

        artifact_key = parsed["artifact_key"]
        artifact_id = parsed["artifact_id"]
        hash_prefix = parsed["hash_prefix"]
        run_id = parsed["run_id"]
        source = parsed["source"]

        selector_count = sum(
            1 for value in (artifact_key, artifact_id, hash_prefix) if value is not None
        )
        if selector_count == 0:
            raise ValueError(
                "artifact_key required (or provide --artifact-id or --hash)."
            )
        if selector_count > 1:
            raise ValueError(
                "Provide exactly one selector: artifact_key, --artifact-id, or --hash."
            )
        if artifact_id is not None:
            try:
                uuid.UUID(str(artifact_id))
            except ValueError as exc:
                raise ValueError("--artifact-id must be a UUID") from exc
        if run_id is not None and artifact_id is not None:
            raise ValueError("--run-id can only be used with artifact_key selection.")
        if source is not None and source not in self._SCHEMA_STUB_SOURCES:
            allowed = "|".join(self._SCHEMA_STUB_SOURCES)
            raise ValueError(f"--source must be one of: {allowed}")

        return parsed

    def do_ls(self, arg: str) -> None:
        """Alias for runs/artifacts. Usage: ls [<run_id>]"""
        parts = self._safe_split(arg)
        if not parts or parts[0].startswith("-"):
            self.do_runs(arg)
            return
        self.do_artifacts(arg)

    def do_cat(self, arg: str) -> None:
        """Alias for preview. Usage: cat <artifact_key>"""
        self.do_preview(arg)

    def do_q(self, arg: str) -> bool:
        """Alias for quit. Usage: q"""
        return self.do_quit(arg)

    def do_cli(self, arg: str) -> None:
        """Run any consist CLI command. Usage: cli <command> [args...]"""
        args = self._safe_split(arg)
        if not args:
            console.print("[red]Error: provide a command, e.g. `cli db inspect`.[/red]")
            return
        self._invoke_cli(args)

    def do_consist(self, arg: str) -> None:
        """Alias for cli. Usage: consist <command> [args...]"""
        self.do_cli(arg)

    def do_context(self, arg: str) -> None:
        """Show active shell defaults used for routed commands."""
        del arg
        defaults = Table(title="Shell Context")
        defaults.add_column("Setting", style="cyan")
        defaults.add_column("Value", style="green")
        defaults.add_row("db_path", self.db_path or "(auto)")
        defaults.add_row("run_dir", self.run_dir or "(tracker default)")
        defaults.add_row("trust_db", str(self.trust_db))
        defaults.add_row(
            "mount_overrides",
            ", ".join(
                f"{name}={root}" for name, root in sorted(self.mount_overrides.items())
            )
            or "(none)",
        )
        defaults.add_row("tracker_run_dir", str(getattr(self.tracker, "run_dir", "")))
        console.print(defaults)

    def do_search(self, arg: str) -> None:
        """Search runs by id/model/tags. Usage: search <query> [--limit N]"""
        self._invoke_cli_command("search", arg)

    def do_validate(self, arg: str) -> None:
        """Validate artifact files. Usage: validate [--fix] [--run-dir PATH]"""
        self._invoke_cli_command("validate", arg)

    def do_scenario(self, arg: str) -> None:
        """Show runs for one scenario. Usage: scenario <scenario_id>"""
        self._invoke_cli_command("scenario", arg)

    def do_lineage(self, arg: str) -> None:
        """Show artifact lineage. Usage: lineage <artifact_key>"""
        self._invoke_cli_command("lineage", arg)

    def do_db(self, arg: str) -> None:
        """Run DB maintenance commands. Usage: db <subcommand> [args...]"""
        self._invoke_cli_command("db", arg)

    def do_schema(self, arg: str) -> None:
        """Run schema subcommands. Usage: schema <capture-file|export|apply-fks> ..."""
        try:
            schema_args = self._safe_split(arg)
            normalized = self._normalize_schema_capture_file_shell_args(schema_args)
            if normalized is None:
                return
            self._invoke_cli(["schema", *normalized])
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_views(self, arg: str) -> None:
        """Run view subcommands. Usage: views create ..."""
        self._invoke_cli_command("views", arg)

    def do_runs(self, arg: str) -> None:
        """List recent runs. Usage: runs [--limit N] [--model NAME] [--status STATUS] [--tag TAG] [--json]"""
        try:
            args = self._safe_split(arg)
            limit = 10
            model_name = None
            status = None
            tags: List[str] = []
            json_output = False

            i = 0
            while i < len(args):
                token = args[i]
                if token == "--limit":
                    if i + 1 >= len(args):
                        raise ValueError("--limit requires a value")
                    limit = _parse_bounded_int(
                        args[i + 1],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
                    i += 2
                elif token == "--model":
                    if i + 1 >= len(args):
                        raise ValueError("--model requires a value")
                    model_name = args[i + 1]
                    i += 2
                elif token == "--status":
                    if i + 1 >= len(args):
                        raise ValueError("--status requires a value")
                    status = args[i + 1]
                    i += 2
                elif token == "--tag":
                    if i + 1 >= len(args):
                        raise ValueError("--tag requires a value")
                    tags.append(args[i + 1])
                    i += 2
                elif token == "--json":
                    json_output = True
                    i += 1
                elif token.startswith("-"):
                    raise ValueError(f"Unknown option: {token}")
                else:
                    raise ValueError(f"Unexpected argument: {token}")

            if json_output:
                with _tracker_session(self.tracker) as session:
                    results = queries.get_runs(
                        session,
                        limit=limit,
                        model_name=model_name,
                        tags=tags if tags else None,
                        status=status,
                    )
                    payload = []
                    for run in results:
                        created_at = getattr(run, "created_at", None)
                        payload.append(
                            {
                                "id": getattr(run, "id", None),
                                "model": getattr(run, "model_name", None),
                                "status": getattr(run, "status", None),
                                "scenario_id": getattr(run, "parent_run_id", None),
                                "year": getattr(run, "year", None),
                                "created_at": (
                                    created_at.isoformat() if created_at else None
                                ),
                                "duration_seconds": getattr(
                                    run, "duration_seconds", None
                                ),
                                "tags": getattr(run, "tags", None),
                                "meta": getattr(run, "meta", None),
                            }
                        )
                output_json(payload)
                return

            _render_runs_table(
                self.tracker, limit, model_name, tags if tags else None, status
            )
            with _tracker_session(self.tracker) as session:
                cached_runs = queries.get_runs(
                    session,
                    limit=limit,
                    model_name=model_name,
                    tags=tags if tags else None,
                    status=status,
                )
            self._last_run_ids = [
                str(getattr(run, "id"))
                for run in cached_runs
                if getattr(run, "id", None) is not None
            ]
            if self._last_run_ids:
                console.print("[dim]Run refs:[/dim]")
                for index, run_id in enumerate(self._last_run_ids, start=1):
                    console.print(f"[dim]  #{index}: {run_id}[/dim]")
                console.print("[dim]Tip: use show #<n> or artifacts #<n>.[/dim]")
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_show(self, arg: str) -> None:
        """Show details for a run. Usage: show <run_id>"""
        args = self._safe_split(arg)
        if args and (len(args) != 1 or args[0].startswith("-")):
            self._invoke_cli_command("show", arg)
            return

        run_id = self._resolve_run_id(args[0] if args else "", command_name="show")
        if run_id is None:
            return

        try:
            run = self.tracker.get_run(run_id)
            if not run:
                console.print(f"[red]Run '{run_id}' not found.[/red]")
                return
            _render_run_details(run)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_artifacts(self, arg: str) -> None:
        """Show artifacts for a run or query by facets. Usage: artifacts <run_id> | artifacts --param ..."""
        args = self._safe_split(arg)
        if args and (len(args) != 1 or args[0].startswith("-")):
            self._invoke_cli_command("artifacts", arg)
            return

        run_id = self._resolve_run_id(args[0] if args else "", command_name="artifacts")
        if run_id is None:
            return

        try:
            run = self.tracker.get_run(run_id)
            if not run:
                console.print(f"[red]Run '{run_id}' not found.[/red]")
                return
            rendered = _render_artifacts_table(self.tracker, run_id)
            self._last_artifact_run_id = run_id
            self._last_artifact_ids = []
            for artifact in rendered:
                artifact_id = getattr(artifact, "id", None)
                if artifact_id is not None:
                    self._last_artifact_ids.append(str(artifact_id))
            if self._last_artifact_ids:
                console.print(
                    "[dim]Tip: use preview @<n>, schema_profile @<n>, or "
                    "schema_stub @<n> for exact artifact rows.[/dim]"
                )
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_preview(self, arg: str) -> None:
        """Preview an artifact. Usage: preview <artifact_key|artifact_id|@ref> [--rows N] | preview --hash <prefix>"""
        try:
            args = self._safe_split(arg)
            if any(
                token in {"--db-path", "--run-dir", "--trust-db", "--mount"}
                or token.startswith("--mount=")
                for token in args
            ):
                self._invoke_cli_command("preview", arg)
                return

            n_rows = 5
            artifact_selector: Optional[str] = None
            hash_prefix: Optional[str] = None
            i = 0
            while i < len(args):
                token = args[i]
                if token in {"--rows", "-n"}:
                    if i + 1 >= len(args):
                        raise ValueError(f"{token} requires a value")
                    n_rows = _parse_bounded_int(
                        args[i + 1],
                        name="rows",
                        minimum=1,
                        maximum=MAX_PREVIEW_ROWS,
                    )
                    i += 2
                elif token == "--hash":
                    if i + 1 >= len(args):
                        raise ValueError("--hash requires a prefix value")
                    hash_prefix = args[i + 1]
                    i += 2
                elif token.startswith("--hash="):
                    hash_prefix = token.split("=", 1)[1]
                    i += 1
                else:
                    if token.startswith("-"):
                        raise ValueError(f"Unknown option: {token}")
                    if artifact_selector is not None:
                        raise ValueError(
                            "preview accepts one selector (artifact_key|artifact_id|@ref) "
                            "or use --hash."
                        )
                    artifact_selector = token
                    i += 1

            if artifact_selector is not None and hash_prefix is not None:
                raise ValueError(
                    "Provide either a positional selector or --hash, not both."
                )

            artifact = None
            if hash_prefix is not None:
                artifact = self._lookup_artifact_by_hash_prefix(
                    hash_prefix, command_name="preview"
                )
            elif artifact_selector is not None:
                artifact_key = self._resolve_artifact_ref(artifact_selector)
                if artifact_key is None:
                    return
                artifact = self.tracker.get_artifact(artifact_key)
            else:
                artifact_key = self._resolve_artifact_key([])
                if artifact_key is None:
                    return
                artifact = self.tracker.get_artifact(artifact_key)

            if not artifact:
                if hash_prefix is None:
                    console.print(
                        f"[red]Artifact '{artifact_selector or '(selected)'}' not found.[/red]"
                    )
                return

            _ensure_tracker_mounts_for_artifact(
                self.tracker, artifact, trust_db=self.trust_db
            )
            resolution_bases, db_metadata_run_dir = _build_relative_resolution_bases(
                self.tracker,
                artifact,
                db_path=self.db_path,
                run_dir=self.run_dir,
                trust_db=self.trust_db,
            )
            data = _load_artifact_with_diagnostics(
                self.tracker,
                artifact,
                n_rows=n_rows,
                resolution_bases=resolution_bases,
                db_metadata_run_dir=db_metadata_run_dir,
                trust_db=self.trust_db,
            )
            if data is None:
                return

            console.print(f"Preview: {artifact.key} [dim]({artifact.driver})[/dim]")

            if isinstance(data, duckdb.DuckDBPyRelation):
                df = data.limit(n_rows).df()
            elif isinstance(data, pd.DataFrame):
                df = data.head(n_rows)
            else:
                df = None

            if df is not None:
                table = Table()
                for col_name in df.columns:
                    style = "cyan"
                    if pd.api.types.is_numeric_dtype(df[col_name]):
                        style = "magenta"
                    elif pd.api.types.is_datetime64_any_dtype(df[col_name]):
                        style = "green"
                    table.add_column(str(col_name), style=style)
                for _, row in df.iterrows():
                    table.add_row(*[str(item) for item in row])
                console.print(table)
                return

            xr = _optional_xarray()
            if xr is not None and isinstance(data, (xr.Dataset, xr.DataArray)):
                ds: xr.Dataset
                if isinstance(data, xr.DataArray):
                    ds = data.to_dataset(name=getattr(data, "name", None) or "data")
                else:
                    ds = data

                dims_table = Table(title="Dimensions")
                dims_table.add_column("Dim", style="cyan")
                dims_table.add_column("Size", style="magenta")
                for dim_name, dim_size in ds.sizes.items():
                    dims_table.add_row(str(dim_name), str(dim_size))
                console.print(dims_table)
                return

            console.print(
                f"[yellow]Preview not implemented for loaded type: {type(data).__name__}[/yellow]"
            )
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_schema_profile(self, arg: str) -> None:
        """Show artifact schema. Usage: schema_profile <artifact_key|artifact_id|@ref> | schema_profile --hash <prefix>"""
        try:
            args = self._safe_split(arg)
            artifact_selector: Optional[str] = None
            hash_prefix: Optional[str] = None
            i = 0
            while i < len(args):
                token = args[i]
                if token == "--hash":
                    if i + 1 >= len(args):
                        raise ValueError("--hash requires a prefix value")
                    hash_prefix = args[i + 1]
                    i += 2
                    continue
                if token.startswith("--hash="):
                    hash_prefix = token.split("=", 1)[1]
                    i += 1
                    continue
                if token.startswith("-"):
                    raise ValueError(f"Unknown option: {token}")
                if artifact_selector is not None:
                    raise ValueError(
                        "schema_profile accepts one selector (artifact_key|artifact_id|@ref) "
                        "or use --hash."
                    )
                artifact_selector = token
                i += 1

            if artifact_selector is not None and hash_prefix is not None:
                raise ValueError(
                    "Provide either a positional selector or --hash, not both."
                )

            if hash_prefix is not None:
                artifact = self._lookup_artifact_by_hash_prefix(
                    hash_prefix, command_name="schema_profile"
                )
            elif artifact_selector is not None:
                artifact_key = self._resolve_artifact_ref(artifact_selector)
                if artifact_key is None:
                    return
                artifact = self.tracker.get_artifact(artifact_key)
            else:
                console.print(
                    "[red]Error: artifact_key required. Pass a key or @<n>, use "
                    "`--hash <prefix>`, or run `artifacts <run_id>` first to "
                    "populate artifact refs.[/red]"
                )
                return
            if not artifact:
                if artifact_selector is not None:
                    console.print(
                        f"[red]Artifact '{artifact_selector}' not found.[/red]"
                    )
                return

            _ensure_tracker_mounts_for_artifact(
                self.tracker, artifact, trust_db=self.trust_db
            )
            resolution_bases, db_metadata_run_dir = _build_relative_resolution_bases(
                self.tracker,
                artifact,
                db_path=self.db_path,
                run_dir=self.run_dir,
                trust_db=self.trust_db,
            )

            if self.tracker.db and artifact.id:
                fetched = self.tracker.db.get_artifact_schema_for_artifact(
                    artifact_id=artifact.id
                )
                if fetched is not None:
                    schema, fields = fetched
                    console.print(
                        f"Schema: {artifact.key} [dim]({artifact.driver}, db profile)[/dim]"
                    )
                    _render_schema_profile(schema, fields)
                    return
            data = _load_artifact_with_diagnostics(
                self.tracker,
                artifact,
                resolution_bases=resolution_bases,
                db_metadata_run_dir=db_metadata_run_dir,
                trust_db=self.trust_db,
            )
            if data is None:
                return

            console.print(f"Schema: {artifact.key} [dim]({artifact.driver})[/dim]")

            if isinstance(data, duckdb.DuckDBPyRelation):
                df = data.limit(1).df()
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                df = None

            if df is not None:
                table = Table()
                table.add_column("Column", style="cyan")
                table.add_column("Dtype", style="magenta")
                for col_name, dtype in df.dtypes.items():
                    table.add_row(str(col_name), str(dtype))
                console.print(table)
                if isinstance(data, duckdb.DuckDBPyRelation):
                    row = data.count("*").fetchone()
                    count = row[0] if row is not None else 0
                    console.print(
                        f"[dim]{int(count)} rows × {int(len(df.columns))} columns[/dim]"
                    )
                else:
                    console.print(
                        f"[dim]{int(df.shape[0])} rows × {int(df.shape[1])} columns[/dim]"
                    )
                return

            xr = _optional_xarray()
            if xr is not None and isinstance(data, (xr.Dataset, xr.DataArray)):
                ds: xr.Dataset
                if isinstance(data, xr.DataArray):
                    ds = data.to_dataset(name=getattr(data, "name", None) or "data")
                else:
                    ds = data

                dims_table = Table(title="Dimensions")
                dims_table.add_column("Dim", style="cyan")
                dims_table.add_column("Size", style="magenta")
                for dim_name, dim_size in ds.sizes.items():
                    dims_table.add_row(str(dim_name), str(dim_size))
                console.print(dims_table)
                return

            console.print(
                f"[yellow]Schema not implemented for loaded type: {type(data).__name__}[/yellow]"
            )
            if artifact.driver in {"h5", "hdf5"} or type(data).__name__ == "HDFStore":
                console.print(
                    "[yellow]Container-level HDF5 schema is not supported. Try "
                    "`artifacts <run_id>` and inspect sibling `h5_table` artifacts."
                    "[/yellow]"
                )
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_schema_stub(self, arg: str) -> None:
        """Export SQLModel schema stub. Usage: schema_stub <artifact_key|artifact_id|@ref>|--artifact-id UUID|--hash PREFIX [--run-id RUN_ID] [--source file|duckdb|user_provided] [--class-name NAME] [--table-name NAME] [--include-system-cols] [--no-stats-comments] [--concrete]"""
        artifact: Any = None
        try:
            parsed = self._parse_schema_stub_args(arg)
            artifact_key = parsed["artifact_key"]
            artifact_id = parsed["artifact_id"]
            hash_prefix = parsed["hash_prefix"]
            run_id = parsed["run_id"]
            source = parsed["source"]

            if artifact_key is not None:
                artifact_key = self._resolve_artifact_ref(artifact_key)
                if artifact_key is None:
                    return

            if hash_prefix is not None:
                artifact = self._lookup_artifact_by_hash_prefix(
                    hash_prefix,
                    command_name="schema_stub",
                    run_id=run_id,
                )
            elif artifact_id is not None:
                artifact = self.tracker.get_artifact(artifact_id)
            else:
                artifact = self.tracker.get_artifact(artifact_key, run_id=run_id)
            if not artifact:
                if artifact_key is not None and run_id is not None:
                    console.print(
                        f"[red]Artifact '{artifact_key}' not found for run '{run_id}'.[/red]"
                    )
                elif artifact_key is not None:
                    console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
                elif hash_prefix is not None:
                    console.print(
                        f"[red]Artifact with hash prefix '{hash_prefix}' not found.[/red]"
                    )
                else:
                    console.print(f"[red]Artifact '{artifact_id}' not found.[/red]")
                return

            export_selector: Dict[str, Any] = {"artifact_id": str(artifact.id)}
            resolver = getattr(
                self.tracker, "select_artifact_schema_for_artifact", None
            )
            if callable(resolver):
                selection = resolver(
                    artifact_id=str(artifact.id),
                    source=source,
                    strict_source=source is not None,
                )
                schema_id = getattr(selection, "schema_id", None)
                selected_source = getattr(selection, "source", None)
                candidate_count = getattr(selection, "candidate_count", None)
                selection_rule = getattr(selection, "selection_rule", None)
                if (
                    isinstance(schema_id, str)
                    and isinstance(selected_source, str)
                    and isinstance(candidate_count, int)
                    and isinstance(selection_rule, str)
                ):
                    console.print(
                        "[dim]Schema selection: "
                        f"source={selected_source}, "
                        f"schema_id={schema_id}, "
                        f"candidates={candidate_count}[/dim]"
                    )
                    console.print(f"[dim]Selection rule: {selection_rule}[/dim]")
                    export_selector = {"schema_id": schema_id}

            code = self.tracker.export_schema_sqlmodel(
                **export_selector,
                class_name=parsed["class_name"],
                table_name=parsed["table_name"],
                abstract=parsed["abstract"],
                include_system_cols=parsed["include_system_cols"],
                include_stats_comments=parsed["include_stats_comments"],
            )
            print(code)
        except KeyError:
            console.print("[red]Captured schema not found for this artifact.[/red]")
            resolved_artifact_id = getattr(artifact, "id", None)
            if resolved_artifact_id:
                console.print(
                    "[yellow]Try `consist schema capture-file --artifact-id "
                    f"{resolved_artifact_id}` to persist a file schema first.[/yellow]"
                )
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_summary(self, arg: str) -> None:
        """Display database summary. Usage: summary"""
        try:
            with _tracker_session(self.tracker) as session:
                summary_data = queries.get_summary(session)
            _render_summary(summary_data)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_scenarios(self, arg: str) -> None:
        """List scenarios. Usage: scenarios [--limit N]"""
        args = self._safe_split(arg)
        limit = 20

        if args:
            try:
                if args[0] == "--limit":
                    if len(args) != 2:
                        raise ValueError("Usage: scenarios [--limit N]")
                    limit = _parse_bounded_int(
                        args[1],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
                else:
                    if len(args) != 1:
                        raise ValueError("Usage: scenarios [--limit N]")
                    limit = _parse_bounded_int(
                        args[0],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
            except ValueError as exc:
                console.print(f"[red]Error: {exc}[/red]")
                return

        try:
            _render_scenarios(self.tracker, limit)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_exit(self, arg: str) -> bool:
        """Exit the shell."""
        self._save_history_once()
        console.print("Goodbye!")
        return True

    def do_quit(self, arg: str) -> bool:
        """Exit the shell (alias)."""
        return self.do_exit(arg)

    def do_EOF(self, arg: str) -> bool:
        """Handle Ctrl+D."""
        print()
        return self.do_exit(arg)

    def complete_runs(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del line, begidx, endidx
        return self._complete_choices(text, self._RUN_COMPLETION_FLAGS)

    def complete_scenarios(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del line, begidx, endidx
        return self._complete_choices(text, self._SCENARIOS_COMPLETION_FLAGS)

    def complete_show(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_run_ref_tokens(), *self._recent_run_ids()]
                ),
            )
        return []

    def complete_artifacts(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if text.startswith("-"):
            return self._complete_choices(text, self._ARTIFACT_COMPLETION_FLAGS)
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_run_ref_tokens(), *self._recent_run_ids()]
                ),
            )
        return []

    def complete_preview(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if text.startswith("-"):
            return self._complete_choices(text, self._PREVIEW_COMPLETION_FLAGS)
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_artifact_ref_tokens(), *self._recent_artifact_keys()]
                ),
            )
        return []

    def complete_schema_profile(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if text.startswith("-"):
            return self._complete_choices(text, ("--hash",))
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_artifact_ref_tokens(), *self._recent_artifact_keys()]
                ),
            )
        return []

    def complete_schema_stub(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if text.startswith("-"):
            return self._complete_choices(text, self._SCHEMA_STUB_COMPLETION_FLAGS)
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_artifact_ref_tokens(), *self._recent_artifact_keys()]
                ),
            )
        return []

    def complete_ls(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        del endidx
        if text.startswith("-"):
            return self._complete_choices(text, self._RUN_COMPLETION_FLAGS)
        if self._is_first_argument(line, begidx):
            return self._complete_choices(
                text,
                self._unique_completion_options(
                    [*self._cached_run_ref_tokens(), *self._recent_run_ids()]
                ),
            )
        return []

    def complete_cat(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        return self.complete_preview(text, line, begidx, endidx)

    def complete_db(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        del endidx
        if self._is_first_argument(line, begidx):
            return self._complete_choices(text, self._DB_SUBCOMMANDS)
        return []

    def complete_schema(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if self._is_first_argument(line, begidx):
            return self._complete_choices(text, self._SCHEMA_SUBCOMMANDS)
        return []

    def complete_views(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        del endidx
        if self._is_first_argument(line, begidx):
            return self._complete_choices(text, self._VIEWS_SUBCOMMANDS)
        return []

    def complete_cli(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        del endidx
        if self._is_first_argument(line, begidx):
            return self._complete_choices(text, self._CLI_ROOT_COMMANDS)
        return []

    def complete_consist(
        self, text: str, line: str, begidx: int, endidx: int
    ) -> List[str]:
        return self.complete_cli(text, line, begidx, endidx)

    def do_help(self, arg: str) -> None:
        super().do_help(arg)
        if arg.strip():
            return
        console.print(
            "[dim]Tip: use `db ...`, `schema ...`, `views ...`, or `cli <command> ...` "
            "to access full CLI features from the shell.[/dim]"
        )
        console.print(
            "[dim]Shell defaults are applied automatically "
            "(db_path/run_dir/trust_db/mounts). Use `context` to inspect them.[/dim]"
        )

    def default(self, line: str) -> None:
        args = self._safe_split(line)
        if args and args[0] in self._PASSTHROUGH_COMMANDS:
            self._invoke_cli(args)
            return
        super().default(line)

    def postloop(self) -> None:
        self._save_history_once()
        super().postloop()

    def emptyline(self) -> bool:
        """Do nothing on empty line (prevent repeating last command)."""
        return False


@app.command()
def show(
    run_id: str = typer.Argument(..., help="The ID of the run to inspect."),
    db_path: Optional[str] = typer.Option(None, help="Path to the DuckDB database."),
) -> None:
    """Display detailed information about a specific run."""
    tracker = get_tracker(db_path)
    run = tracker.get_run(run_id)
    if not run:
        console.print(f"[red]Run '{run_id}' not found.[/red]")
        raise typer.Exit(CLI_EXIT_RUNTIME_ERROR)

    _render_run_details(run)


@app.command()
def shell(
    db_path: Optional[str] = typer.Option(
        None, help="Path to the Consist DuckDB database."
    ),
    run_dir: Optional[str] = typer.Option(
        None,
        "--run-dir",
        help="Base directory for resolving relative artifact paths in shell preview/schema_profile.",
    ),
    mount: Optional[List[str]] = typer.Option(
        None,
        "--mount",
        help=(
            "Mount override mapping (repeatable): NAME=PATH. "
            "Example: --mount workspace=/path/to/archive"
        ),
    ),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow metadata-based mount and run-dir inference for artifact resolution.",
    ),
) -> None:
    """
    Start an interactive shell for exploring the provenance database.
    The database is loaded once and reused across shell commands.
    """
    resolved_db_path = find_db_path(db_path)
    mount_overrides = _resolve_mount_overrides_or_exit(mount)

    tracker = get_tracker(
        resolved_db_path,
        run_dir=run_dir,
        mounts=mount_overrides or None,
    )
    resolved_db_path = getattr(tracker, "db_path", None)
    if not isinstance(resolved_db_path, str) or not resolved_db_path:
        resolved_db_path = find_db_path(db_path)
    console.print(f"[green]✓ Loaded database: {resolved_db_path}[/green]")
    if mount_overrides:
        mounts_text = ", ".join(
            f"{name}={path}" for name, path in mount_overrides.items()
        )
        console.print(f"[dim]Using mount override(s): {mounts_text}[/dim]")
    ConsistShell(
        tracker,
        trust_db=trust_db,
        db_path=resolved_db_path,
        run_dir=run_dir,
        mount_overrides=mount_overrides,
    ).cmdloop()


if __name__ == "__main__":
    app()
