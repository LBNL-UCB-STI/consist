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
import shlex
import json
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Mapping, Tuple, Literal, cast

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
from consist.core.persistence import DatabaseManager
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaField
from consist.tools import queries

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.models.artifact import Artifact
    from consist.models.run import Run

app = typer.Typer(rich_markup_mode="markdown")
schema_app = typer.Typer(rich_markup_mode="markdown")
console = Console()

app.add_typer(schema_app, name="schema")

MAX_CLI_LIMIT = 1_000_000
MAX_PREVIEW_ROWS = 1_000_000
MAX_SEARCH_QUERY_LENGTH = 256


def _optional_xarray() -> Any | None:
    try:
        import xarray as xr
    except ImportError:
        return None
    return xr


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


def _escape_like_pattern(value: str) -> str:
    value = value.replace("\\", "\\\\")
    value = value.replace("%", "\\%")
    value = value.replace("_", "\\_")
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
def get_tracker(db_path: Optional[str] = None) -> Tracker:
    """Initializes and returns a Tracker instance."""
    resolved_path = find_db_path(db_path)
    if not Path(resolved_path).exists():
        console.print(f"[red]Database not found at {resolved_path}[/red]")
        console.print(
            "[yellow]Hint: Use --db-path to specify location or set CONSIST_DB environment variable[/yellow]"
        )
        raise typer.Exit(1)
    return Tracker(run_dir=Path("."), db_path=resolved_path)


@contextmanager
def _tracker_session(tracker: Tracker) -> Iterable[Session]:
    db = getattr(tracker, "db", None)
    if not isinstance(db, DatabaseManager):
        with Session(tracker.engine) as session:
            yield session
        return
    with db.session_scope() as session:
        yield session


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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
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
        raise typer.Exit(2)
    if artifact_id is not None:
        try:
            uuid.UUID(artifact_id)
        except ValueError:
            console.print("[red]--artifact-id must be a UUID[/red]")
            raise typer.Exit(2)

    # Validate prefer_source if provided
    if prefer_source is not None and prefer_source not in ("file", "duckdb"):
        console.print("[red]--prefer-source must be either 'file' or 'duckdb'[/red]")
        raise typer.Exit(2)
    resolved_prefer_source: Optional[Literal["file", "duckdb"]] = None
    if prefer_source is not None:
        resolved_prefer_source = cast(Literal["file", "duckdb"], prefer_source)

    tracker = get_tracker(db_path)
    if artifact_key is not None:
        artifact = tracker.get_artifact(artifact_key)
        if artifact is None:
            console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
            raise typer.Exit(1)
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
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(2)

    if out is None:
        print(code)
    else:
        console.print(f"[green]Wrote SQLModel stub to {out}[/green]")


@schema_app.command("apply-fks")
def schema_apply_fks(
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
) -> None:
    """Best-effort application of physical foreign key constraints."""
    tracker = get_tracker(db_path)
    if not tracker.db:
        console.print("[red]Tracker database not initialized.[/red]")
        raise typer.Exit(1)
    applied = tracker.db.apply_physical_fks()
    console.print(
        f"[green]Applied {applied} foreign key constraint(s) (best-effort).[/green]"
    )


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


def _render_artifacts_table(tracker: Tracker, run_id: str) -> None:
    """Shared logic for displaying artifacts for a run."""
    run_artifacts = tracker.get_artifacts_for_run(run_id)

    table = Table(title=f"Artifacts for Run [cyan]{run_id}[/cyan]")
    table.add_column("Direction", style="yellow")
    table.add_column("Key", style="green")
    table.add_column("URI", style="dim")
    table.add_column("Driver")
    table.add_column("Hash", style="magenta")

    inputs = sorted(run_artifacts.inputs.values(), key=lambda x: x.key)
    outputs = sorted(run_artifacts.outputs.values(), key=lambda x: x.key)

    for artifact in inputs:
        table.add_row(
            "[blue]Input[/blue]",
            artifact.key,
            artifact.container_uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )

    if inputs and outputs:
        table.add_section()

    for artifact in outputs:
        table.add_row(
            "[green]Output[/green]",
            artifact.key,
            artifact.container_uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
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
                    col(Run.id).contains(escaped_query, escape="\\"),
                    col(Run.model_name).contains(escaped_query, escape="\\"),
                    col(Run.parent_run_id).contains(escaped_query, escape="\\")
                    if escaped_query
                    else False,
                )
            )
            .order_by(col(Run.created_at).desc())
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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
    fix: bool = typer.Option(
        False, help="Attempt to fix issues (mark artifacts as missing)."
    ),
) -> None:
    """Check that artifacts referenced in DB actually exist on disk."""
    tracker = get_tracker(db_path)

    with _tracker_session(tracker) as session:
        missing = []
        batch_size = 1000
        if env_batch_size := os.getenv("CONSIST_VALIDATE_BATCH_SIZE"):
            try:
                batch_size = max(1, int(env_batch_size))
            except ValueError:
                batch_size = 1000

        for _, key, uri, run_id in _iter_artifact_rows(session, batch_size=batch_size):
            try:
                abs_path = tracker.resolve_uri(uri)
                if not Path(abs_path).exists():
                    missing.append((key, uri, run_id))
            except Exception:
                missing.append((key, uri, run_id))

        if not missing:
            console.print("[green]✓ All artifacts validated successfully[/green]")
            return

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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
    limit: int = typer.Option(20, help="Maximum scenarios to display."),
) -> None:
    """List all scenarios and their run counts."""
    tracker = get_tracker(db_path)
    _render_scenarios(tracker, limit)


@app.command()
def scenario(
    scenario_id: str = typer.Argument(..., help="The scenario ID to inspect."),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
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
            raise typer.Exit(1)

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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
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
            print(json.dumps(output, indent=2))
            return

    _render_runs_table(tracker, limit, model_name, tag, status)


@app.command()
def artifacts(
    run_id: str = typer.Argument(..., help="The ID of the run to inspect."),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Displays the input and output artifacts for a specific run."""
    tracker = get_tracker(db_path)
    run = tracker.get_run(run_id)
    if not run:
        console.print(f"[red]Run with ID '{run_id}' not found.[/red]")
        raise typer.Exit(1)

    _render_artifacts_table(tracker, run_id)


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


@app.command()
def lineage(
    artifact_key: str = typer.Argument(
        ..., help="The key or ID of the artifact to trace."
    ),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
) -> None:
    """Traces and displays the full lineage of an artifact."""
    tracker = get_tracker(db_path)
    lineage_data = tracker.get_artifact_lineage(artifact_key)

    if not lineage_data:
        console.print(
            f"[red]Could not find artifact with key or ID '{artifact_key}'.[/red]"
        )
        raise typer.Exit(1)

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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    n_rows: int = typer.Option(5, "--rows", "-n", help="Number of rows to display."),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow mount inference from database metadata for artifact resolution.",
    ),
) -> None:
    """Shows a small preview of an artifact (tabular or array-like when supported)."""
    tracker = get_tracker(db_path)

    # Fetch artifact first to give a precise message (unsupported driver vs missing)
    artifact = tracker.get_artifact(artifact_key)
    if not artifact:
        console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        raise typer.Exit(1)

    _ensure_tracker_mounts_for_artifact(tracker, artifact, trust_db=trust_db)

    try:
        import consist

        load_kwargs: Dict[str, Any] = {}
        if artifact.driver == "csv":
            # Avoid loading the full file when we only need a head() preview.
            load_kwargs["nrows"] = n_rows

        data = consist.load(
            artifact, tracker=tracker, db_fallback="always", **load_kwargs
        )
    except FileNotFoundError:
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
        console.print(
            f"[red]Artifact file not found at: {artifact.container_uri}[/red]\n{help_text}"
        )
        raise typer.Exit(1)
    except ImportError as e:
        console.print(
            f"[red]Missing optional dependency while loading artifact: {e}[/red]"
        )
        if artifact.driver == "zarr":
            console.print(
                "[yellow]Hint:[/] install Zarr support: `pip install -e '.[zarr]'`"
            )
        elif artifact.driver in {"h5", "hdf5", "h5_table"}:
            console.print(
                "[yellow]Hint:[/] install HDF5 support: `pip install -e '.[hdf5]'`"
            )
        elif artifact.driver in {"geojson", "shapefile", "geopackage"}:
            console.print(
                "[yellow]Hint:[/] install spatial support: `pip install -e '.[spatial]'`"
            )
        raise typer.Exit(1)
    except ValueError as e:
        console.print(
            f"[red]Unsupported artifact driver '{artifact.driver}': {e}[/red]"
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error loading artifact: {e}[/red]")
        raise typer.Exit(1)

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

    def __init__(self, tracker: Tracker, *, trust_db: bool = False):
        super().__init__()
        self.tracker = tracker
        self.trust_db = trust_db

    def do_runs(self, arg: str) -> None:
        """List recent runs. Usage: runs [--limit N] [--model NAME] [--status STATUS] [--tag TAG]"""
        try:
            args = shlex.split(arg)
            limit = 10
            model_name = None
            status = None
            tags: List[str] = []

            i = 0
            while i < len(args):
                if args[i] == "--limit" and i + 1 < len(args):
                    limit = _parse_bounded_int(
                        args[i + 1],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
                    i += 2
                elif args[i] == "--model" and i + 1 < len(args):
                    model_name = args[i + 1]
                    i += 2
                elif args[i] == "--status" and i + 1 < len(args):
                    status = args[i + 1]
                    i += 2
                elif args[i] == "--tag" and i + 1 < len(args):
                    tags.append(args[i + 1])
                    i += 2
                else:
                    i += 1

            _render_runs_table(
                self.tracker, limit, model_name, tags if tags else None, status
            )
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_show(self, arg: str) -> None:
        """Show details for a run. Usage: show <run_id>"""
        if not arg.strip():
            console.print("[red]Error: run_id required[/red]")
            return

        try:
            run = self.tracker.get_run(arg.strip())
            if not run:
                console.print(f"[red]Run '{arg.strip()}' not found.[/red]")
                return
            _render_run_details(run)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_artifacts(self, arg: str) -> None:
        """Show artifacts for a run. Usage: artifacts <run_id>"""
        if not arg.strip():
            console.print("[red]Error: run_id required[/red]")
            return

        try:
            run = self.tracker.get_run(arg.strip())
            if not run:
                console.print(f"[red]Run '{arg.strip()}' not found.[/red]")
                return
            _render_artifacts_table(self.tracker, arg.strip())
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_preview(self, arg: str) -> None:
        """Preview an artifact. Usage: preview <artifact_key> [--rows N]"""
        try:
            args = shlex.split(arg)
            if not args:
                console.print("[red]Error: artifact_key required[/red]")
                return

            artifact_key = args[0]
            n_rows = 5
            i = 1
            while i < len(args):
                if args[i] in {"--rows", "-n"} and i + 1 < len(args):
                    n_rows = _parse_bounded_int(
                        args[i + 1],
                        name="rows",
                        minimum=1,
                        maximum=MAX_PREVIEW_ROWS,
                    )
                    i += 2
                else:
                    i += 1

            artifact = self.tracker.get_artifact(artifact_key)
            if not artifact:
                console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
                return

            _ensure_tracker_mounts_for_artifact(
                self.tracker, artifact, trust_db=self.trust_db
            )

            try:
                import consist

                load_kwargs: Dict[str, Any] = {}
                if artifact.driver == "csv":
                    load_kwargs["nrows"] = n_rows
                data = consist.load(
                    artifact, tracker=self.tracker, db_fallback="always", **load_kwargs
                )
            except FileNotFoundError:
                abs_path = self.tracker.resolve_uri(artifact.container_uri)
                from consist.tools.mount_diagnostics import (
                    build_mount_resolution_hint,
                    format_missing_artifact_mount_help,
                )

                hint = build_mount_resolution_hint(
                    artifact.container_uri,
                    artifact_meta=artifact.meta,
                    mounts=self.tracker.mounts,
                )
                help_text = (
                    format_missing_artifact_mount_help(hint, resolved_path=abs_path)
                    if hint
                    else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
                )
                console.print(
                    f"[red]Artifact file not found at: {artifact.container_uri}[/red]\n{help_text}"
                )
                return
            except ImportError as e:
                console.print(
                    f"[red]Missing optional dependency while loading artifact: {e}[/red]"
                )
                return
            except ValueError as e:
                console.print(
                    f"[red]Unsupported artifact driver '{artifact.driver}': {e}[/red]"
                )
                return
            except Exception as e:
                console.print(f"[red]Error loading artifact: {e}[/red]")
                return

            console.print(f"Preview: {artifact_key} [dim]({artifact.driver})[/dim]")

            if isinstance(data, duckdb.DuckDBPyRelation):
                df = consist.to_df(data.limit(n_rows), close=True)
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
        """Show artifact schema. Usage: schema_profile <artifact_key>"""
        try:
            args = shlex.split(arg)
            if not args:
                console.print("[red]Error: artifact_key required[/red]")
                return

            artifact_key = args[0]
            artifact = self.tracker.get_artifact(artifact_key)
            if not artifact:
                console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
                return

            _ensure_tracker_mounts_for_artifact(
                self.tracker, artifact, trust_db=self.trust_db
            )

            if self.tracker.db and artifact.id:
                fetched = self.tracker.db.get_artifact_schema_for_artifact(
                    artifact_id=artifact.id
                )
                if fetched is not None:
                    schema, fields = fetched
                    console.print(
                        f"Schema: {artifact_key} [dim]({artifact.driver}, db profile)[/dim]"
                    )
                    _render_schema_profile(schema, fields)
                    return

            try:
                import consist

                data = consist.load(
                    artifact, tracker=self.tracker, db_fallback="always"
                )
            except FileNotFoundError:
                abs_path = self.tracker.resolve_uri(artifact.container_uri)
                from consist.tools.mount_diagnostics import (
                    build_mount_resolution_hint,
                    format_missing_artifact_mount_help,
                )

                hint = build_mount_resolution_hint(
                    artifact.container_uri,
                    artifact_meta=artifact.meta,
                    mounts=self.tracker.mounts,
                )
                help_text = (
                    format_missing_artifact_mount_help(hint, resolved_path=abs_path)
                    if hint
                    else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
                )
                console.print(
                    f"[red]Artifact file not found at: {artifact.container_uri}[/red]\n{help_text}"
                )
                return
            except ImportError as e:
                console.print(
                    f"[red]Missing optional dependency while loading artifact: {e}[/red]"
                )
                return
            except ValueError as e:
                console.print(
                    f"[red]Unsupported artifact driver '{artifact.driver}': {e}[/red]"
                )
                return
            except Exception as e:
                console.print(f"[red]Error loading artifact: {e}[/red]")
                return

            console.print(f"Schema: {artifact_key} [dim]({artifact.driver})[/dim]")

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
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_schema_stub(self, arg: str) -> None:
        """Export SQLModel schema stub. Usage: schema_stub <artifact_key> [--class-name NAME] [--table-name NAME] [--include-system-cols] [--no-stats-comments] [--concrete]"""
        try:
            args = shlex.split(arg)
            if not args:
                console.print("[red]Error: artifact_key required[/red]")
                return

            artifact_key = args[0]
            class_name = None
            table_name = None
            include_system_cols = False
            include_stats_comments = True
            abstract = True

            i = 1
            while i < len(args):
                if args[i] == "--class-name" and i + 1 < len(args):
                    class_name = args[i + 1]
                    i += 2
                elif args[i] == "--table-name" and i + 1 < len(args):
                    table_name = args[i + 1]
                    i += 2
                elif args[i] == "--include-system-cols":
                    include_system_cols = True
                    i += 1
                elif args[i] == "--no-stats-comments":
                    include_stats_comments = False
                    i += 1
                elif args[i] == "--concrete":
                    abstract = False
                    i += 1
                else:
                    i += 1

            artifact = self.tracker.get_artifact(artifact_key)
            if not artifact:
                console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
                return

            code = self.tracker.export_schema_sqlmodel(
                artifact_id=str(artifact.id),
                class_name=class_name,
                table_name=table_name,
                abstract=abstract,
                include_system_cols=include_system_cols,
                include_stats_comments=include_stats_comments,
            )
            print(code)
        except KeyError:
            console.print("[red]Captured schema not found for this artifact.[/red]")
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
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
        args = shlex.split(arg)
        limit = 20

        if args:
            try:
                if args[0] == "--limit" and len(args) > 1:
                    limit = _parse_bounded_int(
                        args[1],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
                else:
                    limit = _parse_bounded_int(
                        args[0],
                        name="limit",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
            except ValueError:
                console.print("[red]Error: limit must be an integer[/red]")
                return

        try:
            _render_scenarios(self.tracker, limit)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def do_exit(self, arg: str) -> bool:
        """Exit the shell."""
        console.print("Goodbye!")
        return True

    def do_quit(self, arg: str) -> bool:
        """Exit the shell (alias)."""
        return self.do_exit(arg)

    def do_EOF(self, arg: str) -> bool:
        """Handle Ctrl+D."""
        print()
        return self.do_exit(arg)

    def emptyline(self) -> bool:
        """Do nothing on empty line (prevent repeating last command)."""
        return False


@app.command()
def show(
    run_id: str = typer.Argument(..., help="The ID of the run to inspect."),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
) -> None:
    """Display detailed information about a specific run."""
    tracker = get_tracker(db_path)
    run = tracker.get_run(run_id)
    if not run:
        console.print(f"[red]Run '{run_id}' not found.[/red]")
        raise typer.Exit(1)

    _render_run_details(run)


@app.command()
def shell(
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    trust_db: bool = typer.Option(
        False,
        "--trust-db",
        help="Allow mount inference from database metadata for artifact resolution.",
    ),
) -> None:
    """
    Start an interactive shell for exploring the provenance database.
    The database is loaded once and reused across shell commands.
    """
    tracker = get_tracker(db_path)
    console.print(f"[green]✓ Loaded database: {db_path}[/green]")
    ConsistShell(tracker, trust_db=trust_db).cmdloop()


if __name__ == "__main__":
    app()
