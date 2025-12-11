"""
Consist Command Line Interface (CLI)

This module provides command-line utilities for interacting with Consist,
primarily for inspecting provenance data stored in the DuckDB database.
It offers functions like listing recent runs, providing a quick overview
of the execution history and status of various models and workflows.
"""

import cmd
import shlex
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from sqlalchemy import select
from sqlmodel import Session

from consist import Tracker
from consist.tools import queries

app = typer.Typer(rich_markup_mode="markdown")
console = Console()


def output_json(data: Any) -> None:
    """Helper to output data as JSON."""
    import json

    print(json.dumps(data, default=str, indent=2))


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


def _render_runs_table(
    tracker: Tracker,
    limit: int = 10,
    model_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
) -> None:
    """Shared logic for displaying runs."""
    with Session(tracker.engine) as session:
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
            artifact.uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )

    if inputs and outputs:
        table.add_section()

    for artifact in outputs:
        table.add_row(
            "[green]Output[/green]",
            artifact.key,
            artifact.uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )

    console.print(table)


def _render_run_details(run: Any) -> None:
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
        else "red" if status_value == "failed" else "yellow"
    )

    info.add_row("ID", run_id)
    info.add_row("Model", model_name)
    info.add_row("Status", f"[{status_style}]{status_value}[/]")

    parent_id = getattr(run, "parent_run_id", None)
    if parent_id:
        info.add_row("Parent", parent_id)

    scenario_value = getattr(run, "scenario_id", None)
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


def _render_scenarios(tracker: Tracker, limit: int = 20) -> None:
    """Shared logic for displaying scenario overview (scenarios are parent runs)."""
    with Session(tracker.engine) as session:
        from consist.models.run import Run
        from sqlmodel import func, select

        # Find parent runs by looking for distinct parent_run_ids
        query = (
            select(
                Run.parent_run_id.label("scenario_id"),  # ← Fixed: was Run.id
                func.count(Run.id).label("run_count"),
                func.min(Run.created_at).label("first_run"),
                func.max(Run.created_at).label("last_run"),
            )
            .where(Run.parent_run_id.is_not(None))
            .group_by(Run.parent_run_id)
            .order_by(func.max(Run.created_at).desc())
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
    """Search for runs by ID, model name, or tags."""
    tracker = get_tracker(db_path)

    with Session(tracker.engine) as session:
        from consist.models.run import Run
        from sqlmodel import select, or_

        # Search in multiple fields
        search_query = (
            select(Run)
            .where(
                or_(
                    Run.id.contains(query),
                    Run.model_name.contains(query),
                    Run.scenario_id.contains(query) if query else False,
                )
            )
            .order_by(Run.created_at.desc())
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
                getattr(run, "scenario_id", "-"),
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

    with Session(tracker.engine) as session:
        from consist.models.artifact import Artifact

        artifacts = session.exec(select(Artifact)).all()

        missing = []
        for art in artifacts:
            try:
                abs_path = tracker.resolve_uri(art.uri)
                if not Path(abs_path).exists():
                    missing.append(art)
            except Exception:
                missing.append(art)

        if not missing:
            console.print("[green]✓ All artifacts validated successfully[/green]")
            return

        console.print(f"[yellow]⚠ Found {len(missing)} missing artifacts:[/yellow]\n")

        table = Table()
        table.add_column("Key", style="yellow")
        table.add_column("URI", style="dim")
        table.add_column("Run ID", style="cyan")

        for art in missing[:50]:  # Limit display
            table.add_row(art.key, art.uri, art.run_id or "-")

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

    with Session(tracker.engine) as session:
        from consist.models.run import Run

        query = (
            select(Run).where(Run.parent_run_id == scenario_id).order_by(Run.created_at)
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
        with Session(tracker.engine) as session:
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
                        "scenario_id": getattr(r, "scenario_id", None),
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
    with Session(tracker.engine) as session:
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
) -> None:
    """Shows a preview of a tabular artifact (e.g., CSV, Parquet)."""
    tracker = get_tracker(db_path)

    # Fetch artifact first to give a precise message (unsupported driver vs missing)
    artifact = tracker.get_artifact(artifact_key)
    if not artifact:
        console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        raise typer.Exit(1)

    # Only certain drivers are previewable
    if artifact.driver not in ["csv", "parquet"]:
        console.print(
            f"[yellow]Cannot preview artifact with driver '{artifact.driver}'.[/yellow]\n"
            "Preview supports: csv, parquet\n"
            "Use 'consist load' or load the artifact programmatically."
        )
        raise typer.Exit(1)

    try:
        df = queries.get_artifact_preview(tracker, artifact_key, limit=n_rows)
    except FileNotFoundError:
        console.print(
            f"[red]Artifact file not found at: {artifact.uri}[/red]\n"
            "The artifact may have been deleted or moved."
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error loading artifact: {e}[/red]")
        raise typer.Exit(1)

    if df is None:
        console.print(
            f"[red]Could not load or preview artifact '{artifact_key}'. The file may be missing or corrupt.[/red]"
        )
        raise typer.Exit(1)

    console.print(f"Preview: {artifact_key}")
    table = Table()

    # Use pandas types to set column styles
    for col_name in df.columns:
        style = "cyan"
        if pd.api.types.is_numeric_dtype(df[col_name]):
            style = "magenta"
        elif pd.api.types.is_datetime64_any_dtype(df[col_name]):
            style = "green"
        table.add_column(col_name, style=style)

    for _, row in df.iterrows():
        table.add_row(*[str(item) for item in row])

    console.print(table)


class ConsistShell(cmd.Cmd):
    """Interactive shell for exploring Consist provenance."""

    intro = "Welcome to Consist Shell. Type help or ? to list commands.\n"
    prompt = "(consist) "

    def __init__(self, tracker: Tracker):
        super().__init__()
        self.tracker = tracker

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
                    limit = int(args[i + 1])
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

    def do_summary(self, arg: str) -> None:
        """Display database summary. Usage: summary"""
        try:
            with Session(self.tracker.engine) as session:
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
                    limit = int(args[1])
                else:
                    limit = int(args[0])
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

    def emptyline(self) -> None:
        """Do nothing on empty line (prevent repeating last command)."""
        pass


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
) -> None:
    """
    Start an interactive shell for exploring the provenance database.
    The database is loaded once and reused across shell commands.
    """
    tracker = get_tracker(db_path)
    console.print(f"[green]✓ Loaded database: {db_path}[/green]")
    ConsistShell(tracker).cmdloop()


if __name__ == "__main__":
    app()
