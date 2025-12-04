"""
Consist Command Line Interface (CLI)

This module provides command-line utilities for interacting with Consist,
primarily for inspecting provenance data stored in the DuckDB database.
It offers functions like listing recent runs, providing a quick overview
of the execution history and status of various models and workflows.
"""

from typing import Dict, Any, Optional, List
import typer
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.panel import Panel
from sqlmodel import Session
import pandas as pd

from consist import Tracker
from consist.tools import queries

app = typer.Typer(rich_markup_mode="markdown")
console = Console()


def get_tracker(db_path: str) -> Tracker:
    """Initializes and returns a Tracker instance, handling DB existence checks."""
    if not Path(db_path).exists():
        console.print(f"[red]Database not found at {db_path}[/red]")
        raise typer.Exit(1)
    # run_dir is not critical for read-only CLI operations
    return Tracker(run_dir=Path("."), db_path=db_path)


@app.command()
def runs(
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    limit: int = typer.Option(10, help="Maximum number of recent runs to display."),
    model_name: Optional[str] = typer.Option(None, "--model", help="Filter by model name."),
    tag: Optional[List[str]] = typer.Option(None, "--tag", help="Filter by runs containing a tag. Can be used multiple times."),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by run status (e.g., 'completed', 'failed')."),
) -> None:
    """
    Lists the most recent Consist runs recorded in the provenance database.
    """
    tracker = get_tracker(db_path)

    with Session(tracker.engine) as session:
        results = queries.get_runs(
            session, limit=limit, model_name=model_name, tags=tag, status=status
        )

        title_parts = ["Recent Runs"]
        if model_name:
            title_parts.append(f"for model [green]{model_name}[/green]")
        if tag:
            title_parts.append(f"with tags [yellow]{', '.join(tag)}[/yellow]")
        if status:
            title_parts.append(f"with status [bold]{status}[/bold]")

        table = Table(title=' '.join(title_parts))
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

    artifacts_with_direction = tracker.get_artifacts_for_run(run_id)

    table = Table(title=f"Artifacts for Run [cyan]{run_id}[/cyan]")
    table.add_column("Direction", style="yellow")
    table.add_column("Key", style="green")
    table.add_column("URI", style="dim")
    table.add_column("Driver")
    table.add_column("Hash", style="magenta")

    inputs = sorted(
        [a for a in artifacts_with_direction if a[1] == "input"], key=lambda x: x[0].key
    )
    outputs = sorted(
        [a for a in artifacts_with_direction if a[1] == "output"], key=lambda x: x[0].key
    )

    for artifact, direction in inputs:
        table.add_row(
            f"[blue]Input[/blue]",
            artifact.key,
            artifact.uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )

    if inputs and outputs:
        table.add_section()

    for artifact, direction in outputs:
        table.add_row(
            f"[green]Output[/green]",
            artifact.key,
            artifact.uri,
            artifact.driver,
            artifact.hash[:12] if artifact.hash else "N/A",
        )

    console.print(table)


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

    if summary_data['total_runs'] == 0:
        console.print(Panel("[yellow]No runs found in the database.[/yellow]", title="Summary"))
        raise typer.Exit()

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
    for model, count in summary_data['models_distribution']:
        model_table.add_row(model, str(count))

    console.print(model_table)


@app.command()
def preview(
    artifact_key: str = typer.Argument(..., help="The key or ID of the artifact to preview."),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    n_rows: int = typer.Option(5, "--rows", "-n", help="Number of rows to display."),
) -> None:
    """Shows a preview of a tabular artifact (e.g., CSV, Parquet)."""
    tracker = get_tracker(db_path)
    df = queries.get_artifact_preview(tracker, artifact_key, limit=n_rows)

    if df is None:
        # Check why it failed
        artifact = tracker.get_artifact(artifact_key)
        if not artifact:
            console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        elif artifact.driver not in ["csv", "parquet"]:
            console.print(f"[yellow]Preview is not supported for driver '{artifact.driver}'.[/yellow]")
        else:
            console.print(f"[red]Could not load or preview artifact '{artifact_key}'. The file may be missing or corrupt.[/red]")
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


if __name__ == "__main__":
    app()