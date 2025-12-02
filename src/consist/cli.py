"""
Consist Command Line Interface (CLI)

This module provides command-line utilities for interacting with Consist,
primarily for inspecting provenance data stored in the DuckDB database.
It offers functions like listing recent runs, providing a quick overview
of the execution history and status of various models and workflows.
"""

import typer
from pathlib import Path
from rich.console import Console
from rich.table import Table
from sqlmodel import Session, select
from consist import Tracker
from consist.models.run import Run

app = typer.Typer()
console = Console()


@app.command()
def runs(
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    limit: int = typer.Option(10, help="Maximum number of recent runs to display."),
) -> None:
    """
    Lists the most recent Consist runs recorded in the provenance database.

    This command provides a quick overview of past executions, showing their unique ID,
    the model name, current status (e.g., completed, failed), and creation timestamp.
    It's useful for monitoring the progress and outcomes of various Consist-managed workflows.

    Parameters
    ----------
    db_path : str, default "provenance.duckdb"
        The file path to the DuckDB database where Consist stores its provenance information.
        This database is created by the `Tracker` when `db_path` is provided during initialization.
    limit : int, default 10
        The maximum number of the most recent runs to fetch and display from the database.

    Raises
    ------
    typer.Exit
        If the specified `db_path` does not point to an existing database file.
    """
    if not Path(db_path).exists():
        console.print(f"[red]Database not found at {db_path}[/red]")
        raise typer.Exit(1)

    tracker = Tracker(run_dir=Path("."), db_path=db_path)

    with Session(tracker.engine) as session:
        statement = select(Run).order_by(Run.created_at.desc()).limit(limit)
        results = session.exec(statement).all()

        table = Table(title=f"Recent Runs (Last {limit})")
        table.add_column("ID", style="cyan")
        table.add_column("Model", style="green")
        table.add_column("Status")
        table.add_column("Created", style="dim")

        for run in results:
            status_style = "green" if run.status == "completed" else "red"
            table.add_row(
                run.id,
                run.model_name,
                f"[{status_style}]{run.status}[/]",
                run.created_at.strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)


if __name__ == "__main__":
    app()
