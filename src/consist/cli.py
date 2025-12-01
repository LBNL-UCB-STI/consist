# src/consist/cli.py
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
def runs(db_path: str = "provenance.duckdb", limit: int = 10):
    """List recent runs."""
    if not Path(db_path).exists():
        console.print(f"[red]Database not found at {db_path}[/red]")
        return

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
