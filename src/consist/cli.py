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
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Mapping, Tuple

import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from sqlalchemy import and_, or_, select
from sqlmodel import Session

from consist import Tracker
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


def _ensure_tracker_mounts_for_artifact(tracker: Tracker, artifact: "Artifact") -> None:
    from consist.tools.mount_diagnostics import parse_mount_uri

    parsed = parse_mount_uri(artifact.uri)
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
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the DuckDB database."
    ),
) -> None:
    """Export a captured artifact schema as an editable SQLModel stub."""
    if (schema_id is None) == (artifact_id is None):
        console.print("[red]Provide exactly one of --schema-id or --artifact-id[/red]")
        raise typer.Exit(2)
    if artifact_id is not None:
        try:
            uuid.UUID(artifact_id)
        except ValueError:
            console.print("[red]--artifact-id must be a UUID[/red]")
            raise typer.Exit(2)

    tracker = get_tracker(db_path)
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


class LineageGraphRenderer:
    def __init__(
        self,
        raw_edges: List[Tuple[str, str]],
        labels: Dict[str, str],
        *,
        max_terminal_nodes: int = 6,
        row_spacing: int = 6,
        label_max_width: Optional[int] = None,
        component_layout: str = "vertical",
    ) -> None:
        try:
            from grandalf.layouts import SugiyamaLayout
            from grandalf.graphs import Vertex, Edge, Graph
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError(
                "Graph rendering requires the 'grandalf' package."
            ) from exc

        self._sugiyama_layout = SugiyamaLayout
        self._vertex = Vertex
        self._edge = Edge
        self._graph = Graph
        self._label_max_width = label_max_width
        self._labels = self._truncate_labels(labels, label_max_width)
        self._edge_style = "grey50"
        if component_layout not in {"vertical", "horizontal"}:
            raise ValueError("component_layout must be 'vertical' or 'horizontal'.")
        self._component_layout = component_layout
        self.pruned_inputs = 0
        self.pruned_outputs = 0

        if max_terminal_nodes < 0:
            raise ValueError("max_terminal_nodes must be >= 0.")
        if row_spacing < 1:
            raise ValueError("row_spacing must be >= 1.")

        self.edges_data = self._prune(raw_edges, max_terminal_nodes)

        nodes_list = set()
        for parent, child in self.edges_data:
            nodes_list.update([parent, child])
        if not nodes_list:
            raise ValueError("No nodes available to render.")

        vs = {name: self._vertex(name) for name in nodes_list}
        es = [self._edge(vs[parent], vs[child]) for parent, child in self.edges_data]
        g = self._graph(vs.values(), es)

        for v in vs.values():
            label = self._labels.get(v.data, v.data)
            v.view = type("View", (object,), {"w": len(label) + 2, "h": 2})()

        self.coords: Dict[str, Dict[str, int]] = {}
        global_x_offset = 0
        global_y_offset = 0

        for component in g.C:
            sug = self._sugiyama_layout(component)
            sug.init_all()
            sug.draw()

            comp_max_x = 0
            comp_max_y = 0
            for layer_idx, layer in enumerate(sug.layers):
                y_pos = layer_idx * row_spacing
                for item in layer:
                    v = getattr(item, "v", item)
                    if hasattr(v, "data"):
                        node_id = v.data
                        label = self._labels.get(node_id, node_id)
                        curr_x = int(v.view.xy[0]) + global_x_offset
                        curr_y = y_pos + global_y_offset
                        self.coords[node_id] = {
                            "x": curr_x,
                            "y": curr_y,
                            "w": len(label) + 2,
                        }
                        comp_max_x = max(comp_max_x, curr_x + len(label) + 10)
                        comp_max_y = max(comp_max_y, curr_y + 4)

            if self._component_layout == "horizontal":
                global_x_offset = comp_max_x
            else:
                global_y_offset = comp_max_y + row_spacing

        min_x = min(c["x"] for c in self.coords.values())
        for c in self.coords.values():
            c["x"] -= min_x

        self.width = max(c["x"] + c["w"] for c in self.coords.values()) + 5
        self.height = max(c["y"] + 4 for c in self.coords.values()) + 2
        self.grid = [[" " for _ in range(self.width)] for _ in range(self.height)]

    def _prune(self, edges: List[Tuple[str, str]], limit: int) -> List[Tuple[str, str]]:
        important = {p for p, c in edges if c.startswith("run:")}
        pruned_edges: List[Tuple[str, str]] = []
        outputs_by_run: Dict[str, List[str]] = {}
        inputs_by_run: Dict[str, List[str]] = {}

        indegree: Dict[str, int] = defaultdict(int)
        outdegree: Dict[str, int] = defaultdict(int)
        for parent, child in edges:
            outdegree[parent] += 1
            indegree[child] += 1

        for parent, child in edges:
            if (
                parent.startswith("run:")
                and not child.startswith("run:")
                and child not in important
            ):
                outputs_by_run.setdefault(parent, []).append(child)
                continue

            if (
                child.startswith("run:")
                and not parent.startswith("run:")
                and indegree.get(parent, 0) == 0
                and outdegree.get(parent, 0) == 1
            ):
                inputs_by_run.setdefault(child, []).append(parent)
                continue

            pruned_edges.append((parent, child))

        for run, artifacts in outputs_by_run.items():
            if limit == 0:
                if artifacts:
                    bundle_id = f"bundle-output:{run}:{len(artifacts)}"
                    label = f"[+{len(artifacts)} files]"
                    self._labels[bundle_id] = self._clip_label(label)
                    pruned_edges.append((run, bundle_id))
                    self.pruned_outputs += len(artifacts)
                continue

            for art_id in artifacts[:limit]:
                pruned_edges.append((run, art_id))
            if len(artifacts) > limit:
                bundle_id = f"bundle-output:{run}:{len(artifacts)}"
                label = f"[+{len(artifacts) - limit} files]"
                self._labels[bundle_id] = self._clip_label(label)
                pruned_edges.append((run, bundle_id))
                self.pruned_outputs += len(artifacts) - limit

        for run, artifacts in inputs_by_run.items():
            if limit == 0:
                if artifacts:
                    bundle_id = f"bundle-input:{run}:{len(artifacts)}"
                    label = f"[+{len(artifacts)} inputs]"
                    self._labels[bundle_id] = self._clip_label(label)
                    pruned_edges.append((bundle_id, run))
                    self.pruned_inputs += len(artifacts)
                continue

            for art_id in artifacts[:limit]:
                pruned_edges.append((art_id, run))
            if len(artifacts) > limit:
                bundle_id = f"bundle-input:{run}:{len(artifacts)}"
                label = f"[+{len(artifacts) - limit} inputs]"
                self._labels[bundle_id] = self._clip_label(label)
                pruned_edges.append((bundle_id, run))
                self.pruned_inputs += len(artifacts) - limit

        return pruned_edges

    def _truncate_labels(
        self, labels: Dict[str, str], max_width: Optional[int]
    ) -> Dict[str, str]:
        if max_width is None or max_width <= 3:
            return labels
        truncated: Dict[str, str] = {}
        for node_id, label in labels.items():
            if len(label) <= max_width:
                truncated[node_id] = label
                continue
            truncated[node_id] = f"{label[: max_width - 3]}..."
        return truncated

    def _clip_label(self, label: str) -> str:
        max_width = self._label_max_width
        if max_width is None or max_width <= 3:
            return label
        if len(label) <= max_width:
            return label
        return f"{label[: max_width - 3]}..."

    def _put(self, x: int, y: int, char: str, style: Optional[str] = None) -> None:
        if 0 <= y < len(self.grid) and 0 <= x < len(self.grid[0]):
            self.grid[y][x] = f"[{style}]{char}[/]" if style else char

    def draw(self) -> str:
        for parent, child in self.edges_data:
            if parent not in self.coords or child not in self.coords:
                continue
            c1, c2 = self.coords[parent], self.coords[child]
            x1 = c1["x"] + c1["w"] // 2
            y1 = c1["y"] + 2
            x2 = c2["x"] + c2["w"] // 2
            y2 = c2["y"]
            mid_y = y1 + (y2 - y1) // 2
            for y in range(y1 + 1, mid_y + 1):
                self._put(x1, y, "│", self._edge_style)
            if x1 != x2:
                self._put(x1, mid_y, "╰" if x2 > x1 else "╯", self._edge_style)
                for x in range(min(x1, x2) + 1, max(x1, x2)):
                    self._put(x, mid_y, "─", self._edge_style)
                self._put(x2, mid_y, "╮" if x2 > x1 else "╭", self._edge_style)
            for y in range(mid_y + 1, y2):
                self._put(x2, y, "│", self._edge_style)
            self._put(x2, y2, "▼", self._edge_style)

        for node_id, c in self.coords.items():
            label = self._labels.get(node_id, node_id)
            if node_id.startswith("bundle:"):
                style = "dim yellow"
            elif node_id.startswith("run:"):
                style = "bold blue"
            else:
                style = "bold green"
            x, y, w = c["x"], c["y"], c["w"]
            for i in range(w):
                self._put(x + i, y, "─", style)
                self._put(x + i, y + 2, "─", style)
            for i in range(3):
                self._put(x, y + i, "│", style)
                self._put(x + w - 1, y + i, "│", style)
            self._put(x, y, "╭", style)
            self._put(x + w - 1, y, "╮", style)
            self._put(x, y + 2, "╰", style)
            self._put(x + w - 1, y + 2, "╯", style)
            for i, char in enumerate(label):
                self._put(x + 1 + i, y + 1, char, style)
        return "\n".join(["".join(row) for row in self.grid])


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
            select(
                Artifact.id,
                Artifact.key,
                Artifact.uri,
                Artifact.run_id,
                Artifact.created_at,
            )
            .order_by(Artifact.created_at, Artifact.id)
            .limit(batch_size)
        )
        if last_created is not None and last_id is not None:
            stmt = stmt.where(
                or_(
                    Artifact.created_at > last_created,
                    and_(
                        Artifact.created_at == last_created,
                        Artifact.id > last_id,
                    ),
                )
            )

        batch = session.exec(stmt).all()
        if not batch:
            break
        for art_id, key, uri, run_id, created_at in batch:
            yield art_id, key, uri, run_id
        last_id = batch[-1][0]
        last_created = batch[-1][4]


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
    """Search for runs by ID, model name, or tags (query length is capped)."""
    tracker = get_tracker(db_path)
    if not query or len(query) > MAX_SEARCH_QUERY_LENGTH:
        raise typer.BadParameter(
            f"Query must be 1-{MAX_SEARCH_QUERY_LENGTH} characters."
        )
    escaped_query = _escape_like_pattern(query)

    with Session(tracker.engine) as session:
        from consist.models.run import Run
        from sqlmodel import select, or_

        # Search in multiple fields
        search_query = (
            select(Run)
            .where(
                or_(
                    Run.id.contains(escaped_query, escape="\\"),
                    Run.model_name.contains(escaped_query, escape="\\"),
                    Run.parent_run_id.contains(escaped_query, escape="\\")
                    if escaped_query
                    else False,
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

    with Session(tracker.engine) as session:
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
def graph(
    run_id: str = typer.Argument(..., help="Run ID or scenario ID to visualize."),
    db_path: str = typer.Option(
        "provenance.duckdb", help="Path to the Consist DuckDB database."
    ),
    max_terminal: int = typer.Option(
        6,
        "--max-terminal",
        help="Maximum terminal outputs per run before collapsing.",
    ),
    label_max: Optional[int] = typer.Option(
        None,
        "--label-max",
        help="Maximum label width (defaults to terminal width heuristic).",
    ),
    row_spacing: int = typer.Option(
        6, "--row-spacing", help="Vertical spacing between graph layers."
    ),
    layout: str = typer.Option(
        "vertical",
        "--layout",
        help="Component layout: vertical or horizontal.",
        case_sensitive=False,
    ),
) -> None:
    """Render a run or scenario lineage graph."""
    tracker = get_tracker(db_path)

    try:
        _render_lineage_graph(
            tracker,
            run_id,
            max_terminal=max_terminal,
            row_spacing=row_spacing,
            label_max=label_max,
            layout=layout,
        )
    except ImportError as exc:
        console.print(f"[red]{exc}[/red]")
        console.print("[yellow]Hint:[/] install with `pip install grandalf`")
        raise typer.Exit(1)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)


def _render_lineage_graph(
    tracker: Tracker,
    run_id: str,
    *,
    max_terminal: int = 6,
    row_spacing: int = 6,
    label_max: Optional[int] = None,
    layout: str = "vertical",
) -> None:
    if max_terminal < 0:
        raise ValueError("max-terminal must be >= 0.")
    if row_spacing < 1:
        raise ValueError("row-spacing must be >= 1.")
    layout_normalized = layout.strip().lower()
    if layout_normalized not in {"vertical", "horizontal"}:
        raise ValueError("layout must be 'vertical' or 'horizontal'.")

    from consist.models.run import Run

    console_width = console.size.width
    if label_max is None:
        label_max = max(12, min(40, console_width // 3))
    elif label_max < 6:
        raise ValueError("label-max must be >= 6.")

    with Session(tracker.engine) as session:
        scenario_exists = (
            session.exec(
                select(Run.id).where(Run.parent_run_id == run_id).limit(1)
            ).first()
            is not None
        )

    if scenario_exists:
        edges, labels = _build_scenario_graph_edges(
            tracker, run_id, label_max_width=label_max
        )
        title = f"Scenario Graph: {run_id}"
    else:
        edges, labels = _build_run_graph_edges(
            tracker, run_id, label_max_width=label_max
        )
        title = f"Run Graph: {run_id}"

    if not edges:
        console.print("[yellow]No lineage edges found to render.[/yellow]")
        return

    target_width = max(40, console_width - 4)
    effective_max_terminal = max_terminal
    renderer = None
    while True:
        renderer = LineageGraphRenderer(
            edges,
            labels,
            max_terminal_nodes=effective_max_terminal,
            row_spacing=row_spacing,
            label_max_width=label_max,
            component_layout=layout_normalized,
        )

        if renderer.width <= target_width or effective_max_terminal == 0:
            break
        effective_max_terminal -= 1

    if (
        renderer.pruned_inputs > 0
        or renderer.pruned_outputs > 0
        or effective_max_terminal != max_terminal
    ):
        console.print(
            "[yellow]Graph trimmed to fit terminal width:[/] "
            f"{renderer.pruned_inputs} inputs hidden, "
            f"{renderer.pruned_outputs} outputs hidden "
            f"(max-terminal={effective_max_terminal})."
        )

    if renderer.width > target_width:
        console.print(
            "[yellow]Graph still exceeds terminal width; consider widening the terminal or lowering --label-max.[/yellow]"
        )

    console.print(Panel(renderer.draw(), title=title, border_style="cyan"))


def _run_node_id(run_id: str) -> str:
    return f"run:{run_id}"


def _artifact_node_id(artifact_id: uuid.UUID) -> str:
    return f"artifact:{artifact_id}"


def _format_artifact_labels(
    artifacts: Iterable["Artifact"], *, label_max_width: Optional[int] = None
) -> Dict[str, str]:
    by_key: Dict[str, List["Artifact"]] = defaultdict(list)
    for artifact in artifacts:
        by_key[artifact.key].append(artifact)

    labels: Dict[str, str] = {}
    max_width = label_max_width
    for key, items in by_key.items():
        if len(items) == 1:
            label = key
            if max_width is not None and len(label) > max_width:
                label = f"{label[: max_width - 3]}..."
            labels[_artifact_node_id(items[0].id)] = label
            continue
        for artifact in items:
            short_id = str(artifact.id).split("-")[0]
            label = f"{key} ({short_id})"
            if max_width is not None and len(label) > max_width:
                label = f"{label[: max_width - 3]}..."
            labels[_artifact_node_id(artifact.id)] = label
    return labels


def _build_run_graph_edges(
    tracker: Tracker, run_id: str, *, label_max_width: Optional[int] = None
) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    run = tracker.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found.")

    run_artifacts = tracker.get_artifacts_for_run(run_id)
    inputs = sorted(run_artifacts.inputs.values(), key=lambda x: x.key)
    outputs = sorted(run_artifacts.outputs.values(), key=lambda x: x.key)

    run_label = run.id
    if label_max_width is not None and len(run_label) > label_max_width:
        run_label = f"{run_label[: label_max_width - 3]}..."
    labels: Dict[str, str] = {_run_node_id(run.id): run_label}
    labels.update(_format_artifact_labels([*inputs, *outputs], label_max_width=label_max_width))

    edges: List[Tuple[str, str]] = []
    run_node = _run_node_id(run.id)
    for artifact in inputs:
        edges.append((_artifact_node_id(artifact.id), run_node))
    for artifact in outputs:
        edges.append((run_node, _artifact_node_id(artifact.id)))

    return edges, labels


def _build_scenario_graph_edges(
    tracker: Tracker, scenario_id: str, *, label_max_width: Optional[int] = None
) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    from consist.models.run import Run, RunArtifactLink
    from consist.models.artifact import Artifact

    with Session(tracker.engine) as session:
        runs_result = session.exec(
            select(Run)
            .where(Run.parent_run_id == scenario_id)
            .order_by(Run.created_at)
        )
        runs = runs_result.all()
        if runs and not isinstance(runs[0], Run):
            runs = [row[0] for row in runs]

        if not runs:
            raise ValueError(f"No runs found for scenario '{scenario_id}'.")

        run_ids = [run.id for run in runs]
        rows = session.exec(
            select(RunArtifactLink.run_id, RunArtifactLink.direction, Artifact)
            .join(
                Artifact,
                Artifact.id == RunArtifactLink.artifact_id,  # ty: ignore[invalid-argument-type]
            )
            .where(RunArtifactLink.run_id.in_(run_ids))
        ).all()

    run_label_base: Dict[str, List["Run"]] = defaultdict(list)
    for run in runs:
        run_label_base[run.model_name or run.id].append(run)

    labels: Dict[str, str] = {}
    for base, items in run_label_base.items():
        for run in items:
            label = base
            if len(items) > 1:
                short_id = run.id.split("-")[-1][:8]
                label = f"{base} ({short_id})"
            if label_max_width is not None and len(label) > label_max_width:
                label = f"{label[: label_max_width - 3]}..."
            labels[_run_node_id(run.id)] = label
    artifacts = [artifact for _, _, artifact in rows]
    labels.update(_format_artifact_labels(artifacts, label_max_width=label_max_width))

    inputs_by_run: Dict[str, List["Artifact"]] = defaultdict(list)
    outputs_by_run: Dict[str, List["Artifact"]] = defaultdict(list)
    for run_id, direction, artifact in rows:
        if direction == "input":
            inputs_by_run[run_id].append(artifact)
        else:
            outputs_by_run[run_id].append(artifact)

    edges: List[Tuple[str, str]] = []
    for run in runs:
        run_node = _run_node_id(run.id)
        for artifact in sorted(inputs_by_run.get(run.id, []), key=lambda x: x.key):
            edges.append((_artifact_node_id(artifact.id), run_node))
        for artifact in sorted(outputs_by_run.get(run.id, []), key=lambda x: x.key):
            edges.append((run_node, _artifact_node_id(artifact.id)))

    return edges, labels


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
    """Shows a small preview of an artifact (tabular or array-like when supported)."""
    tracker = get_tracker(db_path)

    # Fetch artifact first to give a precise message (unsupported driver vs missing)
    artifact = tracker.get_artifact(artifact_key)
    if not artifact:
        console.print(f"[red]Artifact '{artifact_key}' not found.[/red]")
        raise typer.Exit(1)

    _ensure_tracker_mounts_for_artifact(tracker, artifact)

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
        abs_path = tracker.resolve_uri(artifact.uri)
        from consist.tools.mount_diagnostics import (
            build_mount_resolution_hint,
            format_missing_artifact_mount_help,
        )

        hint = build_mount_resolution_hint(
            artifact.uri, artifact_meta=artifact.meta, mounts=tracker.mounts
        )
        help_text = (
            format_missing_artifact_mount_help(hint, resolved_path=abs_path)
            if hint
            else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
        )
        console.print(
            f"[red]Artifact file not found at: {artifact.uri}[/red]\n{help_text}"
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

    if isinstance(data, pd.DataFrame):
        df = data.head(n_rows)
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

    try:
        import xarray as xr
    except ImportError:
        xr = None

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

    def do_graph(self, arg: str) -> None:
        """Render a run/scenario graph. Usage: graph <run_id> [--max-terminal N] [--row-spacing N] [--label-max N] [--layout vertical|horizontal]"""
        try:
            args = shlex.split(arg)
            if not args:
                console.print("[red]Error: run_id required[/red]")
                return

            run_id = args[0]
            max_terminal = 6
            row_spacing = 6
            label_max: Optional[int] = None
            layout = "vertical"

            i = 1
            while i < len(args):
                if args[i] == "--max-terminal" and i + 1 < len(args):
                    max_terminal = _parse_bounded_int(
                        args[i + 1],
                        name="max-terminal",
                        minimum=0,
                        maximum=MAX_CLI_LIMIT,
                    )
                    i += 2
                elif args[i] == "--row-spacing" and i + 1 < len(args):
                    row_spacing = _parse_bounded_int(
                        args[i + 1],
                        name="row-spacing",
                        minimum=1,
                        maximum=MAX_CLI_LIMIT,
                    )
                    i += 2
                elif args[i] == "--label-max" and i + 1 < len(args):
                    label_max = _parse_bounded_int(
                        args[i + 1],
                        name="label-max",
                        minimum=6,
                        maximum=MAX_CLI_LIMIT,
                    )
                    i += 2
                elif args[i] == "--layout" and i + 1 < len(args):
                    layout = args[i + 1]
                    i += 2
                else:
                    i += 1

            _render_lineage_graph(
                self.tracker,
                run_id,
                max_terminal=max_terminal,
                row_spacing=row_spacing,
                label_max=label_max,
                layout=layout,
            )
        except ImportError as exc:
            console.print(f"[red]{exc}[/red]")
            console.print("[yellow]Hint:[/] install with `pip install grandalf`")
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")

    def _recent_graph_ids(self, limit: int = 50) -> List[str]:
        from consist.models.run import Run

        def _normalize(rows: List[Any]) -> List[str]:
            if not rows:
                return []
            if isinstance(rows[0], str):
                return rows
            if isinstance(rows[0], Run):
                return [row.id for row in rows]
            return [row[0] for row in rows]

        with Session(self.tracker.engine) as session:
            run_rows = session.exec(
                select(Run.id).order_by(Run.created_at.desc()).limit(limit)
            ).all()
            scenario_rows = session.exec(
                select(Run.parent_run_id)
                .where(Run.parent_run_id.is_not(None))
                .order_by(Run.created_at.desc())
                .limit(limit)
            ).all()

        run_ids = _normalize(run_rows)
        scenario_ids = [row for row in _normalize(scenario_rows) if row]
        seen = set()
        combined: List[str] = []
        for value in [*run_ids, *scenario_ids]:
            if value not in seen:
                combined.append(value)
                seen.add(value)
        return combined

    def complete_graph(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        options = ["--max-terminal", "--row-spacing", "--label-max", "--layout"]
        layouts = ["vertical", "horizontal"]

        try:
            args = shlex.split(line)
        except ValueError:
            args = line.split()

        if not text.startswith("-") and len(args) <= 2:
            try:
                return [
                    candidate
                    for candidate in self._recent_graph_ids()
                    if candidate.startswith(text)
                ]
            except Exception:
                return []

        if args and args[-1] == "--layout":
            return [opt for opt in layouts if opt.startswith(text)]

        if len(args) >= 2 and args[-2] == "--layout":
            return [opt for opt in layouts if opt.startswith(text)]

        if text.startswith("-"):
            return [opt for opt in options if opt.startswith(text)]

        return []

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

            _ensure_tracker_mounts_for_artifact(self.tracker, artifact)

            try:
                import consist

                load_kwargs: Dict[str, Any] = {}
                if artifact.driver == "csv":
                    load_kwargs["nrows"] = n_rows
                data = consist.load(
                    artifact, tracker=self.tracker, db_fallback="always", **load_kwargs
                )
            except FileNotFoundError:
                abs_path = self.tracker.resolve_uri(artifact.uri)
                from consist.tools.mount_diagnostics import (
                    build_mount_resolution_hint,
                    format_missing_artifact_mount_help,
                )

                hint = build_mount_resolution_hint(
                    artifact.uri,
                    artifact_meta=artifact.meta,
                    mounts=self.tracker.mounts,
                )
                help_text = (
                    format_missing_artifact_mount_help(hint, resolved_path=abs_path)
                    if hint
                    else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
                )
                console.print(
                    f"[red]Artifact file not found at: {artifact.uri}[/red]\n{help_text}"
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

            if isinstance(data, pd.DataFrame):
                df = data.head(n_rows)
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

            try:
                import xarray as xr
            except ImportError:
                xr = None

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

    def do_schema(self, arg: str) -> None:
        """Show artifact schema. Usage: schema <artifact_key>"""
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

            _ensure_tracker_mounts_for_artifact(self.tracker, artifact)

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
                abs_path = self.tracker.resolve_uri(artifact.uri)
                from consist.tools.mount_diagnostics import (
                    build_mount_resolution_hint,
                    format_missing_artifact_mount_help,
                )

                hint = build_mount_resolution_hint(
                    artifact.uri,
                    artifact_meta=artifact.meta,
                    mounts=self.tracker.mounts,
                )
                help_text = (
                    format_missing_artifact_mount_help(hint, resolved_path=abs_path)
                    if hint
                    else f"Resolved path: {abs_path}\nThe artifact may have been deleted, moved, or your mounts are misconfigured."
                )
                console.print(
                    f"[red]Artifact file not found at: {artifact.uri}[/red]\n{help_text}"
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

            if isinstance(data, pd.DataFrame):
                table = Table()
                table.add_column("Column", style="cyan")
                table.add_column("Dtype", style="magenta")
                for col_name, dtype in data.dtypes.items():
                    table.add_row(str(col_name), str(dtype))
                console.print(table)
                console.print(
                    f"[dim]{int(data.shape[0])} rows × {int(data.shape[1])} columns[/dim]"
                )
                return

            try:
                import xarray as xr
            except ImportError:
                xr = None

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
