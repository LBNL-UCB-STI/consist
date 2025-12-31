import uuid
from typing import Dict, Any, Iterable, TYPE_CHECKING, Set, Optional, Union

try:
    from rich.tree import Tree
    from rich import print as rprint

    _RICH_AVAILABLE = True
except ImportError:  # pragma: no cover - rich is optional at runtime
    Tree = None  # type: ignore[assignment]
    rprint = None  # type: ignore[assignment]
    _RICH_AVAILABLE = False

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import Run, RunArtifacts


def build_lineage_tree(
    tracker: "Tracker",
    artifact_key_or_id: Union[str, uuid.UUID],
    *,
    max_depth: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Build a lineage tree for a starting artifact.

    Parameters
    ----------
    tracker : Tracker
        Tracker instance used for run and artifact lookups.
    artifact_key_or_id : Union[str, uuid.UUID]
        Artifact key or UUID to start the lineage traversal from.
    max_depth : Optional[int], optional
        Maximum depth to traverse. When set to 0, only the starting artifact
        node is returned.

    Returns
    -------
    Optional[Dict[str, Any]]
        Lineage tree rooted at the requested artifact, or None if lineage
        cannot be resolved (e.g., no DB configured or artifact missing).
    """
    if not tracker.engine:
        return None

    start_artifact = tracker.get_artifact(key_or_id=artifact_key_or_id)
    if not start_artifact:
        return None

    run_cache: Dict[str, Optional["Run"]] = {}
    run_artifacts_cache: Dict[str, "RunArtifacts"] = {}
    lineage_cache: Dict[tuple[str, Optional[int]], Dict[str, Any]] = {}

    def _trace(
        artifact: "Artifact", visited_runs: Set[str], depth: int
    ) -> Dict[str, Any]:
        lineage_node: Dict[str, Any] = {"artifact": artifact, "producing_run": None}

        if max_depth is not None and depth >= max_depth:
            return lineage_node

        cache_key = (
            str(artifact.id),
            None if max_depth is None else max_depth - depth,
        )
        cached = lineage_cache.get(cache_key)
        if cached is not None:
            return cached

        producing_run_id = artifact.run_id
        if not producing_run_id or producing_run_id in visited_runs:
            return lineage_node

        visited_runs.add(producing_run_id)
        producing_run = run_cache.get(producing_run_id)
        if producing_run is None and producing_run_id not in run_cache:
            producing_run = tracker.get_run(producing_run_id)
            run_cache[producing_run_id] = producing_run
        if not producing_run:
            return lineage_node

        run_artifacts = run_artifacts_cache.get(producing_run.id)
        if run_artifacts is None:
            run_artifacts = tracker.get_artifacts_for_run(producing_run.id)
            run_artifacts_cache[producing_run.id] = run_artifacts

        run_node: Dict[str, Any] = {"run": producing_run, "inputs": []}

        for input_artifact in run_artifacts.inputs.values():
            run_node["inputs"].append(
                _trace(input_artifact, visited_runs.copy(), depth + 1)
            )

        lineage_node["producing_run"] = run_node
        lineage_cache[cache_key] = lineage_node
        return lineage_node

    return _trace(start_artifact, set(), 0)


def format_lineage_tree(
    lineage: Optional[Dict[str, Any]],
    *,
    indent: int = 2,
) -> str:
    """
    Format a lineage tree into a human-readable string.

    Parameters
    ----------
    lineage : Optional[Dict[str, Any]]
        Lineage tree from `build_lineage_tree`.
    indent : int, default 2
        Spaces per nesting level.

    Returns
    -------
    str
        A formatted lineage string (empty if lineage is None).
    """
    if lineage is None:
        return ""

    def _artifact_label(artifact: "Artifact") -> str:
        key = getattr(artifact, "key", None)
        driver = getattr(artifact, "driver", None)
        if key and driver:
            return f"{key} ({driver})"
        if key:
            return key
        return str(getattr(artifact, "id", "artifact"))

    def _run_label(run: "Run") -> str:
        model = getattr(run, "model_name", None)
        run_id = getattr(run, "id", None)
        if model and run_id:
            return f"{model} [{run_id}]"
        return str(run_id or model or "run")

    lines: list[str] = []

    def _walk(node: Dict[str, Any], depth: int) -> None:
        artifact = node.get("artifact")
        prefix = " " * (indent * depth)
        if artifact is not None:
            lines.append(f"{prefix}- artifact: {_artifact_label(artifact)}")
        producing_run = node.get("producing_run")
        if not producing_run:
            return
        run = producing_run.get("run")
        if run is not None:
            lines.append(f"{prefix}{' ' * indent}- run: {_run_label(run)}")
        inputs = producing_run.get("inputs") or []
        for input_node in _coerce_inputs(inputs):
            _walk(input_node, depth + 2)

    def _coerce_inputs(items: Iterable[Any]) -> Iterable[Dict[str, Any]]:
        for item in items:
            if isinstance(item, dict):
                yield item

    _walk(lineage, 0)
    return "\n".join(lines)


def print_lineage(
    tracker: "Tracker",
    artifact_key_or_id: Union[str, uuid.UUID],
    *,
    max_depth: Optional[int] = None,
    show_run_ids: bool = False,
) -> None:
    """
    Print a lineage tree with Rich formatting.
    """
    lineage = build_lineage_tree(tracker, artifact_key_or_id, max_depth=max_depth)
    if not lineage:
        if _RICH_AVAILABLE:
            rprint("[dim]No lineage available[/dim]")
        else:
            print("No lineage available")
        return

    if not _RICH_AVAILABLE:
        formatted = format_lineage_tree(lineage)
        if formatted:
            print(formatted)
        return

    def _add_node(parent: "Tree", node: Dict[str, Any]) -> None:
        artifact = node.get("artifact")
        if artifact is None:
            return

        key = getattr(artifact, "key", "?")
        driver = getattr(artifact, "driver", None)
        art_label = f"[bold cyan]{key}[/bold cyan]"
        if driver:
            art_label += f" [dim]({driver})[/dim]"

        art_branch = parent.add(art_label)

        producing_run = node.get("producing_run")
        if not producing_run:
            return

        run = producing_run.get("run")
        if run is None:
            return

        model = getattr(run, "model_name", "?")
        iteration = getattr(run, "iteration", None)
        run_label = f"[dim]\u2190[/dim] [yellow]{model}[/yellow]"
        if iteration is not None:
            run_label += f" [dim](iter {iteration})[/dim]"
        if show_run_ids:
            run_id = getattr(run, "id", None)
            if run_id:
                short_id = run_id[:40] + "..." if len(run_id) > 40 else run_id
                run_label += f" [dim]{short_id}[/dim]"

        run_branch = art_branch.add(run_label)

        for input_node in producing_run.get("inputs", []):
            if isinstance(input_node, dict):
                _add_node(run_branch, input_node)

    tree = Tree("[bold]Lineage[/bold]")
    _add_node(tree, lineage)
    rprint(tree)


class LineageService:
    """
    Helper for lineage tree retrieval and formatting.
    """

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def get_lineage(
        self,
        artifact_key_or_id: Union[str, uuid.UUID],
        *,
        max_depth: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        return build_lineage_tree(
            tracker=self._tracker,
            artifact_key_or_id=artifact_key_or_id,
            max_depth=max_depth,
        )

    def print_lineage(
        self,
        artifact_key_or_id: Union[str, uuid.UUID],
        *,
        max_depth: Optional[int] = None,
        show_run_ids: bool = False,
    ) -> None:
        print_lineage(
            self._tracker,
            artifact_key_or_id,
            max_depth=max_depth,
            show_run_ids=show_run_ids,
        )
