import uuid
from typing import Dict, Any, TYPE_CHECKING, Set, Optional, Union

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact


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

    run_cache: Dict[str, Optional[Any]] = {}
    run_artifacts_cache: Dict[str, Any] = {}
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
