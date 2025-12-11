from typing import Dict, Any, TYPE_CHECKING, Set

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact


def build_lineage_tree(tracker: "Tracker", artifact_key_or_id: str) -> Dict[str, Any]:
    if not tracker.engine:
        return {}

    start_artifact = tracker.get_artifact(key_or_id=artifact_key_or_id)
    if not start_artifact:
        return {}

    def _trace(artifact: "Artifact", visited_runs: Set[str]) -> Dict[str, Any]:
        lineage_node: Dict[str, Any] = {"artifact": artifact, "producing_run": None}

        producing_run_id = artifact.run_id
        if not producing_run_id or producing_run_id in visited_runs:
            return lineage_node

        visited_runs.add(producing_run_id)
        producing_run = tracker.get_run(producing_run_id)
        if not producing_run:
            return lineage_node

        run_artifacts = tracker.get_artifacts_for_run(producing_run.id)

        run_node: Dict[str, Any] = {"run": producing_run, "inputs": []}

        # Recursively trace inputs
        for input_artifact in run_artifacts.inputs.values():
            run_node["inputs"].append(_trace(input_artifact, visited_runs.copy()))

        lineage_node["producing_run"] = run_node
        return lineage_node

    return _trace(start_artifact, set())
