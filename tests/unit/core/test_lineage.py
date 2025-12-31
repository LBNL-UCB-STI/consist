from unittest.mock import MagicMock
from consist.core.lineage import build_lineage_tree, format_lineage_tree, LineageService
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifacts


def test_build_lineage_tree():
    """
    Test a simple lineage:
    Run_A -> Art_1
    Run_B -> Art_1 (Input) -> Art_2 (Output)

    Tracing Art_2 should reveal Run_B and Run_A.
    """
    mock_tracker = MagicMock()

    # Setup Data
    run_a = Run(id="run_a", model_name="model_a")
    run_b = Run(id="run_b", model_name="model_b")

    art_1 = Artifact(id="a1", key="data_v1", run_id="run_a")  # Produced by A
    art_2 = Artifact(id="a2", key="data_v2", run_id="run_b")  # Produced by B

    # Mock Tracker lookups
    def get_artifact(key_or_id):
        if key_or_id == "a2":
            return art_2
        if key_or_id == "a1":
            return art_1
        return None

    def get_run(run_id):
        if run_id == "run_a":
            return run_a
        if run_id == "run_b":
            return run_b
        return None

    def get_artifacts_for_run(run_id):
        if run_id == "run_b":
            # Run B took Art 1 as input
            return RunArtifacts(inputs={"data_v1": art_1}, outputs={"data_v2": art_2})
        if run_id == "run_a":
            # Run A had no inputs
            return RunArtifacts(inputs={}, outputs={"data_v1": art_1})
        return RunArtifacts()

    mock_tracker.get_artifact.side_effect = get_artifact
    mock_tracker.get_run.side_effect = get_run
    mock_tracker.get_artifacts_for_run.side_effect = get_artifacts_for_run

    # Execute
    tree = build_lineage_tree(mock_tracker, "a2")

    # Verify Structure
    # Root -> Art 2
    assert tree["artifact"] == art_2

    # Producer -> Run B
    producer_b = tree["producing_run"]
    assert producer_b["run"] == run_b

    # Run B Inputs -> Art 1
    input_node = producer_b["inputs"][0]
    assert input_node["artifact"] == art_1

    # Art 1 Producer -> Run A
    producer_a = input_node["producing_run"]
    assert producer_a["run"] == run_a


def test_format_lineage_tree():
    """
    Verify lineage formatting produces a readable string.
    """
    run = Run(id="run_x", model_name="model_x")
    art = Artifact(id="a1", key="data_v1", run_id="run_x", driver="csv")
    tree = {"artifact": art, "producing_run": {"run": run, "inputs": []}}

    formatted = format_lineage_tree(tree)
    assert "artifact" in formatted
    assert "run" in formatted
    assert "data_v1" in formatted


def test_lineage_service_prints(monkeypatch):
    """
    Verify LineageService.print_lineage formats and prints output.
    """
    run = Run(id="run_x", model_name="model_x")
    art = Artifact(id="a1", key="data_v1", run_id="run_x", driver="csv")
    tree = {"artifact": art, "producing_run": {"run": run, "inputs": []}}

    def fake_build_lineage_tree(*args, **kwargs):
        return tree

    monkeypatch.setattr(
        "consist.core.lineage.build_lineage_tree", fake_build_lineage_tree
    )

    printed = []

    def fake_rprint(obj):
        printed.append(obj)

    monkeypatch.setattr("consist.core.lineage.rprint", fake_rprint)

    class DummyTracker:
        pass

    service = LineageService(DummyTracker())
    service.print_lineage("data_v1")

    assert printed
