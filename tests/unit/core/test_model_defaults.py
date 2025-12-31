from __future__ import annotations

from consist.models.artifact import Artifact
from consist.models.artifact_schema import ArtifactSchema
from consist.models.run import ConsistRecord, Run, RunArtifacts


def test_run_defaults_are_isolated() -> None:
    run_a = Run(id="run_a", model_name="model")
    run_b = Run(id="run_b", model_name="model")

    run_a.tags.append("tag-a")
    run_a.meta["alpha"] = "a"

    assert run_b.tags == []
    assert "alpha" not in run_b.meta


def test_run_artifacts_defaults_are_isolated() -> None:
    first = RunArtifacts()
    second = RunArtifacts()

    first.inputs["input"] = Artifact(
        key="input",
        uri="inputs://input.csv",
        driver="csv",
    )
    first.outputs["output"] = Artifact(
        key="output",
        uri="outputs://output.csv",
        driver="csv",
    )

    assert "input" not in second.inputs
    assert "output" not in second.outputs


def test_consist_record_defaults_are_isolated() -> None:
    run_a = Run(id="run_a", model_name="model")
    run_b = Run(id="run_b", model_name="model")
    record_a = ConsistRecord(run=run_a)
    record_b = ConsistRecord(run=run_b)

    record_a.inputs.append(
        Artifact(
            key="input",
            uri="inputs://input.csv",
            driver="csv",
        )
    )
    record_a.outputs.append(
        Artifact(
            key="output",
            uri="outputs://output.csv",
            driver="csv",
        )
    )
    record_a.config["alpha"] = "a"
    record_a.facet["beta"] = "b"

    assert record_b.inputs == []
    assert record_b.outputs == []
    assert record_b.config == {}
    assert record_b.facet == {}


def test_artifact_defaults_are_isolated() -> None:
    art_a = Artifact(key="a", uri="inputs://a.csv", driver="csv")
    art_b = Artifact(key="b", uri="inputs://b.csv", driver="csv")

    art_a.meta["alpha"] = "a"

    assert "alpha" not in art_b.meta


def test_artifact_schema_defaults_are_isolated() -> None:
    schema_a = ArtifactSchema(id="schema_a")
    schema_b = ArtifactSchema(id="schema_b")

    schema_a.summary_json["alpha"] = "a"

    assert "alpha" not in schema_b.summary_json
