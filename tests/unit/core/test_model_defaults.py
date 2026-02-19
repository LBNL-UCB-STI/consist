from __future__ import annotations

from consist.models.artifact import Artifact
from consist.models.artifact_schema import ArtifactSchema
from consist.models.run import ConsistRecord, Run, RunArtifacts


def test_run_defaults_are_isolated() -> None:
    run_a = Run(id="run_a", model_name="model", config_hash=None, git_hash=None)
    run_b = Run(id="run_b", model_name="model", config_hash=None, git_hash=None)

    run_a.tags.append("tag-a")
    run_a.meta["alpha"] = "a"

    assert run_b.tags == []
    assert "alpha" not in run_b.meta


def test_run_artifacts_defaults_are_isolated() -> None:
    first = RunArtifacts()
    second = RunArtifacts()

    first.inputs["input"] = Artifact(
        key="input",
        container_uri="inputs://input.csv",
        driver="csv",
    )
    first.outputs["output"] = Artifact(
        key="output",
        container_uri="outputs://output.csv",
        driver="csv",
    )

    assert "input" not in second.inputs
    assert "output" not in second.outputs


def test_consist_record_defaults_are_isolated() -> None:
    run_a = Run(id="run_a", model_name="model", config_hash=None, git_hash=None)
    run_b = Run(id="run_b", model_name="model", config_hash=None, git_hash=None)
    record_a = ConsistRecord(run=run_a)
    record_b = ConsistRecord(run=run_b)

    record_a.inputs.append(
        Artifact(
            key="input",
            container_uri="inputs://input.csv",
            driver="csv",
        )
    )
    record_a.outputs.append(
        Artifact(
            key="output",
            container_uri="outputs://output.csv",
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
    art_a = Artifact(key="a", container_uri="inputs://a.csv", driver="csv")
    art_b = Artifact(key="b", container_uri="inputs://b.csv", driver="csv")

    art_a.meta["alpha"] = "a"

    assert "alpha" not in art_b.meta


def test_artifact_schema_defaults_are_isolated() -> None:
    schema_a = ArtifactSchema(id="schema_a")
    schema_b = ArtifactSchema(id="schema_b")

    schema_a.summary_json["alpha"] = "a"

    assert "alpha" not in schema_b.summary_json


def test_run_identity_summary_exposes_components() -> None:
    run = Run(
        id="identity_run",
        model_name="activitysim",
        config_hash="cfg_hash",
        input_hash="inp_hash",
        git_hash="code_hash",
        signature="sig_hash",
        year=2030,
        iteration=2,
        meta={
            "config_adapter": "activitysim",
            "config_adapter_version": "0.1",
            "config_bundle_hash": "adapter_hash",
            "consist_hash_inputs": {"extra_dep.yaml": "dep_hash"},
            "cache_epoch": 9,
            "cache_version": 3,
            "code_identity": "repo_git",
            "code_identity_extra_deps": ["extra_dep.yaml"],
        },
    )

    summary = run.identity_summary

    assert summary["signature"] == "sig_hash"
    assert summary["code_version"] == "code_hash"
    assert summary["config_hash"] == "cfg_hash"
    assert summary["input_hash"] == "inp_hash"
    assert summary["adapter"]["name"] == "activitysim"
    assert summary["adapter"]["hash"] == "adapter_hash"
    assert summary["identity_inputs"] == {"extra_dep.yaml": "dep_hash"}
    assert summary["identity_inputs_count"] == 1
    assert summary["run_fields"]["year"] == 2030
    assert summary["run_fields"]["cache_epoch"] == 9
    assert summary["run_fields"]["cache_version"] == 3
