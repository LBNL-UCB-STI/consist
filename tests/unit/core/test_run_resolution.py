from __future__ import annotations

from pathlib import Path

import pytest

from consist.core import run_resolution
from consist.models.artifact import Artifact
from consist.types import ExecutionOptions


def test_tracker_run_trace_helpers_use_run_resolution_module(tracker) -> None:
    helpers = tracker._run_trace._helpers
    assert helpers.resolve_input_refs is run_resolution.resolve_input_refs
    assert helpers.preview_run_artifact_dir is run_resolution.preview_run_artifact_dir
    assert helpers.resolve_output_path is run_resolution.resolve_output_path
    assert helpers.is_xarray_dataset is run_resolution.is_xarray_dataset
    assert helpers.write_xarray_dataset is run_resolution.write_xarray_dataset


def test_resolve_input_reference_returns_single_output_for_run_result(tracker) -> None:
    def step(ctx) -> None:
        out_path = ctx.run_dir / "out.txt"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("ok\n")

    produced = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    resolved = run_resolution.resolve_input_reference(tracker, produced, key=None)
    assert isinstance(resolved, Artifact)
    assert resolved.key == "out"


def test_resolve_input_reference_configured_honors_string_ref_resolver(
    tracker, tmp_path: Path
) -> None:
    from_coupler = tmp_path / "from_coupler.txt"
    from_coupler.write_text("value", encoding="utf-8")

    resolved = run_resolution.resolve_input_reference_configured(
        tracker,
        "upstream_key",
        key=None,
        type_label="Scenario inputs",
        missing_path_error="unused {path!s}",
        missing_string_error="unused {value!r} {path!s}",
        string_ref_resolver=lambda value: (
            from_coupler if value == "upstream_key" else None
        ),
    )

    assert resolved == from_coupler


def test_resolve_input_refs_preserves_keyed_artifacts_and_depends_on(
    tracker, tmp_path: Path
) -> None:
    data_path = tmp_path / "data.csv"
    data_path.write_text("x,y\n1,2\n", encoding="utf-8")
    dep_path = tmp_path / "dep.txt"
    dep_path.write_text("dep", encoding="utf-8")

    resolved_inputs, keyed = run_resolution.resolve_input_refs(
        tracker,
        inputs={"data": f"file://{data_path}"},
        depends_on=[dep_path],
        include_keyed_artifacts=True,
    )

    assert len(resolved_inputs) == 2
    assert "data" in keyed
    assert keyed["data"].key == "data"
    assert keyed["data"].path == data_path
    assert resolved_inputs[1] == dep_path


def test_resolve_output_path_handles_uri_relative_and_absolute_refs(
    tracker, tmp_path: Path
) -> None:
    base_dir = tmp_path / "run_outputs"
    base_dir.mkdir(parents=True, exist_ok=True)

    absolute_target = tmp_path / "absolute.txt"
    uri_target = tmp_path / "uri_target.txt"

    assert (
        run_resolution.resolve_output_path(tracker, absolute_target, base_dir)
        == absolute_target
    )
    assert (
        run_resolution.resolve_output_path(tracker, "relative.txt", base_dir)
        == base_dir / "relative.txt"
    )
    assert (
        run_resolution.resolve_output_path(tracker, f"file://{uri_target}", base_dir)
        == uri_target.resolve()
    )


def test_resolve_input_reference_configured_raises_custom_missing_string_error(
    tracker,
) -> None:
    with pytest.raises(ValueError, match="did not resolve"):
        run_resolution.resolve_input_reference_configured(
            tracker,
            "missing_key",
            key=None,
            type_label="Scenario inputs",
            missing_path_error="path missing {path!s}",
            missing_string_error="did not resolve {value!r}",
            string_ref_resolver=lambda _: None,
        )
