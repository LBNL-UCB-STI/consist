import pytest

import consist


def test_consist_output_path_uses_active_run_context(tracker) -> None:
    with tracker.start_run("api_output_path", model="demo"):
        path = consist.output_path("tables/final", ext=".PARQUET")

        assert path == tracker.run_artifact_dir() / "tables" / "final.parquet"
        assert path.parent.exists()


def test_consist_output_path_requires_active_run_context() -> None:
    with pytest.raises(RuntimeError, match="No active Consist run found"):
        consist.output_path("result")


def test_consist_output_dir_uses_active_run_context(tracker) -> None:
    with tracker.start_run("api_output_dir", model="demo"):
        path = consist.output_dir("plots")

        assert path == tracker.run_artifact_dir() / "plots"
        assert path.exists()


def test_consist_output_path_rejects_suspended_scenario_header(tracker) -> None:
    with tracker.scenario("api_scenario_header"):
        with pytest.raises(RuntimeError, match="No active Consist run found"):
            consist.output_path("result")


def test_consist_output_dir_rejects_suspended_scenario_header(tracker) -> None:
    with tracker.scenario("api_scenario_header_dir"):
        with pytest.raises(RuntimeError, match="No active Consist run found"):
            consist.output_dir("plots")
