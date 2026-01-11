from pathlib import Path

import consist


def test_log_output_noop_returns_artifact(tmp_path: Path) -> None:
    out_path = tmp_path / "out.csv"
    artifact = consist.log_output(out_path, key="out", enabled=False)

    assert artifact.key == "out"
    assert artifact.get_path() == out_path


def test_log_input_noop_returns_artifact(tmp_path: Path) -> None:
    in_path = tmp_path / "in.csv"
    artifact = consist.log_input(in_path, key="in", enabled=False)

    assert artifact.key == "in"
    assert artifact.get_path() == in_path


def test_log_artifacts_noop_includes_metadata(tmp_path: Path) -> None:
    out_path = tmp_path / "out.csv"
    artifacts = consist.log_artifacts(
        {"out": out_path},
        enabled=False,
        metadata_by_key={"out": {"role": "primary"}},
        dataset="demo",
    )

    assert artifacts["out"].meta["dataset"] == "demo"
    assert artifacts["out"].meta["role"] == "primary"
