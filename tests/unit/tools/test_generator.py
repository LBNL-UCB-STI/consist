from __future__ import annotations

from pathlib import Path

from consist.tools.generator import generate_models_from_run


def test_generate_models_from_run_is_noop(tmp_path: Path) -> None:
    """
    Placeholder generator should be callable without side effects.
    """
    output_path = tmp_path / "models.py"
    result = generate_models_from_run("run_id", str(output_path))
    assert result is None
