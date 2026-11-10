"""Syntax, idiom, and smoke-execution checks for docs snippets and examples."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS_DIR = REPO_ROOT / "docs"
EXAMPLES_DIR = REPO_ROOT / "examples"

PYTHON_FENCE_RE = re.compile(
    r"^([ \t]*)```\s*(?:python|py)\b[^\n]*\n(.*?)^[ \t]*```",
    re.MULTILINE | re.DOTALL,
)


def _iter_markdown_files() -> list[Path]:
    files = [REPO_ROOT / "README.md"]
    for path in sorted(DOCS_DIR.rglob("*.md")):
        rel = path.relative_to(REPO_ROOT)
        if "superpowers" in rel.parts:
            continue
        files.append(path)
    return files


def _iter_notebook_files() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.ipynb"))


@pytest.mark.parametrize(
    "md_path",
    _iter_markdown_files(),
    ids=lambda p: str(p.relative_to(REPO_ROOT)),
)
def test_markdown_python_code_blocks_parse_cleanly(md_path: Path) -> None:
    text = md_path.read_text(encoding="utf-8")
    rel = md_path.relative_to(REPO_ROOT)

    for idx, match in enumerate(PYTHON_FENCE_RE.finditer(text), start=1):
        indent = match.group(1)
        body = match.group(2)
        if indent:
            body = "".join(
                line[len(indent) :] if line.startswith(indent) else line
                for line in body.splitlines(keepends=True)
            )
        line_no = text[: match.start()].count("\n") + 1
        try:
            ast.parse(body, filename=f"{rel}:{line_no}")
        except SyntaxError as exc:
            pytest.fail(
                f"SyntaxError in Python block #{idx} at {rel}:{line_no}: {exc}\n\n{body}"
            )


@pytest.mark.parametrize(
    "nb_path",
    _iter_notebook_files(),
    ids=lambda p: str(p.relative_to(REPO_ROOT)),
)
def test_example_notebooks_parse_and_follow_v05_idioms(nb_path: Path) -> None:
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    rel = nb_path.relative_to(REPO_ROOT)

    disallowed_patterns = (
        (
            "deprecated top-level discover_gtfs_members import",
            r"from consist import[^\n]*discover_gtfs_members",
        ),
        ("legacy consist.load_df(...)", r"\bconsist\.load_df\("),
        ("legacy .as_path(...)", r"\.as_path\("),
        (
            "legacy run.meta.get('cache_hit')",
            r"\.meta(?:\s+or\s+\{\})?\)?\.get\(\s*[\"']cache_hit[\"']",
        ),
    )

    for idx, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        raw_lines = cell.get("source", [])
        py_lines = [line for line in raw_lines if not line.lstrip().startswith("%")]
        code = "".join(py_lines)
        if not code.strip():
            continue

        try:
            ast.parse(code, filename=f"{rel}:cell_{idx}")
        except SyntaxError as exc:
            pytest.fail(f"SyntaxError in {rel} code cell {idx}: {exc}\n\n{code}")

        for label, pattern in disallowed_patterns:
            assert not re.search(pattern, code), (
                f"Non-idiomatic pattern ({label}) found in {rel} cell {idx}"
            )


@pytest.mark.parametrize(
    "script_rel",
    [
        "examples/scripts/research_pipeline_native.py",
        "examples/scripts/model_integrator_wrapper.py",
        "examples/scripts/artifact_recovery_materialization.py",
        "examples/scripts/command_exec_calibration_audit.py",
    ],
)
def test_runnable_example_scripts_execute_in_tmp_path(
    script_rel: str,
    tmp_path: Path,
) -> None:
    script_path = REPO_ROOT / script_rel
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    src_path = str(REPO_ROOT / "src")
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else src_path
    )

    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (
        f"{script_rel} failed with exit code {proc.returncode}\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
