"""Automated terminology, syntax, and structure checks for user-facing v0.5 docs."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import tomllib

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS_DIR = REPO_ROOT / "docs"
ZENSICAL_TOML = REPO_ROOT / "zensical.toml"

USER_FACING_MENTAL_MODEL_PAGES: tuple[Path, ...] = (
    DOCS_DIR / "architecture.md",
    DOCS_DIR / "glossary.md",
    DOCS_DIR / "migrating-to-0.5.md",
    DOCS_DIR / "getting-started" / "quickstart.md",
    DOCS_DIR / "getting-started" / "first-workflow.md",
)

BANNED_INTERNAL_TERMS: tuple[str, ...] = (
    "anchor",
    "observation",
    "witness",
    "run_binding",
    "slot",
    "container_uri",
    "abs_path",
    "content_id",
    "RunArtifactLink",
    "_physical_run_dir",
    "action-v2",
    "input_hash",
    "recovery_roots",
    "consist promote",
    "consist.store.manifest",
)

EXPECTED_REUSE_TABLE_ROWS: tuple[tuple[str, str], ...] = (
    ("the contents of an input", "No"),
    ("a config value", "No"),
    ("your function's code", "No"),
    ("which parameter an input goes to", "No"),
    (
        "a declared output's name or file format (`cleaned.parquet` -> `cleaned.csv`)",
        "No",
    ),
    ("where an input or run directory lives (moved, archived, copied)", "**Yes**"),
    ("an upstream step, which then produced identical output", "**Yes**"),
    ("`runtime_kwargs`", "**Yes**"),
)

PYTHON_FENCE_RE = re.compile(
    r"^([ \t]*)```\s*(?:python|py)\b[^\n]*\n(.*?)^[ \t]*```",
    re.MULTILINE | re.DOTALL,
)


@pytest.mark.parametrize(
    "page_path", USER_FACING_MENTAL_MODEL_PAGES, ids=lambda p: p.name
)
@pytest.mark.parametrize("banned_term", BANNED_INTERNAL_TERMS)
def test_user_facing_mental_model_pages_exclude_banned_internal_terms(
    page_path: Path,
    banned_term: str,
) -> None:
    content = page_path.read_text(encoding="utf-8")
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(banned_term)}(?![A-Za-z0-9_])")
    matches = list(pattern.finditer(content))
    assert not matches, (
        f"Banned internal term {banned_term!r} found in {page_path.relative_to(REPO_ROOT)}"
    )


def test_architecture_doc_reuse_table_and_required_sections() -> None:
    arch_path = DOCS_DIR / "architecture.md"
    content = arch_path.read_text(encoding="utf-8")

    assert '<a id="how-caching-works"></a>' in content
    assert '<a id="data-virtualization"></a>' in content

    for heading in (
        "# How Consist Works",
        "## A run is a recipe",
        "## When is a result reused?",
        "## What you get on a cache hit",
        "### Ghost Mode",
        "## Where files live",
        "## What gets recorded",
        "## Upgraded from 0.4?",
        "## Going deeper",
    ):
        assert heading in content

    parsed_rows: dict[str, tuple[str, str]] = {}
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or stripped.startswith("| :---"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) == 3 and cells[0] != "If you change...":
            parsed_rows[cells[0]] = (cells[1], cells[2])

    for change_desc, expected_reused in EXPECTED_REUSE_TABLE_ROWS:
        assert change_desc in parsed_rows, f"Missing reuse table row: {change_desc!r}"
        reused_val, why_val = parsed_rows[change_desc]
        assert reused_val == expected_reused
        assert len(why_val) > 0


def test_internals_doc_exists_and_is_registered_in_zensical_toml() -> None:
    internals_path = DOCS_DIR / "internals.md"
    assert internals_path.is_file()
    internals_text = internals_path.read_text(encoding="utf-8")
    assert "consist migrate" in internals_text
    assert "Ephemeral Mode" in internals_text

    config = tomllib.loads(ZENSICAL_TOML.read_text(encoding="utf-8"))
    nav_repr = repr(config["project"]["nav"])
    assert "architecture.md" in nav_repr
    assert "internals.md" in nav_repr
    assert "migrating-to-0.5.md" in nav_repr


@pytest.mark.parametrize(
    "md_path",
    USER_FACING_MENTAL_MODEL_PAGES + (DOCS_DIR / "internals.md",),
    ids=lambda p: p.name,
)
def test_v05_doc_python_blocks_parse_cleanly(md_path: Path) -> None:
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
