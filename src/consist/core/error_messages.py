from __future__ import annotations


def format_problem_cause_fix(*, problem: str, cause: str, fix: str) -> str:
    """Return a standardized diagnostic message."""
    return f"Problem: {problem}\nCause: {cause}\nFix: {fix}"
