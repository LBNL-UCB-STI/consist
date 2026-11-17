from __future__ import annotations

from pathlib import Path
from typing import Union


def format_problem_cause_fix(*, problem: str, cause: str, fix: str) -> str:
    """Return a standardized user-facing diagnostic message."""
    return f"Problem: {problem}\nLikely cause: {cause}\nSuggested fix: {fix}"


class MissingOutputError(FileNotFoundError):
    """Raised when a declared output path does not exist after step execution."""

    def __init__(self, *, role: str, path: Union[str, Path]) -> None:
        self.role = role
        self.path = Path(path)
        super().__init__(f"Declared output {role!r} was not produced at {self.path}.")


class StaleOutputError(RuntimeError):
    """Raised when a pre-existing declared output target was left untouched by a step."""

    def __init__(self, *, role: str, path: Union[str, Path]) -> None:
        self.role = role
        self.path = Path(path)
        super().__init__(
            f"Declared output {role!r} at {self.path} already existed before the step "
            "and was not modified (stat and content/member digest are unchanged)."
        )
