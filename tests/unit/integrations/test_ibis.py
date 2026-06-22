from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest
from sqlmodel import Field, SQLModel

from consist.core.tracker import Tracker
from consist.integrations import ibis as ibis_integration


class Person(SQLModel, table=True):
    __tablename__ = "ibis_person_unit"

    person_id: int = Field(primary_key=True)
    age: int


def test_import_consist_without_ibis() -> None:
    code = dedent(
        """
        import builtins
        import sys

        real_import = builtins.__import__

        def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "ibis" or name.startswith("ibis."):
                raise ModuleNotFoundError(name)
            return real_import(name, globals, locals, fromlist, level)

        builtins.__import__ = blocked_import
        sys.path.insert(0, "src")

        import consist
        """
    )
    repo_root = Path(__file__).resolve().parents[3]
    subprocess.run([sys.executable, "-c", code], check=True, cwd=repo_root)


def test_ibis_connection_missing_dependency(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker = Tracker(run_dir=tmp_path, db_path=str(tmp_path / "provenance.duckdb"))

    def _missing_ibis() -> object:
        raise ImportError(
            "Ibis support is optional. Install it with `pip install -e '.[ibis]'` "
            "or `pip install 'consist[ibis]'`."
        )

    monkeypatch.setattr(ibis_integration, "_import_ibis", _missing_ibis)

    with pytest.raises(ImportError, match="consist\\[ibis\\]"):
        ibis_integration.ibis_connection(tracker)


def test_ibis_view_requires_duckdb_database(tmp_path) -> None:
    tracker = Tracker(run_dir=tmp_path)

    with pytest.raises(RuntimeError, match="DuckDB database configured"):
        ibis_integration.ibis_view(tracker, model=Person)
