from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
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


def test_ibis_grouped_view_requires_profiled_schema(tmp_path) -> None:
    tracker = Tracker(run_dir=tmp_path, db_path=str(tmp_path / "provenance.duckdb"))

    with pytest.raises(RuntimeError, match="No stored schema was found"):
        with ibis_integration.ibis_grouped_view(
            tracker,
            view_name="v_sweep",
            artifact_id=uuid.uuid4(),
        ):
            pass


def test_ibis_grouped_view_resolves_schema_and_closes_backend(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker = Tracker(run_dir=tmp_path, db_path=str(tmp_path / "provenance.duckdb"))
    calls: dict[str, object] = {}
    backend = SimpleNamespace(disconnected=False)
    sentinel = object()

    def fake_get_artifact_schema_for_artifact(
        *, artifact_id: uuid.UUID
    ) -> tuple[SimpleNamespace, list[object]]:
        calls["artifact_id"] = artifact_id
        return (SimpleNamespace(id="schema-123"), [])

    def fake_create_grouped_view(**kwargs):
        calls["grouped_view_kwargs"] = kwargs
        return None

    def fake_table(name: str):
        calls["table_name"] = name
        return sentinel

    def fake_disconnect() -> None:
        backend.disconnected = True

    backend.table = fake_table
    backend.disconnect = fake_disconnect

    monkeypatch.setattr(
        tracker.db,
        "get_artifact_schema_for_artifact",
        fake_get_artifact_schema_for_artifact,
    )
    monkeypatch.setattr(tracker, "create_grouped_view", fake_create_grouped_view)
    monkeypatch.setattr(
        ibis_integration, "ibis_connection", lambda resolved_tracker: backend
    )

    artifact_id = uuid.uuid4()
    with ibis_integration.ibis_grouped_view(
        tracker,
        view_name="v_sweep",
        artifact_id=artifact_id,
        attach_facets=["setting_id"],
        mode="hybrid",
    ) as series:
        assert series is sentinel
        assert backend.disconnected is False

    assert backend.disconnected is True
    assert calls["artifact_id"] == artifact_id
    assert calls["table_name"] == "v_sweep"
    assert calls["grouped_view_kwargs"] == {
        "view_name": "v_sweep",
        "schema_id": "schema-123",
        "namespace": None,
        "params": None,
        "drivers": None,
        "attach_facets": ["setting_id"],
        "include_system_columns": True,
        "mode": "hybrid",
        "if_exists": "replace",
        "missing_files": "warn",
        "run_id": None,
        "parent_run_id": None,
        "model": None,
        "status": None,
        "year": None,
        "iteration": None,
        "schema_compatible": False,
    }
