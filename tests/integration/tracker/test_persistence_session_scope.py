from datetime import datetime
from unittest.mock import patch

from consist.core.persistence import DatabaseManager
from consist.models.run import Run


def _seed_run(db: DatabaseManager, run_id: str = "run1") -> None:
    now = datetime(2025, 1, 1, 12, 0)
    run = Run(
        id=run_id,
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="completed",
        created_at=now,
        started_at=now,
        ended_at=now,
    )
    with db.session_scope() as session:
        session.add(run)
        session.commit()


def test_session_scope_reuses_existing_session(tmp_path):
    db = DatabaseManager(str(tmp_path / "session_scope.db"))
    with db.session_scope() as session_outer:
        with db.session_scope() as session_inner:
            assert session_inner is session_outer


def test_db_methods_use_session_scope(tmp_path):
    db = DatabaseManager(str(tmp_path / "session_scope_calls.db"))
    _seed_run(db)

    with patch.object(db, "session_scope", wraps=db.session_scope) as scope:
        result = db.get_run("run1")
        assert result is not None
        assert result.id == "run1"
        assert scope.call_count == 1
