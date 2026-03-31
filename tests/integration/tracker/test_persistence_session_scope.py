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


def test_run_stage_phase_backfill_on_database_open(tmp_path):
    db_path = tmp_path / "run_stage_phase_backfill.db"
    legacy_db = DatabaseManager(str(db_path))
    now = datetime(2025, 1, 1, 12, 0)
    legacy_run = Run(
        id="legacy_run",
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="completed",
        stage=None,
        phase=None,
        meta={"stage": "supply_demand_loop", "phase": "traffic_assignment"},
        created_at=now,
        started_at=now,
        ended_at=now,
    )
    with legacy_db.session_scope() as session:
        session.add(legacy_run)
        session.commit()

    legacy_db.engine.dispose()
    reopened = DatabaseManager(str(db_path))
    found = reopened.find_runs(
        stage="supply_demand_loop",
        phase="traffic_assignment",
        status="completed",
        limit=10,
    )

    assert [run.id for run in found] == ["legacy_run"]
    assert found[0].stage == "supply_demand_loop"
    assert found[0].phase == "traffic_assignment"
    assert found[0].meta["stage"] == "supply_demand_loop"
    assert found[0].meta["phase"] == "traffic_assignment"


def test_sync_run_prefers_canonical_stage_phase_over_stale_meta(tmp_path):
    db = DatabaseManager(str(tmp_path / "run_stage_phase_sync.db"))
    now = datetime(2025, 1, 1, 12, 0)
    run = Run(
        id="canonical_stage_phase",
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="completed",
        stage="canonical_stage",
        phase="canonical_phase",
        meta={"stage": "stale_stage", "phase": "stale_phase"},
        created_at=now,
        started_at=now,
        ended_at=now,
    )

    db.sync_run(run)

    persisted = db.get_run("canonical_stage_phase")
    assert persisted is not None
    assert persisted.stage == "canonical_stage"
    assert persisted.phase == "canonical_phase"
    assert persisted.meta["stage"] == "canonical_stage"
    assert persisted.meta["phase"] == "canonical_phase"
