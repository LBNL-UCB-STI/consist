from datetime import datetime
from unittest.mock import patch

from sqlmodel import select

from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink


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


def _legacy_identity_snapshot(db: DatabaseManager):
    with db.session_scope() as session:
        runs = session.exec(select(Run).order_by(Run.id)).all()
        links = session.exec(
            select(RunArtifactLink).order_by(
                RunArtifactLink.run_id,
                RunArtifactLink.artifact_id,
            )
        ).all()
    return (
        [
            (
                run.id,
                run.config_hash,
                run.input_hash,
                run.git_hash,
                run.signature,
                run.meta,
            )
            for run in runs
        ],
        [
            (str(link.run_id), str(link.artifact_id), link.direction, link.is_implicit)
            for link in links
        ],
    )


def test_pre_action_v2_rows_remain_queryable_without_identity_rewrites(tmp_path):
    db_path = tmp_path / "pre_action_v2_rows.duckdb"
    legacy_db = DatabaseManager(str(db_path))
    now = datetime(2025, 1, 1, 12, 0)
    producer = Run(
        id="legacy_producer",
        model_name="prepare",
        status="completed",
        stage="legacy",
        phase="prepare",
        config_hash="legacy_producer_config",
        input_hash="legacy_producer_input",
        git_hash="legacy_code",
        signature="legacy_producer_signature",
        meta={"legacy": True},
        created_at=now,
        started_at=now,
        ended_at=now,
    )
    consumer = Run(
        id="legacy_consumer",
        model_name="report",
        status="completed",
        stage="legacy",
        phase="report",
        config_hash="legacy_consumer_config",
        input_hash="legacy_consumer_input",
        git_hash="legacy_code",
        signature="legacy_consumer_signature",
        meta={"legacy": True},
        created_at=now,
        started_at=now,
        ended_at=now,
    )
    prepared = Artifact(
        key="prepared",
        container_uri="outputs://prepared.csv",
        driver="csv",
        hash="legacy_prepared_hash",
        run_id=producer.id,
    )
    report = Artifact(
        key="report",
        container_uri="outputs://report.csv",
        driver="csv",
        hash="legacy_report_hash",
        run_id=consumer.id,
    )
    consumer_id = consumer.id
    prepared_id = prepared.id
    report_id = report.id
    with legacy_db.session_scope() as session:
        session.add_all([producer, consumer, prepared, report])
        session.add_all(
            [
                RunArtifactLink(
                    run_id=producer.id, artifact_id=prepared.id, direction="output"
                ),
                RunArtifactLink(
                    run_id=consumer.id, artifact_id=prepared.id, direction="input"
                ),
                RunArtifactLink(
                    run_id=consumer.id, artifact_id=report.id, direction="output"
                ),
            ]
        )
        session.commit()

    before = _legacy_identity_snapshot(legacy_db)
    legacy_db.engine.dispose()
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)

    loaded_consumer = tracker.get_run(consumer_id)
    assert loaded_consumer is not None
    assert "input_identity" not in loaded_consumer.identity_summary
    artifacts = tracker.get_artifacts_for_run(consumer_id)
    assert artifacts.inputs["prepared"].id == prepared_id
    assert artifacts.outputs["report"].id == report_id

    lineage = tracker.get_artifact_lineage(report_id)
    assert lineage is not None
    assert lineage["producing_run"]["run"].id == consumer_id
    assert lineage["producing_run"]["inputs"][0]["artifact"].id == prepared_id
    assert (
        tracker.find_matching_run(
            config_hash="legacy_consumer_config",
            input_hash=f"sha256:action-v2:{'a' * 64}",
            git_hash="legacy_code",
        )
        is None
    )
    assert _legacy_identity_snapshot(tracker.db) == before
