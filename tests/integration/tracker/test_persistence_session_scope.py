from datetime import datetime
from unittest.mock import patch

import pytest

from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker
from consist.models.run import Run
from tests.helpers.artifact_factory import make_artifact


def _make_run(run_id: str = "run1") -> Run:
    now = datetime(2025, 1, 1, 12, 0)
    return Run(
        id=run_id,
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="completed",
        created_at=now,
        started_at=now,
        ended_at=now,
    )


def _seed_run(db: DatabaseManager, run_id: str = "run1") -> None:
    db.sync_run(_make_run(run_id))


def test_ledger_transaction_reuses_cursor_and_commits_or_rolls_back(tmp_path):
    db = DatabaseManager(str(tmp_path / "ledger_tx.duckdb"))
    try:
        with db.ledger.transaction() as cur_outer:
            with db.ledger.transaction() as cur_inner:
                assert cur_inner is cur_outer
            with db.ledger.exec_cursor() as exec_cur:
                assert exec_cur is cur_outer
            db.ledger.upsert_run(_make_run("committed_run"))

        assert db.get_run("committed_run") is not None

        with pytest.raises(RuntimeError, match="boom"):
            with db.ledger.transaction():
                db.ledger.upsert_run(_make_run("rolled_back_run"))
                raise RuntimeError("boom")

        assert db.get_run("rolled_back_run") is None
    finally:
        db.close()


def test_db_methods_use_ledger_cursors(tmp_path):
    db = DatabaseManager(str(tmp_path / "ledger_calls.duckdb"))
    try:
        with patch.object(
            db.ledger, "transaction", wraps=db.ledger.transaction
        ) as tx_spy:
            _seed_run(db)
            assert tx_spy.call_count == 1

        with patch.object(
            db.ledger, "exec_cursor", wraps=db.ledger.exec_cursor
        ) as cursor_spy:
            result = db.get_run("run1")
            assert result is not None
            assert result.id == "run1"
            assert cursor_spy.call_count >= 1
    finally:
        db.close()


def test_run_stage_phase_backfill_on_database_open(tmp_path):
    db_path = tmp_path / "run_stage_phase_backfill.duckdb"
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
    legacy_db.sync_run(legacy_run)
    legacy_db.close()

    reopened = DatabaseManager(str(db_path))
    try:
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
    finally:
        reopened.close()


def test_sync_run_prefers_canonical_stage_phase_over_stale_meta(tmp_path):
    db = DatabaseManager(str(tmp_path / "run_stage_phase_sync.duckdb"))
    try:
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
    finally:
        db.close()


def _legacy_identity_snapshot(db: DatabaseManager):
    runs = sorted(db.find_runs(limit=100), key=lambda r: r.id)
    bindings_df = db.query_df(
        "SELECT run_id, CAST(artifact_id AS VARCHAR) AS artifact_id, direction "
        "FROM main.run_binding ORDER BY run_id, artifact_id, direction"
    )
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
            (str(row["run_id"]), str(row["artifact_id"]), str(row["direction"]))
            for _, row in bindings_df.iterrows()
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
    prepared = make_artifact(
        key="prepared",
        container_uri="outputs://prepared.csv",
        driver="csv",
        hash="legacy_prepared_hash",
        run_id=producer.id,
    )
    report = make_artifact(
        key="report",
        container_uri="outputs://report.csv",
        driver="csv",
        hash="legacy_report_hash",
        run_id=consumer.id,
    )
    consumer_id = consumer.id
    prepared_id = prepared.id
    report_id = report.id

    legacy_db.sync_run(producer)
    legacy_db.sync_run(consumer)
    legacy_db.sync_artifact(prepared, run_id=producer.id, direction="output")
    legacy_db.link_artifact_to_run(prepared.id, run_id=consumer.id, direction="input")
    legacy_db.sync_artifact(report, run_id=consumer.id, direction="output")

    before = _legacy_identity_snapshot(legacy_db)
    legacy_db.close()

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)
    try:
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
        legacy_match = tracker.find_matching_run(
            config_hash="legacy_consumer_config",
            input_hash="legacy_consumer_input",
            git_hash="legacy_code",
        )
        assert legacy_match is not None
        assert legacy_match.id == consumer_id
        assert (
            tracker.find_matching_run(
                config_hash="legacy_consumer_config",
                input_hash=f"sha256:action-v2:{'a' * 64}",
                git_hash="legacy_code",
            )
            is None
        )
        assert tracker.db is not None
        assert _legacy_identity_snapshot(tracker.db) == before
    finally:
        tracker.close()
