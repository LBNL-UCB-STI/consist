from datetime import datetime
from unittest.mock import patch

import duckdb
import pytest

from consist.core.persistence import (
    DatabaseManager,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
    TransactionAbortedError,
)
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink


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


def test_transaction_reentrancy_and_poison_rollback(tmp_path):
    db = DatabaseManager(str(tmp_path / "native_tx.duckdb"))
    try:
        with db.transaction() as cur_outer:
            with db.transaction() as cur_inner:
                assert cur_inner is cur_outer
            db.sync_run(_make_run("committed_run"))

        assert db.get_run("committed_run") is not None

        with pytest.raises(TransactionAbortedError):
            with db.transaction():
                db.sync_run(_make_run("outer_poisoned_run"))
                try:
                    with db.transaction():
                        db.sync_run(_make_run("inner_poisoned_run"))
                        raise RuntimeError("inner failure")
                except RuntimeError:
                    pass

        assert db.get_run("outer_poisoned_run") is None
        assert db.get_run("inner_poisoned_run") is None
    finally:
        db.close()


def test_db_methods_use_transaction(tmp_path):
    db = DatabaseManager(str(tmp_path / "tx_calls.duckdb"))
    try:
        with patch.object(db, "transaction", wraps=db.transaction) as tx_spy:
            _seed_run(db)
            assert tx_spy.call_count == 1
        result = db.get_run("run1")
        assert result is not None
        assert result.id == "run1"
    finally:
        db.close()


def test_v1_schema_raises_migration_required(tmp_path):
    db_path = tmp_path / "legacy_v1.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE consist_schema (version INTEGER, db_uid VARCHAR)")
        con.execute("INSERT INTO consist_schema VALUES (1, 'legacy-uid')")
    finally:
        con.close()

    with pytest.raises(DatabaseMigrationRequired) as exc_info:
        DatabaseManager(str(db_path))
    assert issubclass(DatabaseMigrationRequired, IncompatibleSchemaError)
    assert "version=1" in str(exc_info.value)


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
    links = db.query_rows(
        "SELECT * FROM main.run_artifact_link ORDER BY run_id, artifact_id, direction",
        model=RunArtifactLink,
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
        uri="outputs://prepared.csv",
        driver="csv",
        digest=None,
        run_id=producer.id,
    )
    report = Artifact(
        key="report",
        uri="outputs://report.csv",
        driver="csv",
        digest=None,
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
