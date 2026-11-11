import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from consist.core.persistence import DatabaseManager
from consist.models.run import Run, RunBindingInvocation


def test_tracker_find_matching_run_prefers_signature(tracker, monkeypatch):
    cached = Run(
        id="cached_sig_hit",
        model_name="model",
        status="completed",
        config_hash=None,
        git_hash=None,
    )
    signature_calls: list[str] = []
    legacy_calls = 0

    def fake_find_by_signature(signature: str):
        signature_calls.append(signature)
        return cached

    def fake_find_matching_run(config_hash: str, input_hash: str, git_hash: str):
        nonlocal legacy_calls
        legacy_calls += 1
        return None

    monkeypatch.setattr(tracker.db, "find_run_by_signature", fake_find_by_signature)
    monkeypatch.setattr(tracker.db, "find_matching_run", fake_find_matching_run)

    matched = tracker.find_matching_run(
        "config_hash",
        "input_hash",
        "git_hash",
        signature="sig_123",
    )

    assert matched is cached
    assert signature_calls == ["sig_123"]
    assert legacy_calls == 0


def test_begin_run_reuse_cache_checks_signature_lookup_first(tracker, monkeypatch):
    signature_calls: list[str] = []
    legacy_calls: list[tuple[str, str, str]] = []

    def fake_find_by_signature(signature: str):
        signature_calls.append(signature)
        return None

    def fake_find_matching_run(config_hash: str, input_hash: str, git_hash: str):
        legacy_calls.append((config_hash, input_hash, git_hash))
        return None

    monkeypatch.setattr(tracker.db, "find_run_by_signature", fake_find_by_signature)
    monkeypatch.setattr(tracker.db, "find_matching_run", fake_find_matching_run)

    tracker.begin_run("run_signature_lookup_first", "test_model", cache_mode="reuse")
    tracker.end_run()

    assert len(signature_calls) == 1
    assert signature_calls[0]
    assert len(legacy_calls) == 1


def test_db_sync_run_upserts_without_readback_across_log_levels(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "sync_run_v2.duckdb"))
    try:
        now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        run = Run(
            id="sync_run_v2",
            model_name="model",
            status="running",
            config_hash="cfg_1",
            git_hash="git_1",
            created_at=now,
            started_at=now,
        )

        original_get_run = db.ledger.get_run
        original_tx = db.ledger.transaction
        get_run_calls = 0
        tx_calls = 0

        def counting_get_run(run_id: str):
            nonlocal get_run_calls
            get_run_calls += 1
            return original_get_run(run_id)

        @contextmanager
        def counting_tx():
            nonlocal tx_calls
            tx_calls += 1
            with original_tx() as cur:
                yield cur

        monkeypatch.setattr(db.ledger, "get_run", counting_get_run)
        monkeypatch.setattr(db.ledger, "transaction", counting_tx)

        logger = logging.getLogger()
        original_level = logger.level
        try:
            logger.setLevel(logging.INFO)
            db.sync_run(run, require_success=True)

            completed = run.model_copy(
                update={
                    "status": "completed",
                    "signature": "sig_v2",
                    "ended_at": datetime(2025, 1, 1, 12, 5, tzinfo=timezone.utc),
                }
            )
            logger.setLevel(logging.DEBUG)
            db.sync_run(completed, require_success=True)
        finally:
            logger.setLevel(original_level)

        assert get_run_calls == 0
        assert tx_calls == 2

        persisted = original_get_run("sync_run_v2")
        assert persisted is not None
        assert persisted.status == "completed"
        assert persisted.signature == "sig_v2"
        assert persisted.config_hash == "cfg_1"
    finally:
        db.close()


def test_db_sync_run_normalizes_meta_and_records_binding_invocation(tmp_path):
    db = DatabaseManager(str(tmp_path / "sync_run_invocation.duckdb"))
    try:
        now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        run = Run(
            id="sync_run_inv",
            model_name="model",
            status="completed",
            config_hash="cfg",
            git_hash="git",
            meta={"stage": "skim", "phase": "post"},
            created_at=now,
            started_at=now,
            ended_at=now,
        )
        inv = RunBindingInvocation(
            requested_run_id="sync_run_inv",
            execution_run_id="sync_run_inv",
            cache_outcome="miss",
            binding_json='{"k":"v"}',
            created_at=now,
        )
        db.sync_run(run, binding_invocation=inv, require_success=True)

        persisted = db.get_run("sync_run_inv")
        assert persisted is not None
        assert persisted.stage == "skim"
        assert persisted.phase == "post"

        fetched_inv = db.get_binding_invocation(inv.id)
        assert fetched_inv is not None
        assert fetched_inv.execution_run_id == "sync_run_inv"
        assert fetched_inv.cache_outcome == "miss"
    finally:
        db.close()
