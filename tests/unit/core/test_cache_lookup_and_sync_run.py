import logging
from contextlib import contextmanager
from datetime import datetime

from consist.core.persistence import DatabaseManager
from consist.models.run import Run


def test_tracker_find_matching_run_prefers_signature(tracker, monkeypatch):
    cached = Run(
        id="cached_sig_hit",
        model_name="model",
        status="completed",
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


def test_db_sync_run_skips_readback_when_not_debug(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "sync_run_no_debug.db"))
    now = datetime(2025, 1, 1, 12, 0)
    run = Run(
        id="sync_run_no_debug",
        model_name="model",
        status="completed",
        created_at=now,
        started_at=now,
        ended_at=now,
    )

    original_scope = db.session_scope
    get_calls = 0

    @contextmanager
    def counting_scope():
        with original_scope() as session:
            original_get = session.get

            def counting_get(model, ident, *args, **kwargs):
                nonlocal get_calls
                if model is Run and ident == "sync_run_no_debug":
                    get_calls += 1
                return original_get(model, ident, *args, **kwargs)

            monkeypatch.setattr(session, "get", counting_get)
            yield session

    monkeypatch.setattr(db, "session_scope", counting_scope)

    logger = logging.getLogger()
    original_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        db.sync_run(run)
    finally:
        logger.setLevel(original_level)

    assert get_calls == 0


def test_db_sync_run_keeps_readback_when_debug(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "sync_run_debug.db"))
    now = datetime(2025, 1, 1, 12, 0)
    run = Run(
        id="sync_run_debug",
        model_name="model",
        status="completed",
        created_at=now,
        started_at=now,
        ended_at=now,
    )

    original_scope = db.session_scope
    get_calls = 0

    @contextmanager
    def counting_scope():
        with original_scope() as session:
            original_get = session.get

            def counting_get(model, ident, *args, **kwargs):
                nonlocal get_calls
                if model is Run and ident == "sync_run_debug":
                    get_calls += 1
                return original_get(model, ident, *args, **kwargs)

            monkeypatch.setattr(session, "get", counting_get)
            yield session

    monkeypatch.setattr(db, "session_scope", counting_scope)

    logger = logging.getLogger()
    original_level = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        db.sync_run(run)
    finally:
        logger.setLevel(original_level)

    assert get_calls == 1
