from __future__ import annotations

from pathlib import Path

from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker


def test_single_store_tracker_initializes_both_stores(tmp_path: Path) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
    )
    try:
        assert tracker.metadata_store is not None
        assert tracker.hot_data_store is not None
    finally:
        tracker.close()


def test_single_store_preserves_compatibility_aliases(tmp_path: Path) -> None:
    db_path = str((tmp_path / "provenance.duckdb").resolve())
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)
    try:
        assert tracker.metadata_store is not None
        assert tracker.hot_data_store is not None

        metadata_db = tracker.metadata_store.db
        hot_db_path = tracker.hot_data_store.db_path
        hot_db = tracker.hot_data_store.db

        assert metadata_db is not None
        assert hot_db_path == db_path
        assert metadata_db.db_path == db_path

        assert tracker.db_path == db_path
        assert tracker.db is metadata_db
        assert hot_db is tracker.db
    finally:
        tracker.close()


def test_single_store_compatibility_aliases_are_derived_from_stores(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
    )

    assert tracker.metadata_store is not None
    assert tracker.hot_data_store is not None

    original_db = tracker.metadata_store.db
    replacement_path = str((tmp_path / "replacement.duckdb").resolve())
    replacement_db = DatabaseManager(replacement_path)

    try:
        tracker.metadata_store.db = replacement_db
        tracker.hot_data_store.db_path = replacement_db.db_path

        assert tracker.db is replacement_db
        assert tracker.hot_data_store.db is replacement_db
        assert tracker.db_path == replacement_db.db_path
    finally:
        replacement_db.close()
        original_db.close()


def test_no_db_path_has_no_stores_and_null_compatibility_aliases(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)

    assert tracker.metadata_store is None
    assert tracker.hot_data_store is None
    assert tracker.db is None
    assert tracker.db_path is None


def test_manual_store_wiring_populates_compatibility_aliases(tmp_path: Path) -> None:
    from consist.core.stores import build_hot_data_store, build_metadata_store

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)
    db = DatabaseManager(str(tmp_path / "manual.duckdb"))
    tracker.metadata_store = build_metadata_store(db)
    tracker.hot_data_store = build_hot_data_store(db)

    try:
        assert tracker.db is db
        assert tracker.db_path == db.db_path
    finally:
        db.close()


def test_hot_data_store_ddl_dml_and_transaction_forwarding(tmp_path: Path) -> None:
    import pytest

    from consist.core.stores import HotDataStore

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
    )
    try:
        store = tracker.hot_data_store
        assert store is not None

        assert store.has_table("custom_items", table_schema="global_tables") is False

        store.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        store.execute(
            "CREATE TABLE global_tables.custom_items (id INTEGER, label VARCHAR)"
        )
        assert store.has_table("custom_items", table_schema="global_tables") is True

        store.executemany(
            "INSERT INTO global_tables.custom_items VALUES (?, ?)",
            [(1, "alpha"), (2, "beta")],
        )

        with store.transaction() as con:
            con.execute(
                "INSERT INTO global_tables.custom_items VALUES (?, ?)",
                [3, "gamma"],
            )

        with pytest.raises(RuntimeError, match="boom"):
            with store.transaction() as con:
                con.execute(
                    "INSERT INTO global_tables.custom_items VALUES (?, ?)",
                    [4, "rolled_back"],
                )
                raise RuntimeError("boom")

        rows = tracker.query_df(
            "SELECT id, label FROM global_tables.custom_items ORDER BY id"
        )
        assert list(rows["id"]) == [1, 2, 3]
        assert list(rows["label"]) == ["alpha", "beta", "gamma"]
    finally:
        tracker.close()

    detached = HotDataStore(db_path=str(tmp_path / "detached.duckdb"))
    with pytest.raises(RuntimeError, match="without an attached database"):
        detached.has_table("custom_items")
    with pytest.raises(RuntimeError, match="without an attached database"):
        detached.execute("SELECT 1")
    with pytest.raises(RuntimeError, match="without an attached database"):
        detached.executemany("INSERT INTO t VALUES (?)", [(1,)])
    with pytest.raises(RuntimeError, match="without an attached database"):
        with detached.transaction():
            pass


def test_tracker_close_is_idempotent_and_supports_context_manager(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "context.duckdb"
    with Tracker(run_dir=tmp_path / "runs", db_path=str(db_path)) as tracker:
        assert tracker.db is not None
        assert not tracker.db._closed
        tracker_ref = tracker

    assert tracker_ref.db is not None
    assert tracker_ref.db._closed
    tracker_ref.close()
    tracker_ref.close()
    assert tracker_ref.db._closed
