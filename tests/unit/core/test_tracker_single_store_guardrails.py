from __future__ import annotations

from pathlib import Path

from consist.core.persistence import DatabaseManager
from consist.core.stores import HotDataStore, MetadataStore
from consist.core.tracker import Tracker


def test_single_store_initializes_metadata_and_hot_data_stores(tmp_path: Path) -> None:
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
        metadata_ledger = tracker.metadata_store.ledger
        hot_db_path = tracker.hot_data_store.db_path
        hot_ledger = tracker.hot_data_store.ledger

        assert metadata_db is not None
        assert metadata_ledger is not None
        assert hot_db_path == db_path
        assert metadata_db.db_path == db_path

        assert tracker.db_path == db_path
        assert tracker.db is metadata_db
        assert tracker.ledger is metadata_ledger
        assert tracker.store is metadata_ledger
        assert hot_ledger is tracker.ledger
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
        assert tracker.ledger is replacement_db.ledger
        assert tracker.hot_data_store.ledger is replacement_db.ledger
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
    assert tracker.ledger is None
    assert tracker.store is None
    assert tracker.db_path is None


def test_compatibility_aliases_derive_from_store_only_tracker_state(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "store-only.duckdb"))
    tracker = Tracker.__new__(Tracker)
    tracker.metadata_store = MetadataStore(db=db)
    tracker.hot_data_store = HotDataStore(
        db_path=db.db_path,
        metadata_store=tracker.metadata_store,
    )

    try:
        assert tracker.db is db
        assert tracker.ledger is db.ledger
        assert tracker.db_path == db.db_path
    finally:
        db.close()
