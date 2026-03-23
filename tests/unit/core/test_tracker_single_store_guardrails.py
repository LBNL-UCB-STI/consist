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

    assert tracker.metadata_store is not None
    assert tracker.hot_data_store is not None


def test_single_store_preserves_compatibility_aliases(tmp_path: Path) -> None:
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)

    assert tracker.metadata_store is not None
    assert tracker.hot_data_store is not None

    metadata_db = getattr(tracker.metadata_store, "db", None)
    metadata_engine = getattr(tracker.metadata_store, "engine", None)
    hot_db_path = getattr(tracker.hot_data_store, "db_path", None)
    hot_engine = getattr(tracker.hot_data_store, "engine", None)

    assert metadata_db is not None
    assert metadata_engine is not None
    assert hot_db_path == db_path
    assert getattr(metadata_db, "db_path", None) == db_path

    assert tracker.db_path == db_path
    assert tracker.db is metadata_db
    assert tracker.engine is metadata_engine
    if hot_engine is not None:
        assert hot_engine is tracker.engine


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
    replacement_path = str(tmp_path / "replacement.duckdb")
    replacement_db = DatabaseManager(replacement_path)

    try:
        tracker.metadata_store.db = replacement_db
        tracker.hot_data_store.db_path = replacement_path

        assert tracker.db is replacement_db
        assert tracker.engine is replacement_db.engine
        assert tracker.db_path == replacement_path
    finally:
        replacement_db.engine.dispose()
        original_db.engine.dispose()


def test_no_db_path_has_no_stores_and_null_compatibility_aliases(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)

    assert tracker.metadata_store is None
    assert tracker.hot_data_store is None
    assert tracker.db is None
    assert tracker.engine is None
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
        assert tracker.engine is db.engine
        assert tracker.db_path == db.db_path
    finally:
        db.engine.dispose()
