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
