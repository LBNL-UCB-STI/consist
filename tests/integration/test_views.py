# tests/integration/test_views.py

import pytest
import pandas as pd
from typing import Optional
from sqlmodel import SQLModel, Field, text
from consist.core.tracker import Tracker


# --- Models ---

class Household(SQLModel, table=True):
    __tablename__ = "households"
    id: int = Field(primary_key=True)
    income: float
    persons: Optional[int] = Field(default=None)


# --- Tests ---

def test_hybrid_view_generation(tracker, tmp_path):
    """
    The 'Happy Path': Unions a Parquet file (Cold) with a DB Table (Hot).
    Verifies Forward Schema Drift (Hot has extra col).
    """
    # 1. Cold Data (Missing 'persons')
    cold_df = pd.DataFrame({"id": [1], "income": [50000.0]})
    cold_path = tmp_path / "cold.parquet"
    cold_df.to_parquet(cold_path)

    with tracker.start_run("run_cold", "model_x", year=2010):
        tracker.log_artifact(str(cold_path), key="households", driver="parquet")

    # 2. Hot Data (Has 'persons')
    hot_data = [{"id": 2, "income": 75000.0, "persons": 3}]
    with tracker.start_run("run_hot", "model_x", year=2011):
        tracker.ingest(
            artifact=tracker.log_artifact("dummy", "households"),
            data=hot_data,
            schema=Household
        )

    # 3. Verify
    tracker.create_view("v_households", "households")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_households ORDER BY consist_year", conn)

        assert len(df) == 2
        # Check Cold Row (year 2010)
        assert df.iloc[0]['consist_year'] == 2010
        assert pd.isna(df.iloc[0]['persons'])
        # Check Hot Row (year 2011)
        assert df.iloc[1]['consist_year'] == 2011
        assert df.iloc[1]['persons'] == 3.0


def test_pure_cold_view(tracker, tmp_path):
    """
    Verifies the view works when NO data is in the database (File-only mode).
    This is critical for the initial migration phase.
    """
    # Create two parquet files
    df1 = pd.DataFrame({"id": [1], "val": ["A"]})
    df2 = pd.DataFrame({"id": [2], "val": ["B"]})

    p1 = tmp_path / "f1.parquet"
    p2 = tmp_path / "f2.parquet"
    df1.to_parquet(p1)
    df2.to_parquet(p2)

    with tracker.start_run("r1", "m", year=2020):
        tracker.log_artifact(str(p1), key="zones", driver="parquet")

    with tracker.start_run("r2", "m", year=2021):
        tracker.log_artifact(str(p2), key="zones", driver="parquet")

    # Create view (No 'global_tables.zones' exists)
    tracker.create_view("v_zones", "zones")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_zones ORDER BY id", conn)
        assert len(df) == 2
        assert df['val'].tolist() == ["A", "B"]
        # Ensure system columns were injected
        assert "consist_run_id" in df.columns


def test_bidirectional_schema_drift(tracker, tmp_path):
    """
    Verifies that if Cold has a column Hot lacks, AND Hot has a column Cold lacks,
    DuckDB preserves BOTH columns (filling with NULLs).
    """
    # Cold: Has 'legacy_col', missing 'new_col'
    cold_df = pd.DataFrame({"id": [1], "legacy_col": ["old"]})
    cold_path = tmp_path / "drift.parquet"
    cold_df.to_parquet(cold_path)

    with tracker.start_run("r_old", "m"):
        tracker.log_artifact(str(cold_path), key="drift_test", driver="parquet")

    # Hot: Has 'new_col', missing 'legacy_col'
    # We define a dynamic model for this
    class DriftModel(SQLModel, table=True):
        __tablename__ = "drift_test"
        id: int = Field(primary_key=True)
        new_col: str

    hot_data = [{"id": 2, "new_col": "fresh"}]
    with tracker.start_run("r_new", "m"):
        tracker.ingest(
            artifact=tracker.log_artifact("dummy", "drift_test"),
            data=hot_data,
            schema=DriftModel
        )

    tracker.create_view("v_drift", "drift_test")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_drift ORDER BY id", conn)

        # Row 1 (Cold): legacy=old, new=NaN
        assert df.iloc[0]['legacy_col'] == "old"
        assert pd.isna(df.iloc[0]['new_col'])

        # Row 2 (Hot): legacy=NaN, new=fresh
        assert pd.isna(df.iloc[1]['legacy_col'])
        assert df.iloc[1]['new_col'] == "fresh"


def test_empty_state_safety(tracker):
    """
    Verifies that asking for a view of a non-existent concept
    returns a valid (but empty) view, rather than crashing.
    """
    tracker.create_view("v_ghost", "ghost_concept")

    with tracker.engine.connect() as conn:
        # Should not raise table not found
        count = conn.execute(text("SELECT count(*) FROM v_ghost")).scalar()
        assert count == 0

        # Ensure it's select-able
        df = pd.read_sql("SELECT * FROM v_ghost", conn)
        assert len(df) == 0


def test_view_with_mounts(tmp_path):
    """
    Verifies that the ViewFactory correctly resolves virtualized URIs
    (e.g., inputs://) to absolute paths required by DuckDB.
    """
    # Setup tracker with mounts (Cannot use default fixture here)
    data_dir = tmp_path / "data_mount"
    data_dir.mkdir()

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
        mounts={"inputs": str(data_dir)}
    )

    # Create file inside the mount point
    df = pd.DataFrame({"x": [100]})
    p_path = data_dir / "mounted.parquet"
    df.to_parquet(p_path)

    # Log using absolute path (Tracker should virtualize it to inputs://)
    with tracker.start_run("r_mount", "m"):
        # We simulate the user providing the absolute path,
        # which Tracker converts to inputs:// internally.
        tracker.log_artifact(str(p_path), key="mounted_data", driver="parquet")

    # Verify internal storage is virtualized
    with tracker.engine.connect() as conn:
        # We query the artifact table directly to prove it stored the virtualization
        uris = pd.read_sql("SELECT uri FROM artifact WHERE key='mounted_data'", conn)['uri'].tolist()
        assert uris[0] == "inputs://mounted.parquet"

    # Create View (This tests if ViewFactory correctly resolves inputs:// back to absolute)
    tracker.create_view("v_mounted", "mounted_data")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_mounted", conn)
        assert len(df) == 1
        assert df.iloc[0]['x'] == 100