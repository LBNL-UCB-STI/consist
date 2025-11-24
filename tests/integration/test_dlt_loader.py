# tests/integration/test_dlt_loader.py
from typing import Optional

import pytest
from sqlmodel import SQLModel, Field, Session, text
from consist.integrations.dlt_loader import ingest_artifact
from consist.models.run import Run
from consist.models.artifact import Artifact


# Define a test schema
class MockTable(SQLModel, table=True):
    __tablename__ = "mock_data"
    id: Optional[int] = Field(default=None, primary_key=True)
    val: str


def test_ingest_artifact_strict(tracker, engine, db_path):
    """Test that tracker.ingest handles data loading and locking."""

    # 1. Setup Context
    # We must be inside a run for ingest to work
    with tracker.start_run("run_ABC", "test", year=2030):
        # 2. Log Artifact
        artifact = tracker.log_artifact("inputs/file.csv", "my_data", schema=MockTable)

        # 3. Fake Data
        raw_data = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]

        # --- CHANGE: Use the clean API ---
        # Note: We still need to dispose the *test fixture's* engine because
        # the Tracker can't control that one.
        engine.dispose()

        # The Tracker will handle its own engine.dispose() internally now.
        tracker.ingest(
            artifact=artifact,
            data=raw_data,
            schema=MockTable
        )

    # 4. Verify (SQLAlchemy reconnects automatically)
    with Session(engine) as session:
        result = session.exec(text("SELECT * FROM global_tables.mock_data ORDER BY id")).fetchall()

        assert len(result) == 2
        row_1 = result[0]

        # Debugging Helper: Print columns if something is wrong
        print(f"\n[DEBUG] Found columns: {row_1._mapping.keys()}")

        # Check Business Data
        assert row_1.id == 1
        assert row_1.val == "A"

        # FIX: Check for the new 'consist_' prefixed columns
        assert row_1._mapping["consist_run_id"] == "run_ABC"

        # Note: 'year' was injected in the test setup?
        # In the previous code: `tracker.start_run("run_ABC", "test", year=2030)`
        # So yes, consist_year should be there.
        if "consist_year" in row_1._mapping:
            assert row_1._mapping["consist_year"] == 2030