# tests/integration/test_csv_support.py

import pandas as pd
from sqlalchemy import text

from consist.core.tracker import Tracker
from consist.api import load


def test_csv_end_to_end(tmp_path):
    """
    Verifies that CSV artifacts function correctly in:
    1. Logging
    2. loading (consist.load)
    3. Querying (Hybrid Views via read_csv_auto)
    """
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # Setup Dummy Data
    csv_path = run_dir / "my_data.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Create CSV
    df = pd.DataFrame(
        {"id": [1, 2], "city": ["New York", "Paris"], "score": [95.5, 88.0]}
    )
    df.to_csv(csv_path, index=False)

    # 1. Log Artifact
    with tracker.start_run("run_csv", model="model_A"):
        # Note: Tracker auto-detects .csv extension -> driver="csv"
        artifact = tracker.log_artifact(csv_path, key="cities_data")

    assert artifact.driver == "csv"

    # 2. Test consist.load()
    loaded_df = load(artifact, tracker=tracker)
    assert len(loaded_df) == 2
    assert loaded_df.iloc[0]["city"] == "New York"

    # 3. Test View Factory (SQL Access)
    # This proves ViewFactory is using read_csv_auto correctly
    tracker.create_view("v_cities", "cities_data")

    with tracker.engine.connect() as conn:
        # Run a query that requires type inference (AVG on score)
        result = conn.execute(text("SELECT AVG(score) FROM v_cities")).fetchone()
        avg_score = result[0]

        # 95.5 + 88.0 = 183.5 / 2 = 91.75
        assert avg_score == 91.75

        # Verify text column
        names = conn.execute(text("SELECT city FROM v_cities ORDER BY city")).fetchall()
        assert names[0][0] == "New York"
        assert names[1][0] == "Paris"
