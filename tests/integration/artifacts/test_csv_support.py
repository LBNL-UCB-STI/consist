# tests/integration/test_csv_support.py

import pandas as pd
from sqlalchemy import text

from consist.core.tracker import Tracker
from consist.api import load


def test_csv_end_to_end(tracker: Tracker):
    """
    Tests the end-to-end functionality for CSV artifacts within Consist.

    This test verifies the seamless integration of CSV files across key Consist features:
    artifact logging, data loading via `consist.load()`, and querying through
    DuckDB's "Hybrid Views" using `read_csv_auto`.

    What happens:
    1. A `Tracker` is initialized with a database path.
    2. A dummy CSV file (`my_data.csv`) is created with sample data.
    3. **Artifact Logging**: The CSV file is logged as an `Artifact` ("cities_data")
       within a `tracker.start_run` context.
    4. **Data Loading**: `consist.load()` is used to load the CSV artifact.
    5. **Hybrid View Creation**: A hybrid view named `v_cities` is created for the
       "cities_data" concept.
    6. **SQL Querying**: SQL queries are executed against `v_cities` to retrieve
       aggregated data and individual records.

    What's checked:
    - The `artifact.driver` is correctly identified as "csv".
    - `consist.load()` successfully loads the CSV data into a Pandas DataFrame,
      and the content matches the original.
    - The hybrid view `v_cities` is successfully created.
    - SQL queries against `v_cities` return correct aggregated results (e.g., average score)
      and individual records, demonstrating `read_csv_auto`'s functionality and type inference.
    """
    # Setup Dummy Data
    csv_path = tracker.run_dir / "my_data.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Create CSV
    df = pd.DataFrame(
        {"id": [1, 2], "city": ["New York", "Paris"], "score": [95.5, 88.0]}
    )
    df.to_csv(csv_path, index=False)

    gzip_path = tracker.run_dir / "my_data.csv.gz"
    df.to_csv(gzip_path, index=False, compression="gzip")

    # 1. Log Artifact
    with tracker.start_run("run_csv", model="model_A"):
        # Note: Tracker auto-detects .csv extension -> driver="csv"
        artifact = tracker.log_artifact(csv_path, key="cities_data")
        gzip_artifact = tracker.log_artifact(gzip_path, key="cities_data_gz")

    assert artifact.driver == "csv"
    assert gzip_artifact.driver == "csv"

    # 2. Test consist.load()
    loaded_df = load(artifact, tracker=tracker)
    assert len(loaded_df) == 2
    assert loaded_df.iloc[0]["city"] == "New York"

    loaded_gz_df = load(gzip_artifact, tracker=tracker)
    assert len(loaded_gz_df) == 2
    assert loaded_gz_df.iloc[1]["city"] == "Paris"

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
