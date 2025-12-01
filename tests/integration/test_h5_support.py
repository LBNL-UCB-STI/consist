import pytest
import pandas as pd
from consist.core.tracker import Tracker
from consist.api import load

# Check for tables
try:
    import tables

    has_tables = True
except ImportError:
    has_tables = False


@pytest.mark.skipif(not has_tables, reason="PyTables not installed")
def test_h5_virtual_artifacts(tmp_path):
    """
    Simulates the PILATES workflow:
    1. Create a monolithic H5 file (Container).
    2. Log it.
    3. Log 'Virtual Artifacts' for specific tables inside it.
    4. Load and Ingest those virtual artifacts.
    """
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # 1. Create H5 File
    h5_path = run_dir / "pipeline.h5"
    h5_path.parent.mkdir(parents=True, exist_ok=True)

    df_persons = pd.DataFrame({"pid": [1, 2], "age": [30, 40]})
    df_households = pd.DataFrame({"hid": [10, 20], "size": [2, 1]})

    with pd.HDFStore(h5_path, mode="w") as store:
        store.put("/2018/persons", df_persons, format="table")
        store.put("/2018/households", df_households, format="table")

    # 2. Log in Consist
    with tracker.start_run("run_h5", model="run_A"):
        # A. Log the Container
        container = tracker.log_artifact(h5_path, key="pipeline_store", driver="h5")

        # B. Log Virtual Artifact (Persons)
        # Note: We point URI to the physical file, but driver="h5_table" + meta distinguishes it
        art_persons = tracker.log_artifact(
            h5_path,
            key="persons_2018",
            driver="h5_table",
            # Flatten metadata into kwargs
            table_path="/2018/persons",
            parent_container_id=str(container.id),
        )

    # 3. Test Loading (Cold)
    # consist.load should inspect driver="h5_table", look at meta['table_path'], and read specific key
    loaded_df = load(art_persons, tracker=tracker)
    assert len(loaded_df) == 2
    assert "pid" in loaded_df.columns

    # 4. Test Ingestion (Hot)
    # This proves dlt_loader can crack open the H5 and read just the table
    with tracker.start_run("ingest_h5", model="run_A", inputs=[art_persons]):
        tracker.ingest(art_persons)

    # 5. Test Ghost Mode (Delete file, read from DB)
    h5_path.unlink()

    # Fetch fresh artifact to get 'is_ingested' flag
    fresh_art = tracker.get_artifact("persons_2018")
    ghost_df = load(fresh_art, tracker=tracker)

    assert len(ghost_df) == 2
    assert ghost_df.iloc[0]["age"] == 30
