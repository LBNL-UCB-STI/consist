from pathlib import Path

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
def test_h5_virtual_artifacts(tracker: Tracker):
    """
    Tests Consist's ability to handle HDF5 files as containers for "virtual artifacts."

    This scenario simulates a workflow where a single, large HDF5 file (e.g., a simulation
    output) contains multiple internal tables or datasets. Consist treats the overall
    HDF5 file as a container and allows specific internal paths within it to be logged
    as individual "virtual artifacts." This is crucial for granular provenance tracking
    within complex, multi-component data files.

    What happens:
    1. A monolithic HDF5 file (`pipeline.h5`) is created, containing two Pandas DataFrames
       stored as tables at specific internal paths (`/2018/persons` and `/2018/households`).
    2. The HDF5 file itself is logged as a Consist `Artifact` with `driver="h5"`.
    3. A specific internal table (`/2018/persons`) is logged as a separate "virtual artifact"
       with `driver="h5_table"` and its internal path stored in its metadata.
    4. The virtual "persons" artifact is loaded from disk.
    5. The virtual "persons" artifact is then ingested into the DuckDB.
    6. The original HDF5 file is deleted to simulate "Ghost Mode."
    7. The "persons" artifact is re-loaded.

    What's checked:
    -   **Initial Load (Cold)**: The `consist.load()` function correctly reads the specific
        `/2018/persons` table from the HDF5 file when using `driver="h5_table"`.
    -   **Ingestion (Hot)**: `tracker.ingest()` successfully extracts and loads the data
        from the specified internal HDF5 table into the DuckDB.
    -   **Ghost Mode**: After deleting the physical HDF5 file, `consist.load()` can
        still retrieve the "persons" data from the DuckDB, demonstrating data recovery
        and continued access even if the original file is lost.
    """
    # 1. Create H5 File
    h5_path = tracker.run_dir / "pipeline.h5"
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
