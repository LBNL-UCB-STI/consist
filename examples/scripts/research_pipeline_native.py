from pathlib import Path

import pandas as pd
import xarray as xr

from consist import Tracker, define_step

# ---------------------------------------------------------------------------------
# Minimal Template: Researcher / Library Developer
#
# Use this template if you are building a native Python simulation or analysis
# pipeline and want to ensure every step is reproducible and self-documenting.
# ---------------------------------------------------------------------------------


# 1. Define your analysis steps using @define_step
# This automatically handles input/output logging and caching.
@define_step(outputs=["clean_data"])
def preprocess(raw_data_path: Path) -> pd.DataFrame:
    """Cleans and prepares raw simulation inputs."""
    df = pd.read_csv(raw_data_path)
    # ... logic ...
    return df


@define_step(outputs=["run_simulation"])
def simulate(clean_data: pd.DataFrame, params: dict) -> xr.Dataset:
    """Runs a core simulation producing multidimensional results (Xarray)."""
    # Consist knows how to handle Xarray Datasets natively
    ds = xr.Dataset({"temperature": (["x", "y"], [[20, 21], [22, 23]])})
    return ds


def main():
    # 2. Setup the Tracker
    tracker = Tracker(
        run_dir=Path("./research_logs"),
        db_path="research_provenance.duckdb",
        project_root=".",
    )

    # 3. Execute the Pipeline as a Scenario
    # Scenario steps are cached and automatically connected through a Coupler.
    # We log administrative metadata here for Lab/Grant tracking.
    with tracker.scenario(
        "exp_001_v2",
        model="climate_analysis",
        grant_id="GRANT_ID_1",  # Custom administrative metadata
        lab_group="ResearchGroup",  # Track by research group
        tags=["baseline_2025"],
    ) as sc:
        # Step 1: Preprocess
        raw_path = Path("examples/data/raw_data.csv")
        sc.run(
            preprocess,
            inputs={"raw_data": raw_path},
            runtime_kwargs={"raw_data_path": raw_path},
        )

        # Step 2: Simulate
        # Consist will cache this result based on the code in 'simulate'
        # AND the contents of the logged inputs.
        sc.run(
            simulate,
            inputs={"clean_data": "clean_data"},
            load_inputs=True,
            runtime_kwargs={"params": {"resolution": "high", "seed": 42}},
        )

        result_artifact = sc.coupler.require("run_simulation")
        print(f"Pipeline complete. Result stored at: {result_artifact.path}")

    # 5. Accessibility for Collaborators
    # Export a SQLModel schema so others know exactly how your data is structured.
    # schema_code = tracker.export_schema_sqlmodel(artifact_id=...)
    # print(schema_code)


if __name__ == "__main__":
    main()
