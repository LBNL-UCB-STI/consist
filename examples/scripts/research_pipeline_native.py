from pathlib import Path

import consist
import pandas as pd
import xarray as xr

from consist import Tracker
from consist.types import ExecutionOptions

# ---------------------------------------------------------------------------------
# Minimal Template: Researcher / Library Developer
#
# Use this template if you are building a native Python simulation or analysis
# pipeline and want to ensure every step is reproducible and self-documenting.
# ---------------------------------------------------------------------------------


# 1. Define plain Python callables (recommended path).
def preprocess(raw_data_path: Path) -> pd.DataFrame:
    """Cleans and prepares raw simulation inputs."""
    df = pd.read_csv(raw_data_path)
    # ... logic ...
    return df


def simulate(clean_data: pd.DataFrame, params: dict) -> xr.Dataset:
    """Runs a core simulation producing multidimensional results (Xarray)."""
    # Consist knows how to handle Xarray Datasets natively
    ds = xr.Dataset({"temperature": (["x", "y"], [[20, 21], [22, 23]])})
    return ds


def _write_example_input(path: Path) -> None:
    """Create the deterministic input used by this self-contained example."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"value": [20, 21, 22, 23]}).to_csv(path, index=False)


def main():
    example_workspace = Path("examples/runs/research_pipeline_native")
    raw_path = example_workspace / "raw_data.csv"
    _write_example_input(raw_path)

    # 2. Setup the Tracker
    tracker = Tracker(
        run_dir=example_workspace / "research_logs",
        db_path=str(example_workspace / "research_provenance.duckdb"),
        project_root=Path(__file__).resolve().parents[2],
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
        preprocess_result = sc.run(
            preprocess,
            inputs={"raw_data": raw_path},
            outputs=["clean_data"],
            execution_options=ExecutionOptions(
                runtime_kwargs={"raw_data_path": raw_path}
            ),
        )

        # Step 2: Simulate
        # Consist will cache this result based on the code in 'simulate',
        # the declared simulation settings, and the logged inputs.
        simulation_params = {"resolution": "high", "seed": 42}
        sc.run(
            simulate,
            inputs={"clean_data": consist.ref(preprocess_result, key="clean_data")},
            config={"params": simulation_params},
            outputs=["run_simulation"],
            execution_options=ExecutionOptions(
                input_binding="loaded",
                runtime_kwargs={"params": simulation_params},
            ),
        )

        result_artifact = sc.coupler.require("run_simulation")
        print(f"Pipeline complete. Result stored at: {result_artifact.path}")

    # 5. Accessibility for Collaborators
    # Export a SQLModel schema so others know exactly how your data is structured.
    # schema_code = tracker.export_schema_sqlmodel(artifact_id=...)
    # print(schema_code)


if __name__ == "__main__":
    main()
