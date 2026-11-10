from pathlib import Path
import shutil
import pandas as pd

import consist
from consist import Tracker, output_path

# ---------------------------------------------------------------------------------
# Minimal Template: Public Sector / Model Integrator
#
# Use this template if you are wrapping an existing simulation model
# (e.g., ActivitySim, SUMO, UrbanSim) to track its inputs and outputs.
# ---------------------------------------------------------------------------------


def _write_example_inputs(inputs_dir: Path) -> None:
    """Create deterministic input files so this template runs self-contained."""
    inputs_dir.mkdir(parents=True, exist_ok=True)
    (inputs_dir / "land_use.csv").write_text(
        "zone_id,households,jobs\n1,1200,800\n2,950,1500\n",
        encoding="utf-8",
    )
    (inputs_dir / "rail_network_v2.gpkg").write_text(
        "mock_gpkg_payload_v2\n",
        encoding="utf-8",
    )


def main():
    example_workspace = Path("examples/runs/model_integrator_wrapper")
    if example_workspace.exists():
        shutil.rmtree(example_workspace)
    inputs_dir = example_workspace / "data" / "raw_inputs"
    outputs_dir = example_workspace / "runs" / "outputs"
    _write_example_inputs(inputs_dir)

    # 1. Initialize the Tracker
    # Point 'run_dir' to where you want logs and outputs to live.
    # Provide 'db_path' to enable a searchable DuckDB of all your "Studies".
    tracker = Tracker(
        run_dir=example_workspace / "runs",
        db_path=example_workspace / "provenance.duckdb",
        project_root=Path(__file__).resolve().parents[2],
        mounts={"inputs": str(inputs_dir), "outputs": str(outputs_dir)},
    )

    # 2. Define a Scenario (or "Study")
    # This groups related runs (e.g., Baseline vs. Policy Alternative).
    with tracker.scenario("seattle_2040_transport_study") as sc:
        print(f"Starting scenario {sc.name}")

        # --- Run 1: Baseline ---
        with sc.trace(
            name="baseline_2040",
            run_id="baseline_2040",
            model="activitysim",
            tags=["baseline", "official"],
            year=2040,
            config={"pop_growth": 1.05, "network": "existing"},
            facet_from=["pop_growth", "network"],
            inputs={"land_use": inputs_dir / "land_use.csv"},
        ):
            print("Running Baseline Simulation...")

            # --- PSEUDO-CODE: Run your model here ---
            # os.system("activitysim run -c ./configs/baseline")
            # ----------------------------------------

            # Prefer managed output helpers over hand-built output folders.
            # This keeps run outputs deterministic and scoped to the active run.
            baseline_summary = output_path("baseline/final_summary", ext="csv")
            baseline_summary.write_text(
                "metric,value\nvmt,123456.0\n", encoding="utf-8"
            )
            tracker.log_output(baseline_summary, key="final_vmt")
            print("Baseline Complete.")

        # --- Run 2: Policy Alternative (e.g., New Light Rail) ---
        with sc.trace(
            name="policy_rail_2040",
            run_id="policy_rail_2040",
            model="activitysim",
            tags=["policy", "light_rail"],
            year=2040,
            config={"pop_growth": 1.05, "network": "rail_expansion_v2"},
            facet_from=["pop_growth", "network"],
            inputs={
                "land_use": inputs_dir / "land_use.csv",
                "network": inputs_dir / "rail_network_v2.gpkg",
            },
        ):
            print("Running Policy Alternative Simulation...")

            # --- PSEUDO-CODE: Run your model here ---
            # os.system("activitysim run -c ./configs/rail_policy")
            # ----------------------------------------

            policy_summary = output_path("policy_rail/final_summary", ext="csv")
            policy_summary.write_text("metric,value\nvmt,111111.0\n", encoding="utf-8")
            tracker.log_output(policy_summary, key="final_vmt")
            print("Policy Run Complete.")

    # 3. Post-Run Insight
    # After the study is done, summarize run-level metadata/facets.
    results = tracker.run_set(model="activitysim", status="completed").to_frame()
    summary_cols = ["run_id", "status", "model", "year", "network", "pop_growth"]
    present_cols = [col for col in summary_cols if col in results.columns]
    print("\nStudy Results Summary:")
    print(results[present_cols])

    # 4. Multi-run analysis with RunSet
    # Build with both constructor styles: RunSet.from_query(...) and tracker.run_set(...).
    all_runs = consist.RunSet.from_query(
        tracker,
        label="activitysim-runs",
        model="activitysim",
        status="completed",
    )
    policy_runs = (
        tracker.run_set(label="policy-runs", model="activitysim", status="completed")
        .filter(run_id="policy_rail_2040")
        .latest(group_by=["model"])
    )
    baseline_runs = all_runs.filter(run_id="baseline_2040").latest(group_by=["model"])

    by_run_id = all_runs.split_by("run_id")
    print(
        "Runs by run_id:",
        {run_id: len(runset) for run_id, runset in by_run_id.items()},
    )

    aligned = baseline_runs.align(policy_runs, on="model")
    print("\nAligned baseline/policy runs:")
    print(aligned.to_frame())

    print("\nConfig changes across aligned runs:")
    print(aligned.config_diffs().query("status != 'same'"))

    def compare_final_vmt(left_run, right_run, model_name):
        left_vmt = float(
            tracker.get_run_outputs(left_run.id)["final_vmt"].as_df().loc[0, "value"]
        )
        right_vmt = float(
            tracker.get_run_outputs(right_run.id)["final_vmt"].as_df().loc[0, "value"]
        )
        return pd.DataFrame(
            [
                {
                    "model": model_name,
                    "baseline_vmt": left_vmt,
                    "policy_vmt": right_vmt,
                    "delta_vmt": right_vmt - left_vmt,
                }
            ]
        )

    print("\nAligned pair metrics (AlignedPair.apply):")
    print(aligned.apply(compare_final_vmt))


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------------
# HOW TO USE THE CLI:
# After running this script, try these commands in your terminal:
#
# 1. List all runs:
#    consist runs
#
# 2. Inspect one run and its outputs:
#    consist show baseline_2040
#    consist artifacts baseline_2040
#
# 3. Explore interactively:
#    consist shell
#    (consist) runs
#    (consist) artifacts #1
#    (consist) preview @1
# ---------------------------------------------------------------------------------
