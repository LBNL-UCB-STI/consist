from pathlib import Path
from consist import Tracker, output_path

# ---------------------------------------------------------------------------------
# Minimal Template: Public Sector / Model Integrator
#
# Use this template if you are wrapping an existing simulation model
# (e.g., ActivitySim, SUMO, UrbanSim) to track its inputs and outputs.
# ---------------------------------------------------------------------------------


def main():
    # 1. Initialize the Tracker
    # Point 'run_dir' to where you want logs and outputs to live.
    # Provide 'db_path' to enable a searchable DuckDB of all your "Studies".
    tracker = Tracker(
        run_dir=Path("./runs"),
        db_path="provenance.duckdb",
        project_root=".",
        mounts={"inputs": "./data/raw_inputs", "outputs": "./runs/outputs"},
    )

    # 2. Define a Scenario (or "Study")
    # This groups related runs (e.g., Baseline vs. Policy Alternative).
    with tracker.scenario("seattle_2040_transport_study") as sc:
        print(f"Starting scenario {sc.name}")

        # --- Run 1: Baseline ---
        with tracker.start_run(
            run_id="baseline_2040",
            model="activitysim",
            tags=["baseline", "official"],
            year=2040,
            config={"pop_growth": 1.05, "network": "existing"},
        ):
            print("Running Baseline Simulation...")

            # Log your input files (using URI scheme for portability)
            tracker.log_artifact("inputs://land_use.csv", key="land_use")

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
        with tracker.start_run(
            run_id="policy_rail_2040",
            model="activitysim",
            tags=["policy", "light_rail"],
            year=2040,
            config={"pop_growth": 1.05, "network": "rail_expansion_v2"},
        ):
            print("Running Policy Alternative Simulation...")

            # Consist detects that 'land_use' is the same as the baseline
            tracker.log_artifact("inputs://land_use.csv", key="land_use")
            tracker.log_artifact("inputs://rail_network_v2.gpkg", key="network")

            # --- PSEUDO-CODE: Run your model here ---
            # os.system("activitysim run -c ./configs/rail_policy")
            # ----------------------------------------

            policy_summary = output_path("policy_rail/final_summary", ext="csv")
            policy_summary.write_text(
                "metric,value\nvmt,111111.0\n", encoding="utf-8"
            )
            tracker.log_output(policy_summary, key="final_vmt")
            print("Policy Run Complete.")

    # 3. Post-Run Insight
    # After the study is done, you can instantly compare key metrics across the scenario.
    results = tracker.queries.pivot_facets(model="activitysim")
    print("\nStudy Results Summary:")
    print(results)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------------
# HOW TO USE THE CLI:
# After running this script, try these commands in your terminal:
#
# 1. List all runs:
#    consist runs list
#
# 2. View the provenance of a specific artifact:
#    consist artifacts show <artifact_id>
#
# 3. Export the results to a CSV for GIS/Tableau:
#    consist query "SELECT * FROM run_config_kv" --format csv > results.csv
# ---------------------------------------------------------------------------------
