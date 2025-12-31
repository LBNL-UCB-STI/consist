# Examples

This directory contains helper modules for the example notebooks in `examples/`. The notebooks demonstrate Consist’s preferred (“happy path”) workflow: provenance logging, DuckDB persistence, schema tracking, schema export to SQLModel, and typed downstream analysis.

## Scientific Simulation Example (Monte Carlo Predator–Prey Sweep)

The primary example (to be added as a Jupyter notebook) uses a simple **Monte Carlo predator–prey** simulation as a stand-in for a realistic scientific workflow. It’s designed to be understandable without domain background (it’s basically “rabbits vs foxes” dynamics with randomness).

- **Cold run**: generate a parameter sweep, then run one simulation per Consist step (so each sweep member has its own run metadata and artifacts).
- **Aggregate**: run a final step that combines per-simulation outputs into canonical analysis tables.
- **Ingest**: materialize selected tables into DuckDB and persist schema profiles.
- **Export schema**: generate SQLModel classes from a persisted artifact schema (stable, typed contract).
- **Warm analysis**: import the generated SQLModel module and run joins/analysis through typed views.
- **Re-run**: show that unchanged steps can be skipped/reused (caching/identity stability), then tweak one parameter to trigger recomputation.

The simulation is intentionally lightweight and deterministic (seeded), but produces artifacts that look like real scientific outputs: per-run metrics, time-series summaries, and a parameter registry for sweep membership.

## Transportation Example (Toy Travel Demand Feedback Loop)

This example demonstrates an iterative travel demand model with feedback between mode choice and congestion. It runs multiple iterations, logs intermediate artifacts, and compares scenario variants (e.g., higher parking costs) to highlight Consist's caching and provenance capabilities.

- **Initialize**: generate zones, population, distances, and initial skims.
- **Iterate**: compute logsums, distribute trips, compute utilities, choose modes, assign volumes, and update skims.
- **Summarize**: produce convergence plots, mode shares, and person-miles traveled (PMT) by mode.
- **Scenario comparison**: run a high-parking-cost scenario and compare outcomes to the baseline.
- **Incremental computation**: extend the scenario to more iterations and observe cache hits.

## Files

- `../01_predator_prey_end_to_end.ipynb`: end-to-end narrative walkthrough.
- `../02_iterative_workflows.ipynb`: iterative workflows and provenance chains (tutorial 2).
- `../03_transportation_demand_modeling.ipynb`: transportation demand modeling with Consist (tutorial 3).
- `synth_simulation.py`: deterministic Monte Carlo simulation + summarization helpers.
- `pipeline_steps.py`: “model steps” used by the notebook to keep it readable (registry creation, single-sim runner, aggregation helpers).
- `checked_models.py`: checked-in SQLModel “contract” definitions (PK/FK/index) to compare against generated stubs.
- `travel_demand_functions.py`: transport model logic (skims, distribution, mode choice, assignment).
- `generated/`: target location for Consist-generated SQLModel modules.

## Running

1. Create a virtualenv and install Consist in editable mode:
   - `pip install -e ".[dev]"`
2. Launch Jupyter from the repo root:
   - `jupyter lab`
3. Open `examples/01_predator_prey_end_to_end.ipynb`, `examples/02_iterative_workflows.ipynb`, or `examples/03_transportation_demand_modeling.ipynb` and run all cells.

## Outputs

The notebooks write outputs and DuckDB databases under `examples/runs/` (exact names are defined in each notebook). Generated SQLModel code is written under `examples/src/generated/`.

## Notes / Conventions

- Generated code: keep Consist-generated SQLModel modules in `examples/src/generated/` (or, in real projects, a dedicated `your_pkg/consist_generated/` package) to make imports stable and reviewable.
- Artifacts: the example prefers Parquet for tabular outputs and uses small binary files (e.g., `.npz`) for raw simulation state snapshots when useful.
