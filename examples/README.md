# Examples

This directory contains end-to-end examples that demonstrate Consist’s preferred (“happy path”) workflow: provenance logging, DuckDB persistence, schema tracking, schema export to SQLModel, and typed downstream analysis.

## Scientific Simulation Example (Monte Carlo Predator–Prey Sweep)

The primary example (to be added as a Jupyter notebook) uses a simple **Monte Carlo predator–prey** simulation as a stand-in for a realistic scientific workflow. It’s designed to be understandable without domain background (it’s basically “rabbits vs foxes” dynamics with randomness).

- **Cold run**: generate a parameter sweep, then run one simulation per Consist step (so each sweep member has its own run metadata and artifacts).
- **Aggregate**: run a final step that combines per-simulation outputs into canonical analysis tables.
- **Ingest**: materialize selected tables into DuckDB and persist schema profiles.
- **Export schema**: generate SQLModel classes from a persisted artifact schema (stable, typed contract).
- **Warm analysis**: import the generated SQLModel module and run joins/analysis through typed views.
- **Re-run**: show that unchanged steps can be skipped/reused (caching/identity stability), then tweak one parameter to trigger recomputation.

The simulation is intentionally lightweight and deterministic (seeded), but produces artifacts that look like real scientific outputs: per-run metrics, time-series summaries, and a parameter registry for sweep membership.

## Files

- `01_predator_prey_end_to_end.ipynb` (planned): end-to-end narrative walkthrough.
- `synth_simulation.py`: deterministic Monte Carlo simulation + summarization helpers.
- `pipeline_steps.py`: “model steps” used by the notebook to keep it readable (registry creation, single-sim runner, aggregation helpers).
- `checked_models.py`: checked-in SQLModel “contract” definitions (PK/FK/index) to compare against generated stubs.
- `generated/`: target location for Consist-generated SQLModel modules.

## Running (once notebook exists)

1. Create a virtualenv and install Consist in editable mode:
   - `pip install -e ".[dev]"`
2. Launch Jupyter from the repo root:
   - `jupyter lab`
3. Open `examples/01_reaction_diffusion_end_to_end.ipynb` and run all cells.

## Outputs

The notebook writes outputs under `examples/runs/` and a DuckDB database file under `examples/` (exact names are defined in the notebook). Generated SQLModel code is written under `examples/generated/`.

## Notes / Conventions

- Generated code: keep Consist-generated SQLModel modules in `examples/generated/` (or, in real projects, a dedicated `your_pkg/consist_generated/` package) to make imports stable and reviewable.
- Artifacts: the example prefers Parquet for tabular outputs and uses small binary files (e.g., `.npz`) for raw simulation state snapshots when useful.
