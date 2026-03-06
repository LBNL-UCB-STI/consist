# Examples

This directory supports the notebook examples under `examples/` and keeps helper
modules importable for local runs and Colab.

Examples are tiered:

- **Essentials**: onboarding-first usage of `run`, `trace`, and `scenario`
- **Advanced**: larger workflows using the same execution styles at higher scale

## Essentials tier

- `../00_quickstart.ipynb`: shortest recommended path walkthrough.
- `../01_parameter_sweep_monte_carlo.ipynb`: moderate-scale sweep with
  `scenario.run(...)` + `scenario.trace(...)`.
- `scripts/research_pipeline_native.py`: compact script template for native
  Python workflows.

## Advanced tier

- `../02_iterative_workflows.ipynb`: iterative scenario loops and cache
  extension runs.
- `../03_transportation_demand_modeling.ipynb`: end-to-end transportation
  scenario analysis with multi-run comparison via `RunSet`.
- `../04_beam_core_demo.ipynb`: BEAM CORE-style linear workflow demo focused
  on provenance diff, lineage, cache behavior, and `RunSet` alignment patterns.
- `scripts/model_integrator_wrapper.py`: integration-style scenario template
  for wrapping external tools, including `RunSet`-based baseline/policy pairing.

For manual lifecycle/decorator APIs (`start_run`, `begin_run/end_run`,
`define_step`), see:
`docs/advanced/manual-lifecycle-and-decorators.md`.

## Helper modules

- `synth_simulation.py`: deterministic Monte Carlo simulation helpers.
- `pipeline_steps.py`: sweep registry and aggregation helpers.
- `checked_models.py`: checked-in SQLModel contracts for comparison with
  generated schema code.
- `travel_demand_functions.py`: transport model logic (skims/distribution/mode
  choice/assignment).
- `generated/`: target location for generated SQLModel modules.

## Running

1. Install editable dev dependencies:
   - `pip install -e ".[dev]"`
2. Launch Jupyter from repo root:
   - `jupyter lab`
3. Open one of the notebooks in `examples/` and run all cells.

## Outputs

Notebooks write run outputs and DuckDB databases under `examples/runs/`.
Generated SQLModel modules are written under `examples/src/generated/`.

## Conventions

- Generated code should stay in `examples/src/generated/` (or equivalent
  dedicated package in real projects).
- Prefer managed output helpers inside active runs:
  `consist.output_path(...)` or injected `ctx.output_path(...)`.

Concise vs explicit input wiring patterns:

```python
from pathlib import Path
import consist

# Concise: map all outputs from a previous run result.
all_links = consist.refs(previous_result)

# Explicit: select/rename only what downstream needs.
aliased_links = consist.refs(previous_result, {"population_df": "persons"})

# identity_inputs supports both forms.
identity_inputs = [Path("./configs/activitysim")]
identity_inputs = [("asim_config", Path("./configs/activitysim"))]
```
