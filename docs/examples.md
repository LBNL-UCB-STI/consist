# Examples

Start with the quickstart notebook, then move into the two tutorial flows.
Each notebook is runnable top-to-bottom and writes its outputs under
`examples/runs/`.

## Quickstart

**Quickstart**: [00_quickstart.ipynb](../examples/00_quickstart.ipynb)

- Shortest "happy path" walkthrough.
- Basic runs, artifacts, and simple queries.

## Tutorial series

**Parameter sweep**: [01_parameter_sweep_monte_carlo.ipynb](../examples/01_parameter_sweep_monte_carlo.ipynb)

- Parameter sweep with provenance.
- Schema export to SQLModel stubs.
- Typed, cross-run analysis via views.

**Iterative workflow**: [02_iterative_workflows.ipynb](../examples/02_iterative_workflows.ipynb)

- Scenario workflows with feedback loops.
- Provenance queries across iterations.

**Transportation workflow (detailed)**: [03_transportation_demand_modeling.ipynb](../examples/03_transportation_demand_modeling.ipynb)

- End-to-end modeling pipeline example.
- Scenario comparison with cached steps.

## Notes

- For an overview of notebook structure and helper modules, see
  [examples/README.md](../examples/README.md).
- The notebooks require `pip install -e ".[dev]"` and Jupyter.
