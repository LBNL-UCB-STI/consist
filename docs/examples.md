# Examples

Each notebook is runnable top-to-bottom and writes outputs under `examples/runs/`.

## Quickstart

**[00_quickstart.ipynb](../examples/00_quickstart.ipynb)**

| Learning outcome |
|-----------------|
| Create a tracked run |
| Log artifacts |
| Query run history |

---

## Tutorial Series

**[01_parameter_sweep_monte_carlo.ipynb](../examples/01_parameter_sweep_monte_carlo.ipynb)**

| Learning outcome |
|-----------------|
| Run parameter sweeps with provenance |
| Export schemas to SQLModel stubs |
| Query across runs with typed views |

---

**[02_iterative_workflows.ipynb](../examples/02_iterative_workflows.ipynb)**

| Learning outcome |
|-----------------|
| Build scenario workflows with feedback loops |
| Query provenance across iterations |
| Use couplers to pass artifacts between steps |

---

**[03_transportation_demand_modeling.ipynb](../examples/03_transportation_demand_modeling.ipynb)**

| Learning outcome |
|-----------------|
| Build end-to-end modeling pipelines |
| Compare scenarios with cached intermediate steps |
| Trace lineage through multi-step workflows |

---

## Prerequisites

```bash
pip install -e ".[dev]"
```

For notebook structure and helper modules, see [examples/README.md](../examples/README.md).
