# Example Gallery

Examples are grouped into two tiers:

- **Essentials**: onboarding notebooks/scripts using the preferred execution path
  (`Tracker.run`, `Tracker.trace`, `scenario`)
- **Advanced**: larger workflows and integration templates that still use the
  same execution patterns, but add scale and system complexity

## Essentials

| Example                                                                                         |                                                                                                                                                         Quick Links                                                                                                                                                         | Key Learning Outcomes                                                                                  |
|:------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------|
| **00 Quickstart**<br>A 5-minute introduction to the core API.                                   |                  [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/00_quickstart.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/00_quickstart.ipynb)                  | • Create a tracked run<br>• Observe cache hits on repeated runs<br>• Query run history                 |
| **01 Monte Carlo Sweeps**<br>Preferred `scenario + tracker.run/trace` usage at moderate scale. | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/01_parameter_sweep_monte_carlo.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/01_parameter_sweep_monte_carlo.ipynb) | • Parameter sweeps with provenance<br>• Mixed `run` and `trace` step styles<br>• Hybrid query workflow |

## Advanced

| Example                                                               |                                                                                                                                                            Quick Links                                                                                                                                                            | Key Learning Outcomes                                                                                                |
|:----------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------|
| **02 Iterative Workflows**<br>Scenario workflows with feedback loops. |            [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/02_iterative_workflows.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/02_iterative_workflows.ipynb)            | • Iterative scenario loops<br>• Cache hydration choices across iterations<br>• Provenance queries for extension runs |
| **03 Demand Modeling**<br>End-to-end transportation simulation.       | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/03_transportation_demand_modeling.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/03_transportation_demand_modeling.ipynb) | • Multi-step model pipelines<br>• Scenario comparison and lineage tracing<br>• Matrix-style downstream analysis      |

For non-preferred lifecycle/decorator APIs, see
[Advanced Usage](advanced/index.md), especially
[Manual Lifecycle and Decorators](advanced/manual-lifecycle-and-decorators.md).

## Installation

```bash
pip install consist
```

If you are running notebooks and need notebook tooling:

```bash
pip install "consist[examples]"
```

For notebook/module layout details, see
[examples/README.md](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/README.md).
