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
| **01 BEAM CORE Demo**<br>A simple linear scenario workflow.                                     |          [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/01_beam_core_demo.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/01_beam_core_demo.ipynb)          | • Linear multi-step scenario<br>• Scenario diffs and lineage<br>• Path-bound input staging             |
| **02 Monte Carlo Sweeps**<br>Parallel `scenario.map_runs(...)` sweep with a wide lineage tree. | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/02_parameter_sweep_monte_carlo.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/02_parameter_sweep_monte_carlo.ipynb) | • Parallel parameter sweeps<br>• Parent scenario with many child runs<br>• Hybrid query workflow |
| **Artifact Recovery Script**<br>One-artifact archive and workspace restore. | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/scripts/artifact_recovery_materialization.py) | • Archive one artifact<br>• Recover bytes from `recovery_roots`<br>• Use `materialize_artifact(...)` |

## Advanced

| Example                                                               |                                                                                                                                                            Quick Links                                                                                                                                                            | Key Learning Outcomes                                                                                                |
|:----------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------|
| **03 Iterative Workflows**<br>Scenario workflows with feedback loops. |            [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/03_iterative_workflows.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/03_iterative_workflows.ipynb)            | • Iterative scenario loops<br>• Cache hydration choices across iterations<br>• Provenance queries for extension runs |
| **04 Demand Modeling**<br>End-to-end transportation simulation.       | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/04_transportation_demand_modeling.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/04_transportation_demand_modeling.ipynb) | • Multi-step model pipelines<br>• Scenario comparison and lineage tracing<br>• Matrix-style downstream analysis      |
| **05 GTFS Support**<br>Raw transit feeds, selected-service identity, and typed GTFS views. | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/05_gtfs_support.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/05_gtfs_support.ipynb) | • Discover raw GTFS feed members<br>• Compare weekday/weekend service slices<br>• Track model lineage from selected GTFS tables |

!!! tip "Transportation integrations"
    The demand modeling example pairs with the [ActivitySim Config Adapter](integrations/config_adapters_activitysim.md) and [BEAM Config Adapter](integrations/config_adapters_beam.md) guides for tracking file-based model configurations. The GTFS support example pairs with the [GTFS Support](integrations/gtfs.md) guide for transit feed canonicalization.

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
