# Example Gallery

Explore real-world applications of **Consist** through these interactive notebooks. Each notebook is runnable top-to-bottom and demonstrates specific core features.

| Example                                                                 |                                                                                                                                                            Quick Links                                                                                                                                                            | Key Learning Outcomes                                                                                                |
|:------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------|
| **00 Quickstart**<br>A 5-minute introduction to the core API.           |                     [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/00_quickstart.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/00_quickstart.ipynb)                     | • Create a tracked run<br>• Log artifacts<br>• Query run history                                                     |
| **01 Monte Carlo Sweeps**<br>Running large scale parameter exploration. |    [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/01_parameter_sweep_monte_carlo.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/01_parameter_sweep_monte_carlo.ipynb)    | • Run parameter sweeps with provenance<br>• Export schemas to SQLModel stubs<br>• Query across runs with typed views |
| **02 Iterative Workflows**<br>Scenario workflows with feedback loops.   |            [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/02_iterative_workflows.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/02_iterative_workflows.ipynb)            | • Build scenarios with feedback loops<br>• Query provenance across iterations<br>• Use couplers to pass artifacts    |
| **03 Demand Modeling**<br>End-to-end transportation simulation.         | [📖 View on GitHub](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/03_transportation_demand_modeling.ipynb)<br>[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LBNL-UCB-STI/consist/blob/main/examples/03_transportation_demand_modeling.ipynb) | • Multi-step modeling pipelines<br>• Compare scenario intermediate steps<br>• Trace lineage through workflows        |

---

### Installation & PyPI Status

> **Note:** We are currently preparing our initial PyPI release! In the meantime, you can install Consist directly from GitHub. This is also how the "Open in Colab" links below handle the installation.

To run these notebooks locally or in a cloud environment:

```bash
# Install from GitHub (Recommended until PyPI release)
pip install git+https://github.com/LBNL-UCB-STI/consist.git

# For local development
pip install -e ".[dev]"
```

For more details on notebook structure and helper modules, see the [examples/README.md](https://github.com/LBNL-UCB-STI/consist/blob/main/examples/README.md).
