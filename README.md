<p align="center">
  <img src="docs/assets/logo.png" alt="Consist" width="320">
</p>

<p align="center">
  <a href="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml">
    <img src="https://github.com/LBNL-UCB-STI/consist/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://doi.org/10.21105/joss.11131"><img src="https://joss.theoj.org/papers/10.21105/joss.11131/status.svg" alt="DOI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BSD--3--Clause-blue.svg" alt="License BSD 3-Clause"></a>
</p>

**Consist** is a caching and provenance layer for scientific simulation
workflows. It records the code, configuration, input data, and output artifacts
behind each run so expensive steps can be skipped safely and results remain
queryable after the fact.

Consist is useful when a workflow has:

- long-running model steps that should cache-hit when inputs are unchanged;
- scenario variants that need explicit lineage and comparison;
- file-based tools that need stable local paths but still need canonical
  provenance;
- post-run questions like "which config produced this output?"

## Installation

```bash
pip install consist
```

Optional integrations are installed as extras:

```bash
pip install "consist[git]"
pip install "consist[ingest]"
pip install "consist[docker]"
pip install "consist[ibis]"
```

> [!NOTE]
> Consist is pre-`1.0`. It is ready for real workflows, but minor releases may
> still include breaking changes while the API settles.

## Quick Example

> **You give inputs and outputs *names*. Consist gives your function *paths* to read and write. Cache hits depend on *what the files contain*, not where they are.**

```python
from pathlib import Path

import pandas as pd

from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def clean_data(raw: Path, cleaned: Path, threshold: float = 0.5) -> None:
    df = pd.read_parquet(raw)
    df[df["value"] > threshold].to_parquet(cleaned, index=False)


first = tracker.run(
    fn=clean_data,
    inputs={"raw": "raw.parquet"},
    outputs={"cleaned": "cleaned.parquet"},
    config={"threshold": 0.5},
)

second = tracker.run(
    fn=clean_data,
    inputs={"raw": "raw.parquet"},
    outputs={"cleaned": "cleaned.parquet"},
    config={"threshold": 0.5},
)

print(first.cache_hit, second.cache_hit)  # False, True

cleaned = second.outputs["cleaned"]
print(cleaned.path)  # Path(".../cleaned.parquet")
df = cleaned.as_df()  # loads the DataFrame directly


# Chain into the next step by passing the artifact handle as an input:
def summarize(cleaned: Path, summary: Path) -> None:
    df = pd.read_parquet(cleaned)
    df.describe().to_csv(summary)


report = tracker.run(
    fn=summarize,
    inputs={"cleaned": first.outputs["cleaned"]},
    outputs={"summary": "summary.csv"},
)
```

Because `raw`, `cleaned`, and `summary` are annotated with `: Path`, Consist
automatically passes resolved local `Path` objects into your function, verifies
that declared output files were written when the step finishes, and fingerprints
every input and output for cache identity and lineage. For tools that need
inputs staged to specific local filenames, see
[Usage Guide](docs/usage-guide.md#path-staging-example).

### Tracking CLI Commands

For workflows that run from the shell, such as compiled binaries, R or Julia scripts,
or legacy pipelines, initialize a workspace with `consist init` and prefix commands
with `consist exec`. Consist records input and output hashes, sidecar logs, and
run-over-run diffs without modifying the underlying tool:

```bash
consist init

consist exec \
  --name traffic_assignment \
  --input network=inputs/network.xml \
  --input config=configs/calibration.yaml \
  --output link_flows=outputs/link_flows.parquet \
  -- ./bin/run_traffic_sim --config configs/calibration.yaml --threads 8
```

## Documentation

| Start here | Use it for |
|:--|:--|
| [Quickstart](docs/getting-started/quickstart.md) | First tracked run and cache hit |
| [First Workflow](docs/getting-started/first-workflow.md) | Two-step pipeline with explicit artifact links |
| [Upgrading to 0.5](docs/migrating-to-0.5.md) | Database migration (`consist migrate`) and 0.5 API updates |
| [Usage Guide](docs/usage-guide.md) | Choosing between `run`, `trace`, and `scenario` |
| [Caching & Hydration](docs/concepts/caching-and-hydration.md) | Cache identity, hit behavior, and output recovery concepts |
| [Historical Recovery](docs/guides/historical-recovery.md) | Restoring archived outputs and staging inputs |
| [CLI Reference](docs/cli-reference.md) | Inspecting runs, artifacts, lineage, and schemas |
| [API Reference](docs/api/index.md) | Public Python API and generated signatures |

## Etymology

In railroad terminology, a **consist** is the lineup of locomotives and cars
that make up a train. In this library, a consist is the immutable record of the
code, config, inputs, and outputs coupled together to produce a result.
