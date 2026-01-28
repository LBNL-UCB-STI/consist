# Installation and Quickstart

Consist is not on PyPI yet. For now, install from source. When the first public
release is published, this page will include the PyPI command as well.

## Prerequisites

- Python 3.11 or newer
- git
- Optional: Docker or Singularity for container workflows
- Optional: Jupyter for running the example notebooks

## Install from source

```bash
git clone https://github.com/LBNL-UCB-STI/consist.git
cd consist
pip install -e .
```

Optional extras:

- Ingestion (DLT): `pip install -e ".[ingest]"`
- Notebooks: `pip install -e ".[examples]"`

## Install from PyPI

**NOT WORKING, DELETE WHEN PUBLIC:** Consist is not published to PyPI yet.

```bash
pip install consist
```

Optional extras:

- Ingestion (DLT): `pip install "consist[ingest]"`
- Notebooks: `pip install "consist[examples]"`

## 5-minute quickstart

This creates a small CSV, runs a tracked transformation, and loads the cached
result.

```python
from pathlib import Path

import pandas as pd
import consist
from consist import Tracker, use_tracker

# 1) Create a tracker (database + run directory)
tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

# 2) Prepare a tiny input file
Path("raw.csv").write_text("category,value\nA,1\nA,2\nB,3\n")

# 3) Define a function to run
def summarize(raw: pd.DataFrame) -> pd.DataFrame:
    return raw.groupby("category")["value"].sum().reset_index()

# 4) Run with Consist
with use_tracker(tracker):
    result = consist.run(
        fn=summarize,
        inputs={"raw": Path("raw.csv")},
        outputs=["summary"],
    )

# 5) Load the output
summary = consist.load_df(result.outputs["summary"])
print(summary)
```

Run the same script again and you should see a cache hit (no re-execution).

## Next steps

- Read the [Concepts](../concepts.md) overview for the mental model.
- Follow the [Usage Guide](../usage-guide.md) for scenarios, couplers, and workflows.
- Explore the [Example Notebooks](../examples.md) if you prefer a guided walkthrough.
