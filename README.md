# Consist

**Provenance tracking, intelligent caching, and data virtualization for scientific simulation workflows.**

Consist automatically records what code, configuration, and input data produced each output in your pipeline. It uses this information to skip redundant computation and to let you query results across many runs without manual bookkeeping.

---

## Etymology

In railroad terminology, a **consist** (noun, pronounced CON-sist) is the specific lineup of locomotives and cars that make up a train. In this library, a **consist** is the immutable record of exactly which components—code version, configuration, input artifacts—were coupled together to produce a particular run's outputs.

---

## Why Consist?

Multi-model simulation workflows accumulate friction over time:

- **Provenance ambiguity.** Which configuration produced the results in Figure 3?
- **Redundant computation.** Re-running an entire pipeline because one downstream parameter changed.
- **Scattered outputs.** Writing one-off scripts to compare results across scenario variants.

Consist addresses these by treating provenance as a first-class concern. Each run is identified by a cryptographic hash of its inputs (code version, configuration, input artifact signatures). Matching signatures return cached results; differing signatures trigger execution and record new lineage.

---

## Installation

```bash
pip install consist
```

---

## Quick Example

```python
from consist import Tracker
from pydantic import BaseModel
from pathlib import Path

class CleaningConfig(BaseModel):
    threshold: float = 0.5

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

@tracker.task()
def clean_data(raw_file: Path, config: CleaningConfig) -> Path:
    output_path = Path("cleaned.parquet")
    # ... processing logic ...
    return output_path

# First call executes; second call with same inputs returns cached result
config = CleaningConfig(threshold=0.8)
result = clean_data(Path("raw.csv"), config)  # Returns an Artifact
result = clean_data(Path("raw.csv"), config)  # Cache hit!
```

Tasks return `Artifact` objects that wrap the output path with provenance metadata. Use `consist.load(artifact)` to read the data or `artifact.path` to access the file directly.

---

## Key Features

- **Signature-based caching**: Runs are keyed by `SHA256(code_hash | config_hash | input_hash)`. Changing any component invalidates only affected downstream runs.

- **Scenario workflows**: Group multi-step simulations under a parent run for easy comparison across variants.

- **Data virtualization**: Query artifacts across runs as SQL views without loading data into memory.

- **Container support**: Track Docker/Singularity containers as functions—image digest and mounts become part of the signature.

- **Ghost Mode**: Provenance remains valid even after intermediate files are deleted.

- **CLI tools**: Inspect runs, trace lineage, and validate artifacts from the command line.

---

## Documentation

- **[Usage Guide](docs/usage-guide.md)**: Detailed patterns for tasks, scenarios, and querying
- **[CLI Reference](docs/cli-reference.md)**: Command-line tools for inspecting provenance
- **[Architecture](docs/architecture.md)**: How caching and lineage tracking work

---

## Current Status

Consist is under active development and used in production for the [PILATES](https://github.com/ual/PILATES) integrated land use and transportation modeling framework.

We welcome feedback from researchers working with multi-model simulation workflows.

---

## License

[MIT License](LICENSE)