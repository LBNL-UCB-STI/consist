# Installation

## Prerequisites

- Python 3.11 or newer
- Git (for source installation and code version tracking)

## Install from PyPI

```bash
pip install consist
```

### Optional Extras

Install with DLT for data ingestion into DuckDB:

```bash
pip install "consist[ingest]"
```

Install with example notebook dependencies:

```bash
pip install "consist[examples]"
```

## Install from Source

Clone the repository and install in editable mode:

```bash
git clone https://github.com/LBNL-UCB-STI/consist.git
cd consist
pip install -e .
```

For development with ingestion support:

```bash
pip install -e ".[ingest]"
```

## Verify Installation

Confirm Consist is installed correctly:

```bash
python -c "import consist; print(consist.__file__)"
```

This prints the path to the installed package.

## Next Steps

Proceed to the [Quickstart](quickstart.md) to run your first cached workflow.
