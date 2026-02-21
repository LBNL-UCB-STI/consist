# Installation

## Prerequisites

- Python 3.11 or newer
- Git (for source installation and code version tracking)

## Install from GitHub (Current)

```bash
pip install git+https://github.com/LBNL-UCB-STI/consist.git
```

> We are currently preparing our initial PyPI release. Until then, install from
> GitHub.

### Optional Extras

If you have a local clone and want optional extras in editable mode:

```bash
git clone https://github.com/LBNL-UCB-STI/consist.git
cd consist
```

Install with DLT for data ingestion into DuckDB:

```bash
pip install -e ".[ingest]"
```

Install with example notebook dependencies:

```bash
pip install -e ".[examples]"
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

## Tutorial Prerequisites

Core installs include `pandas`, which is used throughout the getting-started
tutorials.

The [First Workflow](first-workflow.md) tutorial writes Parquet output via
`pandas.DataFrame.to_parquet(...)`, which requires a Parquet engine such as
`pyarrow`. From a local clone, install the optional Parquet extra before running
that tutorial:

```bash
pip install -e ".[parquet]"
```

## Next Steps

Proceed to the [Quickstart](quickstart.md) to run your first cached workflow.
