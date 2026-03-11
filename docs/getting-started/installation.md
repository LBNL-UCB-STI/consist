# Installation

## Prerequisites

- Python 3.11 or newer
- Git (only needed for source installs)

## Install from PyPI (Recommended)

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

Install with Parquet support for the getting-started tutorials:

```bash
pip install "consist[parquet]"
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

## Install Latest GitHub `main`

Use this only if you specifically want unreleased changes:

```bash
pip install git+https://github.com/LBNL-UCB-STI/consist.git
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
`pyarrow`. Install the optional Parquet extra before running that tutorial:

```bash
pip install "consist[parquet]"
```

## Next Steps

Proceed to the [Quickstart](quickstart.md) to run your first cached workflow.
