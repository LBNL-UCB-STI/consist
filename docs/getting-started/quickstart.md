# Quickstart

Consist records the code, config, and input data behind each step of a
workflow, and skips a step when it has already run with the same ones.

```bash
pip install consist
```

## A step that runs once

Create some input data:

```bash
printf "value\n4\n9\n16\n" > input.csv
```

Save this as `quickstart.py`:

```python
from pathlib import Path

import pandas as pd
from consist import Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")


def summarize(data: Path, summary: Path) -> None:
    print("summarize is running")
    mean = pd.read_csv(data)["value"].mean()
    summary.write_text(f"mean={mean:.1f}\n")


result = tracker.run(
    fn=summarize,
    inputs={"data": "input.csv"},
    outputs={"summary": "summary.txt"},
)

print("cache hit:", result.cache_hit)
print(result.outputs["summary"].path.read_text())
```

Run it twice:

```console
$ python quickstart.py
summarize is running
cache hit: False
mean=9.7

$ python quickstart.py
cache hit: True
mean=9.7
```

The second time, `summarize` didn't run. Consist found an earlier run with
the same code and the same input contents and returned its output.

`summarize` contains no Consist code. The keys of `inputs` and `outputs`
match its parameter names, and Consist passes it a path for each: the input
file to read, and a location inside the run's own folder to write to.

## Change the input

```console
$ printf "value\n4\n9\n25\n" > input.csv
$ python quickstart.py
summarize is running
cache hit: False
mean=12.7
```

Now put the original data back:

```console
$ printf "value\n4\n9\n16\n" > input.csv
$ python quickstart.py
cache hit: True
mean=9.7
```

The file was rewritten, so its timestamp is new, but its contents match the
first run. Consist decides based on contents.

## Read outputs through the artifact

`result.outputs["summary"]` is an *artifact*: a handle to the output, not a
path you need to remember. Use `.path` to get a file you can read, or
`.as_df()` for tables. On a cache hit, `.path` points to wherever the
earlier run's output is now, so don't build paths into `runs/` yourself.

## Connect two steps

Pass an output artifact straight into the next step:

```python
def make_report(summary: Path, report: Path) -> None:
    report.write_text("# Report\n\n" + summary.read_text())


report_result = tracker.run(
    fn=make_report,
    inputs={"summary": result.outputs["summary"]},
    outputs={"report": "report.md"},
)
```

Consist records that `report.md` was made from `summary.txt`, which came from
`input.csv`. If you change `summarize` and it produces the same summary as
before, `make_report` is still skipped.

## Ephemeral Mode (`Tracker()`)

When you want to run the same `tracker.run(...)` or `tracker.scenario(...)` code
in unit tests or lightweight scripts without writing `provenance.duckdb`,
`consist.json`, or creating a `runs/` directory, instantiate `Tracker()` with no
arguments:

```python
from consist import Tracker

tracker = Tracker()  # Ephemeral mode: db_path=None, run_dir=None
```

In Ephemeral Mode, Consist uses lightweight file-stat fingerprints with
sub-millisecond overhead and leaves the filesystem untouched outside your step's
own outputs. To write `consist.json` without DuckDB (for example, on DB-less
worker nodes), pass `run_dir=...` with `db_path=None`.

## Next steps

- [How Consist Works](../architecture.md): what decides whether a step is
  skipped, and where files live
- [First Workflow](first-workflow.md): a multi-step pipeline with `scenario`
- [Usage Guide](../usage-guide.md): steps that should always run (`trace`),
  receiving DataFrames instead of paths, and placing inputs at fixed filenames
  for external tools
- [Upgrading to 0.5](../migrating-to-0.5.md): if you have a project from 0.4
