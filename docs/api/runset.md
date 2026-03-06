# RunSet and Alignment

`RunSet` is a fluent multi-run analysis primitive built on top of tracker
query APIs. Use it when you need to:

- Partition runs by a field/facet (`split_by`)
- Filter runs by mixed run-field/facet predicates (`filter`)
- Keep latest runs globally or per group (`latest`)
- Align two run collections for 1:1 comparison (`align`)

## Minimal example

```python
from consist import RunSet, Tracker

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

baseline = RunSet.from_query(tracker, label="baseline", scenario_id="base")
policy = RunSet.from_query(tracker, label="policy", scenario_id="policy")

pair = baseline.align(policy, on="year")
diffs = pair.config_diffs(namespace="beam")
```

## API reference

::: consist.runset.RunSet
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true
      members:
        - from_query
        - from_runs
        - split_by
        - filter
        - latest
        - align
        - to_frame
        - __iter__
        - __len__
        - __getitem__

::: consist.runset.AlignedPair
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true
      members:
        - pairs
        - apply
        - config_diffs
        - to_frame
