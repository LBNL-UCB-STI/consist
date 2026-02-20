# Building a Domain Tracker

Consist works well as a direct API, but teams usually get better long-term
ergonomics by wrapping it in a domain-specific class.

Use this guide when you are building a reusable model tool (for example,
calibration, policy testing, or scenario comparison) and want to expose
domain verbs instead of low-level run wiring.

## Wrapper Class Pattern

The core pattern is a wrapper class that owns a `Tracker`:

```python
from pathlib import Path
from typing import Any

import consist
from sqlalchemy import text
from consist import ExecutionOptions, Tracker
from consist.integrations.activitysim import ActivitySimConfigAdapter, ConfigOverrides


class CalibrationTracker:
    def __init__(self, *, workspace: Path, base_config_dir: Path) -> None:
        self.workspace = workspace
        self.base_config_dir = base_config_dir
        self.tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )
        self.adapter = ActivitySimConfigAdapter(root_dirs=[base_config_dir])
        self.identity_inputs = [("asim_config", base_config_dir)]

    def evaluate(self, *, run_name: str, output_dir: Path) -> Any:
        """Run a baseline evaluation with stable identity wiring."""
        with consist.use_tracker(self.tracker):
            return self.tracker.run(
                fn=run_activitysim,  # your domain callable
                name=run_name,
                model="activitysim_calibration",
                adapter=self.adapter,
                identity_inputs=self.identity_inputs,
                outputs=["persons", "trips", "metrics"],
                execution_options=ExecutionOptions(
                    runtime_kwargs={"config_dir": self.base_config_dir, "output_dir": output_dir}
                ),
            )

    def run_sweep(self, *, overrides_by_label: dict[str, ConfigOverrides]) -> dict[str, Any]:
        """Evaluate multiple override sets with one domain method."""
        results: dict[str, Any] = {}
        with consist.use_tracker(self.tracker):
            for label, overrides in overrides_by_label.items():
                results[label] = self.tracker.run_with_config_overrides(
                    adapter=self.adapter,
                    base_run_id="baseline_run_id",
                    overrides=overrides,
                    output_dir=self.workspace / "materialized",
                    fn=run_activitysim,
                    name=f"calibration_{label}",
                    model="activitysim_calibration",
                    outputs=["metrics"],
                    override_runtime_kwargs={"config_dir": "selected_root_dir"},
                    execution_options=ExecutionOptions(
                        runtime_kwargs={"output_dir": self.workspace / "asim_outputs" / label}
                    ),
                )
        return results

    def query_top_k(self, *, metric: str, k: int = 5):
        """Expose domain-level query helpers, not raw SQL plumbing."""
        query = text(
            """
        SELECT run_id, value_num
        FROM global_tables.run_config_kv
        WHERE key = :metric
        ORDER BY value_num DESC
        LIMIT :k
        """
        )
        return self.tracker.run_query(query.params(metric=metric, k=k))
```

The wrapper centralizes:

- tracker construction (`self.tracker = Tracker(...)`)
- adapter setup
- default `identity_inputs`
- runtime kwargs injection
- consistent output key conventions

## Domain Verbs Over Framework Verbs

Expose methods that match your model language:

- `evaluate(...)`
- `run_sweep(...)`
- `query_top_k(...)`

Inside those methods, call Consist primitives (`run`, `scenario`, `trace`,
`run_with_config_overrides`) and keep implementation details hidden.

This keeps downstream code focused on model intent, while still preserving full
provenance/caching/query capability.

## Hiding Adapter, Identity, and Runtime Wiring

A useful rule: callers should supply domain inputs, not Consist mechanics.

Good wrapper API:

```python
cal = CalibrationTracker(workspace=Path("./work"), base_config_dir=Path("./configs"))
result = cal.evaluate(run_name="baseline", output_dir=Path("./outputs/baseline"))
```

Leaky direct call shape (inside application code):

```python
tracker.run(
    fn=run_activitysim,
    adapter=adapter,
    identity_inputs=[("asim_config", config_dir)],
    execution_options=ExecutionOptions(runtime_kwargs={"config_dir": config_dir}),
    ...
)
```

Both are valid; wrapper style is easier to maintain across many callsites.

## When to Wrap vs Use Direct APIs

| Prefer | When |
|---|---|
| **Domain wrapper** | Shared codebase, repeated adapter/identity/runtime wiring, multiple users of the same model workflow |
| **Direct Consist API** | One-off analysis scripts, debugging, prototyping new step boundaries, low-level experimentation |

If you are onboarding a new project, start direct for a small spike, then
promote to a wrapper once callsites begin repeating the same run wiring.

## See Also

- [Usage Guide](usage-guide.md)
- [Workflow Contexts API](api/workflow.md)
- [ActivitySim Config Adapter](integrations/config_adapters_activitysim.md)
