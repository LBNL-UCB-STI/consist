# Building a Domain Tracker

Consist works well as a direct API, but for teams that run the same model
repeatedly with different configurations, wrapping it in a thin class pays off.
The wrapper holds the tracker, the config directory, and any other fixed inputs
in one place — so individual run calls stay short and nothing important gets
accidentally omitted.

Use the direct API for one-off scripts or early exploration. Build a wrapper
when you find yourself copying the same setup across multiple scripts or run
calls.

---

## Why it matters for correctness

Every `tracker.run(...)` call that uses `identity_inputs` must include the right
config directory, or Consist will produce a stale cache hit without any error.
In a shared codebase with many contributors, that's easy to miss. A wrapper
fixes those inputs at construction time so individual callers can't forget them.

---

## Full Example

A calibration workflow with multiple run types and a query helper:

```python
from pathlib import Path
from typing import Any

import consist
from sqlalchemy import text
from consist import ExecutionOptions, Tracker


class CalibrationTracker:

    def __init__(self, *, workspace: Path, config_dir: Path) -> None:
        self.workspace = workspace
        self.config_dir = config_dir
        self._tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )
        # Fixed for all runs — callers cannot forget these
        self._identity_inputs = [("model_config", config_dir)]
        self._cm = None

    def __enter__(self) -> "CalibrationTracker":
        self._cm = consist.use_tracker(self._tracker)
        self._cm.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self._cm.__exit__(*exc)

    def evaluate(self, *, run_name: str, config: dict, output_dir: Path) -> Any:
        return self._tracker.run(
            fn=run_model,
            name=run_name,
            model="my_model_calibration",
            config=config,
            identity_inputs=self._identity_inputs,
            outputs=["persons", "trips", "metrics"],
            execution_options=ExecutionOptions(
                runtime_kwargs={
                    "config_dir": self.config_dir,
                    "output_dir": output_dir,
                }
            ),
        )

    def sweep(self, *, configs_by_label: dict[str, dict]) -> dict[str, Any]:
        return {
            label: self.evaluate(
                run_name=f"sweep_{label}",
                config=config,
                output_dir=self.workspace / "outputs" / label,
            )
            for label, config in configs_by_label.items()
        }

    def query_top_k(self, *, metric: str, k: int = 5):
        query = text("""
            SELECT run_id, value_num
            FROM global_tables.run_config_kv
            WHERE key = :metric
            ORDER BY value_num ASC
            LIMIT :k
        """)
        return self._tracker.run_query(query.params(metric=metric, k=k))
```

Usage:

```python
with CalibrationTracker(
    workspace=Path("./calibration_work"),
    config_dir=Path("./configs/my_model"),
) as cal:
    baseline = cal.evaluate(
        run_name="baseline",
        config={"sample_rate": 0.1, "mode_choice_coeff": 0.5},
        output_dir=Path("./outputs/baseline"),
    )
    sweep_results = cal.sweep(
        configs_by_label={
            "low_coeff":  {"sample_rate": 0.1, "mode_choice_coeff": 0.3},
            "high_coeff": {"sample_rate": 0.1, "mode_choice_coeff": 0.8},
        }
    )
    top_runs = cal.query_top_k(metric="mode_share_error", k=3)
```

Callers supply domain inputs (`run_name`, `config`, `output_dir`). The wrapper
owns tracker construction, identity inputs, model name, and output keys.

---

## Design Notes

**Name methods after model concepts.** `cal.evaluate(year=2030)` is clearer than
`tracker.run(fn=evaluate_fn, model="sim", year=2030, ...)` at every call site.
Inside the method, call Consist's `run`, `trace`, or `scenario` directly.

**`__enter__` / `__exit__` is optional.** It's only needed if you want
module-level `consist.*` helpers (e.g. `consist.log_output(...)`,
`consist.scenario(...)`) to find the tracker automatically. Direct calls to
`self._tracker.run(...)` work fine without it.

**Don't over-abstract early.** A single script that calls `tracker.run(...)`
a few times doesn't need a wrapper. The pattern earns its keep when multiple
scripts or team members all need the same fixed inputs.

---

## See Also

- [Usage Guide](usage-guide.md)
- [Workflow Contexts API](api/workflow.md)
- [Config, Facets, and Identity Inputs](concepts/config-management.md)
- [ActivitySim Config Adapter](integrations/config_adapters_activitysim.md)
