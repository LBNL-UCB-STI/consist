# Building a Domain Tracker

Consist works well as a direct API, but teams get better long-term ergonomics
by wrapping it in a domain-specific class. A domain tracker centralizes all
the wiring that would otherwise repeat at every callsite — tracker construction,
default identity inputs, output key conventions — and exposes domain verbs
instead of framework mechanics.

## When to Wrap vs Use Direct APIs

| Prefer | When |
|---|---|
| **Domain wrapper** | Shared codebase, repeated identity/runtime wiring, multiple callers of the same workflow |
| **Direct Consist API** | One-off analysis scripts, debugging, prototyping new step boundaries |

Start direct for an initial spike. Promote to a wrapper once callsites begin
repeating the same run wiring.

---

## Core Pattern

The core pattern is a class that:

1. Owns a `Tracker` internally
2. Exposes domain methods that call `self._tracker` methods directly
3. Optionally adds `__enter__` / `__exit__` later if it wants active-tracker ergonomics

```python
from pathlib import Path
from typing import Any

from consist import Tracker


class SimulationTracker:
    def __init__(self, *, workspace: Path) -> None:
        self.workspace = workspace
        self._tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )

    # --- Domain methods ---

    def run_model(self, *, name: str, config: dict, inputs: dict[str, Any]) -> Any:
        """Run one scenario with consistent provenance wiring."""
        return self._tracker.run(
            fn=my_model_fn,
            name=name,
            config=config,
            inputs=inputs,
            outputs=["results"],
        )

    def find_best(self, *, metric: str, k: int = 5):
        """Query top-k runs by a scalar metric stored in facets."""
        from sqlalchemy import text
        query = text("""
            SELECT run_id, value_num
            FROM global_tables.run_config_kv
            WHERE key = :metric
            ORDER BY value_num DESC
            LIMIT :k
        """)
        return self._tracker.run_query(query.params(metric=metric, k=k))
```

Usage:

```python
sim = SimulationTracker(workspace=Path("./work"))
result = sim.run_model(
    name="baseline",
    config={"sample_rate": 0.1, "year": 2030},
    inputs={"population": Path("data/population.parquet")},
)
top_runs = sim.find_best(metric="rmse", k=3)
```

This is the simplest wrapper shape and is enough for many teams. Direct
instance methods like `self._tracker.run(...)` and `self._tracker.run_query(...)`
do not require a global active-tracker context.

### Optional: context-managed wrapper

Add `__enter__` / `__exit__` when your wrapper wants to call top-level
`consist.*` helpers that resolve the active tracker implicitly, such as
`consist.output_path(...)`, `consist.log_output(...)`, or `consist.scenario(...)`.

```python
import consist


class SimulationTracker:
    def __init__(self, *, workspace: Path) -> None:
        self.workspace = workspace
        self._tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )
        self._cm = None

    def __enter__(self) -> "SimulationTracker":
        self._cm = consist.use_tracker(self._tracker)
        self._cm.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self._cm.__exit__(*exc)
```

That pattern is optional ergonomics, not a requirement for a domain wrapper.

---

## Centralizing Defaults

The wrapper is the right place to fix wiring that should not vary across
callsites: the model name, default identity inputs, output keys, adapter.

```python
class SimulationTracker:
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

    def __enter__(self) -> "SimulationTracker":
        self._cm = consist.use_tracker(self._tracker)
        self._cm.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self._cm.__exit__(*exc)

    def run_model(self, *, name: str, config: dict) -> Any:
        return self._tracker.run(
            fn=my_model_fn,
            name=name,
            model="my_model",                          # fixed
            config=config,
            identity_inputs=self._identity_inputs,     # fixed
            outputs=["results"],                       # fixed
            execution_options=ExecutionOptions(
                runtime_kwargs={"config_dir": self.config_dir}
            ),
        )
```

Callers supply domain inputs (`name`, `config`). The wrapper owns everything
else. One missed `identity_inputs` in a direct call is a stale cache;
the wrapper makes that impossible.

---

## Domain Verbs Over Framework Verbs

Name your methods after model concepts, not Consist operations:

| Framework call | Domain method |
|---|---|
| `tracker.run(fn=evaluate_fn, ...)` | `sim.evaluate(year=2030)` |
| `tracker.run(fn=calibrate_fn, ...)` | `sim.calibrate(overrides={"coeff": 0.8})` |
| `tracker.run_query(...)` | `sim.top_k_runs(metric="mode_share_error")` |

Inside those methods, call Consist primitives (`run`, `scenario`, `trace`)
and keep the wiring hidden from callers.

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
    """Domain tracker for a calibration workflow."""

    def __init__(self, *, workspace: Path, config_dir: Path) -> None:
        self.workspace = workspace
        self.config_dir = config_dir
        self._tracker = Tracker(
            run_dir=workspace / "runs",
            db_path=workspace / "provenance.duckdb",
        )
        self._identity_inputs = [("model_config", config_dir)]
        self._cm = None

    def __enter__(self) -> "CalibrationTracker":
        self._cm = consist.use_tracker(self._tracker)
        self._cm.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self._cm.__exit__(*exc)

    def evaluate(self, *, run_name: str, config: dict, output_dir: Path) -> Any:
        """Run a single evaluation with stable identity wiring."""
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
        """Evaluate multiple configurations with one domain method."""
        return {
            label: self.evaluate(
                run_name=f"sweep_{label}",
                config=config,
                output_dir=self.workspace / "outputs" / label,
            )
            for label, config in configs_by_label.items()
        }

    def query_top_k(self, *, metric: str, k: int = 5):
        """Return top-k runs ranked by a scalar metric."""
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

---

## See Also

- [Usage Guide](usage-guide.md)
- [Workflow Contexts API](api/workflow.md)
- [Config, Facets, and Identity Inputs](concepts/config-management.md)
- [ActivitySim Config Adapter](integrations/config_adapters_activitysim.md)
