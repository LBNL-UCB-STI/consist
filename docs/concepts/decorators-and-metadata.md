# Decorators & Metadata

This page explains how `@define_step` metadata, templates, and resolver defaults work
so you can reduce boilerplate without losing clarity or cache correctness.

---

## Decorator Defaults

`@define_step` lets you declare defaults once and reuse them across `scenario.run(...)`
or `tracker.run(...)` calls.

```python
@define_step(
    outputs=["analysis"],
    cache_hydration="outputs-all",
    name_template="{func_name}__y{year}__i{iteration}",
)
def analyze(population: pd.DataFrame, config: dict) -> pd.DataFrame:
    return run_model(config["year"], population)
```

### Precedence Rule

Call-site args always override decorator defaults:

```python
@define_step(outputs=["decorator"])
def step() -> None:
    return None

sc.run(step, outputs=["call_site"])
```

---

## Callable Metadata

Decorator values can be callables that resolve at runtime using a `StepContext`.

```python
@define_step(
    outputs=lambda ctx: [f"results_{ctx.year}"],
    name_template=lambda ctx: f"{ctx.func_name}__y{ctx.year}",
)
def simulate(year: int) -> None:
    ...

sc.run(simulate, year=2030)
```

Callable metadata can use:

- `year`, `iteration`, `phase`, `stage`
- `model`, `func_name`
- `settings` or `runtime_kwargs`

---

## Name Templates

Name templates help prevent collisions and keep run directories organized.

```python
@define_step(name_template="{func_name}__y{year}__phase_{phase}")
def analyze(population, year=2030, phase="demand"):
    ...
```

Scenario-level defaults apply to every step unless a decorator overrides them:

```python
with tracker.scenario("baseline", name_template="{func_name}_{year}") as sc:
    sc.run(preprocess, year=2030)  # name="preprocess_2030"
```

Missing fields are rendered as empty strings (no errors).

---

## Cache Invalidation Controls

Use these when the workflow semantics change but code/config do not.

- Global: `Tracker(cache_epoch=N)`
- Scenario: `tracker.scenario(..., cache_epoch=N)`
- Step: `@define_step(cache_version=N)`

See **[Caching & Hydration](caching-and-hydration.md)** for details.

---

## Schema Introspection

`collect_step_schema` derives a coupler schema from decorated steps.

```python
from consist.utils import collect_step_schema

schema = collect_step_schema(
    steps=[preprocess, analyze],
    settings=settings,
    extra_keys={"init/raw": "Initialization data"},
)
coupler.declare_outputs(*schema.keys(), description=schema)
```

If outputs are dynamic, provide `schema_outputs=[...]` in `@define_step` so schema
building stays deterministic.

---

## Artifact Key Registries

Use `ArtifactKeyRegistry` to centralize key names in larger workflows:

```python
from consist.utils import ArtifactKeyRegistry

class Keys(ArtifactKeyRegistry):
    RAW = "raw"
    PREPROCESSED = "preprocessed"
    ANALYSIS = "analysis"

sc.run(fn=preprocess, inputs={Keys.RAW: "raw.csv"}, outputs=[Keys.PREPROCESSED])
```
