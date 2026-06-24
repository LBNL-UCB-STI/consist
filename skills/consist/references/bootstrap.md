# Bootstrap a New Project

## Contents

- Install and verify Consist
- Find the first boundary
- Start with a teaching slice
- Choose the initial tracker surface
- Wire the first run
- Verify the bootstrap

## Install And Verify Consist

- Install the package the way the project expects:
  - `pip install consist`
  - `pip install -e .` for a local source checkout
  - add an extras set only when the project needs it, such as
    `pip install "consist[ingest]"`
- Verify the import before editing the integration:
  - `python -c "import consist; print(consist.__file__)"`
- If the project already has a lockfile or dependency manager, use that
  project’s normal install path first, then verify the import again.

## Find The First Boundary

- Identify the application entrypoint, dependency manager, and test command.
- Find the first expensive, deterministic, or file-writing step worth
  tracking.
- Decide where `Tracker` should live:
  - app/script/notebook code: usually a local `Tracker` plus
    `use_tracker(...)`
  - reusable library or framework adapter: explicit `Tracker` ownership

## Start With A Teaching Slice

For legacy scientific pipelines, start with a small coherent teaching slice:
one or two upstream file-writing steps plus their immediate downstream
consumer. Prefer clear declared inputs, declared outputs, and inspection docs
over broad first-pass coverage. Leave later steps as documented future
extensions until the first slice is understandable and inspectable.

## Choose The Initial Tracker Surface

- Use `run(...)` for the first cacheable step.
- Use `trace(...)` when the code must always execute but still needs
  provenance.
- Use `scenario(...)` only after the first single-step instrumentation is
  working or when the project already has a clear multi-step workflow boundary.

## Wire The First Run

- Keep the first instrumentation small and concrete.
- Declare every real dependency in `inputs=...`, `config=...`, or
  `identity_inputs=[...]`.
- Use `ExecutionOptions(input_binding="paths")` for path-bound code.
- Use `consist.ref(...)` / `consist.refs(...)` for step-to-step handoff.
- Use `output_paths={...}` or `capture_dir=...` when the wrapped tool writes
  files itself.
- For container or subprocess boundaries, preserve the real file boundary in
  the wrapper instead of hiding it behind globals.

## Verify The Bootstrap

- Run the smallest example twice and confirm the second run cache-hits.
- If a path-bound step expects fixed filenames, verify the requested staging
  path on both cache miss and cache hit.
- Use the CLI to inspect what was recorded:
  - `consist runs`
  - `consist show <run_id>`
  - `consist artifacts <run_id>`
  - `consist preview <artifact-key>`
- If the first run does not cache-hit, inspect `cache_hydration`,
  `validate_cached_outputs`, `output_paths`, and the run identity summary
  before changing the wrapper shape.
