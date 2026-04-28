# Container Integration Guide

Consist can run Docker or Singularity commands while recording the container
image, command, inputs, outputs, environment hash, and mount layout used for
cache identity.

Use `run_container(...)` for external tools that are easiest to package as a
container. For normal Python workflow code, prefer `tracker.run(...)`,
`tracker.trace(...)`, or `consist.scenario(...)`.

## When to Use Containers

| Use container execution | Prefer native Consist execution |
| --- | --- |
| Existing command-line tools such as BEAM, ActivitySim, SUMO, R scripts, or compiled models | Python functions you control |
| Dependencies are hard to reproduce locally | Fast iterative development |
| The tool requires a fixed filesystem layout | Fine-grained function-level caching |
| You need to preserve the runtime image as provenance | The runtime is already your project environment |

Containers are a coarse workflow boundary. Keep the container step large enough
that startup overhead is not dominant, and pass every cache-relevant config or
input file through `inputs=`.

## Minimal Docker Example

```python
from pathlib import Path

from consist import Tracker
from consist.integrations.containers import run_container

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    mounts={
        "data": str(Path("data").resolve()),
        "runs": str(Path("runs").resolve()),
    },
)

result = run_container(
    tracker=tracker,
    run_id="model_step",
    image="my-org/my-model:v1.0",
    command=["python", "process.py", "--input", "/inputs/input.csv"],
    volumes={
        str(Path("data").resolve()): "/inputs",
        str(Path("runs/model_step").resolve()): "/outputs",
    },
    inputs=[Path(tracker.mounts["data"]) / "input.csv"],
    outputs=[Path(tracker.mounts["runs"]) / "model_step/result.csv"],
    backend_type="docker",
)

if result.cache_hit:
    print(f"hydrated from {result.cache_source}")
```

The host paths can differ by machine. The container paths (`/inputs`,
`/outputs`) are part of the command contract and should stay stable.

## Minimal Singularity Example

On clusters, keep `.sif` files on a shared filesystem visible from all compute
nodes:

```bash
singularity pull /project/containers/my_model_v1.0.sif docker://my-org/my-model:v1.0
```

Then run through the same API with tracker mounts for the shared input root and
scratch output root:

```python
tracker = Tracker(
    run_dir="/scratch/my-run/outputs",
    db_path="/project/provenance.duckdb",
    mounts={
        "inputs": "/project/shared/inputs",
        "outputs": "/scratch/my-run/outputs",
    },
)

result = run_container(
    tracker=tracker,
    run_id="hpc_model_step",
    image="/project/containers/my_model_v1.0.sif",
    command=["python", "process.py", "--input", "/inputs/input.csv"],
    volumes={
        "/project/shared/inputs": "/inputs",
        "/scratch/my-run/outputs": "/outputs",
    },
    inputs=[Path("/project/shared/inputs/input.csv")],
    outputs=[Path("/scratch/my-run/outputs/result.csv")],
    backend_type="singularity",
)
```

You can also use registry URIs directly when the cluster's Singularity runtime
supports them:

```python
result = run_container(
    tracker=tracker,
    run_id="hpc_model_step",
    image="docker://my-org/my-model:v1.0",
    command=["python", "process.py"],
    volumes={"/project/inputs": "/inputs", "/scratch/outputs": "/outputs"},
    inputs=[Path("/project/inputs/input.csv")],
    outputs=[Path("/scratch/outputs/result.csv")],
    backend_type="singularity",
    strict_mounts=False,  # or configure tracker mounts for these roots
)
```

## Cache Identity

Consist builds the container cache key from deterministic runtime identity:

| Component | In cache key | Notes |
| --- | --- | --- |
| Image identity | Yes | Docker image digest when resolved; local `.sif` path or registry URI for Singularity |
| Command | Yes | Exact argv values |
| Environment | Yes | Stored as a deterministic hash, not raw values |
| Container mount paths | Yes | For example `/inputs`, `/outputs` |
| Host mount paths | No | Host-specific paths are excluded |
| Input content | Yes | Paths passed in `inputs=` are hashed |

On a cache hit, `run_container(...)` does not execute the container. It
materializes the requested output files at the requested host paths and returns
the prior run as `result.cache_source`.

### Docker Image Tags

Pin image versions. If you use mutable tags such as `latest`, cache behavior is
only as good as the resolved image identity. Use `pull_latest=True` only when you
want Consist to re-check the registry for a changed digest.

### Local `.sif` Files

For local Singularity images, the path is the image identity. Do not overwrite a
`.sif` in place:

```text
/project/containers/my_model_v1.0.sif  # good
/project/containers/my_model_v1.1.sif  # good
/project/containers/my_model.sif       # risky if overwritten
```

Replacing `/project/containers/my_model.sif` with new bytes at the same path can
reuse the old cache entry because the path string did not change.

## Mounts

Keep three path layers separate:

| Layer | Example | Purpose |
| --- | --- | --- |
| Host path | `/project/inputs` | Real local or cluster path |
| Container path | `/inputs` | Path the command sees |
| Consist mount name | `inputs` | Optional portable alias in `Tracker(mounts=...)` |

Use stable container paths and include cache-relevant files in `inputs=`:

```python
tracker = Tracker(
    run_dir="/scratch/consist-runs",
    db_path="/project/provenance.duckdb",
    mounts={
        "inputs": "/project/inputs",
        "runs": "/scratch/consist-runs",
    },
)

inputs_root = Path(tracker.mounts["inputs"])
runs_root = Path(tracker.mounts["runs"])

result = run_container(
    tracker=tracker,
    run_id="activitysim_baseline",
    image="my-org/activitysim:v1.0",
    command=[
        "python",
        "-m",
        "activitysim.core.workflow",
        "-c",
        "/configs",
        "-o",
        "/outputs",
    ],
    volumes={
        str(inputs_root / "configs"): "/configs",
        str(runs_root / "activitysim_baseline"): "/outputs",
    },
    inputs=[
        inputs_root / "configs/settings.yaml",
        inputs_root / "configs/tour_mode_choice.yaml",
    ],
    outputs=[runs_root / "activitysim_baseline/final_trips.csv"],
    backend_type="docker",
)
```

By default, Consist expects host paths to live under configured mounts or
`tracker.run_dir`. Use `strict_mounts=False` or `allow_external_paths=True` only
when the workflow genuinely needs paths outside those roots.

## Outputs

`outputs=` can be a list of paths or a mapping from artifact key to path:

```python
run_output = Path(tracker.mounts["runs"]) / "model_step"

outputs=[run_output / "result.csv"]

outputs={
    "result": run_output / "result.csv",
    "diagnostics": run_output / "log.txt",
}
```

Consist scans host output paths after the container exits. If the tool writes to
a different container path than the one mounted to the expected host location,
the output will not be found.

## Environment Variables

Environment variables are included in the cache key by hash:

```python
result = run_container(
    tracker=tracker,
    run_id="model_step",
    image="my-org/my-model:v1.0",
    command=["python", "process.py"],
    volumes={
        str(Path(tracker.mounts["data"]).resolve()): "/inputs",
        str(Path(tracker.mounts["runs"]).resolve() / "model_step"): "/outputs",
    },
    inputs=[Path(tracker.mounts["data"]) / "input.csv"],
    outputs=[Path(tracker.mounts["runs"]) / "model_step/result.csv"],
    environment={"MODEL_YEAR": "2030"},
    backend_type="docker",
)
```

Consist does not persist raw environment values in run metadata. Put
reproducibility-critical settings in files and pass those files through
`inputs=`.

## Top Gotchas

1. **Mounted config is not enough for cache invalidation.** If a file matters,
   include it in `inputs=`.
2. **Host paths do not identify the cache entry.** Moving the same inputs to a
   different machine should not invalidate the run.
3. **Container paths do identify the cache entry.** Changing `/inputs` to
   `/data` changes the command/runtime contract.
4. **Do not overwrite local `.sif` images in place.** Use versioned filenames.
5. **Avoid mutable image tags for production runs.** Prefer immutable tags or
   digest-aware pulls.
6. **Expected outputs must be written through mounted paths.** A file created
   only inside the container filesystem disappears when the container exits.
7. **Batch tiny work.** Container startup overhead makes many one-row or
   one-file invocations expensive.

## Troubleshooting Checklist

- Run the equivalent `docker run` or `singularity run` command manually.
- Confirm every host input path exists before the container starts.
- Confirm every output directory is writable by the container process.
- Check that the command uses container paths, not host paths.
- Confirm changed config files are listed in `inputs=`.
- For registry images, verify authentication with `docker pull` or the cluster's
  Singularity pull mechanism.

## API Links

- [Containers API Reference](integrations/containers.md)
- [Mounts and Portability](mounts-and-portability.md)
- [Caching and Hydration](concepts/caching-and-hydration.md)
- [Config Adapters](integrations/config_adapters.md)
