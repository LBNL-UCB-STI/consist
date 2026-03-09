# Container Integration Guide

Consist executes containerized tools (Docker, Singularity) with automatic provenance tracking and caching based on **image digest** and parameters.

!!! note "Image Digest"
    An image digest is the SHA256 hash of a container image's content (e.g., `sha256:a1b2c3...`). Unlike tags (`:latest`, `:v1.0`), digests are immutable—the same digest always refers to the same image bytes.

!!! note "Recommended path"
    `run_container(...)` is an integration-specific API for external tools. For
    Python workflow steps, the recommended path is `consist.run(...)`,
    `consist.trace(...)`, or `consist.scenario(...)`.

---

## When to Use Containers

| Use case | Recommendation |
|----------|----------------|
| Wrapping existing tools (ActivitySim, SUMO, BEAM, R scripts) | Container |
| Complex dependencies easier to package than install | Container |
| Black-box or non-deterministic tools | Container |
| Tool expects specific file paths | Container |
| Simple Python functions | `consist.run()` |
| Fine-grained step caching | `scenario()` + `consist.run()` |
| Development/debugging with fast iteration | Native execution |

---

## How Caching Works

Consist computes a container signature from:

| Component | Included in signature | Notes |
|-----------|----------------------|-------|
| Image digest | Yes | Resolved from registry if `pull_latest=True` |
| Command | Yes | Exact command string and arguments |
| Environment variables | Yes | Deterministic hash |
| Container mount paths | Yes | e.g., `/inputs`, `/outputs` |
| Host paths | **No** | Run-specific; not part of cache key |
| Input file content | Yes | SHA256 hashes |

If all components match a prior run, Consist returns cached outputs without executing the container.

!!! warning "Host paths are not cached"
    Volume host paths (e.g., `./data:/inputs`) are excluded from the cache key because they vary between runs. To ensure config changes invalidate cache, pass config files via `inputs=`.

---

## Basic Example: Single Container Run

```python
from consist import Tracker
from consist.integrations.containers import run_container
from pathlib import Path

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

# Create input data
input_path = Path("./data/input.csv")
input_path.parent.mkdir(exist_ok=True)
input_path.write_text("x,y\n1,2\n3,4\n")

# Execute container
result = run_container(
    tracker=tracker,
    run_id="my_model_run",
    image="my-org/my-model:v1.0",
    command=["python", "process.py", "--input", "/inputs/input.csv"],
    volumes={
        "./data": "/inputs",      # Host path → Container path
        "./outputs": "/outputs",
    },
    inputs=[input_path],          # Files to hash (for cache key)
    outputs=["./outputs/result.csv"],  # Files to capture as artifacts
    backend_type="docker",
)

# Access results
if result.cache_hit:
    print(f"Cache hit from {result.cache_source}")
else:
    print("Container executed")

for key, artifact in result.artifacts.items():
    print(f"Output: {key} → {artifact.path}")
```

**What happened:**

1. Consist created a signature from image + command + inputs
2. Checked if this signature exists in the database
3. If no prior run: executed the container, scanned outputs, logged them as artifacts
4. If prior run exists: copied cached artifacts to `./outputs/result.csv` (no container execution)

---

## ActivitySim Integration

ActivitySim is a commonly used transportation demand modeling tool. Here's how to integrate it with Consist:

### Setup

1. Create a Docker image with ActivitySim installed:

    ```dockerfile
    FROM python:3.11
    RUN pip install activitysim numpy pandas
    COPY . /workspace
    WORKDIR /workspace
    ```

    Build and push to registry:

    ```bash
    docker build -t my-org/activitysim:v1.0 .
    docker push my-org/activitysim:v1.0
    ```

2. Create ActivitySim config files (as usual) in a local directory:

    ```text
    ./configs/
    ├── settings.yaml
    ├── accessibility_coefficients.csv
    ├── mode_choice_coefficients.csv
    └── ...
    ```

### Running a Scenario

```python
from consist import Tracker
from consist.integrations.containers import run_container
from pathlib import Path

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def run_activitysim_scenario(scenario_name: str, configs_dir: Path):
    """Execute ActivitySim with Consist provenance."""

    # Ensure output directory exists
    output_dir = Path(f"./outputs/{scenario_name}")
    output_dir.mkdir(parents=True, exist_ok=True)

    result = run_container(
        tracker=tracker,
        run_id=f"activitysim_{scenario_name}",
        image="my-org/activitysim:v1.0",
        command=[
            "python", "-m", "activitysim.core.workflow",
            "-c", "/configs",
            "-o", f"/outputs/{scenario_name}",
        ],
        volumes={
            str(configs_dir): "/configs",
            "./outputs": "/outputs",
        },
        inputs=[configs_dir],  # Hash all configs
        outputs=[f"./outputs/{scenario_name}"],  # Capture all outputs
        environment={
            "ACTIVITYSIM_CHUNK_SIZE": "100000",  # Optional: tune performance
        },
        backend_type="docker",
    )

    return result

# Run baseline scenario
result_baseline = run_activitysim_scenario(
    "baseline",
    configs_dir=Path("./configs"),
)

# Run scenario with modified coefficients
# (New output directory and modified config) → new cache entry
result_modified = run_activitysim_scenario(
    "high_income_sensitivity",
    configs_dir=Path("./configs_modified"),
)
```

### Querying Results

After running scenarios, query outputs across runs:

```python
import pandas as pd
from pathlib import Path

# Find all ActivitySim outputs
for run_subdir in Path("./runs").glob("activitysim_*/"):
    output_artifacts = tracker.get_artifacts_for_run(run_subdir.name)
    for key, artifact in output_artifacts.outputs.items():
        if "summary" in key:
            df = pd.read_csv(artifact.path)
            print(f"Scenario: {run_subdir.name}")
            print(df.head())
```

Or use Consist's database queries:

```python
from sqlmodel import Session, select

with Session(tracker.engine) as session:
    # Find all ActivitySim runs
    runs = session.exec(
        select(Run).where(Run.model == "activitysim")
    ).all()

    for run in runs:
        artifacts = tracker.get_artifacts_for_run(run.id)
        print(f"Run: {run.id}, Outputs: {list(artifacts.outputs.keys())}")
```

---

## Singularity / Apptainer (HPC)

Docker is unavailable on most HPC clusters due to privilege requirements.
Singularity and its community fork Apptainer are the standard alternatives —
both run rootless and are accepted on shared compute resources. Consist supports
both with the same `run_container()` API; swap `backend_type="singularity"` for
`backend_type="apptainer"` depending on which is installed (`which apptainer`).

### Getting a `.sif` File

The typical HPC workflow is to pull a Docker image and convert it to a Singularity
Image File (`.sif`) on a machine where Docker is available, then transfer it to
a shared filesystem the cluster can read:

```bash
# Pull from Docker Hub and convert (run locally or on a build/login node)
singularity pull my_model.sif docker://my-org/my-model:v1.0

# Or build from a definition file
singularity build my_model.sif my_model.def
```

Place `.sif` files on a **shared filesystem** (e.g., `/project/containers/`) that
all compute nodes can access. A node-local path breaks caching across nodes because
the same file lives at different absolute paths.

### Running with Consist

```python
from pathlib import Path
from consist import Tracker
from consist.integrations.containers import run_container

tracker = Tracker(
    run_dir="/scratch/consist_runs",
    db_path="/project/provenance.duckdb",
    mounts={
        "inputs": "/project/shared/inputs",
        "runs": "/scratch/consist_runs",
    },
)

result = run_container(
    tracker=tracker,
    run_id="hpc_job",
    image="/project/containers/my_model.sif",  # shared filesystem path
    command=["python", "model.py", "--input", "/inputs/data.csv"],
    volumes={
        "/project/shared/inputs": "/inputs",
        "/scratch/outputs": "/outputs",
    },
    inputs=[Path("/project/shared/inputs/data.csv")],
    outputs=["/scratch/outputs/result.csv"],
    backend_type="singularity",  # or "apptainer"
)
```

### Cache Identity for Local `.sif` Files

For local `.sif` files, Consist uses the **absolute file path** as the cache
identity — not the file's content. The path string is what gets hashed into the
run signature.

Practical implications:

- Same `.sif` path, same content → cache hit ✓
- Same `.sif` path, **file replaced in place** → **cache hit** (path unchanged — stale result)
- New versioned filename (`my_model_v1.1.sif`) → cache miss ✓

!!! danger "Overwriting a `.sif` in place silently reuses the old cache"
    If you rebuild and overwrite `my_model.sif` at the same path, Consist sees
    the same path string and returns cached outputs from the previous image.
    **Always use versioned filenames** (`my_model_v1.0.sif`, `my_model_v1.1.sif`)
    and never overwrite an existing `.sif`. The version is the only signal Consist
    has that the image changed.

### Alternative: `docker://` URIs (Recommended for Most Cases)

Singularity and Apptainer can pull directly from Docker registries at runtime
using `docker://` URIs, without creating a local `.sif` file:

```python
result = run_container(
    tracker=tracker,
    run_id="hpc_job",
    image="docker://my-org/my-model:v1.0",  # pulled at runtime
    command=["python", "model.py"],
    volumes={"/scratch/inputs": "/inputs", "/scratch/outputs": "/outputs"},
    inputs=[Path("/scratch/inputs/data.csv")],
    outputs=["/scratch/outputs/result.csv"],
    backend_type="singularity",
)
```

The URI string (including the tag) becomes the cache identity. Bumping
`v1.0` → `v1.1` causes an automatic cache miss — no local file management
required. This avoids the overwrite hazard entirely and is the pattern used
in production PILATES workflows.

---

## Volume Mounting Best Practices

### Portable Mapping: `Tracker(mounts=...)` + container `volumes={...}`

For portable container runs across machines, keep container paths stable
(`"/inputs"`, `"/outputs"`) and map host paths from tracker mounts.

```python
from pathlib import Path
from consist import Tracker
from consist.integrations.containers import run_container

tracker = Tracker(
    run_dir="/data/project_scratch/consist_runs",
    db_path="./provenance.duckdb",
    mounts={
        "inputs": "/data/project_inputs",
        "runs": "/data/project_scratch/consist_runs",
    },
)

inputs_root = Path(tracker.mounts["inputs"]).resolve()
runs_root = Path(tracker.mounts["runs"]).resolve()

result = run_container(
    tracker=tracker,
    run_id="asim_baseline",
    image="my-org/activitysim:v1.0",
    command=["python", "-m", "activitysim.core.workflow", "-c", "/inputs", "-o", "/outputs"],
    volumes={
        str(inputs_root): "/inputs",   # host mount root -> stable container path
        str(runs_root): "/outputs",
    },
    inputs=[inputs_root / "settings.yaml", inputs_root / "households.csv"],
    outputs=[runs_root / "asim_baseline" / "final_trips.csv"],
    backend_type="docker",
)
```

Why this works:

- Different users can map the same mount names to different local paths.
- Container-internal paths stay stable, so command lines do not change.
- Requested outputs stay under `tracker.run_dir` (`runs_root`), so defaults work
  with `strict_mounts=True` and `allow_external_paths=False`.
- `strict_mounts=True` (default) enforces that host paths are under configured
  tracker mounts.

See [Mounts & Portability](mounts-and-portability.md) for mount semantics and
cross-machine path resolution.

### Mount Paths

Map host directories to container paths consistently:

```python
volumes = {
    "/host/absolute/path": "/container/path",  # Use absolute paths
    "./relative": "/container/relative",        # Or relative (resolved against cwd)
}
```

By default, Consist only allows host paths that live under configured mounts. If you need
to allow arbitrary absolute host paths, pass `strict_mounts=False` to `run_container()`.

To ensure config changes invalidate the cache, pass config files via `inputs`:

```python
result = run_container(
    ...
    volumes={
        "./config_dir": "/configs",
        "./data": "/data",
    },
    inputs=[Path("./config_dir/settings.yaml"), Path("./data/input.csv")],
    ...
)
```

### Handling Outputs

Outputs can be specified as:

- **List of paths** (logged with filename as key):
  ```python
  outputs=["./outputs/result.csv", "./outputs/logs.txt"]
  # Artifact keys: "result.csv", "logs.txt"
  ```

- **Dict mapping keys to paths** (custom artifact keys):
  ```python
  outputs={
      "main_result": "./outputs/result.csv",
      "diagnostics": "./outputs/logs.txt",
  }
  # Artifact keys: "main_result", "diagnostics"
  ```

Consist scans output directories on the **host** after container exits. Files created inside the container at mounted paths are detected automatically.

Outputs must live under `tracker.run_dir` or a configured mount unless
`allow_external_paths=True` (or `CONSIST_ALLOW_EXTERNAL_PATHS=1`) is set on the tracker.

**Warning:** If the container doesn't create files at the expected paths, Consist logs a warning but doesn't fail (use `lineage_mode="full"` to capture what was created).

---

## Environment Variables & Configuration

Pass environment variables to the container:

```python
result = run_container(
    ...
    environment={
        "MODEL_PARAM_1": "value1",
        "MODEL_PARAM_2": "value2",
        "DEBUG": "true",
    },
    ...
)
```

These variables are **part of the cache key**. Changing them invalidates the cache.

Consist does **not** persist raw environment values in run metadata or container
manifests. It stores a deterministic hash instead. If you need those values for
reproducibility, include them in config files and add those files to `inputs`.

For large configurations, mount config files instead of passing via environment:

```python
# DON'T do this for large configs:
environment={"CONFIG": json.dumps(huge_config)}  # Bad: unreadable, cache-unfriendly

# DO this:
volumes={"./config.json": "/app/config.json"}
inputs=[Path("./config.json")]
command=["python", "app.py", "--config", "/app/config.json"]
```

---

## Cache Behavior & Hydration

### Cache Hits

On a cache hit:

1. Consist finds a prior run with the same signature
2. **No container execution** occurs
3. Cached output files are **materialized (copied) to requested host output paths**
4. `result.cache_hit == True`, `result.cache_source` = prior run ID

```python
result = run_container(...)
if result.cache_hit:
    print(f"Cache hit from {result.cache_source}")
    # result.artifacts are materialized (files exist on disk)
```

!!! note "Container cache hits vs standard run hydration"
    `run_container(...)` materializes requested output files on cache hits so
    expected host output paths exist. This differs from default
    `consist.run(...)` behavior (`cache_hydration="metadata"`), which hydrates
    artifact metadata without copying bytes unless explicitly requested.

For byte-recovery and hydration policy details, see
[Mounts & Portability](mounts-and-portability.md) and
[Caching & Hydration](concepts/caching-and-hydration.md).

### Cache Invalidation

Cache is invalidated (new run executed) if any of these change:

- **Image**: `my-model:v1` → `my-model:v2` (or image pulled with different digest if `pull_latest=True`)
- **Command**: Arguments to the tool
- **Environment**: Any env var changes
- **Inputs**: Hash of input files changes
- **Volumes**: Container mount paths change

### Manual Cache Bypass

To force re-execution even with matching signature:

!!! note "Advanced lifecycle pattern"
    The `tracker.begin_run(...)`/`tracker.end_run()` wrapper below is low-level.
    Prefer the recommended path (`run`/`trace`/`scenario`) unless you need manual
    lifecycle control around container orchestration.

```python
# No built-in flag, but you can:
# 1. Change a trivial env var to force cache miss:
environment={"CACHE_BYPASS": str(time.time())}

# 2. Or use a cache_mode on the enclosing run:
tracker.begin_run(..., cache_mode="overwrite")
result = run_container(...)
tracker.end_run()
```

---

## Nested Containers (Inside Scenarios)

Use `run_container()` inside `tracker.scenario()` for multi-step workflows where
each container step caches independently.

=== "Without Consist"

    ```python
    import subprocess

    # Step 1: Preprocess
    subprocess.run([
        "singularity", "run",
        "--bind", "/scratch/raw_data:/data",
        "/containers/preprocess.sif",
        "python", "preprocess.py",
    ], check=True)

    # Step 2: Model — assumes Step 1 wrote to a known path
    subprocess.run([
        "singularity", "run",
        "--bind", "/project/configs:/configs",
        "--bind", "/scratch/model_outputs:/outputs",
        "/containers/activitysim.sif",
        "python", "-m", "activitysim.core.workflow",
    ], check=True)
    ```

    Both steps re-run every time. There is no record of which image version or
    config produced which outputs, and no way to skip a step whose inputs haven't
    changed.

=== "With Consist"

    ```python
    from pathlib import Path
    from consist import Tracker
    from consist.integrations.containers import run_container

    tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

    with tracker.scenario("multi_model_workflow") as sc:

        # Step 1: Preprocess — cached by image digest + inputs
        preprocess_result = run_container(
            tracker=tracker,
            run_id="preprocess",
            image="/containers/preprocess.sif",
            command=["python", "preprocess.py"],
            volumes={"/scratch/raw_data": "/data"},
            inputs=[Path("/scratch/raw_data")],
            outputs=["/scratch/preprocessed"],
            backend_type="singularity",
        )
        sc.coupler.set("preprocessed_data", preprocess_result.output)

        # Step 2: Model — skipped automatically if image + configs unchanged
        model_result = run_container(
            tracker=tracker,
            run_id="activitysim",
            image="/containers/activitysim.sif",
            command=["python", "-m", "activitysim.core.workflow"],
            volumes={
                "/project/configs": "/configs",
                "/scratch/model_outputs": "/outputs",
            },
            inputs=[Path("/project/configs")],
            outputs=["/scratch/model_outputs"],
            backend_type="singularity",
        )
        sc.coupler.set("model_outputs", model_result.output)
    ```

    Each container step caches independently. If preprocessing inputs haven't
    changed, that step is skipped. Lineage links both steps to the scenario,
    so you can query which image version and config produced any output.

---

## Error Handling

### Container Execution Fails

If the container exits with a non-zero code:

```python
try:
    result = run_container(...)
except RuntimeError as e:
    print(f"Container failed: {e}")
    # Debug: check container logs, input paths, volume mounts
```

**Debugging steps:**
1. Run the container manually to check for errors:
   ```bash
   docker run -it -v ./data:/inputs my-org/my-model:v1 python process.py
   ```
2. Check that input paths exist and are readable by the container
3. Ensure output directories are writable (containers often run as root)
4. Check volume mount paths are absolute or correctly resolved

### Output Files Not Found

If Consist doesn't find expected outputs:

```python
result = run_container(...)
# Warning logged: "Expected output not found: ./outputs/result.csv"

# Check what was actually created:
import subprocess
subprocess.run(["docker", "run", "--rm",
                "-v", "./outputs:/outputs",
                "my-org/my-model:v1",
                "ls", "-la", "/outputs"])
```

### Image Pull Errors

If the image cannot be pulled:

```python
result = run_container(
    ...
    image="my-org/my-model:v1.0",
    pull_latest=False,  # Avoid registry roundtrip if not needed
    backend_type="docker",
)
# Check docker auth:
# docker login
# docker pull my-org/my-model:v1.0
```

---

## Performance Tuning

### Container Startup Overhead

Container creation/startup is ~1-2 seconds. For workflows with many short-lived steps, batch them:

```python
# DON'T do this (N containers, N startups):
for i in range(100):
    run_container(...)

# DO this (1 container, batch processing inside):
run_container(
    command=["python", "batch_process.py", "--n", "100"],
    ...
)
```

### Image Size & Registry

Large images slow down pulls. Optimize:
- Use slim base images (`python:3.11-slim` not `python:3.11`)
- Only install required dependencies
- Use multi-stage builds

### Persistent Caching

If you re-run the same container frequently, Consist's cache avoids re-execution. But if cache is disabled or cleared:

```python
# Cache is per (image_digest, command, inputs) signature
# To maximize cache reuse:
# 1. Pin image versions (don't use :latest)
# 2. Use `pull_latest=False` unless you need latest code
# 3. Log inputs consistently (same file paths, same hashes)
```

---

## Lineage Mode: "full" vs "none"

By default, `lineage_mode="full"` performs provenance tracking. If you need to execute containers without Consist logging (for external tools), use `lineage_mode="none"`:

```python
result = run_container(
    ...
    lineage_mode="none",  # Don't create a Consist run
)
# No provenance logged, but you still get manifest_hash for external tracking
```

This is useful if you're embedding Consist-executed containers inside a non-Consist workflow.

---

## See Also

- [Usage Guide: Pattern 3 (Container Integration)](usage-guide.md#pattern-3-container-integration)
- [Architecture: Container Integration](architecture.md#container-integration)
- [Troubleshooting: Container Execution](troubleshooting.md#container-execution-issues)
- [Mounts & Portability](mounts-and-portability.md) — host path remapping and historical resolution
- [Containers API Reference](integrations/containers.md) — `run_container` and `ContainerDefinition`
