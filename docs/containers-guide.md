# Container Integration Guide

Consist can execute containerized tools and models (Docker, Singularity) while automatically tracking provenance, inputs, outputs, and caching based on image digest and parameters.

This guide covers when and how to use containers, with practical examples for ActivitySim, SUMO, and other tools.

---

## Why Containers?

Containers are ideal when:

- **Wrapping existing tools** (ActivitySim, SUMO, BEAM, R scripts, legacy code) without modifying source
- **Tool has complex dependencies** that are easier to package than to install
- **Tool is non-deterministic or black-box** (you can't inspect its internal caching)
- **Tool expects specific file paths** or directory structures
- **Tool is in a different language** (Python, R, Java, compiled binary)
- **Tool is already containerized** and you want to integrate it into a Consist workflow

**Not ideal for:**
- Simple Python functions (use `consist.run()` instead)
- Workflows where you want fine-grained step caching (use `scenario()` + `consist.run()`)
- Development/debugging (container startup adds latency during iteration, but can be fine for lightweight images)

---

## How Caching Works

When you execute a container, Consist computes a **container signature** from:

1. **Image identity**: Full image digest (resolved from registry if `pull_latest=True`)
2. **Command**: The exact command string and arguments
3. **Environment**: All environment variables passed to the container
4. **Volumes**: Container mount paths (but NOT host paths, which are run-specific)
5. **Input data**: SHA256 hashes of input files

If all these match a prior run, Consist returns cached outputs **without executing the container**.

**Key insight**: Changing the image tag (e.g., `my-model:v1` → `my-model:v2`) invalidates the cache. If you use `my-model:latest`, consider setting `pull_latest=True` to refresh the digest from the registry and ensure cache invalidation when the tag updates (use it when `latest` is truly mutable).

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
```
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

## SUMO Integration

SUMO (Simulation of Urban Mobility) is a microscopic traffic simulation tool. Integration is similar to ActivitySim:

```python
from consist.integrations.containers import run_container
from pathlib import Path

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

def run_sumo_simulation(scenario_name: str, config_file: Path, network_file: Path):
    """Execute SUMO traffic simulation with Consist provenance."""

    output_dir = Path(f"./sumo_outputs/{scenario_name}")
    output_dir.mkdir(parents=True, exist_ok=True)

    result = run_container(
        tracker=tracker,
        run_id=f"sumo_{scenario_name}",
        image="my-org/sumo:latest",
        command=[
            "sumo",
            "-c", str(config_file),  # SUMO config file
            "--output-prefix", f"/outputs/{scenario_name}/",
        ],
        volumes={
            "./network": "/network",   # SUMO network files
            "./scenarios": "/scenarios",
            f"./sumo_outputs/{scenario_name}": f"/outputs/{scenario_name}",
        },
        inputs=[config_file, network_file],
        outputs=[str(output_dir)],
        environment={
            "SUMO_HOME": "/usr/share/sumo",
        },
        backend_type="docker",
    )

    return result

# Run with baseline network
result = run_sumo_simulation(
    "baseline_2024",
    config_file=Path("./scenarios/baseline.sumo.cfg"),
    network_file=Path("./network/network.net.xml"),
)
```

---

## Singularity / Apptainer Support

If using Singularity (common on HPC clusters):

```python
from consist.integrations.containers import run_container

result = run_container(
    tracker=tracker,
    run_id="hpc_job",
    image="/path/to/my_model.sif",  # Local .sif file path
    command=["python", "model.py", "--param", "value"],
    volumes={
        "/scratch/inputs": "/inputs",
        "/scratch/outputs": "/outputs",
    },
    inputs=[Path("/scratch/inputs/data.csv")],
    outputs=["/scratch/outputs"],
    backend_type="singularity",  # Use Singularity instead of Docker
)
```

Key differences:
- Image is typically a **local file path** (.sif) not a registry URL
- Volume syntax is the same
- Caching behavior is identical (based on image path digest)

---

## Volume Mounting Best Practices

### Mount Paths

Map host directories to container paths consistently:

```python
volumes = {
    "/host/absolute/path": "/container/path",  # Use absolute paths
    "./relative": "/container/relative",        # Or relative (resolved against cwd)
}
```

**Important:** Host paths in `volumes` are **not part of the cache key** (they're run-specific). Only `inputs` are hashed for the signature. If you want config changes to invalidate cache, pass config files in `inputs`:

```python
result = run_container(
    ...
    volumes={
        "./config_dir": "/configs",
        "./data": "/data",
    },
    inputs=[Path("./config_dir/settings.yaml"), Path("./data/input.csv")],  # These are hashed
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
3. Cached output files are **copied to host paths**
4. `result.cache_hit == True`, `result.cache_source` = prior run ID

```python
result = run_container(...)
if result.cache_hit:
    print(f"Cache hit from {result.cache_source}")
    # result.artifacts are materialized (files exist on disk)
```

### Cache Invalidation

Cache is invalidated (new run executed) if any of these change:

- **Image**: `my-model:v1` → `my-model:v2` (or image pulled with different digest if `pull_latest=True`)
- **Command**: Arguments to the tool
- **Environment**: Any env var changes
- **Inputs**: Hash of input files changes
- **Volumes**: Container mount paths change

### Manual Cache Bypass

To force re-execution even with matching signature:

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

Use `run_container()` inside `scenario()` for multi-step workflows:

```python
import consist
from consist import Tracker, use_tracker
from consist.integrations.containers import run_container

tracker = Tracker(run_dir="./runs", db_path="./provenance.duckdb")

with use_tracker(tracker):
    with consist.scenario("multi_model_workflow") as sc:

        # Step 1: Data preprocessing (Python)
        preprocess_result = run_container(
            tracker=tracker,
            run_id="preprocess",
            image="my-org/preprocess:v1",
            command=["python", "preprocess.py"],
            volumes={"./raw_data": "/data"},
            inputs=[Path("./raw_data")],
            outputs=["./preprocessed"],
        )
        sc.coupler.set("preprocessed_data", preprocess_result.output)

        # Step 2: Model execution (ActivitySim)
        model_result = run_container(
            tracker=tracker,
            run_id="activitysim",
            image="my-org/activitysim:v1",
            command=["python", "-m", "activitysim.core.workflow"],
            volumes={
                "./configs": "/configs",
                "./model_outputs": "/outputs",
            },
            inputs=[Path("./configs")],
            outputs=["./model_outputs"],
        )
        sc.coupler.set("model_outputs", model_result.output)

        # Step 3: Analysis (Python)
        with sc.trace(name="analysis"):
            # Load previous results
            preprocessed = consist.load(sc.coupler.require("preprocessed_data"))
            model_output = consist.load(sc.coupler.require("model_outputs"))
            # ... analysis code ...
```

Each container step caches independently. If preprocessing input hasn't changed, skip it. If model config hasn't changed, skip it.

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
- [Architecture: Container Support](architecture.md#container-support)
- [Troubleshooting: Container Execution](troubleshooting.md#container-errors)
