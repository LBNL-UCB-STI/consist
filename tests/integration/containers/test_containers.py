import pytest
from pathlib import Path
from typing import List, Dict, Optional, Union
from unittest.mock import patch

# Import Consist components
from consist.core.tracker import Tracker
from consist.integrations.containers.api import run_container
from consist.integrations.containers.backends import ContainerBackend


# --- Mocks for Logic Testing ---


class MockBackend(ContainerBackend):
    """
    A dummy container backend implementation used for testing Consist's container integration logic.

    This mock simulates container execution without actually interacting with a Docker
    daemon or Singularity. It allows testing the caching, provenance tracking,
    and input/output handling aspects of `run_container` in isolation.
    It simulates side effects by creating dummy files in specified host output volumes.

    Attributes
    ----------
    run_count : int
        A counter that tracks how many times the `run` method of this mock backend has been called.
        Useful for asserting cache hits/misses.
    """

    def __init__(self, pull_latest: bool = False) -> None:
        """
        Initializes the MockBackend.

        Parameters
        ----------
        pull_latest : bool, default False
            A placeholder parameter to match the signature of real backends.
            Has no functional effect in the mock.
        """
        self.run_count = 0

    def resolve_image_digest(self, image: str) -> str:
        """
        Simulates resolving a container image to a mock digest.

        Parameters
        ----------
        image : str
            The image name or reference.

        Returns
        -------
        str
            A deterministic mock SHA digest string based on the image name.
        """
        return f"sha256:mock_digest_for_{image}"

    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
        """
        Simulates running a command within a container.

        This mock method increments an internal counter and, if an output volume
        is detected, creates a dummy `result.txt` file within the host path
        of that volume to simulate container output.

        Parameters
        ----------
        image : str
            The container image to use (mocked).
        command : Union[str, List[str]]
            The command to execute inside the container (mocked).
        volumes : Dict[str, str]
            A dictionary mapping host paths to container paths for volume mounts.
            Used to identify simulated output directories.
        env : Dict[str, str]
            Environment variables (mocked).
        working_dir : Optional[str], optional
            Working directory inside the container (mocked).

        Returns
        -------
        bool
            Always returns `True` to simulate a successful container execution.
        """
        self.run_count += 1
        for host_path, container_path in volumes.items():
            # Create mock output if the path suggests it's an output directory
            if "out" in str(host_path):
                Path(host_path).mkdir(parents=True, exist_ok=True)
                (Path(host_path) / "result.txt").write_text("mock data")
        return True


# --- Fixtures ---


@pytest.fixture
def clean_tracker(tmp_path: Path) -> Tracker:
    """
    Pytest fixture that returns a fresh `Tracker` instance with a database enabled.

    This fixture creates a new `Tracker` instance for each test, ensuring test
    isolation. The tracker is configured to use a temporary directory for run logs
    and a dedicated DuckDB file for provenance tracking, which is essential for
    testing caching mechanisms.

    Parameters
    ----------
    tmp_path : Path
        A `pytest` fixture providing a unique temporary directory for the test.

    Returns
    -------
    Tracker
        A newly initialized `Tracker` instance.
    """
    return Tracker(
        run_dir=tmp_path / "runs", db_path=str(tmp_path / "provenance.duckdb")
    )


@pytest.fixture
def input_file(tmp_path: Path) -> Path:
    """
    Pytest fixture that creates a dummy input file in a temporary directory.

    This fixture is used to provide a consistent and isolated input file
    for container tests, simulating a host-side input dependency.

    Parameters
    ----------
    tmp_path : Path
        A `pytest` fixture providing a unique temporary directory for the test.

    Returns
    -------
    Path
        The path to the created dummy input file (`data.csv`).
    """
    p = tmp_path / "inputs" / "data.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("input_data")
    return p


# --- Tests ---


def test_container_caching_logic(clean_tracker: Tracker, input_file: Path):
    """
    Tests the core caching logic of `run_container`, including cache hits and misses.

    This test verifies that `run_container` correctly identifies identical container
    executions as cache hits and re-executes the container only when relevant
    parameters (image, command, inputs, etc.) change.

    What happens:
    1. A `MockBackend` is used to simulate container execution.
    2. **First Run**: `run_container` is called with a set of parameters.
    3. **Second Run**: `run_container` is called with identical parameters to the first run.
    4. **Third Run**: `run_container` is called with a modified command.

    What's checked:
    - After the first run, the `MockBackend.run_count` is 1 (container executed).
    - After the second run (identical parameters), `MockBackend.run_count` remains 1 (cache hit).
    - After the third run (modified command), `MockBackend.run_count` increments to 2 (cache miss, container re-executed).
    - The expected output file exists on the host after the first run.
    """
    mock_backend_instance = MockBackend()
    output_dir = clean_tracker.run_dir / "outputs_run1"
    output_dir.mkdir(parents=True, exist_ok=True)

    with patch(
        "consist.integrations.containers.api.DockerBackend",
        return_value=mock_backend_instance,
    ):
        # 1. First Run (Cold)
        result1 = run_container(
            tracker=clean_tracker,
            run_id="run_1",
            image="my_model:v1",
            command=["python", "run.py"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 1
        assert result1.cache_hit is False
        assert (output_dir / "result.txt").exists()

        # 2. Second Run (Identical) -> Cache Hit
        result2 = run_container(
            tracker=clean_tracker,
            run_id="run_2",
            image="my_model:v1",
            command=["python", "run.py"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 1
        assert result2.cache_hit is True

        # 3. Third Run (Changed Command) -> Execute
        result3 = run_container(
            tracker=clean_tracker,
            run_id="run_3",
            image="my_model:v1",
            command=["python", "run.py", "--fast"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 2
        assert result3.cache_hit is False


def test_nested_container_execution(clean_tracker: Tracker, input_file: Path):
    """
    Tests that `run_container` correctly behaves as a step when called inside
    an existing active run (Nested Mode).

    Verifies:
    1. It detects the active run.
    2. It executes the backend directly (does not start a new run).
    3. It logs inputs/outputs to the *parent* run.
    4. It logs the container config to the *parent* run's metadata.
    """
    mock_backend = MockBackend()

    # Setup output directory
    output_dir = clean_tracker.run_dir / "nested_out"
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_output_file = output_dir / "result.txt"

    with patch(
        "consist.integrations.containers.api.DockerBackend",
        return_value=mock_backend,
    ):
        # 1. Start a PARENT run explicitly
        with clean_tracker.start_run(run_id="parent_run", model="pipeline_root"):
            assert clean_tracker.current_consist is not None

            # 2. Call run_container nested inside
            result = run_container(
                tracker=clean_tracker,
                run_id="inner_step_1",  # This ID is used for logs, not a DB Run
                image="nested_image:v1",
                command=["echo", "nested"],
                volumes={str(input_file.parent): "/in", str(output_dir): "/out"},
                inputs=[input_file],
                outputs=[expected_output_file],
                backend_type="docker",
            )

            assert result.cache_hit is False

            # --- Assertions on Parent Run State ---

            # A. Check backend execution
            assert mock_backend.run_count == 1

            # B. Check Artifact Logging to Parent
            inputs = clean_tracker.current_consist.inputs
            outputs = clean_tracker.current_consist.outputs

            # Input file should be logged
            assert any(str(input_file.name) in a.uri for a in inputs)
            # Mock backend created result.txt, so it should be logged as output
            assert any("result.txt" in a.uri for a in outputs)

            # C. Check Metadata Injection
            meta = clean_tracker.current_consist.run.meta
            # Ensure keys like "container_step_<timestamp>" exist
            step_keys = [k for k in meta.keys() if k.startswith("container_step_")]
            assert len(step_keys) == 1

            # Check content of the metadata config
            config_dump = meta[step_keys[0]]
            assert config_dump["image"] == "nested_image:v1"
            assert config_dump["backend"] == "docker"


# --- Singularity Mock Test ---


def test_singularity_mocked_execution(clean_tracker: Tracker, input_file: Path):
    """
    Tests the integration of the Singularity backend using a mocked `subprocess.run`.

    This test verifies that `run_container` correctly constructs the `singularity`
    command and interacts with the `SingularityBackend` when `backend_type="singularity"`
    is specified, without requiring a live Singularity installation.

    What happens:
    1. The `subprocess.run` function within the `SingularityBackend` is mocked
       to prevent actual execution and control its return value.
    2. `run_container` is called with `backend_type="singularity"`, a sample image,
       command, and inputs/outputs.

    What's checked:
    - The mocked `subprocess.run` was called.
    - The arguments passed to `subprocess.run` start with "singularity", confirming
      that the correct container command was constructed.
    """
    with patch("consist.integrations.containers.backends.subprocess.run") as mock_sub:
        mock_sub.return_value.returncode = 0

        run_container(
            tracker=clean_tracker,
            run_id="singularity_run_1",
            image="library/ubuntu:latest",
            command=["echo", "hello"],
            volumes={str(input_file.parent): "/in"},
            inputs=[input_file],
            outputs=[],
            backend_type="singularity",
        )

        assert mock_sub.called
        cmd_args = mock_sub.call_args[0][0]
        assert cmd_args[0] == "singularity"


def test_no_lineage_mode_executes_without_tracking(
    clean_tracker: Tracker, input_file: Path
):
    """
    When lineage_mode="none", run_container should:
    - Execute the backend.
    - Not create a Consist run row.
    - Not scan or log output artifacts.
    - Return a stable manifest_hash for callers to incorporate upstream.
    """
    mock_backend_instance = MockBackend()
    output_dir = clean_tracker.run_dir / "no_lineage_out"
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_output_file = output_dir / "result.txt"

    with patch(
        "consist.integrations.containers.api.DockerBackend",
        return_value=mock_backend_instance,
    ):
        result = run_container(
            tracker=clean_tracker,
            run_id="no_lineage_run",
            image="my_model:v1",
            command=["python", "run.py"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[expected_output_file],
            lineage_mode="none",
        )

    assert mock_backend_instance.run_count == 1
    assert clean_tracker.get_run("no_lineage_run") is None
    assert result.artifacts == {}
    assert result.cache_hit is False
    assert result.manifest_hash is not None
    assert result.image_digest is not None


# --- Granular Hashing Tests ---


@pytest.mark.parametrize(
    "change_type, change_val",
    [
        ("env", {"NEW_VAR": "1"}),
        ("volume", "DUMMY_MARKER"),  # Use marker, calculate path dynamically
        ("image", "my_model:v2"),
        ("input_content", "new_content"),
        ("backend", "singularity"),
    ],
)
def test_granular_cache_invalidation(
    clean_tracker: Tracker,
    input_file: Path,
    change_type: str,
    change_val: Union[str, Dict],
):
    """
    Verifies that changing specific, granular aspects of a container run correctly
    triggers a cache invalidation (cache miss), leading to re-execution.

    This test parameterizes different types of changes (environment variables,
    volumes, image, input content, backend type) to ensure that Consist's
    hashing mechanism is sensitive to all relevant parameters that define
    a container's identity.

    What happens:
    1. A `MockBackend` is used to simulate container execution, and both `DockerBackend`
       and `SingularityBackend` are patched to use this mock, ensuring the `run_count`
       is correctly tracked regardless of the `backend_type`.
    2. **Base Run**: `run_container` is executed with a standard set of arguments.
    3. **Modified Run**: A copy of the base arguments is made, and a single parameter
       (`change_type`) is modified with `change_val`.
    4. `run_container` is executed again with the modified arguments.

    What's checked:
    - After the base run, `mock_backend.run_count` is 1.
    - After the modified run, `mock_backend.run_count` increments to 2, confirming
      that the cache was invalidated and the container was re-executed due to the change.
    """
    mock_backend = MockBackend()
    output_dir = clean_tracker.run_dir / "outputs"

    # Patch BOTH backends so switching backend type still uses our counter
    with (
        patch(
            "consist.integrations.containers.api.DockerBackend",
            return_value=mock_backend,
        ),
        patch(
            "consist.integrations.containers.api.SingularityBackend",
            return_value=mock_backend,
        ),
    ):
        base_args = dict(
            tracker=clean_tracker,
            run_id="base_run",
            image="my_model:v1",
            command=["run"],
            volumes={str(input_file.parent): "/in", str(output_dir): "/out"},
            inputs=[input_file],
            outputs=[output_dir / "res.txt"],
            environment={"A": "1"},
            backend_type="docker",
        )

        # 1. BASE RUN
        run_container(**base_args)
        assert mock_backend.run_count == 1

        # 2. MODIFIED RUN
        new_args = base_args.copy()
        new_args["run_id"] = "mod_run"

        if change_type == "env":
            new_args["environment"] = change_val
        elif change_type == "volume":
            # FIX: Create a safe temporary directory for the volume change
            safe_vol_path = clean_tracker.run_dir / "extra_vol"
            safe_vol_path.mkdir(parents=True, exist_ok=True)

            v = base_args["volumes"].copy()
            v.update({str(safe_vol_path): "/extra_mount"})
            new_args["volumes"] = v
        elif change_type == "image":
            new_args["image"] = change_val
        elif change_type == "backend":
            new_args["backend_type"] = change_val
        elif change_type == "input_content":
            input_file.write_text(change_val)

        # Execute
        run_container(**new_args)

        # Assert Cache Miss
        assert mock_backend.run_count == 2, (
            f"Failed to invalidate cache on change: {change_type}"
        )


# --- Real Docker Integration Test ---


def has_docker() -> bool:
    """
    Checks if Docker is installed and the Docker daemon is reachable.

    This helper function is used to conditionally skip tests that require
    a functional Docker environment.

    Returns
    -------
    bool
        True if Docker is available and responsive, False otherwise.
    """
    try:
        import docker

        client = docker.from_env()
        client.ping()
        # This test suite runs Linux images (e.g., alpine). On Windows runners, Docker may
        # be in Windows-container mode; in that case, skip to avoid false failures.
        info = client.info()
        if info.get("OSType") != "linux":
            return False
        return True
    except Exception:
        return False


@pytest.mark.skipif(not has_docker(), reason="Docker not available")
def test_docker_real_execution(clean_tracker: Tracker, tmp_path: Path):
    """
    Tests `run_container` with a real Docker execution, verifying end-to-end functionality.

    This integration test uses a simple `alpine` Docker image to perform a file
    copy operation, confirming that `run_container` correctly orchestrates Docker
    execution, handles volume mounts, and logs input/output artifacts. It also
    verifies the caching mechanism with a real Docker run.

    What happens:
    1. A host input directory (`host_in_dir`) is created with an `input.txt` file.
    2. A host output directory (`host_out_dir`) is created.
    3. **First Docker Run**: `run_container` is called to execute a Docker command
       that copies `input.txt` from the input volume to `output.txt` in the output volume.
       `pull_latest` is set to True to ensure the image is available.
    4. **Second Docker Run**: `run_container` is called again with identical parameters
       to test the caching mechanism.

    What's checked:
    - The first `run_container` call returns `True`, indicating success.
    - The `output.txt` file is correctly created in `host_out_dir` with the expected content.
    - The second `run_container` call also returns `True`, confirming a cache hit
      (i.e., the Docker command was not re-executed, but the result was "hydrated").
    """
    host_in_dir = (tmp_path / "host_in").resolve()
    host_in_dir.mkdir(parents=True, exist_ok=True)
    (host_in_dir / "input.txt").write_text("hello world")

    host_out_dir = (tmp_path / "host_out").resolve()
    host_out_dir.mkdir(parents=True, exist_ok=True)

    image = "alpine:latest"
    command = ["sh", "-c", "cat /data/in/input.txt > /data/out/output.txt"]

    volumes = {str(host_in_dir): "/data/in", str(host_out_dir): "/data/out"}

    # Run 1
    result = run_container(
        tracker=clean_tracker,
        run_id="docker_real_1",
        image=image,
        command=command,
        volumes=volumes,
        inputs=[host_in_dir / "input.txt"],
        outputs=[host_out_dir / "output.txt"],
        backend_type="docker",
        pull_latest=True,
    )
    assert result.cache_hit is False
    assert (host_out_dir / "output.txt").read_text().strip() == "hello world"

    # Run 2 (Cache Hit)
    tracker2 = Tracker(run_dir=clean_tracker.run_dir, db_path=clean_tracker.db_path)
    result_2 = run_container(
        tracker=tracker2,
        run_id="docker_real_2",
        image=image,
        command=command,
        volumes=volumes,
        inputs=[host_in_dir / "input.txt"],
        outputs=[host_out_dir / "output.txt"],
        backend_type="docker",
    )
    assert result_2.cache_hit is True
