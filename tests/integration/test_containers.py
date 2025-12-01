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
    A dummy backend that simulates container execution by writing files
    directly to the host paths provided in 'volumes'.
    """

    def __init__(self, pull_latest=False):
        self.run_count = 0

    def resolve_image_digest(self, image: str) -> str:
        return f"sha256:mock_digest_for_{image}"

    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
        self.run_count += 1
        for host_path, container_path in volumes.items():
            if "out" in str(host_path):
                Path(host_path).mkdir(parents=True, exist_ok=True)
                (Path(host_path) / "result.txt").write_text("mock data")
        return True


# --- Fixtures ---


@pytest.fixture
def clean_tracker(tmp_path):
    """Returns a fresh Tracker instance with DB enabled for Caching."""
    return Tracker(
        run_dir=tmp_path / "runs", db_path=str(tmp_path / "provenance.duckdb")
    )


@pytest.fixture
def input_file(tmp_path):
    """Creates a dummy input file."""
    p = tmp_path / "inputs" / "data.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("input_data")
    return p


# --- Tests ---


def test_container_caching_logic(clean_tracker, input_file):
    """The 'Usage Pattern' Test."""
    mock_backend_instance = MockBackend()
    output_dir = clean_tracker.run_dir / "outputs_run1"
    output_dir.mkdir(parents=True, exist_ok=True)

    with patch(
        "consist.integrations.containers.api.DockerBackend",
        return_value=mock_backend_instance,
    ):
        # 1. First Run (Cold)
        run_container(
            tracker=clean_tracker,
            run_id="run_1",
            image="my_model:v1",
            command=["python", "run.py"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 1
        assert (output_dir / "result.txt").exists()

        # 2. Second Run (Identical) -> Cache Hit
        run_container(
            tracker=clean_tracker,
            run_id="run_2",
            image="my_model:v1",
            command=["python", "run.py"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 1

        # 3. Third Run (Changed Command) -> Execute
        run_container(
            tracker=clean_tracker,
            run_id="run_3",
            image="my_model:v1",
            command=["python", "run.py", "--fast"],
            volumes={str(input_file.parent): "/inputs", str(output_dir): "/outputs"},
            inputs=[input_file],
            outputs=[output_dir / "result.txt"],
        )
        assert mock_backend_instance.run_count == 2


# --- Singularity Mock Test ---


def test_singularity_mocked_execution(clean_tracker, input_file):
    """Verifies the Singularity flow using a Mocked subprocess."""
    # Mock subprocess inside the backend module
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
    clean_tracker, input_file, change_type, change_val
):
    """
    Verifies that changing SPECIFIC aspects of the container run trigger a Cache Miss.
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
        assert (
            mock_backend.run_count == 2
        ), f"Failed to invalidate cache on change: {change_type}"


# --- Real Docker Integration Test ---


def has_docker():
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return True
    except:
        return False


@pytest.mark.skipif(not has_docker(), reason="Docker not available")
def test_docker_real_execution(clean_tracker, tmp_path):
    """Real Integration Test using Alpine."""
    host_in_dir = (tmp_path / "host_in").resolve()
    host_in_dir.mkdir(parents=True, exist_ok=True)
    (host_in_dir / "input.txt").write_text("hello world")

    host_out_dir = (tmp_path / "host_out").resolve()
    host_out_dir.mkdir(parents=True, exist_ok=True)

    image = "alpine:latest"
    command = ["sh", "-c", "cat /data/in/input.txt > /data/out/output.txt"]

    volumes = {str(host_in_dir): "/data/in", str(host_out_dir): "/data/out"}

    # Run 1
    success = run_container(
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
    assert success is True
    assert (host_out_dir / "output.txt").read_text().strip() == "hello world"

    # Run 2 (Cache Hit)
    tracker2 = Tracker(run_dir=clean_tracker.run_dir, db_path=clean_tracker.db_path)
    success_2 = run_container(
        tracker=tracker2,
        run_id="docker_real_2",
        image=image,
        command=command,
        volumes=volumes,
        inputs=[host_in_dir / "input.txt"],
        outputs=[host_out_dir / "output.txt"],
        backend_type="docker",
    )
    assert success_2 is True
