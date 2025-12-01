import os
import pytest
from unittest.mock import patch, MagicMock
from consist.integrations.containers.backends import SingularityBackend, DockerBackend


# --- Singularity Tests ---


def test_singularity_command_construction():
    """Verify CLI arguments are assembled correctly."""

    # We must patch os.makedirs because SingularityBackend.__init__ tries to create cache dirs
    with (
        patch("os.makedirs"),
        patch("pathlib.Path.mkdir"),
    ):  # Patch mkdir for the run() method

        backend = SingularityBackend(cache_base_options=["/tmp"])

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            backend.run(
                image="my_image.sif",
                command=["python", "script.py"],
                volumes={"/host/data": "/data"},
                env={"DEBUG": "1"},
                working_dir="/work",
            )

            # Check the call args
            args = mock_run.call_args[0][0]

            # Core flags
            assert args[0] == "singularity"
            assert "run" in args
            assert "--cleanenv" in args

            # Binds
            assert "-B" in args
            idx_b = args.index("-B")
            assert args[idx_b + 1] == "/host/data:/data"

            # Env
            assert "--env" in args
            idx_e = args.index("--env")
            assert args[idx_e + 1] == "DEBUG=1"

            # Workdir
            assert "--pwd" in args
            idx_w = args.index("--pwd")
            assert args[idx_w + 1] == "/work"

            # Image and Command
            assert args[-3] == "my_image.sif"
            assert args[-2] == "python"
            assert args[-1] == "script.py"


@patch("os.statvfs")
@patch("os.path.exists")
@patch("os.access")
@patch("os.makedirs")  # FIX: Mock filesystem write
def test_singularity_cache_selection(
    mock_makedirs, mock_access, mock_exists, mock_statvfs
):
    """
    Test the PILATES logic: verify it picks the directory with free space.
    """

    # Setup: /fast_local exists, is writable, has tons of space
    # side_effect allows us to say /fast_local exists, but maybe others don't
    def exists_side_effect(path):
        return path == "/fast_local"

    mock_exists.side_effect = exists_side_effect
    mock_access.return_value = True

    # Mock statvfs to return > 20GB free
    # f_bavail * f_frsize = bytes
    # 20GB = 20 * 1024**3 bytes
    mock_stat = MagicMock()
    mock_stat.f_bavail = 30 * (1024**3)
    mock_stat.f_frsize = 1
    mock_statvfs.return_value = mock_stat

    backend = SingularityBackend(cache_base_options=["/fast_local"])

    # Check that environment vars were set correctly pointing to /fast_local
    assert os.environ["APPTAINER_CACHEDIR"] == "/fast_local/.apptainer/cache"
    assert os.environ["SINGULARITY_CACHEDIR"] == "/fast_local/.singularity/cache"

    # Verify we tried to create them
    mock_makedirs.assert_any_call("/fast_local/.apptainer/cache", exist_ok=True)


# --- Docker Tests ---


def test_docker_command_list_handling():
    """Verify we pass lists to Docker SDK, not strings."""
    # Mock the docker client
    mock_client = MagicMock()

    # We need to mock the import in the backend module if docker isn't installed
    # But assuming dev env, we can patch the class logic

    with patch("consist.integrations.containers.backends.docker") as mock_lib:
        mock_lib.from_env.return_value = mock_client
        mock_client.containers.run.return_value.wait.return_value = {"StatusCode": 0}

        backend = DockerBackend(client=mock_client)
        backend.run(image="img", command=["sh", "-c", "echo hi"], volumes={}, env={})

        # Assert 'command' arg was passed as list, NOT joined string
        call_kwargs = mock_client.containers.run.call_args[1]
        assert call_kwargs["command"] == ["sh", "-c", "echo hi"]


# --- Additional Docker Tests ---


def test_docker_missing_dependency():
    """Verify generic ImportError if docker module is missing."""
    with patch("consist.integrations.containers.backends.docker", None):
        with pytest.raises(ImportError) as exc:
            DockerBackend()
        assert "package is required" in str(exc.value)


def test_docker_digest_resolution_logic():
    """Test pull behavior and digest fallback mechanisms."""
    mock_client = MagicMock()
    mock_img = MagicMock()

    # Setup mock image
    mock_img.attrs = {"RepoDigests": ["sha256:specific_digest"]}
    mock_client.images.get.return_value = mock_img

    # Case 1: Pull Latest = True
    backend = DockerBackend(client=mock_client, pull_latest=True)
    digest = backend.resolve_image_digest("my_img")

    mock_client.images.pull.assert_called_with("my_img")
    assert digest == "sha256:specific_digest"

    # Case 2: No RepoDigests (Local build), Fallback to ID
    mock_img.attrs = {}  # No digests
    mock_img.id = "sha256:local_id"
    digest = backend.resolve_image_digest("my_img")
    assert digest == "sha256:local_id"


def test_docker_run_exception_handling():
    """Verify run returns False gracefully on docker errors."""
    mock_client = MagicMock()
    mock_client.containers.run.side_effect = Exception("Docker Daemon Down")

    backend = DockerBackend(client=mock_client)
    success = backend.run("img", "cmd", {}, {})

    assert success is False


# --- Additional Singularity Tests ---


@patch("os.statvfs")
@patch("os.path.exists")
@patch("os.access")
@patch("os.makedirs")
def test_singularity_cache_fallback(
    mock_makedirs, mock_access, mock_exists, mock_statvfs
):
    """
    Verify fallback to CWD if no suitable cache storage is found.
    """
    # Everything exists and is writable...
    mock_exists.return_value = True
    mock_access.return_value = True

    # ... BUT zero space available
    mock_stat = MagicMock()
    mock_stat.f_bavail = 0
    mock_stat.f_frsize = 1
    mock_statvfs.return_value = mock_stat

    # Should fallback to current working directory (getcwd) logic
    # The code sets cache_base = os.getcwd() if loop fails

    # We need to capture the logger warning to be thorough, but checking the result path is enough
    with patch("os.getcwd", return_value="/home/user/project"):
        backend = SingularityBackend(cache_base_options=["/full_disk"])

        # Should rely on CWD
        assert os.environ["APPTAINER_CACHEDIR"] == "/home/user/project/.apptainer/cache"


def test_singularity_executable_missing():
    """Verify behavior when 'singularity' command is not found."""
    # Patch makedirs so init doesn't fail
    with patch("os.makedirs"), patch("pathlib.Path.mkdir"):
        backend = SingularityBackend()

        # Simulate FileNotFoundError during subprocess execution
        with patch("subprocess.run", side_effect=FileNotFoundError):
            success = backend.run("img", "cmd", {}, {})
            assert success is False
