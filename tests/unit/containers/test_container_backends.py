import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from consist.integrations.containers.backends import SingularityBackend, DockerBackend


# --- Singularity Tests ---


def test_singularity_command_construction():
    """
    Tests that the `SingularityBackend` correctly constructs the `singularity run` CLI command
    from the provided parameters.

    This test ensures that host-to-container volume mappings, environment variables,
    working directory, and the command itself are correctly translated into the
    `singularity` command-line arguments.

    What happens:
    1. `os.makedirs` and `pathlib.Path.mkdir` are mocked to prevent actual filesystem operations
       during `SingularityBackend` initialization and `run` method execution.
    2. A `SingularityBackend` instance is created.
    3. The `run` method is called with a sample image, command, volumes, environment variables,
       and a working directory.
    4. `subprocess.run` is mocked to capture the arguments it would receive.

    What's checked:
    - The captured command-line arguments passed to `subprocess.run` start with "singularity".
    - Core Singularity flags like "run" and "--cleanenv" are present.
    - Volume binds (`-B`) are correctly formatted with host and container paths.
    - Environment variables (`--env`) are correctly formatted.
    - The working directory (`--pwd`) is correctly specified.
    - The image and command components are correctly appended at the end of the argument list.
    """
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


def test_singularity_env_with_spaces_uses_passthrough_env():
    """
    Values with whitespace (e.g., JAVA_OPTS) should NOT be passed via `--env` because
    Singularity/Apptainer parses `--env` as a list of KEY=VALUE pairs and can split
    whitespace-containing values.

    Instead, Consist should pass these via host env vars SINGULARITYENV_/APPTAINERENV_.
    """
    with (
        patch("os.makedirs"),
        patch("pathlib.Path.mkdir"),
    ):
        backend = SingularityBackend(cache_base_options=["/tmp"])

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            backend.run(
                image="my_image.sif",
                command=["python", "script.py"],
                volumes={"/host/data": "/data"},
                env={"JAVA_OPTS": "-Xms1g -Xmx1g", "DEBUG": "1"},
                working_dir="/work",
            )

            cmd_args = mock_run.call_args[0][0]
            run_env = mock_run.call_args[1]["env"]

            # DEBUG can still go through --env
            assert "--env" in cmd_args
            assert "DEBUG=1" in cmd_args

            # JAVA_OPTS should be passed via SINGULARITYENV_/APPTAINERENV_
            assert "JAVA_OPTS=-Xms1g -Xmx1g" not in cmd_args
            assert run_env["SINGULARITYENV_JAVA_OPTS"] == "-Xms1g -Xmx1g"
            assert run_env["APPTAINERENV_JAVA_OPTS"] == "-Xms1g -Xmx1g"


@patch("consist.integrations.containers.backends.shutil.disk_usage")
@patch("os.path.exists")
@patch("os.access")
@patch("os.makedirs")
def test_singularity_cache_selection(
    mock_makedirs: MagicMock,
    mock_access: MagicMock,
    mock_exists: MagicMock,
    mock_disk_usage: MagicMock,
):
    """
    Tests the `SingularityBackend`'s logic for selecting an appropriate cache directory.

    This test verifies that `SingularityBackend` correctly prioritizes and selects
    a cache location based on the availability of sufficient free disk space and
    write permissions, as described in Consist's HPC optimization strategy.

    What happens:
    1. Mock `shutil.disk_usage`, `os.path.exists`, and `os.access` to simulate a specific
       filesystem state: a `/fast_local` directory exists, is writable, and has
       more than 20GB of free space.
    2. A `SingularityBackend` instance is initialized with `cache_base_options`
       including `/fast_local`.

    What's checked:
    - The `APPTAINER_CACHEDIR` and `SINGULARITY_CACHEDIR` environment variables
      are set to paths within the `/fast_local` directory, confirming that
      the backend successfully identified and chose the preferred location.
    - `os.makedirs` was called to create the necessary cache subdirectories within `/fast_local`.
    """

    # Setup: /fast_local exists, is writable, has tons of space
    # side_effect allows us to say /fast_local exists, but maybe others don't
    def exists_side_effect(path):
        return path == "/fast_local"

    mock_exists.side_effect = exists_side_effect
    mock_access.return_value = True

    # Mock disk_usage to return > 20GB free
    mock_disk_usage.return_value = MagicMock(free=30 * (1024**3))

    SingularityBackend(cache_base_options=["/fast_local"])

    # Check that environment vars were set correctly pointing to /fast_local
    assert (
        Path(os.environ["APPTAINER_CACHEDIR"]).as_posix()
        == "/fast_local/.apptainer/cache"
    )
    assert (
        Path(os.environ["SINGULARITY_CACHEDIR"]).as_posix()
        == "/fast_local/.singularity/cache"
    )

    # Verify we tried to create them (path separator agnostic)
    created_dirs = {
        Path(call.args[0]).as_posix()
        for call in mock_makedirs.call_args_list
        if call.kwargs.get("exist_ok") is True and call.args
    }
    assert "/fast_local/.apptainer/cache" in created_dirs
    assert "/fast_local/.apptainer/tmp" in created_dirs
    assert "/fast_local/.singularity/cache" in created_dirs
    assert "/fast_local/.singularity/tmp" in created_dirs


# --- Docker Tests ---


def test_docker_command_list_handling():
    """
    Verifies that the `DockerBackend` passes the `command` argument as a list
    directly to the Docker SDK's `containers.run` method, rather than joining it into a string.

    This is important because Docker's SDK handles list-form commands correctly
    (exec form), preserving arguments with spaces or special characters, unlike
    shell-form strings which require careful quoting.

    What happens:
    1. The `docker` library and its `containers.run` method are mocked.
    2. A `DockerBackend` instance is created.
    3. The `run` method is called with a command specified as a list of strings.

    What's checked:
    - The `command` argument in the mocked `containers.run` call was passed
      exactly as the original list of strings `["sh", "-c", "echo hi"]`,
      and not as a single joined string.
    """
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


def test_docker_digest_resolution_logic():
    """
    Tests the `DockerBackend`'s logic for resolving Docker image digests and handling
    `pull_latest` behavior and fallback mechanisms.

    This test ensures that `DockerBackend` can retrieve stable, content-addressable
    image identifiers (`RepoDigests`) for reproducibility and correctly falls back
    to local image IDs when `RepoDigests` are not available.

    What happens:
    1. A mock Docker client is set up to simulate Docker API responses.
    2. **Case 1 (Pull Latest = True)**: A `DockerBackend` is initialized with `pull_latest=True`,
       and `resolve_image_digest` is called. The mock client is configured to return a
       `RepoDigest`.
    3. **Case 2 (No RepoDigests)**: The mock image's attributes are changed to simulate
       an image without `RepoDigests`, and `resolve_image_digest` is called again.

    What's checked:
    - **Case 1**: `mock_client.images.pull` is called (due to `pull_latest=True`),
      and the returned digest is the expected `RepoDigest`.
    - **Case 2**: When `RepoDigests` are absent, the method falls back to returning
      the `id` of the mock image.
    """
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
    """
    Verifies that the `DockerBackend.run` method gracefully handles exceptions
    raised by the Docker SDK during container execution, returning `False`.

    This ensures that internal Docker errors do not propagate unexpectedly
    and that `run_container` can reliably determine the success status of
    the container operation.

    What happens:
    1. A mock Docker client is configured such that its `containers.run` method
       raises an exception (simulating a Docker daemon being down or other API error).
    2. The `DockerBackend.run` method is called.

    What's checked:
    - The `run` method returns `False`, indicating that the container execution failed.
    """
    mock_client = MagicMock()
    mock_client.containers.run.side_effect = Exception("Docker Daemon Down")

    backend = DockerBackend(client=mock_client)
    success = backend.run("img", "cmd", {}, {})

    assert success is False


# --- Additional Singularity Tests ---


@patch("consist.integrations.containers.backends.shutil.disk_usage")
@patch("os.path.exists")
@patch("os.access")
@patch("os.makedirs")
def test_singularity_cache_fallback(
    mock_makedirs: MagicMock,
    mock_access: MagicMock,
    mock_exists: MagicMock,
    mock_disk_usage: MagicMock,
):
    """
    Verifies that `SingularityBackend` correctly falls back to using the current
    working directory as the cache base if no suitable fast local storage options
    are found (e.g., due to insufficient disk space).

    This test ensures robustness in HPC environments where dedicated scratch
    space might be full or unavailable.

    What happens:
    1. `shutil.disk_usage`, `os.path.exists`, and `os.access` are mocked to simulate
       a scenario where all provided `cache_base_options` exist and are writable,
       but none have sufficient free disk space (mocking `free` to 0).
    2. `os.getcwd` is patched to return a predictable current working directory.
    3. A `SingularityBackend` instance is initialized.

    What's checked:
    - The `APPTAINER_CACHEDIR` environment variable is set to a path within
      the mocked current working directory, confirming the fallback mechanism.
    """
    # Everything exists and is writable...
    mock_exists.return_value = True
    mock_access.return_value = True

    # ... BUT zero space available
    mock_disk_usage.return_value = MagicMock(free=0)

    # Should fallback to current working directory (getcwd) logic
    # The code sets cache_base = os.getcwd() if loop fails

    # We need to capture the logger warning to be thorough, but checking the result path is enough
    with patch("os.getcwd", return_value="/home/user/project"):
        SingularityBackend(cache_base_options=["/full_disk"])

        # Should rely on CWD
        assert (
            Path(os.environ["APPTAINER_CACHEDIR"]).as_posix()
            == "/home/user/project/.apptainer/cache"
        )


def test_singularity_executable_missing():
    """
    Verifies that the `SingularityBackend.run` method gracefully handles a `FileNotFoundError`
    if the `singularity` executable is not found in the system's PATH.

    This ensures that `run_container` can provide a clear indication of failure
    when the underlying container runtime is not installed or accessible.

    What happens:
    1. `os.makedirs` and `pathlib.Path.mkdir` are mocked to prevent filesystem operations.
    2. A `SingularityBackend` instance is initialized.
    3. `subprocess.run` is mocked to raise a `FileNotFoundError`, simulating
       the `singularity` command not being found.
    4. The `SingularityBackend.run` method is called.

    What's checked:
    - The `run` method returns `False`, indicating a failed execution.
    """
    with patch("os.makedirs"), patch("pathlib.Path.mkdir"):
        backend = SingularityBackend()

        # Simulate FileNotFoundError during subprocess execution
        with patch("subprocess.run", side_effect=FileNotFoundError):
            success = backend.run("img", "cmd", {}, {})
            assert success is False
