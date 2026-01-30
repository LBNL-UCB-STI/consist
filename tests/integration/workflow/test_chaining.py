from pathlib import Path
from unittest.mock import patch
from typing import List, Dict, Optional, Union

from sqlmodel import Session, select
import pandas as pd

from consist.core.tracker import Tracker
from consist.api import load_df
from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink
from consist.integrations.containers.api import run_container
from consist.integrations.containers.backends import ContainerBackend


# --- Mock Backend for Container Tests ---


class MockChainingBackend(ContainerBackend):
    """
    A mock container backend used to simulate container execution for testing
    Consist's chaining and caching logic.

    This mock is designed to simulate a container that produces a specific
    output file, allowing tests to verify how Consist tracks provenance
    from containerized steps to subsequent Python steps.

    Attributes
    ----------
    run_count : int
        A counter that tracks how many times the `run` method of this mock backend has been called.
        Used to assert whether a container execution was a cache hit or miss.
    """

    def __init__(self, pull_latest: bool = False) -> None:
        """
        Initializes the MockChainingBackend.

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
        return f"sha256:mock_digest_{image}"

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
        is detected, creates a dummy `container_output.csv` file within the host path
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

        # Simulate the container writing the expected output file
        # We look for the output mount in volumes
        for host_path, container_path in volumes.items():
            if "out" in str(host_path):
                p = Path(host_path)
                p.mkdir(parents=True, exist_ok=True)
                (p / "container_output.csv").write_text("col1,col2\n1,2")

        return True


# --- Existing Tests ---


def test_pipeline_chaining(tracker: Tracker):
    """
    Tests the fundamental pipeline chaining mechanism: passing an `Artifact` object
    from one run directly into another as an input.

    This test verifies Consist's ability to automatically establish lineage
    between runs when an output `Artifact` of one run becomes an input to a subsequent run.
    This is a core feature for building reproducible multi-step workflows.

    What happens:
    1. A `Tracker` instance is initialized.
    2. **Phase 1 (Generation)**: A first run ("run_gen") creates a dummy `data.csv` file
       and logs it as an output `Artifact` ("my_data").
    3. **Phase 2 (Consumption)**: A second run ("run_con") is started, taking the
       `generated_artifact` object directly as an input.
    4. **Phase 3 (Verification)**: The database is queried for artifacts and links.

    What's checked:
    - The `generated_artifact` is correctly associated with "run_gen".
    - The `input_artifact` in "run_con" correctly identifies the `generated_artifact`
      by its key and URI.
    - The database records reflect that there is one unique `Artifact` and two
      `RunArtifactLink` entries (one for "run_gen" output, one for "run_con" input).
    """
    # --- Phase 1: Generation ---
    generated_artifact = None

    with tracker.start_run("run_gen", model="generator"):
        outfile = tracker.run_dir / "data.csv"
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.write_text("id,val\n1,100")

        generated_artifact = tracker.log_artifact(
            str(outfile), key="my_data", direction="output"
        )

    assert generated_artifact is not None
    assert generated_artifact.run_id == "run_gen"

    # --- Phase 2: Consumption ---

    with tracker.start_run("run_con", model="consumer", inputs=[generated_artifact]):
        assert len(tracker.current_consist.inputs) == 1
        input_artifact = tracker.current_consist.inputs[0]

        # Verify Identity
        assert input_artifact.key == "my_data"
        assert input_artifact.container_uri == generated_artifact.container_uri

    # --- Phase 3: Verification (Database) ---
    with Session(tracker.engine) as session:
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 1

        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 2


def test_implicit_file_chaining(tracker: Tracker):
    """
    Tests implicit file chaining: when a `Path` string (not an `Artifact` object)
    is passed as input, Consist should correctly link it to a previous run if the URI matches.

    This test verifies Consist's "Auto-Forking" feature, where it can automatically
    detect lineage from a file's URI. If a file path passed as an input matches the
    URI of a previously logged output artifact, Consist should link them,
    establishing the provenance chain.

    What happens:
    1. A `Tracker` instance is initialized.
    2. **Run A**: Generates a file (`handoff.csv`) and logs it as an output artifact.
    3. **Run B**: Takes the *file path string* (`handoff.csv`) as an input.
    4. **Verification**: The database is queried for `RunArtifactLink` entries.

    What's checked:
    - In-memory state: The input artifact for "run_B" correctly has `run_id` set
      to "run_A", indicating implicit lineage discovery.
    - Database links: There are exactly two `RunArtifactLink` entries, and they both
      refer to the same `artifact_id`, confirming that the same physical artifact
      is linked as an output for "run_A" and an input for "run_B".
    """
    # 1. Run A generates a file
    file_path = tracker.run_dir / "handoff.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with tracker.start_run("run_A", model="step1"):
        file_path.write_text("a,b\n1,2")
        tracker.log_artifact(str(file_path), key="handoff", direction="output")

    # 2. Run B inputs that FILE PATH
    with tracker.start_run("run_B", model="step2"):
        tracker.log_artifact(str(file_path), key="handoff", direction="input")
        # Verify In-Memory State
        inp = tracker.current_consist.inputs[0]
        assert inp.run_id == "run_A"

        # 3. Verify Database Links
    with Session(tracker.engine) as session:
        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 2
        assert links[0].artifact_id == links[1].artifact_id


# --- NEW TEST: Container Chaining & Caching ---


def test_container_chaining_with_caching(tracker: Tracker):
    """
    Tests a "Hybrid Workflow" scenario involving both containerized steps and Python steps,
    with a focus on correct chaining and caching behavior.

    This test verifies that:
    1. Outputs from a containerized run can be correctly identified as inputs for a Python run.
    2. Repeated containerized runs with identical parameters result in cache hits.
    3. Changes in container parameters (e.g., image tag) correctly invalidate the cache.

    What happens:
    1. A `MockChainingBackend` is used to simulate container execution.
    2. `IdentityManager.get_code_version` is patched to return a stable hash,
       preventing spurious cache misses due to dirty Git states.
    3. **Phase 1 (Container Generator)**: `run_container` is called to simulate
       a container producing an output file (`container_output.csv`).
    4. **Phase 2 (Python Consumer)**: A standard Python run (`tracker.start_run`)
       takes the file generated by the container as an input.
    5. **Phase 3 (Cache HIT)**: `run_container` is called again with the *exact same*
       parameters as Phase 1 (except for a different `run_id`).
    6. **Phase 4 (Cache MISS)**: `run_container` is called with a *modified image tag*
       compared to Phase 1.

    What's checked:
    - After Phase 1, the mock backend's `run_count` is 1, and the expected output file exists.
    - In Phase 2, the input artifact for the Python consumer correctly shows its `run_id`
      as "container_run_1", demonstrating successful lineage tracking from containers.
    - After Phase 3, the mock backend's `run_count` remains 1, confirming a cache hit.
    - After Phase 4, the mock backend's `run_count` increments to 2, confirming a cache miss
      due to the change in image tag.
    """
    # Setup directories
    host_out_dir = (tracker.run_dir / "host_outputs").resolve()
    host_out_dir.mkdir(parents=True, exist_ok=True)

    expected_output_file = host_out_dir / "container_output.csv"

    # Instantiate Mock Backend
    mock_backend = MockChainingBackend()

    # Patch DockerBackend to use our mock
    # FIX: Patch IdentityManager to return a stable code hash.
    # Without this, if the repo is dirty, get_code_version() returns a timestamped hash
    # which changes every second, causing a false Cache Miss.
    with (
        patch(
            "consist.integrations.containers.api.DockerBackend",
            return_value=mock_backend,
        ),
        patch(
            "consist.core.identity.IdentityManager.get_code_version",
            return_value="stable_git_hash",
        ),
    ):
        # --- Phase 1: Container Generator ---
        run_container(
            tracker=tracker,
            run_id="container_run_1",
            image="data_prep:v1",
            command=["python", "prep.py"],
            volumes={str(host_out_dir): "/data/out"},
            inputs=[],
            outputs=[expected_output_file],
            backend_type="docker",
        )

        assert mock_backend.run_count == 1
        assert expected_output_file.exists()

        # --- Phase 2: Python Consumer (Lineage Check) ---
        # We start a standard python run that consumes the file created by the container

        with tracker.start_run(
            "python_consumer_1", model="analysis", inputs=[str(expected_output_file)]
        ):
            # Check Consist's In-Memory state
            # It should have auto-discovered that this file came from 'container_run_1'
            assert len(tracker.current_consist.inputs) == 1
            input_art = tracker.current_consist.inputs[0]

            assert input_art.run_id == "container_run_1"
            assert input_art.key == "container_output.csv"  # Auto-derived from filename

        # --- Phase 3: Cache HIT (Repeat Container) ---
        # Run exactly the same container step again
        run_container(
            tracker=tracker,
            run_id="container_run_2",  # Different ID, but logic is same
            image="data_prep:v1",
            command=["python", "prep.py"],
            volumes={str(host_out_dir): "/data/out"},
            inputs=[],
            outputs=[expected_output_file],
            backend_type="docker",
        )

        # Count should NOT increase
        assert mock_backend.run_count == 1

        # --- Phase 4: Cache MISS (Change Image) ---
        # Change the image tag
        run_container(
            tracker=tracker,
            run_id="container_run_3",
            image="data_prep:v2",  # CHANGED
            command=["python", "prep.py"],
            volumes={str(host_out_dir): "/data/out"},
            inputs=[],
            outputs=[expected_output_file],
            backend_type="docker",
        )

        # Count SHOULD increase
        assert mock_backend.run_count == 2


def test_cross_tracker_input_hash_ignores_container_uri(tmp_path: Path):
    """
    Ensure cache reuse when an upstream artifact is moved and consumed by a different
    tracker, as long as provenance (run_id) is preserved.
    """
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"
    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)

    # Producer run (Tracker A)
    with tracker_a.start_run("producer", model="producer"):
        source_path = tracker_a.run_dir / "data.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("a,b\n1,2")
        produced = tracker_a.log_artifact(
            str(source_path), key="shared_data", direction="output"
        )

    # Move the artifact to a different workspace location (Tracker B)
    moved_path = tracker_b.run_dir / "moved" / "data.csv"
    moved_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.rename(moved_path)

    moved_artifact = Artifact(
        key=produced.key,
        container_uri=tracker_b.fs.virtualize_path(str(moved_path)),
        driver=produced.driver,
        run_id=produced.run_id,
        hash=produced.hash,
        table_path=produced.table_path,
        array_path=produced.array_path,
    )

    # First consumer run: establish cache entry using original artifact
    with tracker_a.start_run("consumer_a", model="analysis", inputs=[produced]):
        out_path = tracker_a.run_dir / "out.csv"
        out_path.write_text("x,y\n10,20")
        tracker_a.log_artifact(str(out_path), key="out", direction="output")

    # Second consumer run: should be a cache hit even though container_uri changed
    with tracker_b.start_run("consumer_b", model="analysis", inputs=[moved_artifact]):
        assert tracker_b.is_cached


def test_cross_tracker_ghost_mode_after_ingest(tmp_path: Path):
    """
    Demonstrate ghost mode across trackers: ingest data, delete file, and load
    from the DB in a separate tracker/run.
    """
    db_path = str(tmp_path / "provenance.db")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"
    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)

    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    file_path = tracker_a.run_dir / "data.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)

    with tracker_a.start_run("producer", model="producer"):
        artifact = tracker_a.log_artifact(file_path, key="ghost_table")

    with tracker_a.start_run("ingest", model="ingester", inputs=[artifact]):
        tracker_a.ingest(artifact, df)

    file_path.unlink()
    assert not file_path.exists()

    with tracker_b.start_run("consumer", model="consumer", inputs=[artifact]):
        ghost_df = load_df(artifact, tracker=tracker_b, db_fallback="always")

    assert len(ghost_df) == 3
    assert ghost_df.iloc[0]["val"] == "a"
