from pathlib import Path
from unittest.mock import patch
from typing import List, Dict, Optional, Union

from sqlmodel import Session, select, create_engine

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink
from consist.integrations.containers.api import run_container
from consist.integrations.containers.backends import ContainerBackend


# --- Mock Backend for Container Tests ---


class MockChainingBackend(ContainerBackend):
    """
    Simulates a container that writes specific output files.
    """

    def __init__(self, pull_latest=False):
        self.run_count = 0

    def resolve_image_digest(self, image: str) -> str:
        return f"sha256:mock_digest_{image}"

    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
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


def test_pipeline_chaining(tmp_path):
    """
    Tests passing an Artifact object from one run directly into another.
    """
    # Setup
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # --- Phase 1: Generation ---
    generated_artifact = None

    with tracker.start_run("run_gen", model="generator"):
        outfile = run_dir / "data.csv"
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
        assert input_artifact.uri == generated_artifact.uri

    # --- Phase 3: Verification (Database) ---
    engine = create_engine(f"duckdb:///{db_path}")
    with Session(engine) as session:
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 1
        db_art = artifacts[0]

        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 2


def test_implicit_file_chaining(tmp_path):
    """
    Tests that passing a FILE PATH string (not an Artifact object)
    still correctly links to the previous run if the URI matches.
    """
    run_dir = tmp_path / "implicit_runs"
    db_path = str(tmp_path / "implicit.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # 1. Run A generates a file
    file_path = run_dir / "handoff.csv"
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
    engine = create_engine(f"duckdb:///{db_path}")
    with Session(engine) as session:
        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 2
        assert links[0].artifact_id == links[1].artifact_id


# --- NEW TEST: Container Chaining & Caching ---


def test_container_chaining_with_caching(tmp_path):
    """
    Tests the "Hybrid Workflow":
    1. Container Run (Generator)
    2. Python Run (Consumer) - Verifies lineage linkage.
    3. Container Run (Repeat) - Verifies Cache Hit.
    4. Container Run (Changed) - Verifies Cache Miss.
    """
    run_dir = tmp_path / "hybrid_runs"
    db_path = str(tmp_path / "hybrid.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # Setup directories
    host_out_dir = (run_dir / "host_outputs").resolve()
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
