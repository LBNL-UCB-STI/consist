from pathlib import Path
from typing import Dict, List, Literal, Mapping, Optional, Union
from unittest.mock import patch

import pandas as pd

from consist.api import load_df
from consist.core.tracker import Tracker
from consist.integrations.containers.api import run_container
from consist.integrations.containers.backends import ContainerBackend
from tests.helpers.artifact_factory import make_artifact


class MockChainingBackend(ContainerBackend):
    """
    A mock container backend used to simulate container execution for testing
    Consist's chaining and caching logic.
    """

    def __init__(self, pull_latest: bool = False) -> None:
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
        volume_modes: Optional[Mapping[str, Literal["ro", "rw"]]] = None,
    ) -> bool:
        self.run_count += 1

        for host_path, _container_path in volumes.items():
            if "out" in str(host_path):
                p = Path(host_path)
                p.mkdir(parents=True, exist_ok=True)
                (p / "container_output.csv").write_text("col1,col2\n1,2")

        return True


def test_pipeline_chaining(tracker: Tracker):
    """
    Tests the fundamental pipeline chaining mechanism: passing an `Artifact` object
    from one run directly into another as an input.
    """
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

    with tracker.start_run("run_con", model="consumer", inputs=[generated_artifact]):
        assert len(tracker.current_consist.inputs) == 1
        input_artifact = tracker.current_consist.inputs[0]

        assert input_artifact.key == "my_data"
        assert input_artifact.primary_location == generated_artifact.primary_location

    artifacts_df = tracker.query_df("SELECT id FROM main.artifact")
    assert len(artifacts_df) == 1

    bindings_df = tracker.query_df(
        "SELECT run_id, artifact_id, direction FROM main.run_binding"
    )
    assert len(bindings_df) == 2


def test_implicit_file_chaining(tracker: Tracker):
    """
    Tests implicit file chaining: when a `Path` string (not an `Artifact` object)
    is passed to `log_artifact(..., direction='input')`, Consist links it to the previous run.
    """
    file_path = tracker.run_dir / "handoff.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with tracker.start_run("run_A", model="step1"):
        file_path.write_text("a,b\n1,2")
        tracker.log_artifact(str(file_path), key="handoff", direction="output")

    with tracker.start_run("run_B", model="step2"):
        tracker.log_artifact(str(file_path), key="handoff", direction="input")
        inp = tracker.current_consist.inputs[0]
        assert inp.run_id == "run_A"

    bindings_df = tracker.query_df(
        "SELECT CAST(artifact_id AS VARCHAR) AS artifact_id, direction "
        "FROM main.run_binding ORDER BY run_id"
    )
    assert len(bindings_df) == 2
    assert bindings_df.iloc[0]["artifact_id"] == bindings_df.iloc[1]["artifact_id"]


def test_container_chaining_with_caching(tracker: Tracker):
    """
    Tests a "Hybrid Workflow" scenario involving both containerized steps and Python steps,
    with a focus on correct chaining and caching behavior.
    """
    host_out_dir = (tracker.run_dir / "host_outputs").resolve()
    host_out_dir.mkdir(parents=True, exist_ok=True)

    expected_output_file = host_out_dir / "container_output.csv"
    mock_backend = MockChainingBackend()

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
            strict_mounts=False,
        )

        assert mock_backend.run_count == 1
        assert expected_output_file.exists()

        # --- Phase 2: Python Consumer (Lineage Check) ---
        with tracker.start_run(
            "python_consumer_1", model="analysis", inputs=[str(expected_output_file)]
        ):
            assert len(tracker.current_consist.inputs) == 1
            input_art = tracker.current_consist.inputs[0]

            assert input_art.run_id == "container_run_1"
            assert input_art.key == "container_output"

        # --- Phase 3: Cache HIT (Repeat Container) ---
        run_container(
            tracker=tracker,
            run_id="container_run_2",
            image="data_prep:v1",
            command=["python", "prep.py"],
            volumes={str(host_out_dir): "/data/out"},
            inputs=[],
            outputs=[expected_output_file],
            backend_type="docker",
            strict_mounts=False,
        )

        assert mock_backend.run_count == 1

        # --- Phase 4: Cache MISS (Change Image) ---
        run_container(
            tracker=tracker,
            run_id="container_run_3",
            image="data_prep:v2",
            command=["python", "prep.py"],
            volumes={str(host_out_dir): "/data/out"},
            inputs=[],
            outputs=[expected_output_file],
            backend_type="docker",
            strict_mounts=False,
        )

        assert mock_backend.run_count == 2


def test_cross_tracker_input_hash_ignores_container_uri(tmp_path: Path):
    """
    Ensure cache reuse when an upstream artifact is moved and consumed by a different
    tracker, as long as provenance (run_id) and content identity are preserved.
    """
    db_path = str(tmp_path / "provenance.duckdb")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"
    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)
    try:
        with tracker_a.start_run("producer", model="producer"):
            source_path = tracker_a.run_dir / "data.csv"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("a,b\n1,2")
            produced = tracker_a.log_artifact(
                str(source_path), key="shared_data", direction="output"
            )

        moved_path = tracker_b.run_dir / "moved" / "data.csv"
        moved_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.rename(moved_path)

        moved_artifact = make_artifact(
            key=produced.key,
            container_uri="workspace://moved/data.csv",
            driver=produced.driver,
            run_id=produced.run_id,
            hash=produced.hash,
            member=produced.member,
        )

        with tracker_a.start_run("consumer_a", model="analysis", inputs=[produced]):
            out_path = tracker_a.run_dir / "out.csv"
            out_path.write_text("x,y\n10,20")
            tracker_a.log_artifact(str(out_path), key="out", direction="output")

        with tracker_b.start_run(
            "consumer_b", model="analysis", inputs=[moved_artifact]
        ):
            assert tracker_b.is_cached
    finally:
        tracker_a.close()
        tracker_b.close()


def test_cross_tracker_ghost_mode_after_ingest(tmp_path: Path):
    """
    Demonstrate ghost mode across trackers: ingest data, delete file, and load
    from the DB in a separate tracker/run.
    """
    db_path = str(tmp_path / "provenance.duckdb")
    run_dir_a = tmp_path / "runs_a"
    run_dir_b = tmp_path / "runs_b"
    tracker_a = Tracker(run_dir=run_dir_a, db_path=db_path)
    tracker_b = Tracker(run_dir=run_dir_b, db_path=db_path)
    try:
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        file_path = tracker_a.run_dir / "data.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path)

        with tracker_a.start_run("producer", model="producer"):
            artifact = tracker_a.log_artifact(file_path, key="ghost_table")

        with tracker_a.start_run("ingest", model="ingester", inputs=[artifact]):
            artifact = tracker_a.ingest(artifact, df)

        file_path.unlink()
        assert not file_path.exists()

        with tracker_b.start_run("consumer", model="consumer", inputs=[artifact]):
            ghost_df = load_df(artifact, tracker=tracker_b, db_fallback="always")

        assert len(ghost_df) == 3
        assert ghost_df.iloc[0]["val"] == "a"
    finally:
        tracker_a.close()
        tracker_b.close()
