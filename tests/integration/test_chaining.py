# tests/test_chaining.py

import json
from sqlmodel import Session, select, create_engine

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink


def test_pipeline_chaining(tmp_path):
    """
    Tests passing an Artifact object from one run directly into another.

    Scenario:
    1. Run A (Generator) creates a file and logs it as output.
    2. We capture the returned Artifact object.
    3. Run B (Consumer) accepts that Artifact object as an input.

    Verifies:
    - The Artifact retains its identity (Key/URI).
    - The Tracker internally resolves the path using the artifact's runtime cache.
    - Database links are correctly established (Run A -> Out, Run B -> In).
    """
    # Setup
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # --- Phase 1: Generation ---
    generated_artifact = None

    with tracker.start_run("run_gen", model="generator"):
        # Create a physical file
        outfile = run_dir / "data.csv"
        outfile.write_text("id,val\n1,100")

        # Log it
        # Note: Tracker will set artifact.abs_path internally
        generated_artifact = tracker.log_artifact(
            str(outfile), key="my_data", direction="output"
        )

    assert generated_artifact is not None
    assert generated_artifact.abs_path is not None
    assert generated_artifact.run_id == "run_gen"

    # --- Phase 2: Consumption ---

    with tracker.start_run("run_con", model="consumer"):
        # PASS THE OBJECT DIRECTLY!
        # No manual path resolution needed.
        input_artifact = tracker.log_artifact(generated_artifact, direction="input")

        # Verify it's the same logical artifact
        assert input_artifact.key == "my_data"
        assert input_artifact.uri == generated_artifact.uri
        # Ideally, it's the same Python object if we passed it in,
        # or a clone with the same metadata.

    # --- Phase 3: Verification (Database) ---
    engine = create_engine(f"duckdb:///{db_path}")
    with Session(engine) as session:
        # 1. Check Artifact exists (should be 1 unique artifact, not 2)
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 1
        db_art = artifacts[0]
        assert db_art.key == "my_data"

        # 2. Check Links (One Input, One Output)
        links = session.exec(select(RunArtifactLink)).all()
        assert len(links) == 2

        gen_link = next(l for l in links if l.run_id == "run_gen")
        con_link = next(l for l in links if l.run_id == "run_con")

        assert gen_link.direction == "output"
        assert gen_link.artifact_id == db_art.id

        assert con_link.direction == "input"
        assert con_link.artifact_id == db_art.id

    # --- Phase 4: Verification (JSON) ---
    # Check Consumer's JSON log
    with open(run_dir / "consist.json") as f:
        # Note: simplistic read; in real usage each run gets its own folder
        # or the file is overwritten. Since we used same run_dir, it's overwritten by run_con.
        data = json.load(f)

    assert data["run"]["id"] == "run_con"
    assert len(data["inputs"]) == 1
    assert data["inputs"][0]["key"] == "my_data"
    # Ensure absolute path didn't leak into JSON
    assert "_abs_path" not in data["inputs"][0]
