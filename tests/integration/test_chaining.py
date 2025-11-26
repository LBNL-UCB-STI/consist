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

    with tracker.start_run("run_con", model="consumer", inputs=[generated_artifact]):
        # Note: We don't need to call log_artifact inside here anymore.
        # But we can check that it happened.

        assert len(tracker.current_consist.inputs) == 1
        input_artifact = tracker.current_consist.inputs[0]

        # Verify Identity
        assert input_artifact.key == "my_data"
        assert input_artifact.uri == generated_artifact.uri

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
    with tracker.start_run("run_A", model="step1"):
        file_path.write_text("a,b\n1,2")
        tracker.log_artifact(str(file_path), key="handoff", direction="output")

    # 2. Run B inputs that FILE PATH
    with tracker.start_run("run_B", model="step2"):
        # We pass the string path.
        # Tracker should auto-discover that this URI belongs to Run A.
        tracker.log_artifact(str(file_path), key="handoff", direction="input")

        # Verify In-Memory State
        inp = tracker.current_consist.inputs[0]
        assert inp.run_id == "run_A"  # <--- Success if this is set!

    # 3. Verify Database Links
    engine = create_engine(f"duckdb:///{db_path}")
    with Session(engine) as session:
        # Should reuse the SAME artifact UUID
        artifacts = session.exec(select(Artifact)).all()
        assert len(artifacts) == 1

        links = session.exec(select(RunArtifactLink)).all()
        # Run A -> Out -> Art 1
        # Run B -> In  -> Art 1
        assert len(links) == 2
        assert links[0].artifact_id == links[1].artifact_id
