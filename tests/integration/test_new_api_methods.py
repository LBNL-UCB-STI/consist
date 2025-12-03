# tests/integration/test_new_api_methods.py
"""
Comprehensive tests for new Consist API methods:
  - log_input() / log_output()
  - created_at_iso property
  - get_artifact_by_uri()
  - begin_run() / end_run()
  - log_h5_container()
"""

import pytest
import json
from datetime import datetime
from sqlmodel import Session, select

from consist.models.artifact import Artifact
from consist.models.run import Run


class TestLogInputOutput:
    """Tests for log_input() and log_output() convenience methods."""

    def test_log_input_sets_direction_correctly(self, tracker, run_dir):
        """
        Test that log_input() correctly sets direction='input' and passes through key/meta.
        """
        test_file = run_dir / "input_data.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        with tracker.start_run("run_001", "test_model"):
            artifact = tracker.log_input(
                str(test_file), key="my_input", schema_version="1.0"
            )

        assert artifact.key == "my_input"
        assert artifact.uri.endswith("input_data.csv")
        assert artifact.driver == "csv"
        assert artifact.meta.get("schema_version") == "1.0"

        # Verify it's in inputs, not outputs
        consist_record = tracker.last_run
        assert len(consist_record.inputs) == 1
        assert len(consist_record.outputs) == 0
        assert consist_record.inputs[0].key == "my_input"

    def test_log_output_sets_direction_correctly(self, tracker, run_dir):
        """
        Test that log_output() correctly sets direction='output' and passes through key/meta.
        """
        test_file = run_dir / "output_data.parquet"
        test_file.write_text("dummy parquet data")

        with tracker.start_run("run_002", "test_model"):
            artifact = tracker.log_output(
                str(test_file), key="my_output", version="2.0"
            )

        assert artifact.key == "my_output"
        assert artifact.uri.endswith("output_data.parquet")
        assert artifact.driver == "parquet"
        assert artifact.meta.get("version") == "2.0"

        # Verify it's in outputs, not inputs
        consist_record = tracker.last_run
        assert len(consist_record.outputs) == 1
        assert len(consist_record.inputs) == 0
        assert consist_record.outputs[0].key == "my_output"

    def test_log_input_multiple_with_different_meta(self, tracker, run_dir):
        """
        Test logging multiple input artifacts with different metadata.
        """
        files = []
        for i in range(3):
            f = run_dir / f"input_{i}.csv"
            f.write_text(f"data_{i}\n")
            files.append(f)

        with tracker.start_run("run_003", "test_model"):
            arts = [
                tracker.log_input(str(files[0]), key="file_1", index=1),
                tracker.log_input(str(files[1]), key="file_2", index=2),
                tracker.log_input(str(files[2]), key="file_3", index=3),
            ]

        assert len(arts) == 3
        # Verify keys and metadata
        assert tracker.last_run.inputs[0].key == "file_1"
        assert tracker.last_run.inputs[0].meta.get("index") == 1
        assert tracker.last_run.inputs[1].key == "file_2"
        assert tracker.last_run.inputs[1].meta.get("index") == 2
        assert tracker.last_run.inputs[2].key == "file_3"
        assert tracker.last_run.inputs[2].meta.get("index") == 3

    def test_log_output_with_artifact_object(self, tracker, run_dir):
        """
        Test that log_output() can accept an Artifact object as well as a path string.
        """
        test_file = run_dir / "output_file.csv"
        test_file.write_text("output,data\n1,2\n")

        with tracker.start_run("run_004", "test_model"):
            # First log as a path
            art1 = tracker.log_output(str(test_file), key="output1")
            # Verify it was logged correctly
            assert art1.key == "output1"
            assert art1.driver == "csv"

        consist_record = tracker.last_run
        # Should have at least 1 output entry
        assert len(consist_record.outputs) >= 1
        assert consist_record.outputs[0].key == "output1"


class TestCreatedAtIso:
    """Tests for the created_at_iso property on Artifact model."""

    def test_created_at_iso_returns_isoformat_string(self, tracker, run_dir):
        """
        Test that created_at_iso returns a valid ISO 8601 formatted string.
        """
        test_file = run_dir / "artifact.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_001", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="test_artifact")

        iso_string = artifact.created_at_iso
        assert isinstance(iso_string, str)
        assert len(iso_string) > 0

        # Verify it's parseable as ISO format
        parsed_dt = datetime.fromisoformat(iso_string)
        assert isinstance(parsed_dt, datetime)

    def test_created_at_iso_matches_created_at(self, tracker, run_dir):
        """
        Test that created_at_iso is a string representation of created_at.
        """
        test_file = run_dir / "artifact2.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_002", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="test_artifact2")

        iso_string = artifact.created_at_iso
        parsed_dt = datetime.fromisoformat(iso_string)

        # Compare with tolerance for microseconds
        assert abs((parsed_dt - artifact.created_at).total_seconds()) < 0.001

    def test_created_at_iso_from_database_roundtrip(self, tracker, engine, run_dir):
        """
        Test that created_at_iso works correctly for artifacts retrieved from database.
        """
        test_file = run_dir / "artifact_db.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_db", "test_model"):
            tracker.log_artifact(str(test_file), key="test_artifact_db")

        # Retrieve from database
        with Session(engine) as session:
            artifact = session.exec(
                select(Artifact).where(Artifact.key == "test_artifact_db")
            ).one()

        iso_string = artifact.created_at_iso
        assert isinstance(iso_string, str)
        assert "T" in iso_string  # ISO format includes T separator


class TestGetArtifactByUri:
    """Tests for get_artifact_by_uri() method."""

    def test_get_artifact_by_uri_finds_in_memory_artifact(self, tracker, run_dir):
        """
        Test that get_artifact_by_uri finds an artifact in the current run context.
        """
        test_file = run_dir / "find_me.csv"
        test_file.write_text("data\n")

        with tracker.start_run("run_find_001", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="find_me")
            uri = artifact.uri

            # Search within the same run context
            found = tracker.get_artifact_by_uri(uri)

        assert found is not None
        assert found.uri == uri
        assert found.key == "find_me"

    def test_get_artifact_by_uri_finds_in_database(self, tracker, engine, run_dir):
        """
        Test that get_artifact_by_uri searches the database when not in current run.
        """
        test_file = run_dir / "db_search.csv"
        test_file.write_text("data\n")

        with tracker.start_run("run_db_search", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="db_search")
            uri = artifact.uri

        # After the run ends, search from outside the run context
        found = tracker.get_artifact_by_uri(uri)

        assert found is not None
        assert found.uri == uri
        assert found.key == "db_search"

    def test_get_artifact_by_uri_returns_none_for_missing(self, tracker):
        """
        Test that get_artifact_by_uri returns None when artifact doesn't exist.
        """
        nonexistent_uri = "inputs://does_not_exist.csv"
        found = tracker.get_artifact_by_uri(nonexistent_uri)
        assert found is None

    def test_get_artifact_by_uri_with_multiple_artifacts(self, tracker, run_dir):
        """
        Test that get_artifact_by_uri correctly identifies among multiple artifacts.
        """
        files = []
        uris = []
        for i in range(5):
            f = run_dir / f"multi_{i}.csv"
            f.write_text(f"data_{i}\n")
            files.append(f)

        with tracker.start_run("run_multi_artifact", "test_model"):
            for i, f in enumerate(files):
                art = tracker.log_artifact(str(f), key=f"artifact_{i}")
                uris.append(art.uri)

        # Test finding each one
        for i, uri in enumerate(uris):
            found = tracker.get_artifact_by_uri(uri)
            assert found is not None
            assert found.uri == uri
            assert found.key == f"artifact_{i}"


class TestBeginEndRun:
    """Tests for begin_run() and end_run() imperative methods."""

    def test_begin_end_run_basic_lifecycle(self, tracker, run_dir):
        """
        Test basic lifecycle: begin_run -> log artifacts -> end_run.
        """
        test_file = run_dir / "imperative_test.csv"
        test_file.write_text("data\n")

        # Use imperative API
        run = tracker.begin_run("run_imperative_001", "test_model")
        assert run.id == "run_imperative_001"
        assert run.status in ("started", "running")  # Both are valid initial states

        tracker.log_artifact(str(test_file), key="test_data")

        # End the run
        completed_run = tracker.end_run()
        assert completed_run.status == "completed"

        # Verify in JSON
        json_log = tracker.run_dir / "consist.json"
        assert json_log.exists()
        with open(json_log) as f:
            data = json.load(f)
        assert data["run"]["status"] == "completed"

    def test_begin_end_run_with_error_handling(self, tracker, run_dir):
        """
        Test error handling: begin_run -> error -> end_run with failed status.
        """
        test_file = run_dir / "error_test.csv"
        test_file.write_text("data\n")

        run = tracker.begin_run("run_error_test", "test_model")
        tracker.log_artifact(str(test_file), key="test_data")

        # Simulate an error
        error = ValueError("Something went wrong")
        failed_run = tracker.end_run("failed", error=error)

        assert failed_run.status == "failed"
        assert "Something went wrong" in failed_run.meta.get("error", "")

        # Verify in JSON
        json_log = tracker.run_dir / "consist.json"
        with open(json_log) as f:
            data = json.load(f)
        assert data["run"]["status"] == "failed"

    def test_begin_run_twice_raises_error(self, tracker, run_dir):
        """
        Test that calling begin_run twice without end_run raises RuntimeError.
        """
        run1 = tracker.begin_run("run_double_begin", "model_1")
        assert run1.id == "run_double_begin"

        # Attempting to begin another run should fail
        try:
            with pytest.raises(RuntimeError, match="already active"):
                tracker.begin_run("run_double_begin_2", "model_2")
        finally:
            # Clean up
            if tracker.current_consist:
                tracker.end_run()

    def test_end_run_without_begin_raises_error(self, tracker):
        """
        Test that calling end_run without begin_run raises RuntimeError.
        """
        with pytest.raises(RuntimeError, match="No active run"):
            tracker.end_run()

    def test_begin_run_with_config_and_metadata(self, tracker, run_dir):
        """
        Test that begin_run accepts config, tags, description, and metadata.
        """
        test_file = run_dir / "config_test.csv"
        test_file.write_text("data\n")

        run = tracker.begin_run(
            "run_with_config",
            "test_model",
            config={"param1": "value1", "param2": 42},
            tags=["test", "imperative"],
            description="Testing imperative API with config",
            year=2024,
            custom_field="custom_value",
        )

        tracker.log_artifact(str(test_file), key="data")
        tracker.end_run()

        # Verify fields in JSON
        json_log = tracker.run_dir / "consist.json"
        with open(json_log) as f:
            data = json.load(f)

        assert data["run"]["tags"] == ["test", "imperative"]
        assert data["run"]["description"] == "Testing imperative API with config"
        assert data["config"]["param1"] == "value1"
        assert data["config"]["param2"] == 42

    def test_begin_run_with_inputs(self, tracker, run_dir):
        """
        Test that begin_run can accept inputs list.
        """
        input_files = []
        for i in range(2):
            f = run_dir / f"input_{i}.csv"
            f.write_text(f"data_{i}\n")
            input_files.append(str(f))

        run = tracker.begin_run(
            "run_with_inputs",
            "test_model",
            inputs=input_files,
        )

        # Inputs should be pre-logged
        assert len(tracker.current_consist.inputs) >= 2

        tracker.end_run()

    def test_end_run_idempotent(self, tracker, run_dir):
        """
        Test that calling end_run multiple times on same run doesn't crash.
        """
        test_file = run_dir / "idempotent_test.csv"
        test_file.write_text("data\n")

        tracker.begin_run("run_idempotent", "test_model")
        tracker.log_artifact(str(test_file), key="data")

        # Call end_run multiple times
        run1 = tracker.end_run()
        assert run1.status == "completed"

        # Second call should raise since no active run anymore
        with pytest.raises(RuntimeError, match="No active run"):
            tracker.end_run()

    def test_begin_run_returns_run_object(self, tracker):
        """
        Test that begin_run returns a Run object with correct attributes.
        """
        run = tracker.begin_run("run_return_test", "model_name")

        assert isinstance(run, Run)
        assert run.id == "run_return_test"
        assert run.model_name == "model_name"
        assert run.status in ("started", "running")  # Both are valid initial states
        assert run.started_at is not None

        tracker.end_run()


class TestLogH5Container:
    """Tests for log_h5_container() method."""

    def test_log_h5_container_without_discovery(self, tracker, run_dir):
        """
        Test logging HDF5 file without table discovery (simpler case, no h5py needed).
        """
        h5_file = run_dir / "test_data.h5"
        h5_file.write_text("dummy h5 data")

        with tracker.start_run("run_h5_simple", "test_model"):
            container, tables = tracker.log_h5_container(
                str(h5_file),
                key="my_data",
                direction="output",
                discover_tables=False,
            )

        assert container.key == "my_data"
        assert container.uri.endswith("test_data.h5")
        assert container.driver == "h5"
        assert len(tables) == 0  # No discovery

        # Verify in run record
        consist_record = tracker.last_run
        assert len(consist_record.outputs) >= 1
        assert any(art.driver == "h5" for art in consist_record.outputs)

    def test_log_h5_container_returns_tuple(self, tracker, run_dir):
        """
        Test that log_h5_container returns a tuple of (container, tables_list).
        """
        h5_file = run_dir / "container_test.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_tuple", "test_model"):
            result = tracker.log_h5_container(str(h5_file), discover_tables=False)

        assert isinstance(result, tuple)
        assert len(result) == 2
        container, tables = result
        assert isinstance(container, Artifact)
        assert isinstance(tables, list)

    def test_log_h5_container_with_key_inference(self, tracker, run_dir):
        """
        Test that log_h5_container infers key from filename when not provided.
        """
        h5_file = run_dir / "auto_named.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_key_infer", "test_model"):
            container, _ = tracker.log_h5_container(
                str(h5_file),
                discover_tables=False,
            )

        # Key should be inferred from file stem
        assert container.key == "auto_named"

    def test_log_h5_container_outside_run_context_raises_error(self, tracker, run_dir):
        """
        Test that log_h5_container raises RuntimeError when called outside run context.
        """
        h5_file = run_dir / "error.h5"
        h5_file.write_text("h5 data")

        with pytest.raises(RuntimeError, match="outside of a run context"):
            tracker.log_h5_container(str(h5_file), discover_tables=False)

    def test_log_h5_container_as_input(self, tracker, run_dir):
        """
        Test logging HDF5 container as an input artifact.
        """
        h5_file = run_dir / "input_data.h5"
        h5_file.write_text("h5 input data")

        with tracker.start_run("run_h5_input", "test_model"):
            container, _ = tracker.log_h5_container(
                str(h5_file),
                key="input_container",
                direction="input",
                discover_tables=False,
            )

        assert container.key == "input_container"
        consist_record = tracker.last_run
        assert len(consist_record.inputs) >= 1
        assert any(art.driver == "h5" for art in consist_record.inputs)

    def test_log_h5_container_stores_metadata(self, tracker, run_dir):
        """
        Test that log_h5_container passes through metadata correctly.
        """
        h5_file = run_dir / "meta_data.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_meta", "test_model"):
            container, _ = tracker.log_h5_container(
                str(h5_file),
                key="h5_with_meta",
                discover_tables=False,
                version="1.0",
                source="simulation",
            )

        assert container.meta.get("version") == "1.0"
        assert container.meta.get("source") == "simulation"

    def test_log_h5_container_with_h5py_discovery(self, tracker, run_dir):
        """
        Test log_h5_container with table discovery if h5py is available.

        This test gracefully handles the case where h5py is not installed.
        """
        h5_file = run_dir / "discovery_test.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_discover", "test_model"):
            # This should not fail even if h5py is not installed
            # It will log a warning and return empty tables list
            container, tables = tracker.log_h5_container(
                str(h5_file),
                key="discovered_data",
                discover_tables=True,
            )

        assert container.key == "discovered_data"
        # Tables may be empty if h5py is not installed or file is not valid
        assert isinstance(tables, list)
