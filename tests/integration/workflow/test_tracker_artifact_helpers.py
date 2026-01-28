"""Artifact-oriented convenience APIs: log_input/output, created_at_iso, lookup, H5 container logging."""

from datetime import datetime
from pathlib import Path

import pytest
from sqlmodel import Session, select

from consist.models.artifact import Artifact


class TestLogInputOutput:
    """Tests for log_input() and log_output() convenience methods."""

    def test_log_input_sets_direction_correctly(self, tracker, run_dir: Path):
        test_file = run_dir / "input_data.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        with tracker.start_run("run_001", "test_model"):
            artifact = tracker.log_input(
                str(test_file), key="my_input", schema_version="1.0"
            )

        assert artifact.key == "my_input"
        assert artifact.container_uri.endswith("input_data.csv")
        assert artifact.driver == "csv"
        assert artifact.meta.get("schema_version") == "1.0"

        consist_record = tracker.last_run
        assert len(consist_record.inputs) == 1
        assert len(consist_record.outputs) == 0
        assert consist_record.inputs[0].key == "my_input"

    def test_log_output_sets_direction_correctly(self, tracker, run_dir: Path):
        test_file = run_dir / "output_data.parquet"
        test_file.write_text("dummy parquet data")

        with tracker.start_run("run_002", "test_model"):
            artifact = tracker.log_output(
                str(test_file), key="my_output", version="2.0"
            )

        assert artifact.key == "my_output"
        assert artifact.container_uri.endswith("output_data.parquet")
        assert artifact.driver == "parquet"
        assert artifact.meta.get("version") == "2.0"

        consist_record = tracker.last_run
        assert len(consist_record.outputs) == 1
        assert len(consist_record.inputs) == 0
        assert consist_record.outputs[0].key == "my_output"

    def test_log_input_multiple_with_different_meta(self, tracker, run_dir: Path):
        files = []
        for i in range(3):
            f = run_dir / f"input_{i}.csv"
            f.write_text(f"data_{i}\n")
            files.append(f)

        with tracker.start_run("run_003", "test_model"):
            tracker.log_input(str(files[0]), key="file_1", index=1)
            tracker.log_input(str(files[1]), key="file_2", index=2)
            tracker.log_input(str(files[2]), key="file_3", index=3)

        assert len(tracker.last_run.inputs) == 3
        assert tracker.last_run.inputs[0].key == "file_1"
        assert tracker.last_run.inputs[0].meta.get("index") == 1
        assert tracker.last_run.inputs[1].key == "file_2"
        assert tracker.last_run.inputs[1].meta.get("index") == 2
        assert tracker.last_run.inputs[2].key == "file_3"
        assert tracker.last_run.inputs[2].meta.get("index") == 3

    def test_log_output_with_artifact_object(self, tracker, run_dir: Path):
        test_file = run_dir / "output_file.csv"
        test_file.write_text("output,data\n1,2\n")

        with tracker.start_run("run_004", "test_model"):
            art1 = tracker.log_output(str(test_file), key="output1")
            assert art1.key == "output1"
            assert art1.driver == "csv"

        consist_record = tracker.last_run
        assert len(consist_record.outputs) >= 1
        assert consist_record.outputs[0].key == "output1"

    def test_log_artifacts_logs_mapping(self, tracker, run_dir: Path):
        first = run_dir / "first.csv"
        second = run_dir / "second.csv"
        first.write_text("a,b\n1,2\n")
        second.write_text("c,d\n3,4\n")

        with tracker.start_run("run_005", "test_model"):
            logged = tracker.log_artifacts(
                {"first": first, "second": second},
                batch="demo",
                metadata_by_key={"second": {"kind": "special"}},
            )

        assert set(logged) == {"first", "second"}
        assert logged["first"].meta.get("batch") == "demo"
        assert logged["second"].meta.get("batch") == "demo"
        assert logged["second"].meta.get("kind") == "special"
        assert [art.key for art in tracker.last_run.outputs] == ["first", "second"]

    def test_log_artifacts_requires_run_context(self, tracker):
        with pytest.raises(RuntimeError, match="run context"):
            tracker.log_artifacts({"orphan": "missing.csv"})


class TestCreatedAtIso:
    """Tests for the created_at_iso property on Artifact model."""

    def test_created_at_iso_returns_isoformat_string(self, tracker, run_dir: Path):
        test_file = run_dir / "artifact.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_001", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="test_artifact")

        iso_string = artifact.created_at_iso
        assert isinstance(iso_string, str)
        assert len(iso_string) > 0
        assert isinstance(datetime.fromisoformat(iso_string), datetime)

    def test_created_at_iso_matches_created_at(self, tracker, run_dir: Path):
        test_file = run_dir / "artifact2.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_002", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="test_artifact2")

        iso_string = artifact.created_at_iso
        parsed_dt = datetime.fromisoformat(iso_string)
        assert abs((parsed_dt - artifact.created_at).total_seconds()) < 0.001

    def test_created_at_iso_from_database_roundtrip(
        self, tracker, engine, run_dir: Path
    ):
        test_file = run_dir / "artifact_db.csv"
        test_file.write_text("test\n")

        with tracker.start_run("run_iso_db", "test_model"):
            tracker.log_artifact(str(test_file), key="test_artifact_db")

        with Session(engine) as session:
            artifact = session.exec(
                select(Artifact).where(Artifact.key == "test_artifact_db")
            ).one()

        iso_string = artifact.created_at_iso
        assert isinstance(iso_string, str)
        assert "T" in iso_string


class TestGetArtifactByUri:
    """Tests for get_artifact_by_uri() method."""

    def test_get_artifact_by_uri_finds_in_memory_artifact(self, tracker, run_dir: Path):
        test_file = run_dir / "find_me.csv"
        test_file.write_text("data\n")

        with tracker.start_run("run_find_001", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="find_me")
            uri = artifact.container_uri
            found = tracker.get_artifact_by_uri(uri)

        assert found is not None
        assert found.container_uri == uri
        assert found.key == "find_me"

    def test_get_artifact_by_uri_finds_in_database(
        self, tracker, engine, run_dir: Path
    ):
        test_file = run_dir / "db_search.csv"
        test_file.write_text("data\n")

        with tracker.start_run("run_db_search", "test_model"):
            artifact = tracker.log_artifact(str(test_file), key="db_search")
            uri = artifact.container_uri

        found = tracker.get_artifact_by_uri(uri)

        assert found is not None
        assert found.container_uri == uri
        assert found.key == "db_search"

    def test_get_artifact_by_uri_returns_none_for_missing(self, tracker):
        nonexistent_uri = "inputs://does_not_exist.csv"
        found = tracker.get_artifact_by_uri(nonexistent_uri)
        assert found is None

    def test_get_artifact_by_uri_with_multiple_artifacts(self, tracker, run_dir: Path):
        files = []
        uris = []
        for i in range(5):
            f = run_dir / f"multi_{i}.csv"
            f.write_text(f"data_{i}\n")
            files.append(f)

        with tracker.start_run("run_multi_artifact", "test_model"):
            for i, f in enumerate(files):
                art = tracker.log_artifact(str(f), key=f"artifact_{i}")
                uris.append(art.container_uri)

        for i, uri in enumerate(uris):
            found = tracker.get_artifact_by_uri(uri)
            assert found is not None
            assert found.container_uri == uri
            assert found.key == f"artifact_{i}"


class TestFindArtifacts:
    """Tests for find_artifacts() method."""

    def test_find_artifacts_by_creator_consumer_and_key(self, tracker, run_dir: Path):
        output_path = run_dir / "artifact_output.csv"
        output_path.write_text("data\n")

        with tracker.start_run("producer_run", "test_model"):
            produced = tracker.log_output(output_path, key="shared_artifact")

        with tracker.start_run("consumer_run", "test_model", inputs=[produced]):
            pass

        by_creator = tracker.find_artifacts(creator="producer_run")
        assert any(art.id == produced.id for art in by_creator)

        by_consumer = tracker.find_artifacts(consumer="consumer_run")
        assert any(art.id == produced.id for art in by_consumer)

        by_key = tracker.find_artifacts(key="shared_artifact")
        assert any(art.id == produced.id for art in by_key)

        by_both = tracker.find_artifacts(
            creator="producer_run", consumer="consumer_run", key="shared_artifact"
        )
        assert [art.id for art in by_both] == [produced.id]


class TestLogH5Container:
    """Tests for log_h5_container() method."""

    def test_log_h5_container_without_discovery(self, tracker, run_dir: Path):
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
        assert container.container_uri.endswith("test_data.h5")
        assert container.driver == "h5"
        assert len(tables) == 0
        assert any(art.driver == "h5" for art in tracker.last_run.outputs)

    def test_log_h5_container_returns_tuple(self, tracker, run_dir: Path):
        h5_file = run_dir / "container_test.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_tuple", "test_model"):
            result = tracker.log_h5_container(str(h5_file), discover_tables=False)

        assert isinstance(result, tuple)
        assert len(result) == 2
        container, tables = result
        assert isinstance(container, Artifact)
        assert isinstance(tables, list)

    def test_log_h5_container_with_key_inference(self, tracker, run_dir: Path):
        h5_file = run_dir / "auto_named.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_key_infer", "test_model"):
            container, _ = tracker.log_h5_container(
                str(h5_file),
                discover_tables=False,
            )

        assert container.key == "auto_named"

    def test_log_h5_container_outside_run_context_raises_error(
        self, tracker, run_dir: Path
    ):
        h5_file = run_dir / "error.h5"
        h5_file.write_text("h5 data")

        with pytest.raises(RuntimeError, match="outside of a run context"):
            tracker.log_h5_container(str(h5_file), discover_tables=False)

    def test_log_h5_container_as_input(self, tracker, run_dir: Path):
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
        assert any(art.driver == "h5" for art in tracker.last_run.inputs)

    def test_log_h5_container_stores_metadata(self, tracker, run_dir: Path):
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

    def test_log_h5_container_with_h5py_discovery(self, tracker, run_dir: Path):
        h5_file = run_dir / "discovery_test.h5"
        h5_file.write_text("h5 data")

        with tracker.start_run("run_h5_discover", "test_model"):
            container, tables = tracker.log_h5_container(
                str(h5_file),
                key="discovered_data",
                discover_tables=True,
            )

        assert container.key == "discovered_data"
        assert isinstance(tables, list)
