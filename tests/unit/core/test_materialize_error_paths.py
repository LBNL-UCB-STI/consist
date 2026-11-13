from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from consist.core.materialize import (
    materialize_artifacts,
    materialize_artifacts_from_sources,
    materialize_ingested_artifact_from_db,
)
from consist.models.artifact import Artifact


def _artifact(*, key: str, driver: str = "csv", is_ingested: bool = False) -> Artifact:
    return Artifact(
        key=key,
        uri=f"./{key}.{driver}",
        driver=driver,
        meta={"is_ingested": is_ingested},
    )


def test_materialize_artifacts_blocks_symlink_destination(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("x\n1\n", encoding="utf-8")

    target = tmp_path / "target.csv"
    target.write_text("x\n2\n", encoding="utf-8")
    destination = tmp_path / "dest.csv"
    destination.symlink_to(target)

    tracker = SimpleNamespace(resolve_uri=lambda _uri: str(source))
    artifact = _artifact(key="symlinked")

    with pytest.raises(ValueError, match="Symlink detected in destination path"):
        materialize_artifacts(
            tracker=tracker, items=[(artifact, destination)], on_missing="raise"
        )


def test_materialize_artifacts_refuses_overwrite_on_destination_type_mismatch(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    destination = tmp_path / "destination"
    destination.mkdir()

    tracker = SimpleNamespace(resolve_uri=lambda _uri: str(source))
    artifact = _artifact(key="type_mismatch")

    with pytest.raises(RuntimeError, match="Destination type mismatch"):
        materialize_artifacts(
            tracker=tracker, items=[(artifact, destination)], on_missing="raise"
        )


def test_materialize_artifacts_from_sources_raises_for_allowed_base_violation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    allowed_base = tmp_path / "sandbox"
    allowed_base.mkdir()
    outside_destination = tmp_path / "outside.csv"
    artifact = _artifact(key="outside")

    with pytest.raises(
        ValueError,
        match="outside allowed base|configured mount root|allow_external_paths",
    ):
        materialize_artifacts_from_sources(
            items=[(artifact, source, outside_destination)],
            allowed_base=allowed_base,
            on_missing="raise",
        )


def test_materialize_artifacts_from_sources_preserves_existing_destination_by_default(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\nexpected\n", encoding="utf-8")
    destination = tmp_path / "destination.csv"
    destination.write_text("value\nexisting\n", encoding="utf-8")
    artifact = _artifact(key="preserved")

    materialized = materialize_artifacts_from_sources(
        items=[(artifact, source, destination)],
        allowed_base=tmp_path,
        on_missing="raise",
    )

    assert materialized == {"preserved": str(destination.resolve())}
    assert destination.read_text(encoding="utf-8") == "value\nexisting\n"


def test_materialize_artifacts_from_sources_overwrites_existing_file_when_requested(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\nexpected\n", encoding="utf-8")
    destination = tmp_path / "destination.csv"
    destination.write_text("value\nstale\n", encoding="utf-8")
    artifact = _artifact(key="stale_file")

    materialized = materialize_artifacts_from_sources(
        items=[(artifact, source, destination)],
        allowed_base=tmp_path,
        on_missing="raise",
        overwrite_existing=True,
    )

    assert materialized == {"stale_file": str(destination.resolve())}
    assert destination.read_text(encoding="utf-8") == "value\nexpected\n"


def test_materialize_artifacts_from_sources_overwrites_existing_directory_when_requested(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source_dir"
    source.mkdir()
    (source / "data.csv").write_text("value\nexpected\n", encoding="utf-8")
    destination = tmp_path / "destination_dir"
    destination.mkdir()
    (destination / "stale.csv").write_text("value\nstale\n", encoding="utf-8")
    artifact = _artifact(key="stale_dir")

    materialized = materialize_artifacts_from_sources(
        items=[(artifact, source, destination)],
        allowed_base=tmp_path,
        on_missing="raise",
        overwrite_existing=True,
    )

    assert materialized == {"stale_dir": str(destination.resolve())}
    assert not (destination / "stale.csv").exists()
    assert (destination / "data.csv").read_text(encoding="utf-8") == "value\nexpected\n"


def test_materialize_artifacts_from_sources_refuses_overwrite_type_mismatch(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\nexpected\n", encoding="utf-8")
    destination = tmp_path / "destination_dir"
    destination.mkdir()
    artifact = _artifact(key="type_mismatch")

    with pytest.raises(RuntimeError, match="Destination type mismatch"):
        materialize_artifacts_from_sources(
            items=[(artifact, source, destination)],
            allowed_base=tmp_path,
            on_missing="raise",
            overwrite_existing=True,
        )

    assert destination.is_dir()


def test_materialize_artifacts_from_sources_refuses_symlink_destination_when_overwriting(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\nexpected\n", encoding="utf-8")
    target = tmp_path / "target.csv"
    target.write_text("value\nstale\n", encoding="utf-8")
    destination = tmp_path / "destination.csv"
    destination.symlink_to(target)
    artifact = _artifact(key="symlinked")

    with pytest.raises(ValueError, match="Symlink detected in destination path"):
        materialize_artifacts_from_sources(
            items=[(artifact, source, destination)],
            allowed_base=tmp_path,
            on_missing="raise",
            overwrite_existing=True,
        )

    assert target.read_text(encoding="utf-8") == "value\nstale\n"


def test_materialize_ingested_artifact_from_db_requires_tracker_db(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", is_ingested=True)
    tracker = SimpleNamespace(db=None)

    with pytest.raises(RuntimeError, match="tracker has no DB"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_rejects_unsupported_driver(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_json", driver="json", is_ingested=True)
    tracker = SimpleNamespace(db=object())

    with pytest.raises(ValueError, match="Only csv/parquet artifacts"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.json",
        )


def test_materialize_ingested_artifact_from_db_wraps_reflection_failure(
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        key="ingested_csv",
        uri="./ingested_csv.csv",
        driver="csv",
        meta={"is_ingested": True},
    )

    class _FailingDB:
        def fetchall(self, *_args, **_kwargs):
            raise RuntimeError("reflection failed")

    tracker = SimpleNamespace(db=_FailingDB())

    with pytest.raises(RuntimeError, match="Failed to reflect table"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_fails_when_table_has_no_columns(
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        key="ingested_csv",
        uri="./ingested_csv.csv",
        driver="csv",
        meta={"is_ingested": True},
    )

    class _EmptyDB:
        def fetchall(self, *_args, **_kwargs):
            return []

    tracker = SimpleNamespace(db=_EmptyDB())

    with pytest.raises(
        RuntimeError,
        match="Failed to reflect table 'global_tables.ingested_csv': table does not exist",
    ):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_requires_consist_artifact_id_column(
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        key="ingested_csv",
        uri="./ingested_csv.csv",
        driver="csv",
        meta={"is_ingested": True},
    )

    class _NoArtifactIdDB:
        def fetchall(self, *_args, **_kwargs):
            return [("value",)]

    tracker = SimpleNamespace(db=_NoArtifactIdDB())

    with pytest.raises(RuntimeError, match="missing consist_artifact_id"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


@pytest.mark.parametrize(
    "artifact",
    [
        Artifact(
            key="ingested_csv",
            uri="./ingested_csv.csv",
            driver="csv",
            meta={"is_ingested": True, "dlt_table_name": 123},
        ),
        Artifact(
            key="",
            uri="./ingested_csv.csv",
            driver="csv",
            meta={"is_ingested": True, "dlt_table_name": ""},
        ),
    ],
)
def test_materialize_ingested_artifact_from_db_rejects_invalid_or_empty_table_name(
    tmp_path: Path, artifact: Artifact
) -> None:
    tracker = SimpleNamespace(db=object())

    with pytest.raises(ValueError, match="table name is missing"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_uses_hot_data_store_db(
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        key="ingested_csv",
        uri="./ingested_csv.csv",
        driver="csv",
        meta={"is_ingested": True},
    )
    captured: dict[str, object] = {}

    class _HotDB:
        def fetchall(self, _sql, params=None):
            captured["fetchall_db"] = self
            captured["queried_params"] = params
            return [("consist_artifact_id",), ("value",)]

        def fetchdf(self, _sql, params=None):
            captured["fetchdf_db"] = self
            captured["fetchdf_params"] = params
            return pd.DataFrame(
                {"consist_artifact_id": [str(artifact.id)], "value": [1]}
            )

    hot_db = _HotDB()
    tracker = SimpleNamespace(
        db=None,
        hot_data_store=SimpleNamespace(db=hot_db),
    )

    output = tmp_path / "reconstructed.csv"
    result = materialize_ingested_artifact_from_db(
        artifact=artifact,
        tracker=tracker,
        destination=output,
    )

    assert captured["fetchall_db"] is hot_db
    assert captured["fetchdf_db"] is hot_db
    assert captured["queried_params"] == ["ingested_csv"]
    assert captured["fetchdf_params"] == [str(artifact.id)]
    assert Path(result).resolve() == output.resolve()
