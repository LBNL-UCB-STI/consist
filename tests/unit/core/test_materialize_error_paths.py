from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from consist.artifact import Artifact
from consist.core.materialize import (
    materialize_artifacts,
    materialize_artifacts_from_sources,
    materialize_ingested_artifact_from_db,
)
from tests.helpers.artifact_factory import make_artifact


def _artifact(*, key: str, driver: str = "csv", is_ingested: bool = False) -> Artifact:
    return make_artifact(
        key=key,
        container_uri=f"./{key}.{driver}",
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


def test_materialize_ingested_artifact_from_db_requires_tracker_ledger(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", is_ingested=True)
    tracker = SimpleNamespace(ledger=None)

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
    tracker = SimpleNamespace(ledger=object())

    with pytest.raises(ValueError, match="Only csv/parquet artifacts"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.json",
        )


def test_materialize_ingested_artifact_from_db_wraps_reflection_failure(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", driver="csv", is_ingested=True)

    class _FailingLedger:
        def exec_cursor(self):
            raise RuntimeError("reflection failed")

    tracker = SimpleNamespace(ledger=_FailingLedger())

    with pytest.raises(RuntimeError, match="Failed to reflect table"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_fails_when_table_has_no_columns(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", driver="csv", is_ingested=True)

    class _EmptyCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return SimpleNamespace(fetchall=lambda: [])

    class _EmptyLedger:
        def exec_cursor(self):
            return _EmptyCursor()

    tracker = SimpleNamespace(ledger=_EmptyLedger())

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
    artifact = _artifact(key="ingested_csv", driver="csv", is_ingested=True)

    class _NoArtifactIdCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return SimpleNamespace(fetchall=lambda: [("value",)])

    class _NoArtifactIdLedger:
        def exec_cursor(self):
            return _NoArtifactIdCursor()

    tracker = SimpleNamespace(ledger=_NoArtifactIdLedger())

    with pytest.raises(RuntimeError, match="missing consist_artifact_id"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


@pytest.mark.parametrize(
    "artifact",
    [
        make_artifact(
            key="ingested_csv",
            container_uri="./ingested_csv.csv",
            driver="csv",
            meta={"is_ingested": True, "dlt_table_name": 123},
        ),
        SimpleNamespace(
            id="id-empty-key",
            key="",
            driver="csv",
            meta={"is_ingested": True, "dlt_table_name": ""},
        ),
    ],
)
def test_materialize_ingested_artifact_from_db_rejects_invalid_or_empty_table_name(
    tmp_path: Path, artifact: object
) -> None:
    tracker = SimpleNamespace(ledger=object())

    with pytest.raises(ValueError, match="table name is missing"):
        materialize_ingested_artifact_from_db(
            artifact=artifact,  # type: ignore[arg-type]
            tracker=tracker,
            destination=tmp_path / "reconstructed.csv",
        )


def test_materialize_ingested_artifact_from_db_uses_hot_data_store_ledger(
    tmp_path: Path,
) -> None:
    artifact = _artifact(key="ingested_csv", driver="csv", is_ingested=True)
    captured: dict[str, object] = {}

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, params=None):
            captured["queried_params"] = params
            return SimpleNamespace(
                fetchall=lambda: [("consist_artifact_id",), ("value",)]
            )

    class _HotLedger:
        def exec_cursor(self):
            captured["cursor_ledger"] = self
            return _Cursor()

        def query_df(self, _sql, params=None):
            captured["query_df_ledger"] = self
            captured["query_df_params"] = params
            return pd.DataFrame({"value": [1]})

    hot_ledger = _HotLedger()
    tracker = SimpleNamespace(
        ledger=None,
        hot_data_store=SimpleNamespace(ledger=hot_ledger),
    )

    output = tmp_path / "reconstructed.csv"
    result = materialize_ingested_artifact_from_db(
        artifact=artifact,
        tracker=tracker,
        destination=output,
    )

    assert captured["cursor_ledger"] is hot_ledger
    assert captured["query_df_ledger"] is hot_ledger
    assert captured["queried_params"] == ["ingested_csv"]
    assert captured["query_df_params"] == [str(artifact.id)]
    assert Path(result).resolve() == output.resolve()
