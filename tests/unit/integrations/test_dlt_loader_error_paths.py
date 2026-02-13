from types import SimpleNamespace

import pytest

from consist.integrations import dlt_loader
from consist.models.artifact import Artifact


def _artifact(*, key: str, driver: str, table_path: str | None = None) -> Artifact:
    return Artifact(
        key=key,
        container_uri=f"./{key}",
        driver=driver,
        table_path=table_path,
    )


def test_ingest_artifact_raises_when_no_data_iterable_or_path_is_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    artifact = _artifact(key="rows", driver="csv")
    run_context = SimpleNamespace(id="run-1", year=2025, iteration=1)

    with pytest.raises(ValueError, match="No data provided for ingestion"):
        dlt_loader.ingest_artifact(
            artifact=artifact,
            run_context=run_context,
            db_path="/tmp/provenance.duckdb",
            data_iterable=None,
        )


def test_ingest_artifact_h5_table_driver_requires_table_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dlt_loader, "dlt", object())

    artifact = _artifact(key="h5_rows", driver="h5_table", table_path=None)
    run_context = SimpleNamespace(id="run-1", year=2025, iteration=1)

    with pytest.raises(ValueError, match="missing 'table_path'"):
        dlt_loader.ingest_artifact(
            artifact=artifact,
            run_context=run_context,
            db_path="/tmp/provenance.duckdb",
            data_iterable="/tmp/data.h5",
        )
