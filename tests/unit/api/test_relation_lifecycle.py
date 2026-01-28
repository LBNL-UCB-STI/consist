import warnings

import pandas as pd
import pytest

from consist.api import (
    RelationConnectionLeakWarning,
    active_relation_count,
    load,
    load_relation,
    to_df,
)
from consist.models.artifact import Artifact


def _csv_artifact(tmp_path: pytest.TempPathFactory) -> Artifact:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "data.csv"
    df.to_csv(path, index=False)
    return Artifact(key="data", container_uri=str(path), driver="csv")


def test_load_relation_closes_connection(tmp_path):
    artifact = _csv_artifact(tmp_path)
    before = active_relation_count()
    with load_relation(artifact) as relation:
        assert active_relation_count() == before + 1
        df = relation.df()
        assert df["a"].tolist() == [1, 2]
    assert active_relation_count() == before


def test_relation_leak_warning_threshold(monkeypatch, tmp_path):
    artifact = _csv_artifact(tmp_path)
    monkeypatch.setenv("CONSIST_RELATION_WARN_THRESHOLD", "1")
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always", RelationConnectionLeakWarning)
        relation = load(artifact)
        assert any(
            isinstance(item.message, RelationConnectionLeakWarning) for item in recorded
        )
    to_df(relation)
