from __future__ import annotations

import pandas as pd
import pytest
from sqlmodel import Field, SQLModel, text


def test_grouped_view_param_parser(tracker) -> None:
    parsed = tracker._parse_artifact_param_expression("beam.year>=2018")
    assert parsed["namespace"] == "beam"
    assert parsed["key_path"] == "year"
    assert parsed["op"] == ">="
    assert parsed["kind"] == "num"
    assert parsed["value"] == 2018

    parsed_eq = tracker._parse_artifact_param_expression("artifact_family=test")
    assert parsed_eq["namespace"] is None
    assert parsed_eq["key_path"] == "artifact_family"
    assert parsed_eq["op"] == "="
    assert parsed_eq["kind"] == "str"
    assert parsed_eq["value"] == "test"

    with pytest.raises(ValueError):
        tracker._parse_artifact_param_expression("broken_predicate")


def test_grouped_view_if_exists_error(tracker, tmp_path) -> None:
    df = pd.DataFrame({"id": [1], "value": [1.0]})
    path = tmp_path / "a.parquet"
    df.to_parquet(path)

    with tracker.start_run(
        "if_exists_run", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        artifact = tracker.log_artifact(
            str(path),
            key="if_exists_key",
            driver="parquet",
            profile_file_schema=True,
            facet={"artifact_family": "if_exists", "year": 2018, "iteration": 0},
            facet_index=True,
        )

    schema_id = artifact.meta.get("schema_id")
    assert schema_id is not None

    tracker.create_grouped_view(
        "v_if_exists",
        schema_id=schema_id,
        namespace="beam",
        params=["artifact_family=if_exists"],
        mode="cold_only",
    )
    with pytest.raises(ValueError):
        tracker.create_grouped_view(
            "v_if_exists",
            schema_id=schema_id,
            namespace="beam",
            params=["artifact_family=if_exists"],
            mode="cold_only",
            if_exists="error",
        )


def test_grouped_view_schema_compatible_mode(tracker, tmp_path) -> None:
    p_base = tmp_path / "base.parquet"
    p_superset = tmp_path / "superset.parquet"
    pd.DataFrame({"id": [1], "value": [10.0]}).to_parquet(p_base)
    pd.DataFrame({"id": [2], "value": [20.0], "extra": ["x"]}).to_parquet(p_superset)

    with tracker.start_run(
        "compat_base", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        base_artifact = tracker.log_artifact(
            str(p_base),
            key="compat_base",
            driver="parquet",
            profile_file_schema=True,
            facet={"artifact_family": "compat", "year": 2018, "iteration": 0},
            facet_index=True,
        )
    with tracker.start_run(
        "compat_superset", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        tracker.log_artifact(
            str(p_superset),
            key="compat_superset",
            driver="parquet",
            profile_file_schema=True,
            facet={"artifact_family": "compat", "year": 2018, "iteration": 0},
            facet_index=True,
        )

    schema_id = base_artifact.meta.get("schema_id")
    assert schema_id is not None

    tracker.create_grouped_view(
        "v_compat_exact",
        schema_id=schema_id,
        namespace="beam",
        params=["artifact_family=compat"],
        mode="cold_only",
        schema_compatible=False,
    )
    with tracker.engine.connect() as conn:
        exact_count = conn.execute(text("SELECT COUNT(*) FROM v_compat_exact")).scalar()
    assert exact_count == 1

    tracker.create_grouped_view(
        "v_compat_wide",
        schema_id=schema_id,
        namespace="beam",
        params=["artifact_family=compat"],
        mode="cold_only",
        schema_compatible=True,
    )
    with tracker.engine.connect() as conn:
        wide_count = conn.execute(text("SELECT COUNT(*) FROM v_compat_wide")).scalar()
    assert wide_count == 2


def test_grouped_view_accepts_schema_class_selector(tracker, tmp_path) -> None:
    class LinkstatsRow(SQLModel, table=True):
        __tablename__ = "linkstats_rows"
        id: int = Field(primary_key=True)
        value: float

    path = tmp_path / "schema_class.parquet"
    pd.DataFrame({"id": [1], "value": [42.0]}).to_parquet(path)

    with tracker.start_run(
        "schema_class_run", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        tracker.log_artifact(
            str(path),
            key="schema_class_key",
            driver="parquet",
            profile_file_schema=True,
            facet={"artifact_family": "schema_class", "year": 2018, "iteration": 0},
            facet_index=True,
        )

    tracker.create_grouped_view(
        "v_schema_class",
        schema=LinkstatsRow,
        namespace="beam",
        params=["artifact_family=schema_class"],
        mode="cold_only",
    )
    with tracker.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM v_schema_class")).scalar()
    assert count == 1


def test_grouped_view_selector_validation(tracker, tmp_path) -> None:
    class SelectorModel(SQLModel, table=True):
        __tablename__ = "selector_rows"
        id: int = Field(primary_key=True)

    with pytest.raises(ValueError, match="exactly one of schema_id or schema"):
        tracker.create_grouped_view("v_bad_selector")

    with pytest.raises(ValueError, match="exactly one of schema_id or schema"):
        tracker.create_grouped_view(
            "v_bad_selector2", schema_id="abc", schema=SelectorModel
        )
