from __future__ import annotations

from pydantic import BaseModel
import pytest

from consist import ParameterizedSQL, pivot_facets, query_df, query_rows


def test_pivot_facets_numeric_and_string(tracker) -> None:
    tracker.begin_run(
        "run_a",
        "simulate",
        facet={"alpha": 0.1, "beta": 2, "mode": "fast"},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_b",
        "simulate",
        facet={"alpha": 0.2, "beta": 3, "mode": "slow"},
    )
    tracker.end_run()

    stmt = pivot_facets(
        namespace="simulate",
        keys=["alpha", "beta", "mode"],
        value_columns={"mode": "value_str"},
    )
    assert isinstance(stmt, ParameterizedSQL)

    df = query_df(stmt, tracker=tracker)
    by_run = df.set_index("run_id").to_dict(orient="index")

    assert by_run["run_a"]["alpha"] == pytest.approx(0.1)
    assert by_run["run_a"]["beta"] == pytest.approx(2)
    assert by_run["run_a"]["mode"] == "fast"
    assert by_run["run_b"]["alpha"] == pytest.approx(0.2)
    assert by_run["run_b"]["beta"] == pytest.approx(3)
    assert by_run["run_b"]["mode"] == "slow"


def test_pivot_facets_labeling_and_namespace_optional(tracker) -> None:
    tracker.begin_run(
        "run_c",
        "simulate",
        facet={"alpha": 0.4, "beta": 4, "mode": "mix"},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_d",
        "other_model",
        facet={"alpha": 0.5, "beta": 5, "mode": "alt"},
    )
    tracker.end_run()

    stmt = pivot_facets(
        namespace=None,
        keys=["alpha", "beta"],
        label_prefix="cfg_",
        label_map={"beta": "cfg_beta_override"},
    )

    class PivotedRow(BaseModel):
        run_id: str
        cfg_alpha: float
        cfg_beta_override: float

    rows = query_rows(stmt, model=PivotedRow, tracker=tracker)
    by_run = {row.run_id: row for row in rows}

    assert by_run["run_c"].cfg_alpha == pytest.approx(0.4)
    assert by_run["run_c"].cfg_beta_override == pytest.approx(4)
    assert by_run["run_d"].cfg_alpha == pytest.approx(0.5)
    assert by_run["run_d"].cfg_beta_override == pytest.approx(5)


def test_pivot_facets_rejects_invalid_value_column(tracker) -> None:
    tracker.begin_run(
        "run_e",
        "simulate",
        facet={"alpha": 0.6},
    )
    tracker.end_run()

    with pytest.raises(ValueError, match="value_column"):
        pivot_facets(namespace="simulate", keys=["alpha"], value_column="value_int")

    with pytest.raises(ValueError, match="value_columns"):
        pivot_facets(
            namespace="simulate",
            keys=["alpha"],
            value_columns={"alpha": "value_int"},
        )


def test_pivot_facets_normalizes_numpy_scalars(tracker) -> None:
    np = pytest.importorskip("numpy")

    tracker.begin_run(
        "run_numpy",
        "simulate",
        facet={"alpha": np.float64(0.7), "beta": np.int64(7)},
    )
    tracker.end_run()

    stmt = pivot_facets(namespace="simulate", keys=["alpha", "beta"])
    df = query_df(stmt, tracker=tracker)
    by_run = df.set_index("run_id").to_dict(orient="index")

    assert by_run["run_numpy"]["alpha"] == pytest.approx(0.7)
    assert by_run["run_numpy"]["beta"] == pytest.approx(7)
