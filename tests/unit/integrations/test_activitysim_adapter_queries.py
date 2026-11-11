from __future__ import annotations

import pytest

from consist import ParameterizedSQL, query_df
from consist.integrations.activitysim import ActivitySimConfigAdapter


def _seed_activitysim_rows(tracker) -> None:
    ledger = tracker.ledger
    assert ledger is not None
    with ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_coefficients_cache ("
            "content_hash VARCHAR, file_name VARCHAR, coefficient_name VARCHAR, "
            "segment VARCHAR, source_type VARCHAR, value_raw VARCHAR, value_num DOUBLE, "
            "dims_json VARCHAR, constrain VARCHAR, is_constrained BOOLEAN"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_constants_cache ("
            "content_hash VARCHAR, file_name VARCHAR, key VARCHAR, value_type VARCHAR, "
            "value_str VARCHAR, value_num DOUBLE, value_bool BOOLEAN, value_json JSON"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_probabilities_entries_cache ("
            "content_hash VARCHAR, file_name VARCHAR, row_index INTEGER, key VARCHAR, value_num DOUBLE"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.activitysim_config_ingest_run_link ("
            "run_id VARCHAR, table_name VARCHAR, content_hash VARCHAR, file_name VARCHAR"
            ")"
        )
        cur.execute(
            "INSERT INTO global_tables.activitysim_coefficients_cache VALUES "
            "('hash-coef', 'coefficients.csv', 'time', '', 'coefficient', '1.2', 1.2, NULL, NULL, NULL), "
            "('hash-coef', 'coefficients.csv', 'cost', '', 'coefficient', '-0.3', -0.3, NULL, NULL, NULL), "
            "('hash-coef-2', 'coefficients.csv', 'time', '', 'coefficient', '1.5', 1.5, NULL, NULL, NULL), "
            "('hash-coef-2', 'coefficients.csv', 'cost', '', 'coefficient', '-0.2', -0.2, NULL, NULL, NULL)"
        )
        cur.execute(
            "INSERT INTO global_tables.activitysim_constants_cache VALUES "
            "('hash-const', 'settings.yaml', 'sample_rate', 'float', NULL, 0.5, NULL, NULL)"
        )
        cur.execute(
            "INSERT INTO global_tables.activitysim_probabilities_entries_cache VALUES "
            "('hash-prob', 'non_mandatory_tour_frequency_probs.csv', 0, 'zero_tours', 0.65), "
            "('hash-prob', 'non_mandatory_tour_frequency_probs.csv', 0, 'one_tour', 0.35)"
        )
        cur.execute(
            "INSERT INTO global_tables.activitysim_config_ingest_run_link VALUES "
            "('run_coeff', 'activitysim_coefficients_cache', 'hash-coef', 'coefficients.csv'), "
            "('run_coeff_b', 'activitysim_coefficients_cache', 'hash-coef-2', 'coefficients.csv'), "
            "('run_const', 'activitysim_constants_cache', 'hash-const', 'settings.yaml'), "
            "('run_prob', 'activitysim_probabilities_entries_cache', 'hash-prob', 'non_mandatory_tour_frequency_probs.csv')"
        )


def test_activitysim_adapter_coefficients_helpers(tracker) -> None:
    adapter = ActivitySimConfigAdapter()
    _seed_activitysim_rows(tracker)

    rows = adapter.coefficients_rows(coefficient="time", tracker=tracker)
    assert sorted(rows) == [
        ("run_coeff", pytest.approx(1.2)),
        ("run_coeff_b", pytest.approx(1.5)),
    ]

    by_run = adapter.coefficients_by_run(
        coefficient="time",
        collapse="first",
        tracker=tracker,
    )
    assert by_run == {
        "run_coeff": pytest.approx(1.2),
        "run_coeff_b": pytest.approx(1.5),
    }

    query = adapter.coefficients_query(coefficient="time")
    assert isinstance(query, ParameterizedSQL)
    df = query_df(query, tracker=tracker)
    assert sorted(zip(df["run_id"], df["value_num"])) == [
        ("run_coeff", pytest.approx(1.2)),
        ("run_coeff_b", pytest.approx(1.5)),
    ]

    combos = adapter.coefficients_combinations(
        coefficients=["time", "cost"],
        collapse="first",
        tracker=tracker,
    )
    assert len(combos) == 2
    for combo, runs in combos.items():
        if runs == ["run_coeff"]:
            assert combo[0] == pytest.approx(1.2)
            assert combo[1] == pytest.approx(-0.3)
        elif runs == ["run_coeff_b"]:
            assert combo[0] == pytest.approx(1.5)
            assert combo[1] == pytest.approx(-0.2)
        else:
            raise AssertionError(f"Unexpected combo mapping: {combo} -> {runs}")


def test_activitysim_adapter_constants_and_probabilities_helpers(tracker) -> None:
    adapter = ActivitySimConfigAdapter()
    _seed_activitysim_rows(tracker)

    const_query = adapter.constants_query(key="sample_rate")
    assert isinstance(const_query, ParameterizedSQL)

    rows = adapter.constants_rows(key="sample_rate", tracker=tracker)
    assert rows == [("run_const", pytest.approx(0.5))]

    by_run = adapter.constants_by_run(
        key="sample_rate",
        collapse="first",
        tracker=tracker,
    )
    assert by_run == {"run_const": pytest.approx(0.5)}

    prob_query = adapter.probabilities_query(key="zero_tours")
    assert isinstance(prob_query, ParameterizedSQL)
    prob_rows = adapter.probabilities_rows(key="zero_tours", tracker=tracker)
    assert prob_rows == [("run_prob", pytest.approx(0.65))]
