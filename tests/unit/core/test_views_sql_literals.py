from consist.core.views import _safe_duckdb_string_literal


def test_safe_duckdb_string_literal_escapes_quotes():
    assert _safe_duckdb_string_literal("run'id") == "'run''id'"


def test_safe_duckdb_string_literal_none():
    assert _safe_duckdb_string_literal(None) == "NULL"
