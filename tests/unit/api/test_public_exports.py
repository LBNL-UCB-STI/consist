from consist import MaterializationResult
from consist.core.materialize import MaterializationResult as CoreMaterializationResult


def test_materialization_result_is_exported_from_top_level() -> None:
    assert MaterializationResult is CoreMaterializationResult
