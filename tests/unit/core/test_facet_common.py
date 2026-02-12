from __future__ import annotations

from pydantic import BaseModel

from consist.core.facet_common import (
    canonical_facet_json_and_id,
    flatten_facet_values,
    infer_schema_name_from_facet,
    normalize_facet_like,
)
from consist.core.identity import IdentityManager


class _FacetModel(BaseModel):
    alpha: int
    nested: dict[str, object]


def _identity() -> IdentityManager:
    return IdentityManager(project_root=".", hashing_strategy="full")


def test_infer_schema_name_from_facet_prefers_pydantic() -> None:
    schema = infer_schema_name_from_facet(_FacetModel(alpha=1, nested={}))
    assert schema == "_FacetModel"
    assert infer_schema_name_from_facet({"alpha": 1}, fallback="dict") == "dict"


def test_normalize_facet_like_supports_pydantic_and_mapping() -> None:
    identity = _identity()
    model = _FacetModel(alpha=1, nested={"flag": True})
    assert normalize_facet_like(identity=identity, facet=model) == {
        "alpha": 1,
        "nested": {"flag": True},
    }
    assert normalize_facet_like(identity=identity, facet={"alpha": 1}) == {"alpha": 1}


def test_canonical_facet_json_and_id_is_order_stable() -> None:
    identity = _identity()
    _, id_a = canonical_facet_json_and_id(
        identity=identity,
        facet_dict={"b": 2, "a": 1},
    )
    _, id_b = canonical_facet_json_and_id(
        identity=identity,
        facet_dict={"a": 1, "b": 2},
    )
    assert id_a == id_b


def test_flatten_facet_values_escapes_dot_keys() -> None:
    rows = flatten_facet_values(
        facet_dict={"a": {"b.c": 1}},
        include_json_leaves=False,
    )
    assert len(rows) == 1
    assert rows[0].key_path == "a.b\\.c"
    assert rows[0].value_type == "int"
    assert rows[0].value_num == 1.0


def test_flatten_facet_values_json_leaf_behavior_is_configurable() -> None:
    facet = {"name": "x", "items": [1, 2, 3]}
    with_json = flatten_facet_values(facet_dict=facet, include_json_leaves=True)
    without_json = flatten_facet_values(facet_dict=facet, include_json_leaves=False)

    with_json_keys = {row.key_path for row in with_json}
    without_json_keys = {row.key_path for row in without_json}

    assert "name" in with_json_keys
    assert "items" in with_json_keys
    assert "name" in without_json_keys
    assert "items" not in without_json_keys
