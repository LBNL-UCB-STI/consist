"""
Integration tests for config hashing + config facet storage.

This file is meant to be a developer-friendly entrypoint for understanding how Consist
handles configuration identity and queryable config facets.

Concepts
--------
Config hashing:
  - `config_hash` hashes the run config (plus optional `hash_inputs` digests).
  - `hash_inputs` lets you fold external file/directory state into identity without
    persisting those values as large blobs in the database.

Config facets:
  - `facet=...` is the *small, queryable* subset of config.
  - Facets are persisted (deduped) to `ConfigFacet` and optionally indexed to `RunConfigKV`.
  - Facets have guardrails (size and key-count) to prevent DB bloat.
"""

import hashlib
import json
from pathlib import Path

import consist
from pydantic import BaseModel


def test_begin_run_persists_facet_and_kv_index(tracker, run_dir: Path):
    """
    Demonstrates the core "facet" happy path:
    - `facet` is written to the run JSON (`consist.json`) for human inspection.
    - The facet is persisted to `ConfigFacet` (deduped by canonical JSON hash).
    - The facet is flattened into `RunConfigKV` rows for query/filter ergonomics.
    """
    tracker.begin_run(
        "run_with_facet",
        "activitysim",
        config={"internal": "not_indexed"},
        facet={
            "household_sample_size": 250000,
            "num_processes": 25,
            "file_format": "parquet",
        },
    )
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)

    assert payload["facet"]["household_sample_size"] == 250000

    facets = tracker.get_config_facets(namespace="activitysim")
    assert len(facets) == 1
    facet = facets[0]
    assert facet.facet_json["file_format"] == "parquet"

    kv = tracker.get_run_config_kv("run_with_facet")
    keys = {row.key for row in kv}
    assert "household_sample_size" in keys
    assert "num_processes" in keys
    assert "file_format" in keys


def test_get_config_helpers_return_typed_values(tracker):
    tracker.begin_run(
        "run_with_helpers",
        "activitysim",
        config={"ignored": "config_snapshot"},
        facet={
            "alpha": 1,
            "beta": 2.5,
            "flag": True,
            "nested": {"inner": 3},
        },
    )
    tracker.end_run()

    values = tracker.get_config_values("run_with_helpers")
    assert values["alpha"] == 1
    assert isinstance(values["alpha"], int)
    assert values["beta"] == 2.5
    assert isinstance(values["beta"], float)
    assert values["flag"] is True
    assert values["nested.inner"] == 3

    assert tracker.get_config_value("run_with_helpers", "nested.inner") == 3
    assert (
        tracker.get_config_value("run_with_helpers", "missing", default="fallback")
        == "fallback"
    )


def test_begin_run_uses_combined_config_facet_persistence(tracker, monkeypatch) -> None:
    combined_calls = 0
    upsert_calls = 0
    kv_calls = 0

    original_combined = tracker.db.persist_config_facet_bundle
    original_upsert = tracker.db.upsert_config_facet
    original_kv = tracker.db.insert_run_config_kv_bulk

    def counting_combined(**kwargs):
        nonlocal combined_calls
        combined_calls += 1
        return original_combined(**kwargs)

    def counting_upsert(*args, **kwargs):
        nonlocal upsert_calls
        upsert_calls += 1
        return original_upsert(*args, **kwargs)

    def counting_kv(*args, **kwargs):
        nonlocal kv_calls
        kv_calls += 1
        return original_kv(*args, **kwargs)

    monkeypatch.setattr(tracker.db, "persist_config_facet_bundle", counting_combined)
    monkeypatch.setattr(tracker.db, "upsert_config_facet", counting_upsert)
    monkeypatch.setattr(tracker.db, "insert_run_config_kv_bulk", counting_kv)

    tracker.begin_run(
        "run_with_combined_facet",
        "activitysim",
        config={"internal": "not_indexed"},
        facet={"household_sample_size": 250000, "num_processes": 25},
    )
    tracker.end_run()

    assert combined_calls == 1
    assert upsert_calls == 0
    assert kv_calls == 0


def test_find_runs_supports_mixed_run_fields_and_facet_predicates(tracker):
    tracker.begin_run(
        "run_a_iter_1",
        "demo_model",
        year=2030,
        iteration=1,
        stage="restart",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_a_iter_2",
        "demo_model",
        year=2030,
        iteration=2,
        stage="restart",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_a_other_seed",
        "demo_model",
        year=2030,
        iteration=3,
        stage="restart",
        facet={"scenario_id": "scenario_a", "seed": 99},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_b_iter_4",
        "demo_model",
        year=2030,
        iteration=4,
        stage="restart",
        facet={"scenario_id": "scenario_b", "seed": 17},
    )
    tracker.end_run()

    runs = tracker.find_runs(
        model="demo_model",
        year=2030,
        stage="restart",
        status="completed",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )

    assert [run.id for run in runs] == ["run_a_iter_2", "run_a_iter_1"]


def test_find_latest_run_supports_facet_predicates(tracker):
    tracker.begin_run(
        "run_a_iter_1",
        "demo_model",
        year=2030,
        iteration=1,
        stage="restart",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_a_iter_2",
        "demo_model",
        year=2030,
        iteration=2,
        stage="restart",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_b_iter_3",
        "demo_model",
        year=2030,
        iteration=3,
        stage="restart",
        facet={"scenario_id": "scenario_b", "seed": 17},
    )
    tracker.end_run()

    latest = consist.find_latest_run(
        tracker=tracker,
        model="demo_model",
        year=2030,
        stage="restart",
        status="completed",
        facet={"scenario_id": "scenario_a", "seed": 17},
    )

    assert latest.id == "run_a_iter_2"


def test_hash_inputs_affects_config_hash_but_ignores_dotfiles(tracker, run_dir: Path):
    """
    Demonstrates `hash_inputs` identity behavior for directory hashing:
    - Digests are folded into `config_hash` so external config state participates in caching.
    - Dotfiles are ignored by default so secrets/venv/editor files don't cause cache misses.
    """
    cfg_dir = run_dir / "beam_cfg"
    cfg_dir.mkdir()
    (cfg_dir / "main.conf").write_text("a=1\n")
    (cfg_dir / ".ignored").write_text("secret=1\n")

    run_a = tracker.begin_run(
        "run_hash_inputs_a",
        "beam",
        config={"sample": 1.0},
        hash_inputs=[("beam_cfg", cfg_dir)],
        facet={"sample": 1.0},
    )
    tracker.end_run()

    # Change ONLY the dotfile; digest should be unchanged.
    (cfg_dir / ".ignored").write_text("secret=2\n")

    run_b = tracker.begin_run(
        "run_hash_inputs_b",
        "beam",
        config={"sample": 1.0},
        hash_inputs=[("beam_cfg", cfg_dir)],
        facet={"sample": 1.0},
    )
    tracker.end_run()

    assert run_a.config_hash == run_b.config_hash

    # Change a non-dotfile; digest should change.
    (cfg_dir / "main.conf").write_text("a=2\n")

    run_c = tracker.begin_run(
        "run_hash_inputs_c",
        "beam",
        config={"sample": 1.0},
        hash_inputs=[("beam_cfg", cfg_dir)],
        facet={"sample": 1.0},
    )
    tracker.end_run()

    assert run_c.config_hash != run_b.config_hash


def test_hash_inputs_stored_in_json_config_and_run_meta(tracker, run_dir: Path):
    """
    Verifies where `hash_inputs` shows up in persisted records:
    - The digest map is stored in `run.meta["consist_hash_inputs"]`.
    - The same map is also folded into the hashed config payload under
      `config["__consist_hash_inputs__"]`, ensuring it contributes to `config_hash`.
    """
    cfg_dir = run_dir / "scenario_cfg"
    cfg_dir.mkdir()
    (cfg_dir / "scenario.conf").write_text("mode=test\n")

    tracker.begin_run(
        "run_hash_inputs_storage",
        "scenario",
        config={"sample": 1},
        hash_inputs=[("scenario_cfg", cfg_dir)],
    )
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)

    digest_map = payload["run"]["meta"]["consist_hash_inputs"]
    assert digest_map["scenario_cfg"]
    assert payload["config"]["__consist_hash_inputs__"] == digest_map


def test_pydantic_config_to_consist_facet_and_schema_version(tracker):
    """
    Demonstrates the Pydantic-based facet convention:
    - If `config` is a Pydantic model with `to_consist_facet()`, Consist uses that output
      as the facet when `facet=` is not explicitly provided.
    - If `facet_schema_version` is present on the config model (or passed explicitly),
      it is recorded in run meta and persisted in `ConfigFacet.schema_version`.
    """

    class MyConfig(BaseModel):
        a: int
        b: str

        def to_consist_facet(self) -> dict:
            return {"a": self.a}

    class MyVersionedConfig(MyConfig):
        facet_schema_version: int = 2

    tracker.begin_run(
        "run_pydantic_facet",
        "mymodel",
        config=MyVersionedConfig(a=7, b="hidden"),
    )
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)
    assert payload["facet"] == {"a": 7}

    run_meta = payload["run"]["meta"]
    assert run_meta["config_facet_namespace"] == "mymodel"
    assert run_meta["config_facet_schema"] == "MyVersionedConfig"
    assert run_meta["config_facet_schema_version"] == 2

    facets = tracker.get_config_facets()
    assert len(facets) == 1
    facet = facets[0]
    assert facet.schema_name == "MyVersionedConfig"
    assert facet.schema_version == "2"
    assert facet.facet_json == {"a": 7}


def test_explicit_facet_overrides_pydantic_extractor(tracker):
    """
    Verifies facet source precedence:
    - An explicit `facet=...` argument always wins over `config.to_consist_facet()`.
    """

    class MyConfig(BaseModel):
        a: int

        def to_consist_facet(self) -> dict:
            return {"a": 1}

    tracker.begin_run(
        "run_facet_precedence",
        "mymodel",
        config=MyConfig(a=123),
        facet={"a": 999},
    )
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)
    assert payload["facet"] == {"a": 999}


def test_config_facet_id_is_canonical_json_hash(tracker):
    """
    Verifies ConfigFacet deduplication identity:
    - `config_facet_id` is `sha256(canonical_json(facet))`, so key ordering doesn't matter.
    """
    facet = {"b": 2, "a": 1}
    tracker.begin_run("run_facet_id", "m", facet=facet)
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)

    expected_id = hashlib.sha256(
        tracker.identity.canonical_json_str(facet).encode("utf-8")
    ).hexdigest()
    assert payload["run"]["meta"]["config_facet_id"] == expected_id

    assert tracker.get_config_facet(expected_id) is not None


def test_kv_flattening_handles_nested_and_dot_keys(tracker):
    """
    Demonstrates how facets become KV rows:
    - Nested dict keys are flattened with "." separators.
    - Literal "." in dict keys is escaped as "\\." to avoid ambiguity.
    """
    tracker.begin_run(
        "run_nested_kv",
        "m",
        facet={"a": {"b.c": 1}},
    )
    tracker.end_run()

    kv = tracker.get_run_config_kv("run_nested_kv")
    keys = {row.key for row in kv}
    assert "a.b\\.c" in keys


def test_kv_flattening_preserves_non_scalar_json_leaves(tracker):
    """
    Regression check for shared facet flattening:
    - Non-scalar leaves (e.g., list) remain indexed as value_type='json'.
    - Scalar nested leaves still keep their native scalar type columns.
    """
    tracker.begin_run(
        "run_json_leaf_kv",
        "m",
        facet={"items": [1, 2, 3], "nested": {"inner": "x"}},
    )
    tracker.end_run()

    kv = tracker.get_run_config_kv("run_json_leaf_kv")
    items_row = next(row for row in kv if row.key == "items")
    inner_row = next(row for row in kv if row.key == "nested.inner")

    assert items_row.value_type == "json"
    assert items_row.value_json == [1, 2, 3]
    assert inner_row.value_type == "str"
    assert inner_row.value_str == "x"


def test_facet_index_false_skips_kv_indexing(tracker):
    """
    Verifies `facet_index=False` behavior:
    - The facet is persisted to `ConfigFacet`.
    - No `RunConfigKV` rows are created.
    """
    tracker.begin_run(
        "run_no_kv",
        "m",
        facet={"a": 1, "b": 2},
        facet_index=False,
    )
    tracker.end_run()

    assert tracker.get_config_facets()
    assert tracker.get_run_config_kv("run_no_kv") == []


def test_facet_kv_row_limit_guardrail(tracker):
    """
    Verifies the KV row count guardrail:
    - Facets that would produce >500 KV rows do not get indexed to `RunConfigKV`.
    - The facet blob is still persisted to `ConfigFacet`.
    """
    facet = {f"k{i}": i for i in range(600)}  # > max_kv_rows=500
    tracker.begin_run(
        "run_kv_limit",
        "m",
        facet=facet,
    )
    tracker.end_run()

    assert tracker.get_config_facets()
    assert tracker.get_run_config_kv("run_kv_limit") == []


def test_facet_size_guardrail_skips_db_persistence_but_keeps_json(tracker):
    """
    Verifies the facet size guardrail:
    - Oversized facets are kept in `consist.json` (human-readable log).
    - DB persistence is skipped: no `ConfigFacet` row and no facet pointers in run meta.
    """
    facet = {"big": "x" * 17_000}  # > max_facet_bytes=16_384 (canonical JSON)
    tracker.begin_run("run_big_facet", "m", facet=facet)
    tracker.end_run()

    with open(tracker.run_dir / "consist.json") as f:
        payload = json.load(f)
    assert payload["facet"]["big"].startswith("x")

    assert "config_facet_id" not in payload["run"]["meta"]
    assert tracker.get_config_facets() == []


def test_find_runs_facet_predicates_require_kv_index_rows(tracker):
    tracker.begin_run(
        "run_indexed",
        "m",
        facet={"scenario_id": "baseline", "seed": 1},
    )
    tracker.end_run()

    tracker.begin_run(
        "run_not_indexed_flag",
        "m",
        facet={"scenario_id": "baseline", "seed": 1},
        facet_index=False,
    )
    tracker.end_run()

    tracker.begin_run(
        "run_not_indexed_guardrail",
        "m",
        facet={"scenario_id": "overflow", **{f"k{i}": i for i in range(600)}},
    )
    tracker.end_run()

    indexed_matches = tracker.find_runs(
        model="m",
        facet={"scenario_id": "baseline", "seed": 1},
    )
    overflow_matches = tracker.find_runs(
        model="m",
        facet={"scenario_id": "overflow"},
    )

    assert [run.id for run in indexed_matches] == ["run_indexed"]
    assert overflow_matches == []
    assert tracker.get_config_facets()
    assert tracker.get_run_config_kv("run_not_indexed_flag") == []
    assert tracker.get_run_config_kv("run_not_indexed_guardrail") == []
