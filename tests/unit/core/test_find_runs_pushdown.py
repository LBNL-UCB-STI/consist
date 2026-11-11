"""Unit tests for SQL pushdown of `tags` and `metadata` filters in `find_runs`."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from consist.core.tracker import Tracker
from consist.models.run import Run
from consist.store.ledger import StoreLedger

UTC = timezone.utc

FindRunsFn = Callable[..., list[Run]]


def _make_query_fn(tracker: Tracker, backend: str) -> FindRunsFn:
    if backend == "db":
        assert tracker.db is not None
        db = tracker.db

        def _db_query(
            *,
            tags: Sequence[str] | None = None,
            metadata: Mapping[str, Any] | None = None,
            limit: int | None = 100,
            offset: int = 0,
        ) -> list[Run]:
            return db.find_runs(
                tags=list(tags) if tags is not None else None,
                metadata=metadata,
                limit=limit,
                offset=offset,
                raise_on_error=True,
            )

        return _db_query

    assert tracker.ledger is not None
    ledger = tracker.ledger

    def _ledger_query(
        *,
        tags: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        limit: int | None = 100,
        offset: int = 0,
    ) -> list[Run]:
        return ledger.find_runs(
            tags=tags,
            metadata=metadata,
            limit=limit,
            offset=offset,
        )

    return _ledger_query


def _insert_raw_run(
    ledger: StoreLedger,
    *,
    run_id: str,
    created_at: datetime,
    raw_tags: str | None,
    raw_meta: str,
) -> None:
    with ledger.transaction() as cur:
        cur.execute(
            "INSERT INTO main.run ("
            "id, model_name, status, cache_status, tags, meta, "
            "started_at, ended_at, created_at, updated_at"
            ") VALUES (?, 'test_model', 'completed', 'live', "
            "CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?)",
            [
                run_id,
                raw_tags,
                raw_meta,
                created_at,
                created_at,
                created_at,
                created_at,
            ],
        )


@pytest.mark.parametrize("backend", ["db", "ledger"])
def test_find_runs_tags_pushdown_and_pagination(tmp_path: Path, backend: str) -> None:
    tracker = Tracker(
        run_dir=tmp_path / f"runs_{backend}",
        db_path=str(tmp_path / f"tags_{backend}.duckdb"),
    )
    try:
        assert tracker.ledger is not None
        t0 = datetime(2026, 1, 1, tzinfo=UTC)

        rows = [
            ("r01", ["alpha"], "{}"),
            ("r02", ["alpha", "beta"], "{}"),
            ("r03", ["alpha", "beta", "gamma"], "{}"),
            ("r04", ['tag"1', "a.b", "$x", "s/p", "t~0"], "{}"),
            ("r05", [], "{}"),
        ]
        for idx, (rid, tag_list, meta_json) in enumerate(rows):
            _insert_raw_run(
                tracker.ledger,
                run_id=rid,
                created_at=t0 + timedelta(seconds=idx),
                raw_tags=json.dumps(tag_list),
                raw_meta=meta_json,
            )

        # Malformed / non-array / null-element JSON tag edge cases
        edge_rows = [
            ("r06_sql_null", None),
            ("r07_json_null", "null"),
            ("r08_json_obj", '{"alpha": true}'),
            ("r09_json_num", "42"),
            ("r10_null_elem", '[null, "ok"]'),
            ("r11_obj_elem", '[{"nested": 1}, "ok"]'),
        ]
        for idx, (rid, raw_tags) in enumerate(edge_rows, start=10):
            _insert_raw_run(
                tracker.ledger,
                run_id=rid,
                created_at=t0 + timedelta(seconds=idx),
                raw_tags=raw_tags,
                raw_meta="{}",
            )

        # Verify record_run normalizes non-str tag elements to str
        tracker.ledger.record_run(
            run_id="r12_coerced_tags",
            model_name="test_model",
            status="completed",
            tags=cast(Sequence[str], [True, 0, "0"]),
            created_at=t0 + timedelta(seconds=30),
        )

        find_fn = _make_query_fn(tracker, backend)

        # Single tag (newest first)
        assert [r.id for r in find_fn(tags=["alpha"])] == ["r03", "r02", "r01"]
        assert [r.id for r in find_fn(tags=["True", "0"])] == ["r12_coerced_tags"]

        # Public Tracker.find_runs parity check
        tracker_res = tracker.find_runs(tags=["alpha"], limit=2)
        assert isinstance(tracker_res, list)
        assert [r.id for r in tracker_res] == ["r03", "r02"]

        # Multi-tag subset (including duplicate requested tag)
        assert [r.id for r in find_fn(tags=["alpha", "beta", "alpha"])] == [
            "r03",
            "r02",
        ]

        # Empty tags=[] adds no tag restriction
        all_ids = [r.id for r in find_fn(tags=[])]
        assert len(all_ids) == 12

        # Special characters in tags
        assert [r.id for r in find_fn(tags=['tag"1', "a.b", "$x", "s/p", "t~0"])] == [
            "r04"
        ]

        # Null/object/corrupt tags excluded; [null, "ok"] matches "ok" and excludes missing
        assert [r.id for r in find_fn(tags=["ok"])] == [
            "r11_obj_elem",
            "r10_null_elem",
        ]
        assert find_fn(tags=["missing_tag"]) == []

        # Limit + offset pagination order
        assert [r.id for r in find_fn(tags=["alpha"], limit=2, offset=0)] == [
            "r03",
            "r02",
        ]
        assert [r.id for r in find_fn(tags=["alpha"], limit=2, offset=1)] == [
            "r02",
            "r01",
        ]
        assert [r.id for r in find_fn(tags=["alpha"], limit=None, offset=2)] == ["r01"]
    finally:
        tracker.close()


@pytest.mark.parametrize("backend", ["db", "ledger"])
def test_find_runs_metadata_scalar_parity_and_special_keys(
    tmp_path: Path, backend: str
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / f"runs_meta_{backend}",
        db_path=str(tmp_path / f"meta_{backend}.duckdb"),
    )
    try:
        assert tracker.ledger is not None
        t0 = datetime(2026, 2, 1, tzinfo=UTC)
        big_exact = (1 << 53) + 1
        big_neighbor = 1 << 53
        neg_big_exact = -((1 << 53) + 7)
        huge_int = 1 << 70

        raw_records: list[tuple[str, str]] = [
            ("m_str", json.dumps({"k": "val", "region": "west"})),
            ("m_int_1", json.dumps({"num": 1, "flag": 1, "zero": 0})),
            ("m_float_1", json.dumps({"num": 1.0, "pi": 3.25})),
            ("m_bool_t", json.dumps({"flag": True, "num": True})),
            ("m_bool_f", json.dumps({"zero": False, "flag": False})),
            ("m_flag_str", json.dumps({"flag": "enabled"})),
            ("m_flag_arr", json.dumps({"flag": [1]})),
            ("m_big_exact", json.dumps({"big": big_exact, "neg_big": neg_big_exact})),
            ("m_big_neighbor", json.dumps({"big": big_neighbor})),
            ("m_big_float", '{"big_f": 9007199254740992.0}'),
            ("m_huge_int", json.dumps({"huge": huge_int})),
            ("m_explicit_null", '{"k": null}'),
            ("m_json_null", "null"),
            ("m_json_array", '["west", 2, 3]'),
            (
                "m_special_keys",
                json.dumps(
                    {
                        "a.b": 1,
                        'k"q': "v",
                        "$x": 2,
                        "a/b": 3,
                        "a~b": 4,
                        "*": 5,
                        "": "empty_key_val",
                    }
                ),
            ),
        ]

        for idx, (rid, raw_meta) in enumerate(raw_records):
            _insert_raw_run(
                tracker.ledger,
                run_id=rid,
                created_at=t0 + timedelta(seconds=idx),
                raw_tags='["all", "0"]',
                raw_meta=raw_meta,
            )

        find_fn = _make_query_fn(tracker, backend)

        # String match + Tracker.find_runs parity
        assert [r.id for r in find_fn(metadata={"k": "val"})] == ["m_str"]
        tracker_meta = tracker.find_runs(metadata={"k": "val"}, limit=10)
        assert isinstance(tracker_meta, list)
        assert [r.id for r in tracker_meta] == ["m_str"]

        # RFC 6901 "/0" must NEVER index into a root JSON array r.meta = '["west", 2, 3]'
        assert find_fn(tags=["0"], metadata={"0": "west"}) == []

        # Non-str metadata keys match Python rn.meta.get(mk) == mv semantics
        untyped_non_none = cast(Mapping[str, Any], {1: "a"})
        untyped_none = cast(Mapping[str, Any], {1: None})
        assert find_fn(metadata=untyped_non_none) == []
        assert len(find_fn(metadata=untyped_none)) == len(raw_records)

        # Int / float cross-matching (1 == 1.0 in Python, excluding True!)
        assert [r.id for r in find_fn(metadata={"num": 1})] == ["m_float_1", "m_int_1"]
        assert [r.id for r in find_fn(metadata={"num": 1.0})] == [
            "m_float_1",
            "m_int_1",
        ]
        assert [r.id for r in find_fn(metadata={"pi": 3.25})] == ["m_float_1"]

        # >= 2^53 cross-type & arbitrary-precision parity (no rounding or OverflowError)
        assert [r.id for r in find_fn(metadata={"big": big_exact})] == ["m_big_exact"]
        assert [r.id for r in find_fn(metadata={"big": big_neighbor})] == [
            "m_big_neighbor"
        ]
        assert [r.id for r in find_fn(metadata={"big": float(1 << 53)})] == [
            "m_big_neighbor"
        ]
        assert [r.id for r in find_fn(metadata={"big_f": 1 << 53})] == ["m_big_float"]
        assert [r.id for r in find_fn(metadata={"huge": huge_int})] == ["m_huge_int"]
        assert find_fn(metadata={"huge": huge_int + 1}) == []
        assert [r.id for r in find_fn(metadata={"neg_big": neg_big_exact})] == [
            "m_big_exact"
        ]

        # NaN / inf / -inf return [] cleanly
        assert find_fn(metadata={"num": math.nan}) == []
        assert find_fn(metadata={"num": math.inf}) == []
        assert find_fn(metadata={"num": -math.inf}) == []

        # Bool strictness + heterogeneous "enabled" / [1] safety (TRY_CAST AS BOOLEAN)
        assert [r.id for r in find_fn(metadata={"flag": True})] == ["m_bool_t"]
        assert [r.id for r in find_fn(metadata={"flag": False})] == ["m_bool_f"]
        assert [r.id for r in find_fn(metadata={"zero": False})] == ["m_bool_f"]
        assert [r.id for r in find_fn(metadata={"zero": 0})] == ["m_int_1"]

        # None matches missing key, explicit {"k": null}, and non-object meta,
        # while excluding {"k": "val"}
        none_ids = {r.id for r in find_fn(metadata={"k": None})}
        assert "m_str" not in none_ids
        assert {
            "m_explicit_null",
            "m_json_null",
            "m_json_array",
            "m_int_1",
        }.issubset(none_ids)

        # Dotted, quoted, dollar, slash, tilde, wildcard, and empty keys
        assert [
            r.id
            for r in find_fn(
                metadata={
                    "a.b": 1,
                    'k"q': "v",
                    "$x": 2,
                    "a/b": 3,
                    "a~b": 4,
                    "*": 5,
                    "": "empty_key_val",
                }
            )
        ] == ["m_special_keys"]
    finally:
        tracker.close()


def test_sql_null_meta_clause_parity() -> None:
    """Verify `r.meta IS NULL` matches `metadata={"k": None}` and excludes non-None."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE r (id VARCHAR, tags JSON, meta JSON)")
        con.executemany(
            "INSERT INTO r VALUES (?, CAST(? AS JSON), CAST(? AS JSON))",
            [
                ("sql_null_meta", '["t"]', None),
                ("explicit_null_key", '["t"]', '{"k": null}'),
                ("missing_key", '["t"]', '{"other": 1}'),
                ("has_val", '["t"]', '{"k": "val"}'),
            ],
        )

        clauses_none: list[str] = []
        params_none: list[Any] = []
        StoreLedger.append_run_tag_and_metadata_clauses(
            clauses_none, params_none, metadata={"k": None}
        )
        matched_none = {
            row[0]
            for row in con.execute(
                f"SELECT id FROM r WHERE {' AND '.join(clauses_none)}", params_none
            ).fetchall()
        }
        assert matched_none == {"sql_null_meta", "explicit_null_key", "missing_key"}

        clauses_val: list[str] = []
        params_val: list[Any] = []
        StoreLedger.append_run_tag_and_metadata_clauses(
            clauses_val, params_val, metadata={"k": "val"}
        )
        matched_val = [
            row[0]
            for row in con.execute(
                f"SELECT id FROM r WHERE {' AND '.join(clauses_val)}", params_val
            ).fetchall()
        ]
        assert matched_val == ["has_val"]
    finally:
        con.close()


@pytest.mark.parametrize("backend", ["db", "ledger"])
def test_find_runs_metadata_nested_structures_and_pagination(
    tmp_path: Path, backend: str
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / f"runs_nested_{backend}",
        db_path=str(tmp_path / f"nested_{backend}.duckdb"),
    )
    try:
        assert tracker.ledger is not None
        t0 = datetime(2026, 3, 1, tzinfo=UTC)

        # Insert unsorted JSON keys and mixed scalar/nested rows
        entries = [
            ("n01", '{"cfg": {"b": 2, "a": 1}, "seq": [1, 2]}'),
            ("n02", '{"cfg": {"a": 1, "b": 999}, "seq": [1, 2]}'),
            ("n03", '{"cfg": {"z": 0, "a": 1, "b": 2}, "seq": [1, 2]}'),
            ("n04", '{"cfg": {"b": 2, "a": 1}, "seq": [1, 2]}'),
            ("n05", '{"cfg": "not_an_object", "seq": "not_a_list"}'),
            ("n06", '{"cfg": {"a": 1, "b": 2}, "seq": [1, 2]}'),
        ]
        for idx, (rid, raw_meta) in enumerate(entries):
            _insert_raw_run(
                tracker.ledger,
                run_id=rid,
                created_at=t0 + timedelta(seconds=idx),
                raw_tags='["nested_suite"]',
                raw_meta=raw_meta,
            )

        find_fn = _make_query_fn(tracker, backend)

        target_meta = {"cfg": {"a": 1, "b": 2}, "seq": [1, 2]}
        assert [r.id for r in find_fn(metadata=target_meta)] == ["n06", "n04", "n01"]
        assert [
            r.id for r in find_fn(tags=["nested_suite"], metadata=target_meta, limit=2)
        ] == ["n06", "n04"]
        assert [
            r.id
            for r in find_fn(
                tags=["nested_suite"], metadata=target_meta, limit=2, offset=1
            )
        ] == ["n04", "n01"]
        assert [r.id for r in find_fn(metadata={"seq": [1, 2]}, limit=2, offset=1)] == [
            "n04",
            "n03",
        ]

        # Verify append_run_tag_and_metadata_clauses signals pushdown vs client filter accurately
        clauses: list[str] = []
        params: list[Any] = []
        assert (
            StoreLedger.append_run_tag_and_metadata_clauses(
                clauses,
                params,
                tags=["nested_suite"],
                metadata={"scalar": 42},
            )
            is False
        )
        assert (
            StoreLedger.append_run_tag_and_metadata_clauses(
                clauses,
                params,
                metadata={"big": 1 << 53},
            )
            is True
        )
        assert (
            StoreLedger.append_run_tag_and_metadata_clauses(
                clauses,
                params,
                metadata=target_meta,
            )
            is True
        )
    finally:
        tracker.close()
