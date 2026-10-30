"""Unit tests for consist.digest module."""

from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from consist.digest import (
    ARRAY_BINARY_PROTOCOL_VERSION,
    FLOAT32_MATRICES_V1_POLICY,
    PRESERVE_CANONICAL_POLICY,
    ArrayCanonicalizationPolicy,
    canonical_json_sha256,
    canonical_json_str,
    canonicalize_float32_array,
    canonicalize_float64_array,
    canonicalize_float_array,
    compute_array_entry_sha256,
    compute_logical_matrices_sha256,
    digest_bytes,
    digest_file,
    digest_state,
    digest_table,
    file_sha256,
    normalize_logical_matrices_identity,
    normalize_table_scalar,
    resolve_canonicalization_policy,
)


def test_digest_bytes() -> None:
    data = b"hello consist digest"
    expected = hashlib.sha256(data).hexdigest()
    assert digest_bytes(data) == expected


def test_file_sha256_and_digest_file(tmp_path: Path) -> None:
    test_file = tmp_path / "sample.txt"
    content = b"deterministic file payload"
    test_file.write_bytes(content)

    expected_sha256 = hashlib.sha256(content).hexdigest()
    assert file_sha256(test_file) == expected_sha256
    assert digest_file(test_file) == f"sha256:file:{expected_sha256}"

    # Directory should raise ValueError
    with pytest.raises(ValueError, match="only supports regular files"):
        digest_file(tmp_path)

    # Missing file should raise FileNotFoundError or ValueError
    with pytest.raises((ValueError, FileNotFoundError)):
        digest_file(tmp_path / "nonexistent.txt")


def test_canonical_json_str_and_sha256() -> None:
    obj = {
        "z": 1,
        "a": [3, 2, 1],
        "b": {"nested_b": 2, "nested_a": 1},
        "excluded": "ignore_me",
    }
    json_str = canonical_json_str(obj, exclude_keys={"excluded"})
    assert "excluded" not in json_str
    # Keys should be sorted, separators compact (no spaces)
    assert json_str == '{"a":[3,2,1],"b":{"nested_a":1,"nested_b":2},"z":1}'

    sha = canonical_json_sha256(obj, exclude_keys={"excluded"})
    assert sha == hashlib.sha256(json_str.encode("utf-8")).hexdigest()


def test_canonical_json_sets_and_numpy() -> None:
    obj = {
        "set_val": {3, 1, 2},
        "np_int": np.int64(42),
        "np_arr": np.array([1, 2, 3]),
    }
    json_str = canonical_json_str(obj)
    assert json_str == '{"np_arr":[1,2,3],"np_int":42,"set_val":[1,2,3]}'


def test_normalize_table_scalar() -> None:
    assert normalize_table_scalar(None) == b"\x00"
    assert normalize_table_scalar(True) == b"true"
    assert normalize_table_scalar(False) == b"false"
    assert normalize_table_scalar(42) == b"42"
    assert normalize_table_scalar(0) == b"0"
    assert normalize_table_scalar(0.0) == b"0.0"
    assert normalize_table_scalar(-0.0) == b"0.0"
    assert normalize_table_scalar(float("nan")) == b"nan"
    assert normalize_table_scalar(3.14159) == b"3.14159"

    # Unicode NFC normalization
    e_acute_decomposed = "e\u0301"
    e_acute_composed = "\u00e9"
    assert normalize_table_scalar(e_acute_decomposed) == normalize_table_scalar(
        e_acute_composed
    )


def test_digest_table_row_order_invariance() -> None:
    schema = pa.schema(
        [
            ("person_id", pa.int64()),
            ("household_id", pa.int64()),
            ("age", pa.int32()),
        ]
    )

    data1 = {
        "person_id": [101, 102, 103],
        "household_id": [1, 1, 2],
        "age": [35, 34, 12],
    }
    # Shuffled rows
    data2 = {
        "person_id": [103, 101, 102],
        "household_id": [2, 1, 1],
        "age": [12, 35, 34],
    }

    t1 = pa.Table.from_pydict(data1, schema=schema)
    t2 = pa.Table.from_pydict(data2, schema=schema)

    cols = ["person_id", "household_id", "age"]
    keys = ["person_id"]

    digest1 = digest_table(t1, cols, keys)
    digest2 = digest_table(t2, cols, keys)

    assert digest1 == digest2
    assert digest1.startswith("ctd1:sha256:")
    assert len(digest1) == len("ctd1:sha256:") + 64


def test_digest_table_format_and_compression_invariance(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("household_id", pa.int64()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
        ]
    )

    data = {
        "household_id": [20, 10, 30],
        "latitude": [30.2672, 37.7749, 41.8781],
        "longitude": [-97.7431, -122.4194, -87.6298],
    }
    cols = ["household_id", "latitude", "longitude"]
    keys = ["household_id"]

    table = pa.Table.from_pydict(data, schema=schema)

    parquet_snappy = tmp_path / "snappy.parquet"
    parquet_zstd = tmp_path / "zstd.parquet"
    sqlite_db = tmp_path / "test.db"

    pq.write_table(table, parquet_snappy, compression="snappy")
    pq.write_table(table, parquet_zstd, compression="zstd")

    con = sqlite3.connect(sqlite_db)
    con.execute(
        "CREATE TABLE households (household_id INTEGER PRIMARY KEY, latitude REAL, longitude REAL)"
    )
    con.executemany(
        "INSERT INTO households VALUES (?, ?, ?)",
        [
            (10, 37.7749, -122.4194),
            (30, 41.8781, -87.6298),
            (20, 30.2672, -97.7431),
        ],
    )
    con.commit()
    con.close()

    d_table = digest_table(table, cols, keys)
    d_snappy = digest_table(parquet_snappy, cols, keys)
    d_zstd = digest_table(parquet_zstd, cols, keys)
    d_sqlite = digest_table(sqlite_db, cols, keys, sqlite_table="households")

    assert d_table == d_snappy
    assert d_snappy == d_zstd
    assert d_zstd == d_sqlite


def test_digest_table_column_order_invariance() -> None:
    t1 = pa.Table.from_pydict({"a": [1, 2], "b": ["x", "y"]})
    t2 = pa.Table.from_pydict({"b": ["x", "y"], "a": [1, 2]})

    cols = ["a", "b"]
    keys = ["a"]

    assert digest_table(t1, cols, keys) == digest_table(t2, cols, keys)


def test_digest_table_null_distinct_from_empty_string() -> None:
    t1 = pa.Table.from_pydict({"id": [1], "val": [None]})
    t2 = pa.Table.from_pydict({"id": [1], "val": [""]})

    cols = ["id", "val"]
    keys = ["id"]

    assert digest_table(t1, cols, keys) != digest_table(t2, cols, keys)


def test_digest_table_dictionary_encoded_column() -> None:
    # Dictionary columns must be decoded/cast without ArrowNotImplementedError,
    # including when used as the sort key.
    dict_arr = pa.DictionaryArray.from_arrays(
        pa.array([1, 0, 1, 0]), pa.array(["cat", "dog"])
    )
    table = pa.Table.from_arrays(
        [pa.array([1, 2, 3, 4]), dict_arr], names=["id", "animal"]
    )
    digest = digest_table(table, ["id", "animal"], ["animal", "id"])
    assert digest.startswith("ctd1:sha256:")


def test_digest_table_empty_keys_and_zero_columns() -> None:
    table = pa.Table.from_pydict({"id": [2, 1], "val": ["b", "a"]})
    # key=[] disables sorting without crashing PyArrow
    digest_no_sort = digest_table(table, ["id", "val"], key=[])
    assert digest_no_sort.startswith("ctd1:sha256:")

    # columns=[] produces a valid header-only digest
    digest_zero_cols = digest_table(table, columns=[], key=[])
    assert digest_zero_cols.startswith("ctd1:sha256:")


def test_digest_table_sqlite_reserved_identifiers_and_duplicate_cols(
    tmp_path: Path,
) -> None:
    sqlite_db = tmp_path / "reserved.db"
    con = sqlite3.connect(sqlite_db)
    con.execute('CREATE TABLE "order" ("group" INTEGER PRIMARY KEY, "route-id" TEXT)')
    con.executemany(
        'INSERT INTO "order" VALUES (?, ?)',
        [(2, "R2"), (1, "R1")],
    )
    con.commit()
    con.close()

    digest = digest_table(
        sqlite_db,
        columns=["group", "route-id"],
        key=["group"],
        sqlite_table="order",
    )
    expected_table = pa.Table.from_pydict({"group": [1, 2], "route-id": ["R1", "R2"]})
    assert digest == digest_table(
        expected_table, columns=["group", "route-id"], key=["group"]
    )

    with pytest.raises(ValueError, match="Duplicate column names"):
        digest_table(expected_table, columns=["group", "group"], key=["group"])


def test_digest_table_sqlite_security_and_validation(tmp_path: Path) -> None:
    sqlite_db = tmp_path / "weird?name#1 .sqlite"
    con = sqlite3.connect(sqlite_db)
    con.execute('CREATE TABLE "items" ("id" INTEGER, "val" TEXT)')
    con.execute("INSERT INTO \"items\" VALUES (1, 'a'), (2, 'b')")
    con.execute('CREATE VIEW "items_view" AS SELECT * FROM "items"')
    con.commit()
    con.close()

    # Path with URI special characters (?, #, space) works in read-only URI mode
    expected = pa.Table.from_pydict({"id": [1, 2], "val": ["a", "b"]})
    assert digest_table(sqlite_db, sqlite_table="items") == digest_table(expected)

    # Nonexistent table raises ValueError instead of returning empty table digest
    with pytest.raises(ValueError, match="SQLite table not found"):
        digest_table(sqlite_db, sqlite_table="no_such_table")

    # Views are rejected (restricted to base tables)
    with pytest.raises(ValueError, match="SQLite table not found"):
        digest_table(sqlite_db, sqlite_table="items_view")

    # Nonexistent columns raise ValueError (preventing SQLite DQS string-literal fallback)
    with pytest.raises(ValueError, match="Columns not found in SQLite table"):
        digest_table(sqlite_db, sqlite_table="items", columns=["id", "nonexistent"])

    # Empty or NUL-containing identifiers are rejected
    with pytest.raises(ValueError, match="non-empty string without NUL bytes"):
        digest_table(sqlite_db, sqlite_table="")

    with pytest.raises(ValueError, match="non-empty string without NUL bytes"):
        digest_table(sqlite_db, sqlite_table="items\x00drop")


def test_digest_table_record_batch_csv_and_duckdb(tmp_path: Path) -> None:
    import duckdb

    batch = pa.RecordBatch.from_pydict({"id": [2, 1], "name": ["b", "a"]})
    table = pa.Table.from_batches([batch])
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name\n2,b\n1,a\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    rel = con.sql("SELECT 2 AS id, 'b' AS name UNION ALL SELECT 1 AS id, 'a' AS name")

    d_table = digest_table(table, ["id", "name"], ["id"])
    assert digest_table(batch, ["id", "name"], ["id"]) == d_table
    assert digest_table(csv_path, ["id", "name"], ["id"]) == d_table
    assert digest_table(rel, ["id", "name"], ["id"]) == d_table
    con.close()


def test_canonical_json_models_enums_dataclasses_and_cycles() -> None:
    from dataclasses import dataclass
    from enum import Enum
    from pydantic import BaseModel

    class Mode(Enum):
        FAST = "fast"

    @dataclass
    class Item:
        path: Path
        mode: Mode

    class SampleModel(BaseModel):
        x: int
        y: str

    class LegacyModel:
        def dict(self) -> object:
            return {"legacy": 7}

        def json(self) -> str:
            return '{"legacy": 7}'

    shared = {"k": 1}
    payload = {
        "item": Item(path=Path("a/b.txt"), mode=Mode.FAST),
        "model": SampleModel(x=3, y="ok"),
        "legacy": LegacyModel(),
        "het_set": {1, "two"},
        "shared_1": shared,
        "shared_2": shared,
    }
    res = canonical_json_str(payload)
    assert '"path":"a/b.txt"' in res
    assert '"mode":"fast"' in res
    assert '"legacy":{"legacy":7}' in res

    cycle: dict[str, object] = {}
    cycle["self"] = cycle
    with pytest.raises(ValueError, match="Circular reference detected"):
        canonical_json_str(cycle)

    class Unserializable:
        pass

    with pytest.raises(TypeError):
        canonical_json_str({"bad": Unserializable()})


def test_digest_table_missing_key_column_raises() -> None:
    table = pa.Table.from_pydict({"a": [1, 2], "b": [3, 4]})
    with pytest.raises(ValueError, match="Key column 'c' must be in declared columns"):
        digest_table(table, columns=["a", "b"], key_columns=["c"])


def test_digest_table_profiles() -> None:
    table = pa.Table.from_pydict({"id": [1, 2], "v": [10, 20]})
    cols = ["id", "v"]
    keys = ["id"]

    d_ctd1 = digest_table(table, cols, keys, profile="ctd1")
    assert d_ctd1.startswith("ctd1:sha256:")

    d_core = digest_table(table, cols, keys, profile="core_facet_table/v1")
    assert d_core.startswith("sha256:")
    assert not d_core.startswith("ctd1:sha256:")

    d_view = digest_table(table, cols, keys, profile="view_table/v1")
    assert d_view.startswith("sha256:")
    assert d_view != d_core

    with pytest.raises(ValueError, match="profile must be a non-empty string"):
        digest_table(table, cols, keys, profile="")


def test_digest_state() -> None:
    facets1 = [
        {
            "facet": "households",
            "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        },
        {
            "facet": "people",
            "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        },
    ]
    facets2 = [
        {
            "facet": "people",
            "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        },
        {
            "facet": "households",
            "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        },
    ]
    facets_dict = {
        "households": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "people": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    }

    digest1 = digest_state(facets1)
    digest2 = digest_state(facets2)
    digest3 = digest_state(facets_dict)

    assert digest1.startswith("sha256:")
    assert len(digest1) == 7 + 64
    assert digest1 == digest2 == digest3

    # Different digest
    facets_diff = [
        {
            "facet": "households",
            "digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333",
        },
        {
            "facet": "people",
            "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        },
    ]
    assert digest_state(facets_diff) != digest1

    with pytest.raises(ValueError, match="Unsupported state digest profile"):
        digest_state(facets1, profile="unsupported")


def test_matrix_canonicalization_and_digest() -> None:
    arr = np.array([-0.0, 1.0, 2.0, np.nan], dtype=np.float32)
    canon = canonicalize_float32_array(arr)
    assert isinstance(canon, bytes)
    canon_arr = np.frombuffer(canon, dtype="<f4")
    # -0.0 normalized to +0.0
    assert np.signbit(canon_arr[0]) == 0
    # NaN bit pattern canonicalized
    assert np.isnan(canon_arr[3])
    # Check 0x7fc00000 bit pattern
    assert canon_arr.view("<u4")[3] == 0x7FC00000

    policy = resolve_canonicalization_policy("preserve_canonical")
    assert policy == PRESERVE_CANONICAL_POLICY
    assert policy.profile == "preserve_canonical"
    assert isinstance(policy, ArrayCanonicalizationPolicy)
    assert ARRAY_BINARY_PROTOCOL_VERSION == "consist.array_binary.v1"

    policy_f32 = resolve_canonicalization_policy("float32_matrices_v1")
    assert policy_f32 == FLOAT32_MATRICES_V1_POLICY
    assert policy_f32.profile == "float32_matrices_v1"

    arr_f64 = np.array([-0.0, 1.0, 2.0, np.nan], dtype=np.float64)
    canon_f64 = canonicalize_float64_array(arr_f64)
    assert isinstance(canon_f64, bytes)

    canon_arr2 = canonicalize_float_array(arr, target_dtype="preserve")
    assert canon_arr2.shape == arr.shape

    entry_hash = compute_array_entry_sha256(
        "matrix_a",
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
    )
    assert len(entry_hash) == 64

    matrices_hash = compute_logical_matrices_sha256(
        {"matrix_a": np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)},
        policy="preserve_canonical",
    )
    assert len(matrices_hash) == 64
    norm_id = normalize_logical_matrices_identity(matrices_hash)
    assert norm_id.startswith("sha256:logical_matrices:")
