from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from consist.core.tracker import Tracker

h5py = pytest.importorskip("h5py")
pytest.importorskip("tables")


def _write_h5(path: Path, people_values: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5_file:
        tables = h5_file.create_group("tables")
        tables.create_dataset("people", data=people_values)
        tables.create_dataset("households", data=[10, 20])


def _update_h5_people(path: Path, people_values: list[int]) -> None:
    with h5py.File(path, "a") as h5_file:
        if "tables/people" in h5_file:
            del h5_file["tables/people"]
        h5_file["tables"].create_dataset("people", data=people_values)


def test_h5_lineage_hashing_demo(tracker: Tracker, run_dir: Path) -> None:
    """
    End-to-end demonstration of HDF5 lineage and hashing rules.

    Behavior under test:
    - First run computes a file hash and table hashes for discovered datasets.
    - A repeat run over the same file logs a new input artifact but keeps the same file hash.
    - After mutating a dataset, the file hash changes and a new container is created.
    - When the file hash changes, table hashing is skipped to avoid extra cost.
    - A single table can be logged directly with schema profiling enabled.
    - Cache hits return the cached H5 container artifact when the run signature matches.

    How it is tested:
    - Run 1 logs the file as input and asserts table hashes are present.
    - Run 2 repeats without changes and asserts the file hash is stable.
    - Run 3 mutates one table and asserts a new container with hash-skip metadata.
    - A final run logs one table and asserts schema metadata is persisted.
    - A cached run logs the same container key and confirms a cache hit.
    """
    h5_path = run_dir / "inputs" / "demo.h5"
    _write_h5(h5_path, people_values=[1, 2, 3])

    with tracker.start_run("h5_demo_v1", model="demo", tags=["h5_demo", "input"]):
        container_v1, tables_v1 = tracker.log_h5_container(
            h5_path,
            key="demo_h5",
            direction="input",
        )

    assert container_v1.meta.get("table_hashes_checked") is True
    assert tables_v1, "Expected discovered tables for initial H5 file."
    assert all("table_hash" in t.meta for t in tables_v1)
    inputs_v1 = tracker.get_run_inputs("h5_demo_v1")
    assert "demo_h5" in inputs_v1

    with tracker.start_run(
        "h5_demo_v1_repeat", model="demo", tags=["h5_demo", "repeat"]
    ):
        container_repeat, tables_repeat = tracker.log_h5_container(
            h5_path,
            key="demo_h5",
            direction="input",
        )

    assert container_repeat.id != container_v1.id
    assert container_repeat.hash == container_v1.hash
    assert container_repeat.meta.get("table_hashes_checked") is True
    assert all("table_hash" in t.meta for t in tables_repeat)

    _update_h5_people(h5_path, people_values=[9, 9, 9])

    with tracker.start_run("h5_demo_v2", model="demo", tags=["h5_demo", "updated"]):
        container_v2, tables_v2 = tracker.log_h5_container(
            h5_path,
            key="demo_h5",
            direction="input",
        )

    assert container_v2.id != container_v1.id
    assert container_v2.meta.get("table_hashes_checked") is False
    assert container_v2.meta.get("table_hashes_skip_reason") == "file_hash_changed"
    assert all("table_hash" not in t.meta for t in tables_v2)

    profile_path = run_dir / "inputs" / "profile.h5"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_df = pd.DataFrame({"person_id": [1, 2, 3], "age": [30, 40, 50]})
    profile_df.to_hdf(profile_path, key="tables/people", mode="w", format="table")

    with tracker.start_run("h5_profile", model="demo", tags=["h5_demo", "profile"]):
        people_table = tracker.log_h5_table(
            profile_path,
            table_path="/tables/people",
            key="people_table",
            direction="output",
            profile_file_schema=True,
            file_schema_sample_rows=10,
        )

    assert people_table.meta.get("schema_id") is not None
    assert people_table.meta.get("schema_summary") is not None

    cache_path = run_dir / "outputs" / "cache_demo.h5"
    _write_h5(cache_path, people_values=[42])

    with tracker.start_run(
        "h5_cache_seed",
        model="demo_cache",
        cache_mode="overwrite",
        config={"cache_demo": True},
        tags=["h5_demo", "cache_seed"],
    ) as t:
        container_seed, _ = t.log_h5_container(
            cache_path,
            key="cached_h5",
            direction="output",
            discover_tables=False,
        )
        assert not t.is_cached

    with tracker.start_run(
        "h5_cache_hit",
        model="demo_cache",
        cache_mode="reuse",
        config={"cache_demo": True},
        tags=["h5_demo", "cache_hit"],
    ) as t:
        container_hit, _ = t.log_h5_container(
            cache_path,
            key="cached_h5",
            direction="output",
            discover_tables=False,
        )
        assert t.is_cached
        assert t.current_consist.cached_run is not None
        assert t.current_consist.cached_run.id == "h5_cache_seed"
        assert container_hit.id == container_seed.id
