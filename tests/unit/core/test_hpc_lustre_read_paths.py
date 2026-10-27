from __future__ import annotations

import errno
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

import consist
from consist.core.drivers import H5TableDriver, ParquetDriver, TableInfo
from consist.core.logical_array_identity import open_h5py_read_only
from consist.core.tracker import Tracker
from consist.core.tracker_config import TrackerConfig
from consist.models.artifact import Artifact


def test_import_consist_defers_optional_heavy_dependencies() -> None:
    script = (
        "import json, sys; "
        "import consist; "
        "mods = ['xarray', 'tables', 'geopandas', 'git']; "
        "print(json.dumps({m: (m in sys.modules) for m in mods}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    loaded = json.loads(proc.stdout.strip())
    assert loaded == {
        "xarray": False,
        "tables": False,
        "geopandas": False,
        "git": False,
    }


def test_open_h5py_read_only_defaults_locking_false_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("HDF5_USE_FILE_LOCKING", raising=False)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class FakeH5py:
        def File(self, *args: object, **kwargs: object) -> str:
            calls.append((args, kwargs))
            return "opened"

    target = tmp_path / "sample.h5"
    result = open_h5py_read_only(target, h5py_module=FakeH5py())
    assert result == "opened"
    assert calls == [((target, "r"), {"locking": False})]


def test_open_h5py_read_only_respects_hdf5_use_file_locking_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HDF5_USE_FILE_LOCKING", "TRUE")
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class FakeH5py:
        def File(self, *args: object, **kwargs: object) -> str:
            calls.append((args, kwargs))
            return "opened-env"

    target = tmp_path / "sample.h5"
    result = open_h5py_read_only(target, h5py_module=FakeH5py())
    assert result == "opened-env"
    assert calls == [((target, "r"), {})]


def test_open_h5py_read_only_falls_back_on_type_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("HDF5_USE_FILE_LOCKING", raising=False)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class LegacyH5py:
        def File(self, path: str, mode: str) -> str:
            calls.append(((path, mode), {}))
            return "opened-legacy"

    target = tmp_path / "sample.h5"
    result = open_h5py_read_only(target, h5py_module=LegacyH5py())
    assert result == "opened-legacy"
    assert calls == [((target, "r"), {})]


def test_h5_table_driver_load_passes_mode_r(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_read_hdf(path: str, **kwargs: object) -> pd.DataFrame:
        captured_kwargs["path"] = path
        captured_kwargs.update(kwargs)
        return pd.DataFrame({"col": [1, 2]})

    monkeypatch.setattr(pd, "read_hdf", fake_read_hdf)
    conn = duckdb.connect()
    try:
        driver = H5TableDriver()
        info = TableInfo(
            role="trips",
            table_path="/trips",
            container_uri=str(tmp_path / "store.h5"),
            driver="h5_table",
        )
        rel = driver.load(info, conn, columns=["col"])
        df = rel.df()
        assert df["col"].tolist() == [1, 2]
        assert captured_kwargs["mode"] == "r"
        assert captured_kwargs["key"] == "/trips"
        assert captured_kwargs["columns"] == ["col"]
    finally:
        conn.close()


def test_read_only_tracker_opens_db_read_only_and_skips_view_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "provenance.duckdb"
    run_dir = tmp_path / "runs"

    writer = Tracker(run_dir=run_dir, db_path=db_path)
    out_file = run_dir / "data.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [10, 20]}).to_parquet(out_file, index=False)
    with writer.start_run("seed_run", "demo_model"):
        writer.log_artifact(out_file, key="table_out", direction="output")
    writer.close()

    view_called = False

    def fail_if_view_called(self: Tracker, schema: object) -> object:
        nonlocal view_called
        view_called = True
        raise AssertionError("Tracker.view() should not be called in read_only mode")

    monkeypatch.setattr(Tracker, "view", fail_if_view_called)

    class DummySchema:
        pass

    with Tracker(
        run_dir=run_dir,
        db_path=db_path,
        access_mode="read_only",
        schemas=[DummySchema],
    ) as reader:
        assert reader.db is not None
        assert reader.db._read_only is True
        assert view_called is False
        run = reader.get_run("seed_run")
        assert run is not None
        assert run.id == "seed_run"


def test_tracker_local_snapshot_copies_db_and_wal_and_cleans_up_auto_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "shared" / "provenance.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "runs"
    slurm_tmp = tmp_path / "slurm_scratch"
    slurm_tmp.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("SLURM_TMPDIR", str(slurm_tmp))

    writer = Tracker(run_dir=run_dir, db_path=db_path)
    out_file = run_dir / "out.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(out_file, index=False)
    with writer.start_run("run_snap", "snap_model"):
        writer.log_artifact(out_file, key="out", direction="output")
    writer.close()

    # Create a dummy .wal file alongside the source DB to verify WAL copy semantics when present.
    wal_source = Path(f"{db_path}.wal")

    with Tracker(
        run_dir=run_dir,
        db_path=db_path,
        access_mode="read_only",
        local_snapshot=True,
    ) as snap_tracker:
        active_db_path = Path(snap_tracker.db_path)
        assert active_db_path != db_path
        assert active_db_path.exists()
        assert active_db_path.parent.parent == slurm_tmp.resolve()
        # Original configured db_path is preserved on to_config()
        cfg = snap_tracker.to_config()
        assert cfg.db_path == str(db_path)
        assert cfg.local_snapshot is True

        run = snap_tracker.get_run("run_snap")
        assert run is not None
        assert run.id == "run_snap"

    # Auto snapshot directory should be removed on context exit / close()
    assert not active_db_path.exists()
    assert not active_db_path.parent.exists()

    # Verify explicit destination path + WAL copy preservation
    wal_source.write_bytes(b"")
    explicit_dest = tmp_path / "explicit_snap" / "copy.duckdb"
    cfg_explicit = TrackerConfig(
        run_dir=run_dir,
        db_path=db_path,
        access_mode="read_only",
        local_snapshot=explicit_dest,
    )
    explicit_tracker = Tracker.from_config(cfg_explicit)
    assert Path(explicit_tracker.db_path) == explicit_dest.resolve()
    assert explicit_dest.exists()
    assert Path(f"{explicit_dest.resolve()}.wal").exists()
    explicit_tracker.close()
    # Explicit snapshot destination is not deleted by close()
    assert explicit_dest.exists()


def test_resolve_run_output_paths_bulk_preserving_order_and_filters(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "provenance.duckdb"
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    f1_a = run_dir / "r1_a.parquet"
    f1_b = run_dir / "r1_b.parquet"
    f2_a = run_dir / "r2_a.parquet"
    f1_a.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"v": [1]}).to_parquet(f1_a, index=False)
    pd.DataFrame({"v": [2]}).to_parquet(f1_b, index=False)
    pd.DataFrame({"v": [3]}).to_parquet(f2_a, index=False)

    with tracker.start_run("run_1", "model_1"):
        tracker.log_artifact(f1_a, key="a", direction="output")
        tracker.log_artifact(f1_b, key="b", direction="output")

    with tracker.start_run("run_2", "model_2"):
        tracker.log_artifact(f2_a, key="a", direction="output")

    # Delete f1_b from disk to test existing_only
    f1_b.unlink()

    all_paths = consist.resolve_run_output_paths(
        ["run_2", "run_1", "missing_run"],
        tracker=tracker,
    )
    assert list(all_paths.keys()) == ["run_2", "run_1", "missing_run"]
    assert set(all_paths["run_2"].keys()) == {"a"}
    assert all_paths["run_2"]["a"] == f2_a.resolve()
    assert set(all_paths["run_1"].keys()) == {"a", "b"}
    assert all_paths["missing_run"] == {}

    filtered_keys = tracker.resolve_run_output_paths(
        ["run_1", "run_2"],
        keys=["b"],
    )
    assert set(filtered_keys["run_1"].keys()) == {"b"}
    assert filtered_keys["run_2"] == {}

    existing_only = tracker.resolve_run_output_paths(
        ["run_1", "run_2"],
        existing_only=True,
    )
    assert set(existing_only["run_1"].keys()) == {"a"}
    assert set(existing_only["run_2"].keys()) == {"a"}
    tracker.close()


def test_columns_projection_across_load_apis_and_db_fallback(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "provenance.duckdb"
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    pq_path = run_dir / "persons.parquet"
    pq_path.parent.mkdir(parents=True, exist_ok=True)
    df_orig = pd.DataFrame(
        {
            "person_id": [1, 2, 3],
            "age": [25, 40, 60],
            "income": [50000, 80000, 120000],
        }
    )
    df_orig.to_parquet(pq_path, index=False)

    with tracker.start_run("run_cols", "model"):
        art = tracker.log_artifact(pq_path, key="persons", direction="output")
        tracker.ingest(art, df_orig)

    # 1. consist.load(..., columns=...)
    rel = consist.load(art, tracker=tracker, columns=["person_id", "age"])
    df_rel = consist.to_df(rel)
    assert list(df_rel.columns) == ["person_id", "age"]

    # 2. consist.load_df(..., columns=...)
    df_loaded = consist.load_df(art, tracker=tracker, columns=["age"])
    assert list(df_loaded.columns) == ["age"]
    assert df_loaded["age"].tolist() == [25, 40, 60]

    # 3. Artifact.as_df(columns=...)
    df_as_df = art.as_df(tracker=tracker, columns=["person_id", "income"])
    assert list(df_as_df.columns) == ["person_id", "income"]

    # 4. Tracker.load_run_output(..., columns=...)
    df_run_out = tracker.load_run_output(
        "run_cols", "persons", columns=["income", "person_id"]
    )
    assert list(df_run_out.columns) == ["income", "person_id"]

    # 5. tracker.load(..., columns=...) and consist.load_relation(..., columns=...)
    rel_tracker = tracker.load(art, columns=["person_id"])
    assert list(consist.to_df(rel_tracker).columns) == ["person_id"]
    with consist.load_relation(
        art, tracker=tracker, columns=["age", "income"]
    ) as rel_ctx:
        assert list(rel_ctx.df().columns) == ["age", "income"]

    # 6. Database fallback ("Ghost Mode") with columns=...
    pq_path.unlink()
    df_db = consist.load_df(
        art,
        tracker=tracker,
        db_fallback="always",
        columns=["person_id", "age"],
    )
    assert list(df_db.columns) == ["person_id", "age"]
    assert sorted(df_db["person_id"].tolist()) == [1, 2, 3]
    tracker.close()


def test_read_only_and_local_snapshot_validation_and_missing_db_errors(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    missing_db = tmp_path / "missing.duckdb"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        Tracker(run_dir=run_dir, db_path=missing_db, access_mode="read_only")

    with pytest.raises(FileNotFoundError, match="does not exist"):
        Tracker(run_dir=run_dir, db_path=missing_db, local_snapshot=True)

    with pytest.raises(ValueError, match="local_snapshot cannot be an empty string"):
        Tracker(run_dir=run_dir, db_path=missing_db, local_snapshot="   ")


def test_resolve_run_output_paths_historical_fallback_when_run_dir_differs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "provenance.duckdb"
    orig_run_dir = tmp_path / "orig_runs"
    writer = Tracker(run_dir=orig_run_dir, db_path=db_path)

    hist_file = orig_run_dir / "hist.parquet"
    hist_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(hist_file, index=False)

    with writer.start_run("hist_run", "model"):
        writer.log_artifact(hist_file, key="hist_out", direction="output")
    writer.close()

    # Open a reader pointed at a different run_dir so primary_path does not exist
    other_run_dir = tmp_path / "other_runs"
    with Tracker(
        run_dir=other_run_dir,
        db_path=db_path,
        access_mode="read_only",
    ) as reader:
        resolved_all = reader.resolve_run_output_paths(
            ["hist_run"], existing_only=False
        )
        assert resolved_all["hist_run"]["hist_out"] == hist_file.resolve()

        # When historical file is also removed, existing_only=False still returns
        # the historical path rather than the non-existent other_run_dir path.
        hist_file.unlink()
        resolved_missing = reader.resolve_run_output_paths(
            ["hist_run"], existing_only=False
        )
        assert resolved_missing["hist_run"]["hist_out"] == hist_file.resolve()
        resolved_existing = reader.resolve_run_output_paths(
            ["hist_run"], existing_only=True
        )
        assert resolved_existing["hist_run"] == {}


def test_parquet_enodata_fallback_retries_with_pyarrow_no_mmap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pq_path = tmp_path / "lustre_file.parquet"
    df_expected = pd.DataFrame({"col_a": [10, 20], "col_b": ["x", "y"]})
    df_expected.to_parquet(pq_path, index=False)

    import pyarrow.parquet as pq

    read_table_calls: list[dict[str, object]] = []
    real_read_table = pq.read_table

    def spy_read_table(path: str, *args: object, **kwargs: object):
        read_table_calls.append({"path": path, **kwargs})
        return real_read_table(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_table", spy_read_table)

    driver = ParquetDriver()
    info = TableInfo(
        role="lustre_file",
        table_path=None,
        container_uri=str(pq_path),
        driver="parquet",
    )

    fake_conn = MagicMock()
    real_conn = duckdb.connect()
    try:
        enodata_err = OSError(errno.ENODATA, "No data available")
        fake_conn.sql.side_effect = enodata_err
        fake_conn.from_arrow.side_effect = real_conn.from_arrow

        rel = driver.load(info, fake_conn, columns=["col_a"])
        result_df = rel.df()
        assert list(result_df.columns) == ["col_a"]
        assert result_df["col_a"].tolist() == [10, 20]
        assert len(read_table_calls) == 1
        assert read_table_calls[0]["columns"] == ["col_a"]
        assert read_table_calls[0].get("memory_map") is False
    finally:
        real_conn.close()


def test_to_df_enodata_fallback_when_materialization_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pq_path = tmp_path / "lazy_enodata.parquet"
    pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_parquet(pq_path, index=False)
    artifact = Artifact(key="lazy", container_uri=str(pq_path), driver="parquet")

    rel = consist.load(artifact, columns=["b"], nrows=2)
    assert isinstance(rel, duckdb.DuckDBPyRelation)

    fake_rel = MagicMock(spec=duckdb.DuckDBPyRelation)
    fake_rel.df.side_effect = duckdb.IOException(
        f"IO Error: Could not read file: errno {errno.ENODATA} (No data available)"
    )
    consist.api._RELATION_PARQUET_FALLBACKS[fake_rel] = (
        consist.api._RELATION_PARQUET_FALLBACKS[rel]
    )
    consist.api._close_relation_connection(rel)

    df = consist.to_df(fake_rel)
    assert list(df.columns) == ["b"]
    assert df["b"].tolist() == [4, 5]
