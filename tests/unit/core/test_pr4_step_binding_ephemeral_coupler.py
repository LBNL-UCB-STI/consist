"""PR 4 tests for step parameter binding, outputs/staleness guard, ephemeral mode, and lazy coupler."""

from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Annotated, Optional
from unittest.mock import patch

import h5py
import pandas as pd
import pyarrow as pa
import pytest

from consist import (
    Artifact,
    CacheOptions,
    ExecutionOptions,
    MissingOutputError,
    OutputArtifactSpec,
    OutputPolicyOptions,
    StaleOutputError,
    Tracker,
)


def test_step_binding_artifact_has_zero_pre_step_hydration_io(tmp_path: Path) -> None:
    """B17: def step(x: Artifact) receives Artifact with zero pre-step hydration/staging I/O."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    src_file = tmp_path / "source.csv"
    src_file.write_text("a,b\n1,2\n", encoding="utf-8")

    def producer(out: Path) -> None:
        out.write_text("a,b\n1,2\n", encoding="utf-8")

    prod_res = tracker.run(
        fn=producer,
        outputs={"out": "produced.csv"},
    )
    prod_art = prod_res.outputs["out"]

    # Even if the artifact file is deleted from disk ("Ghost Mode") and
    # cache_hydration="inputs-missing", an Artifact-typed parameter must NOT
    # trigger pre-step hydration or staging I/O.
    Path(prod_art.path).unlink()

    def consumer(x: Artifact, out: Path) -> None:
        assert isinstance(x, Artifact)
        assert x.key == "out"
        out.write_text("ok\n", encoding="utf-8")

    with (
        patch(
            "consist.core.tracker_orchestration.resolve_verified_local_path"
        ) as mock_resolve,
        patch.object(tracker, "stage_artifact") as mock_stage,
    ):
        res = tracker.run(
            fn=consumer,
            inputs={"x": prod_art},
            outputs={"out": "final.txt"},
            cache_options=CacheOptions(cache_hydration="inputs-missing"),
        )
        assert mock_resolve.call_count == 0
        assert mock_stage.call_count == 0
        assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "ok\n"


def test_step_binding_path_and_dataframe_and_annotations(tmp_path: Path) -> None:
    """Step binding handles Path, os.PathLike, DataFrame, Table, Annotated, Optional, and closure NameError."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    raw_csv = tmp_path / "raw.csv"
    raw_csv.write_text("x,y\n1,10\n2,20\n", encoding="utf-8")

    raw_parquet = tmp_path / "raw.parquet"
    pd.DataFrame({"x": [3, 4], "y": [30, 40]}).to_parquet(raw_parquet)

    def make_step_with_closure_error():
        # UndefinedLocalType is intentionally missing from module globals
        # to verify per-parameter annotation resolution isolates NameError.
        def step(
            p: Annotated[Path, "meta"],
            p_opt: Optional[os.PathLike[str]],
            df: pd.DataFrame,
            tbl: pa.Table,
            out: Path | None,
            extra: "UndefinedLocalType" = None,  # type: ignore[name-defined]  # ty: ignore[unresolved-reference]  # noqa: F821
        ) -> None:
            assert isinstance(p, Path)
            assert p.read_text(encoding="utf-8").startswith("x,y")
            assert isinstance(p_opt, Path)
            assert isinstance(df, pd.DataFrame)
            assert list(df["x"]) == [1, 2]
            assert isinstance(tbl, pa.Table)
            assert tbl.num_rows == 2
            assert isinstance(out, Path)
            assert extra is None
            out.write_text("done\n", encoding="utf-8")

        return step

    step_fn = make_step_with_closure_error()
    res = tracker.run(
        fn=step_fn,
        inputs={"p": raw_csv, "p_opt": raw_csv, "df": raw_csv, "tbl": raw_parquet},
        outputs={"out": "bound_out.txt"},
    )
    assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "done\n"


def test_step_binding_unannotated_respects_execution_options_and_defaults_to_artifact(
    tmp_path: Path,
) -> None:
    """Unannotated parameters default to Artifact, but honor explicit input_binding or load_inputs."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    raw_csv = tmp_path / "data.csv"
    raw_csv.write_text("a\n1\n", encoding="utf-8")

    seen: list[type] = []

    def unannotated_default(data, out: Path) -> None:
        seen.append(type(data))
        assert isinstance(data, Artifact)
        out.write_text("a\n", encoding="utf-8")

    tracker.run(
        fn=unannotated_default,
        inputs={"data": raw_csv},
        outputs={"out": "o1.txt"},
    )
    assert seen == [Artifact]

    def unannotated_paths(data, out: Path) -> None:
        assert isinstance(data, Path)
        out.write_text("b\n", encoding="utf-8")

    tracker.run(
        fn=unannotated_paths,
        inputs={"data": raw_csv},
        outputs={"out": "o2.txt"},
        execution_options=ExecutionOptions(input_binding="paths"),
    )

    def unannotated_loaded(data, out: Path) -> None:
        assert isinstance(data, pd.DataFrame)
        out.write_text("c\n", encoding="utf-8")

    tracker.run(
        fn=unannotated_loaded,
        inputs={"data": raw_csv},
        outputs={"out": "o3.txt"},
        execution_options=ExecutionOptions(load_inputs=True),
    )


def test_declared_outputs_identity_and_config_cleanliness(tmp_path: Path) -> None:
    """outputs={...} distinguishes multi-dot stems and extensions without polluting run.config."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def writer(out: Path) -> None:
        out.write_text("hello", encoding="utf-8")

    r1 = tracker.run(
        fn=writer,
        outputs={"out": "sim.2020.parquet"},
        config={"alpha": 1},
    )
    r2 = tracker.run(
        fn=writer,
        outputs={"out": "sim.2021.parquet"},
        config={"alpha": 1},
    )
    r3 = tracker.run(
        fn=writer,
        outputs={"out": "a.csv"},
        config={"alpha": 1},
    )
    r4 = tracker.run(
        fn=writer,
        outputs={"out": "b.csv"},
        config={"alpha": 1},
    )

    assert r1.run.config_hash != r2.run.config_hash
    assert r3.run.config_hash != r4.run.config_hash
    assert r1.run.signature != r2.run.signature
    assert r3.run.signature != r4.run.signature

    cfg = tracker.get_run_config(r1.run.id)
    assert cfg == {"alpha": 1}
    assert "__consist_declared_outputs__" not in cfg


def test_output_paths_alias_normalizes_to_outputs(tmp_path: Path) -> None:
    """output_paths= works as a thin compatibility alias for outputs=."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def writer(out: Path) -> None:
        out.write_text("via_alias", encoding="utf-8")

    res = tracker.run(
        fn=writer,
        output_paths={"out": "alias.txt"},
    )
    assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "via_alias"


def test_staleness_guard_regular_file_dir_and_hdf5_member(tmp_path: Path) -> None:
    """B9: Pre-existing untouched regular file, directory/Zarr, or HDF5 member raises StaleOutputError."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    # 1. Untouched regular file
    stale_file = tmp_path / "stale.txt"
    stale_file.write_text("old", encoding="utf-8")

    def noop_step(out: Path) -> None:
        _ = out

    with pytest.raises(StaleOutputError):
        tracker.run(
            fn=noop_step,
            outputs={"out": stale_file},
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    # 2. Missing declared output raises MissingOutputError
    missing_file = tmp_path / "never_created.txt"
    with pytest.raises(MissingOutputError):
        tracker.run(
            fn=noop_step,
            outputs={"out": missing_file},
            cache_options=CacheOptions(cache_mode="overwrite"),
            output_policy=OutputPolicyOptions(output_missing="error"),
        )

    # 3. Untouched directory / Zarr store
    stale_dir = tmp_path / "store.zarr"
    stale_dir.mkdir()
    (stale_dir / "zgroup").write_text("{}", encoding="utf-8")

    with pytest.raises(StaleOutputError):
        tracker.run(
            fn=noop_step,
            outputs={"out": stale_dir},
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    # Modifying a nested file inside the directory passes the staleness guard
    def update_dir(out: Path) -> None:
        (out / "zarray").write_text("updated", encoding="utf-8")

    res_dir = tracker.run(
        fn=update_dir,
        outputs={"out": stale_dir},
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    assert (Path(res_dir.outputs["out"].path) / "zarray").exists()

    # 4. Untouched HDF5 member
    h5_path = tmp_path / "bundle.h5"
    with h5py.File(h5_path, "w") as f:
        f.create_dataset("m1", data=[1, 2, 3])

    with pytest.raises(StaleOutputError):
        tracker.run(
            fn=noop_step,
            outputs={"out": OutputArtifactSpec(path=h5_path, member="m1")},
            cache_options=CacheOptions(cache_mode="overwrite"),
        )

    # Updating the HDF5 member passes
    def update_h5(out: Path) -> None:
        with h5py.File(out, "a") as f:
            del f["m1"]
            f.create_dataset("m1", data=[4, 5, 6])

    res_h5 = tracker.run(
        fn=update_h5,
        outputs={"out": OutputArtifactSpec(path=h5_path, member="m1")},
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    assert res_h5.outputs["out"].key == "out"


def test_ephemeral_mode_vs_json_only_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B11: Tracker() is ephemeral (no mkdir, no DB, no consist.json, sub-ms); run_dir!=None is JSON-only."""
    work_cwd = tmp_path / "clean_cwd"
    work_cwd.mkdir()
    monkeypatch.chdir(work_cwd)

    ephem = Tracker()
    assert ephem.is_ephemeral is True
    assert ephem.db is None

    big_file = tmp_path / "big.bin"
    big_file.write_bytes(b"x" * (10 * 1024 * 1024))

    def step(inp: Path, out: Path) -> None:
        out.write_text(f"size={inp.stat().st_size}", encoding="utf-8")

    out_target = tmp_path / "ephem_out.txt"
    t0 = time.perf_counter()
    res = ephem.run(
        fn=step,
        inputs={"inp": big_file},
        outputs={"out": out_target},
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    assert out_target.read_text(encoding="utf-8") == f"size={10 * 1024 * 1024}"
    assert Path(res.outputs["out"].path) == out_target
    # Ephemeral mode must not create any directories or consist.json in cwd
    assert list(work_cwd.iterdir()) == []
    # Sanity bound: should not spend seconds SHA-256 hashing 10 MB synchronously
    assert elapsed_ms < 500.0

    # JSON-Only mode (db_path=None, run_dir=<path>) computes full digests & writes consist.json
    json_run_dir = tmp_path / "json_runs"
    json_tracker = Tracker(run_dir=json_run_dir, db_path=None)
    assert json_tracker.is_ephemeral is False
    assert json_tracker.db is None

    json_res = json_tracker.run(
        fn=step,
        inputs={"inp": big_file},
        outputs={"out": "json_out.txt"},
    )
    assert (json_run_dir / "consist.json").exists()
    assert json_res.outputs["out"].digest is not None
    assert json_res.outputs["out"].digest.scheme == "sha256:file"


def test_lazy_coupler_delays_hashing_detects_edits_and_caches(tmp_path: Path) -> None:
    """B10: Coupler.set() is lazy, resolves & verifies on consumption, detects post-set edits, and caches."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    data_file = tmp_path / "coupler_input.csv"
    data_file.write_text("v\n1\n", encoding="utf-8")

    with tracker.scenario("sc_lazy_1") as sc:
        with patch.object(
            tracker.artifacts,
            "create_artifact",
            wraps=tracker.artifacts.create_artifact,
        ) as mock_create:
            sc.coupler.set("tbl", data_file)
            assert mock_create.call_count == 0

        # Mutate the file after set() before consuming
        data_file.write_text("v\n2\n", encoding="utf-8")

        def consume(tbl: pd.DataFrame, out: Path) -> None:
            out.write_text(f"val={int(tbl['v'].iloc[0])}", encoding="utf-8")

        r1 = sc.run(
            fn=consume,
            inputs={"tbl": sc.coupler["tbl"]},
            outputs={"out": "res.txt"},
        )
        assert Path(r1.outputs["out"].path).read_text(encoding="utf-8") == "val=2"
        assert r1.cache_hit is False

    # Second scenario with identical file hits cache
    with tracker.scenario("sc_lazy_2") as sc2:
        sc2.coupler.set("tbl", data_file)

        def consume(tbl: pd.DataFrame, out: Path) -> None:
            out.write_text(f"val={int(tbl['v'].iloc[0])}", encoding="utf-8")

        r2 = sc2.run(
            fn=consume,
            inputs={"tbl": sc2.coupler["tbl"]},
            outputs={"out": "res.txt"},
        )
        assert r2.cache_hit is True
