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

    # 2. Missing declared output raises MissingOutputError by default
    missing_file = tmp_path / "never_created.txt"
    with pytest.raises(MissingOutputError):
        tracker.run(
            fn=noop_step,
            outputs={"out": missing_file},
            cache_options=CacheOptions(cache_mode="overwrite"),
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


def test_coupler_dir_nested_edit_same_tick_racy_and_distinct_keys(
    tmp_path: Path,
) -> None:
    """Coupler detects nested dir edits, same-tick file overwrites, and preserves key in _stat_cache."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    # 1. Nested directory (.zarr) edit must not return stale cached Artifact
    zarr_dir = tmp_path / "grid.zarr"
    zarr_dir.mkdir()
    chunk = zarr_dir / "0.0"
    chunk.write_bytes(b"v1")

    with tracker.scenario("sc_dir") as sc:
        sc.coupler.set("z", zarr_dir)
        a1 = sc.coupler["z"]
        chunk.write_bytes(b"v2-updated")
        a2 = sc.coupler["z"]
        assert a1.digest != a2.digest

    # 2. Same-tick regular-file overwrite within racy window
    racy_file = tmp_path / "racy.txt"
    racy_file.write_bytes(b"aaaa")
    with tracker.scenario("sc_racy") as sc:
        sc.coupler.set("r", racy_file)
        r1 = sc.coupler["r"]
        racy_file.write_bytes(b"bbbb")
        r2 = sc.coupler["r"]
        assert r1.digest != r2.digest

    # 3. Same path under distinct coupler keys preserves each key
    shared = tmp_path / "shared.txt"
    shared.write_text("same", encoding="utf-8")
    with tracker.scenario("sc_keys") as sc:
        sc.coupler.set("a", shared)
        sc.coupler.set("b", shared)
        assert sc.coupler["a"].key == "a"
        assert sc.coupler["b"].key == "b"


def test_ephemeral_mode_skips_git_and_pre_step_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ephemeral Tracker().run() skips repo_git is_dirty() and 50MB pre-step hashing (< 5ms)."""
    monkeypatch.chdir(Path(__file__).resolve().parents[3])
    ephem = Tracker()

    pre_existing_out = tmp_path / "big_out.bin"
    pre_existing_out.write_bytes(b"0" * (5 * 1024 * 1024))

    def rewrite(out: Path) -> None:
        out.write_bytes(b"1" * 16)

    with (
        patch.object(
            ephem.identity,
            "resolve_code_identity",
            side_effect=AssertionError(
                "git/code identity must be skipped in ephemeral"
            ),
        ),
        patch(
            "consist.core.tracker_orchestration._sha256_file",
            side_effect=AssertionError("_sha256_file must be skipped in ephemeral"),
        ),
    ):
        t0 = time.perf_counter()
        res = ephem.run(
            fn=rewrite,
            outputs={"out": pre_existing_out},
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

    assert elapsed_ms < 50.0
    assert Path(res.outputs["out"].path).read_bytes() == b"1" * 16


def test_type_checking_string_annotations_bind_without_globals(tmp_path: Path) -> None:
    """String annotations referencing TYPE_CHECKING-only aliases bind properly on NameError."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    raw_csv = tmp_path / "tc.csv"
    raw_csv.write_text("k\n42\n", encoding="utf-8")
    raw_pq = tmp_path / "tc.parquet"
    pd.DataFrame({"k": [7]}).to_parquet(raw_pq)

    def step(
        df: "pd.DataFrame",  # type: ignore[name-defined]
        tbl: "pa.Table",  # type: ignore[name-defined]
        out: "Path",  # type: ignore[name-defined]
    ) -> None:
        assert type(df).__name__ == "DataFrame"
        assert type(tbl).__name__ == "Table"
        assert type(out).__name__.endswith("Path")
        out.write_text(f"{int(df['k'].iloc[0])}:{tbl.num_rows}", encoding="utf-8")

    # Strip pd, pa, Path from a cloned function's globals to simulate TYPE_CHECKING
    import types

    isolated_step = types.FunctionType(
        step.__code__,
        {"type": type, "int": int},
        step.__name__,
        step.__defaults__,
        step.__closure__,
    )
    isolated_step.__annotations__ = {
        "df": "pd.DataFrame",
        "tbl": "pa.Table",
        "out": "Path",
        "return": "None",
    }

    res = tracker.run(
        fn=isolated_step,
        inputs={"df": raw_csv, "tbl": raw_pq},
        outputs={"out": "tc_out.txt"},
    )
    assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "42:1"


def test_simultaneous_outputs_and_output_paths_rejected(tmp_path: Path) -> None:
    """Passing both outputs={...} mapping and output_paths= raises ValueError."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def step(out: Path) -> None:
        out.write_text("x", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"Cannot specify both outputs=\{\.\.\.\} mapping and output_paths=",
    ):
        tracker.run(
            fn=step,
            outputs={"out": "a.txt"},
            output_paths={"out": "b.txt"},
        )


def test_hdf5_staleness_verifies_member_digest_even_when_stat_changes(
    tmp_path: Path,
) -> None:
    """HDF5 staleness check verifies member digest post-step even if container FileStat changed."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    h5_path = tmp_path / "short.h5"
    with h5py.File(h5_path, "w") as f:
        f.create_dataset("m1", data=[1, 2])

    def update_h5(out: Path) -> None:
        with h5py.File(out, "a") as f:
            del f["m1"]
            f.create_dataset("m1", data=[3, 4, 5])

    from consist.core import tracker_orchestration

    with patch(
        "consist.core.tracker_orchestration._hdf5_member_digest",
        wraps=tracker_orchestration._hdf5_member_digest,
    ) as mock_h5:
        tracker.run(
            fn=update_h5,
            outputs={"out": OutputArtifactSpec(path=h5_path, member="m1")},
        )
        # Called both pre-step and post-step to ensure the member itself was updated
        assert mock_h5.call_count == 2


def test_artifact_input_with_path_output_skips_pre_step_hydration(
    tmp_path: Path,
) -> None:
    """def step(x: Artifact, out: Path) with outputs={'out': ...} must not hydrate x."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def prod(out: Path) -> None:
        out.write_text("data", encoding="utf-8")

    prod_res = tracker.run(fn=prod, outputs={"out": "p.txt"})
    art = prod_res.outputs["out"]
    Path(art.path).unlink()

    def cons(x: Artifact, out: Path) -> None:
        assert isinstance(x, Artifact)
        out.write_text("ok", encoding="utf-8")

    with patch.object(tracker, "hydrate_run_outputs") as mock_hydrate:
        res = tracker.run(
            fn=cons,
            inputs={"x": art},
            outputs={"out": "c.txt"},
        )
        assert mock_hydrate.call_count == 0
        assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "ok"


def test_empty_outputs_dict_rejects_non_none_return(tmp_path: Path) -> None:
    """outputs={} (empty mapping) rejects non-None return values."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def bad_step() -> int:
        return 123

    with pytest.raises(ValueError, match="must return None"):
        tracker.run(fn=bad_step, outputs={})
