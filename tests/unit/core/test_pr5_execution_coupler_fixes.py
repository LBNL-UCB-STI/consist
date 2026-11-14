"""Targeted unit tests for execution, step binding, coupler, ephemeral mode, and v0.4 migration errors."""

from __future__ import annotations

from pathlib import Path
import types
from typing import Any
from unittest.mock import patch
import warnings

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import consist
from consist import (
    Artifact,
    CacheOptions,
    Coupler,
    DeprecatedViewsError,
    OutputArtifactSpec,
    StaleOutputError,
    Tracker,
)
from consist.core.tracker_orchestration import _hdf5_member_digest


def test_ephemeral_second_run_executes_without_cache_crash(tmp_path: Path) -> None:
    """Second identical run on Ephemeral Tracker() executes cleanly and returns outputs."""
    tracker = Tracker()
    calls = 0

    def step(out: Path) -> None:
        nonlocal calls
        calls += 1
        out.write_text(f"call-{calls}", encoding="utf-8")

    out_path = tmp_path / "ephem.txt"
    res1 = tracker.run(fn=step, name="ephem_step", outputs={"out": out_path})
    res2 = tracker.run(fn=step, name="ephem_step", outputs={"out": out_path})

    assert calls == 2
    assert "out" in res1.outputs
    assert "out" in res2.outputs
    assert Path(res2.outputs["out"].path).read_text(encoding="utf-8") == "call-2"


def test_hdf5_member_staleness_catches_other_member_update(tmp_path: Path) -> None:
    """Updating a sibling HDF5 dataset does not bypass StaleOutputError on untouched member."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    h5_path = tmp_path / "data.h5"

    with h5py.File(h5_path, "w") as handle:
        handle.create_dataset("target", data=np.array([1, 2, 3], dtype=np.int64))
        handle.create_dataset("other", data=np.array([10, 20], dtype=np.int64))

    def touch_only_other(out: Path) -> None:
        with h5py.File(out, "a") as handle:
            del handle["other"]
            handle.create_dataset(
                "other", data=np.array([99, 100, 101], dtype=np.int64)
            )

    with pytest.raises(StaleOutputError):
        tracker.run(
            fn=touch_only_other,
            outputs={"out": OutputArtifactSpec(path=h5_path, member="/target")},
        )


def test_hdf5_member_digest_uses_slab_streaming_hasher(tmp_path: Path) -> None:
    """_hdf5_member_digest delegates to consist.digest.compute_h5_fixed_digest."""
    h5_path = tmp_path / "slab.h5"
    with h5py.File(h5_path, "w") as handle:
        handle.create_dataset("arr", data=np.arange(10, dtype=np.int64))

    with patch(
        "consist.digest.compute_h5_fixed_digest", return_value="h5_fixed_v1:sha256:abc"
    ) as mock_hash:
        digest = _hdf5_member_digest(h5_path, "/arr")

    assert digest == "h5_fixed_v1:sha256:abc"
    mock_hash.assert_called_once_with(h5_path, member="/arr")


def test_ephemeral_racy_rewrite_does_not_raise_false_stale_error(
    tmp_path: Path,
) -> None:
    """Ephemeral mode skips pre-hash and does not falsely raise StaleOutputError on racy rewrite."""
    tracker = Tracker()
    out_path = tmp_path / "racy.txt"
    out_path.write_bytes(b"aaaa")

    def rewrite_same_size(out: Path) -> None:
        out.write_bytes(b"bbbb")

    res = tracker.run(fn=rewrite_same_size, outputs={"out": out_path})
    assert Path(res.outputs["out"].path).read_bytes() == b"bbbb"


def test_scenario_run_forwards_source_root_and_promotes_artifact_list_inputs(
    tmp_path: Path,
) -> None:
    """ScenarioContext.run preserves materialize_cached_outputs_source_root and promotes Artifact list inputs."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    src_root = tmp_path / "alt_root"
    src_root.mkdir()

    def producer(data: Path) -> None:
        data.write_text("hello", encoding="utf-8")

    def consumer(data: Artifact, final: Path) -> None:
        assert isinstance(data, Artifact)
        assert data.key == "data"
        final.write_text("ok", encoding="utf-8")

    with tracker.scenario("sc") as sc:
        prod = sc.run(fn=producer, outputs={"data": "data.txt"})
        sc.coupler.set("data", prod.outputs["data"])
        cons = sc.run(
            fn=consumer,
            inputs=["data"],
            outputs={"final": "final.txt"},
            cache_options=CacheOptions(
                cache_hydration="outputs-requested",
                validate_cached_outputs="eager",
                materialize_cached_outputs_source_root=src_root,
            ),
        )
        assert Path(cons.outputs["final"].path).read_text(encoding="utf-8") == "ok"

    # Verify validate_materialized_inputs is accepted on ScenarioContext.trace
    with tracker.scenario("sc_trace") as sc:
        with sc.trace(
            "manual_step",
            cache_hydration="inputs-missing",
            validate_materialized_inputs=True,
        ) as t:
            assert t.current_consist is not None


def test_scenario_run_cache_options_passes_materialize_source_root(
    tmp_path: Path,
) -> None:
    """ScenarioContext.run passes materialize_cached_outputs_source_root to Tracker.run."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    src_root = tmp_path / "archive_root"
    src_root.mkdir()

    def step(out: Path) -> None:
        out.write_text("x", encoding="utf-8")

    captured_options: list[CacheOptions] = []
    orig_run = tracker.run

    def spy_run(*args: Any, **kwargs: Any) -> Any:
        captured_options.append(kwargs["cache_options"])
        return orig_run(*args, **kwargs)

    with patch.object(tracker, "run", side_effect=spy_run):
        with tracker.scenario("sc_spy") as sc:
            sc.run(
                fn=step,
                outputs={"out": "out.txt"},
                cache_options=CacheOptions(
                    cache_hydration="outputs-requested",
                    validate_cached_outputs="eager",
                    materialize_cached_outputs_source_root=src_root,
                ),
            )

    assert len(captured_options) == 1
    assert captured_options[0].materialize_cached_outputs_source_root == src_root


def test_pyarrow_table_return_value_supported(tmp_path: Path) -> None:
    """Steps returning pyarrow.Table write Parquet and log a parquet Artifact."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def build_table() -> pa.Table:
        return pa.table({"a": [1, 2], "b": ["x", "y"]})

    res1 = tracker.run(fn=build_table, name="pa_default", outputs=["tbl"])
    art1 = res1.outputs["tbl"]
    assert art1.driver == "parquet"
    assert pq.read_table(art1.path).to_pydict() == {"a": [1, 2], "b": ["x", "y"]}

    def build_table2() -> pa.Table:
        return pa.table({"a": [3, 4], "b": ["z", "w"]})

    custom_path = tmp_path / "custom.parquet"
    res2 = tracker.run(
        fn=build_table2,
        name="pa_custom",
        config={"variant": "custom"},
        outputs=["tbl"],
        output_paths={"tbl": custom_path},
    )
    art2 = res2.outputs["tbl"]
    assert Path(art2.path) == custom_path
    assert pq.read_table(custom_path).num_rows == 2


def test_union_string_annotations_bind_without_globals(tmp_path: Path) -> None:
    """_unwrap_string_annotation unwraps Union[T, None] and typing.Union[None, T]."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("v\n5\n", encoding="utf-8")

    def step(
        df: "Union[pd.DataFrame, None]",  # type: ignore[name-defined]  # ty: ignore[unresolved-reference]  # noqa: F821
        out: "typing.Union[None, Path]",  # type: ignore[name-defined]  # ty: ignore[unresolved-reference]  # noqa: F821
    ) -> None:
        if not isinstance(df, pd.DataFrame):
            raise AssertionError(f"Expected DataFrame, got {type(df)}")
        if not isinstance(out, Path):
            raise AssertionError(f"Expected Path, got {type(out)}")
        out.write_text(str(int(df["v"].iloc[0])), encoding="utf-8")

    isolated = types.FunctionType(
        step.__code__,
        {
            "isinstance": isinstance,
            "AssertionError": AssertionError,
            "int": int,
            "str": str,
            "Path": Path,
            "pd": pd,
            "type": type,
        },
        step.__name__,
        step.__defaults__,
        step.__closure__,
    )
    isolated.__annotations__ = dict(step.__annotations__)
    res = tracker.run(
        fn=isolated,
        inputs={"df": csv_path},
        outputs={"out": "out.txt"},
    )
    assert Path(res.outputs["out"].path).read_text(encoding="utf-8") == "5"


def test_empty_outputs_consistent_on_cache_miss_and_hit(tmp_path: Path) -> None:
    """outputs={} returns {} on both cache miss and cache hit even if log_artifact is called."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    side_file = tmp_path / "side.txt"
    side_file.write_text("side", encoding="utf-8")

    def step() -> None:
        tracker.log_artifact(side_file, key="side", direction="output")

    miss_res = tracker.run(fn=step, name="empty_out_step", outputs={})
    hit_res = tracker.run(fn=step, name="empty_out_step", outputs={})

    assert miss_res.cache_hit is False
    assert hit_res.cache_hit is True
    assert miss_res.outputs == {}
    assert hit_res.outputs == {}


def test_infer_artifact_outputs_validates_all_entries_before_logging(
    tmp_path: Path,
) -> None:
    """Invalid dict entries in return value prevent partial artifact logging."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    valid_path = tmp_path / "valid.txt"
    valid_path.write_text("ok", encoding="utf-8")

    def partial_bad_return() -> dict[Any, Any]:
        return {"good": valid_path, 123: "not_an_artifact"}

    res = tracker.run(fn=partial_bad_return, name="partial_bad")
    assert res.outputs == {}
    assert tracker.last_run is not None
    assert tracker.last_run.outputs == []


def test_coupler_path_relative_resolution_and_declare_outputs_warn(
    tmp_path: Path,
) -> None:
    """Coupler.path resolves relative paths against tracker.run_dir and declare_outputs(warn_undefined=True) works without names."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    coupler = Coupler(tracker=tracker)

    coupler.set("rel_str", "subdir/out.csv")
    coupler.set("rel_path", Path("subdir/out2.csv"))
    assert coupler.path("rel_str") == (tracker.run_dir / "subdir/out.csv").resolve()
    assert coupler.path("rel_path") == (tracker.run_dir / "subdir/out2.csv").resolve()
    assert coupler.path("missing", required=False) is None

    coupler.declare_outputs(warn_undefined=True)
    with pytest.warns(UserWarning, match="Setting undefined coupler key"):
        coupler.set("undeclared_key", "file.txt")


def test_removed_v04_exports_raise_migration_errors(tmp_path: Path) -> None:
    """Removed v0.4 APIs raise DeprecatedViewsError, NotImplementedError, or ImportError with migration guidance."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(DeprecatedViewsError):
            consist.view(object)
        with pytest.raises(DeprecatedViewsError):
            consist.register_views(object)
        with pytest.raises(DeprecatedViewsError):
            tracker.view(object)
        with pytest.raises(DeprecatedViewsError):
            tracker.register_views(object)
        with pytest.raises(DeprecatedViewsError):
            _ = tracker.views

    with pytest.raises(NotImplementedError, match="db_session"):
        tracker.db_session()
    with pytest.raises(NotImplementedError, match="db_session"):
        consist.api.db_session()
    with pytest.raises(NotImplementedError, match="noop_scenario"):
        consist.api.noop_scenario()

    with pytest.raises(ImportError, match="NoopTracker"):
        from consist import NoopTracker  # noqa: F401
    with pytest.raises(ImportError, match="noop_scenario"):
        from consist import noop_scenario  # noqa: F401
    with pytest.raises(ImportError, match="db_session"):
        from consist import db_session  # noqa: F401


def test_legacy_output_paths_does_not_inject_and_optional_input_not_overwritten_by_output(
    tmp_path: Path,
) -> None:
    """Legacy output_paths= does not inject parameters, and omitted optional inputs are not overwritten by outputs."""
    from typing import Optional

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    seen_legacy: list[Any] = []

    def legacy_step(out: Optional[Path] = None) -> None:
        seen_legacy.append(out)
        target = tracker.run_artifact_dir() / "legacy.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("ok", encoding="utf-8")

    tracker.run(fn=legacy_step, name="legacy", output_paths={"out": "legacy.txt"})
    assert seen_legacy == [None]

    seen_optional: list[Any] = []

    @consist.define_step(
        optional_input_keys=["data"],
        outputs={"data": "collision.txt"},
    )
    def optional_collision_step(data: Optional[Path] = None) -> None:
        seen_optional.append(data)
        target = tracker.run_artifact_dir() / "collision.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("written", encoding="utf-8")

    with tracker.scenario("sc_collision") as sc:
        sc.run(fn=optional_collision_step)
    assert seen_optional == [None]


def test_scenario_run_binding_result_authoritative_inputs(tmp_path: Path) -> None:
    """ScenarioContext.run(binding=BindingResult(...)) does not backfill @define_step keys from coupler."""
    from typing import Optional
    from consist.core.workflow import BindingResult

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    a_file = tmp_path / "a.txt"
    b_file = tmp_path / "b.txt"
    a_file.write_text("a", encoding="utf-8")
    b_file.write_text("b", encoding="utf-8")

    seen: dict[str, Any] = {}

    @consist.define_step(
        input_keys=["a"],
        optional_input_keys=["b"],
    )
    def step(a: Path, b: Optional[Path] = None) -> None:
        seen["a"] = a
        seen["b"] = b

    with tracker.scenario("sc_binding") as sc:
        with tracker.start_run("seed", model="seed"):
            art_a = tracker.log_input(a_file, key="a")
            art_b = tracker.log_input(b_file, key="b")
        sc.coupler.set("b", art_b)

        sc.run(fn=step, binding=BindingResult(inputs={"a": art_a}))

    assert seen["a"] == a_file
    assert seen["b"] is None
