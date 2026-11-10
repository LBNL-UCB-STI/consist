from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import consist
from consist import CacheOptions, ExecutionOptions, LocalRef, Tracker, observe
from consist.artifact import (
    Artifact as V2Artifact,
    MissingOutputError,
)
from consist.core.coupler import Coupler
from consist.core.noop import NoopCoupler
from consist.core.resolved_binding import (
    ArtifactIdentity,
    ResolvedBindingBuilder,
    step_contract_identity,
)
from consist.digest import Digest, Identity
from consist.integrations.containers.api import run_container
from consist.location import FileStat, Location, Observation
from consist.store.detect import ConsistError


def test_coupler_accepts_v2_and_legacy_artifacts_and_auto_observes_paths(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / "inputs.csv"
    data_file.write_text("a,b\n1,2\n", encoding="utf-8")

    coupler = Coupler()
    observed = coupler.set("raw", data_file)
    assert isinstance(observed, V2Artifact)
    assert observed.key == "raw"
    assert observed.anchor.scheme == "sha256:file"
    assert coupler.require("raw") == observed
    assert coupler.path("raw") == data_file.resolve()

    # Stat cache hit on unchanged file (even under a different role key)
    cached_same = coupler.set("raw", str(data_file))
    assert cached_same is observed
    cached_rekeyed = coupler.set("raw_alias", data_file)
    assert isinstance(cached_rekeyed, V2Artifact)
    assert cached_rekeyed.key == "raw_alias"
    assert cached_rekeyed.anchor == observed.anchor

    # Modify file -> stat cache invalidates and re-hashes
    data_file.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    updated = coupler.set("raw", data_file)
    assert isinstance(updated, V2Artifact)
    assert updated.anchor != observed.anchor

    # Rejects invalid types
    with pytest.raises(TypeError, match="expected an Artifact or path"):
        coupler.set("bad", 12345)  # type: ignore[arg-type]


def test_coupler_history_and_previous_across_views(tmp_path: Path) -> None:
    f1 = tmp_path / "iter1.csv"
    f2 = tmp_path / "iter2.csv"
    f3 = tmp_path / "iter3.csv"
    f1.write_text("x\n1\n", encoding="utf-8")
    f2.write_text("x\n22\n", encoding="utf-8")
    f3.write_text("x\n333\n", encoding="utf-8")

    coupler = Coupler()
    beam = coupler.view("beam")

    assert beam.history("skims") == ()
    assert beam.previous("skims") is None

    a1 = beam.set("skims", f1)
    assert beam.history("skims") == (a1,)
    assert beam.previous("skims") is None
    assert coupler.history("beam/skims") == (a1,)

    a2 = beam.set("skims", f2)
    assert beam.require("skims") == a2
    assert beam.previous("skims") == a1
    assert beam.history("skims") == (a1, a2)

    a3 = beam.set("skims", f3)
    assert beam.require("skims") == a3
    assert beam.previous("skims") == a2
    assert beam.history("skims") == (a1, a2, a3)
    assert coupler.previous("beam/skims") == a2

    beam.clear()
    assert beam.history("skims") == ()
    assert beam.previous("skims") is None


def test_coupler_path_resolves_v2_artifact_with_tracker_mounts(tmp_path: Path) -> None:
    mount_dir = tmp_path / "mounted_workspace"
    mount_dir.mkdir(parents=True)
    target_file = mount_dir / "skims.parquet"
    pd.DataFrame({"orig": [1], "dest": [2]}).to_parquet(target_file)

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "prov.duckdb",
        mounts={"work": str(mount_dir)},
    )
    art = observe(target_file, key="skims", mounts={"work": mount_dir})
    assert any(obs.location.uri == "work://skims.parquet" for obs in art.hints)

    # Construct a v2 artifact with only the mount-backed location hint
    mount_obs = Observation(
        identity=art.identity,
        location=Location(uri="work://skims.parquet"),
        access_driver=art.driver,
        witness=FileStat.from_os_stat(target_file.stat()),
        basis="computed",
    )
    mount_only_art = V2Artifact(
        id=art.id,
        key="skims",
        driver=art.driver,
        identity=art.identity,
        hints=(mount_obs,),
    )

    coupler = Coupler(tracker=tracker)
    coupler.set("skims", mount_only_art)
    assert coupler.path("skims") == target_file.resolve()


def test_local_ref_and_noop_coupler_zero_io_and_ergonomics(tmp_path: Path) -> None:
    nonexistent = tmp_path / "does_not_exist.parquet"
    ref = LocalRef(nonexistent, key="planned")
    assert ref.path == nonexistent
    assert ref.driver == "parquet"
    assert os.fspath(ref) == str(nonexistent)
    assert ref.get_path() == nonexistent
    assert ref.as_path() == nonexistent
    assert ref.locate() == nonexistent

    with_meta = ref.with_meta(iteration=2)
    assert with_meta.meta["iteration"] == 2
    assert "iteration" not in ref.meta

    csv_path = tmp_path / "table.csv"
    csv_path.write_text("col1,col2\n10,20\n", encoding="utf-8")
    csv_ref = LocalRef(csv_path, key="table")
    df = csv_ref.as_df()
    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0]["col1"] == 10

    noop_coupler = NoopCoupler()
    r1 = noop_coupler.set("out", nonexistent)
    r2 = noop_coupler.set("out", csv_path)
    assert isinstance(r1, LocalRef)
    assert isinstance(r2, LocalRef)
    assert noop_coupler.require("out") == r2
    assert noop_coupler.previous("out") == r1
    assert noop_coupler.history("out") == (r1, r2)
    assert noop_coupler.path("out") == csv_path


def test_legacy_artifact_as_df_without_tracker(tmp_path: Path) -> None:
    pq_path = tmp_path / "standalone.parquet"
    expected = pd.DataFrame({"id": [1, 2], "val": [3.5, 4.5]})
    expected.to_parquet(pq_path)

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def make_file(table: Path) -> None:
        expected.to_parquet(table)

    res = tracker.run(
        fn=make_file,
        outputs={"table": pq_path},
        execution_options=ExecutionOptions(inject_context=False),
    )
    art = res.outputs["table"]
    # Detach tracker reference to verify direct local file fallback
    object.__setattr__(art, "_tracker", None)
    loaded = art.as_df()
    pd.testing.assert_frame_equal(loaded, expected)


def test_symmetric_outputs_mapping_binds_target_path_and_validates_existence(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    target_out = tmp_path / "nested" / "dir" / "cleaned.parquet"

    def write_cleaned(cleaned: Path, multiplier: int = 2) -> None:
        # Parent directory should be pre-created by Consist before running the step
        assert cleaned.parent.exists()
        df = pd.DataFrame({"x": [1 * multiplier, 2 * multiplier]})
        df.to_parquet(cleaned)

    run_result = tracker.run(
        fn=write_cleaned,
        config={"multiplier": 3},
        outputs={"cleaned": target_out},
    )
    assert target_out.exists()
    assert "cleaned" in run_result.outputs
    df = run_result.outputs["cleaned"].as_df()
    assert list(df["x"]) == [3, 6]

    # Missing output raises MissingOutputError (which is both ConsistError and FileNotFoundError)
    missing_out = tmp_path / "never_written.parquet"

    def forget_to_write(cleaned: Path) -> None:
        pass

    with pytest.raises(MissingOutputError) as exc_info:
        tracker.run(
            fn=forget_to_write,
            outputs={"cleaned": missing_out},
        )
    assert isinstance(exc_info.value, ConsistError)
    assert isinstance(exc_info.value, FileNotFoundError)
    assert exc_info.value.role == "cleaned"

    # Returning a non-None value when outputs={role: path} is used raises ValueError
    def bad_return(cleaned: Path) -> Path:
        cleaned.write_text("hi", encoding="utf-8")
        return cleaned

    with pytest.raises(ValueError, match="must return None"):
        tracker.run(
            fn=bad_return,
            outputs={"cleaned": tmp_path / "bad_ret.txt"},
        )


def test_symmetric_outputs_directory_declaration(tmp_path: Path) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    bundle_dir = tmp_path / "bundles" / "out_bundle"

    def write_bundle(bundle: Path) -> None:
        assert bundle.is_dir()
        (bundle / "part1.txt").write_text("hello", encoding="utf-8")

    # Declare trailing slash to indicate directory output
    res = tracker.run(
        fn=write_bundle,
        outputs={"bundle": f"{bundle_dir}/"},
    )
    assert "bundle" in res.outputs
    assert (bundle_dir / "part1.txt").exists()


def test_golden_d1_declared_output_target_path_exclusion_from_signature(
    tmp_path: Path,
) -> None:
    """
    Golden Test D1:
    Moving or renaming the target output destination path (with the same role,
    is_dir flag, and lowercase extension) MUST produce the exact same config_hash
    and run signature (resulting in a cache hit + output hydration).
    Changing the role name, extension, or file-vs-directory flag MUST change the
    config_hash and signature.
    """
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    calls = 0

    def produce(cleaned: Path, scale: int = 1) -> None:
        nonlocal calls
        calls += 1
        pd.DataFrame({"v": [10 * scale]}).to_parquet(cleaned)

    out_a = tmp_path / "runs" / "loc_a" / "subdir_1" / "first_name.parquet"
    out_b = tmp_path / "runs" / "loc_b" / "subdir_2" / "completely_renamed.parquet"

    res_a = tracker.run(
        fn=produce,
        config={"scale": 5},
        outputs={"cleaned": out_a},
    )
    assert calls == 1
    assert not res_a.cache_hit
    assert out_a.exists()

    # Run with relocated & renamed target path -> MUST hit cache and hydrate out_b!
    res_b = tracker.run(
        fn=produce,
        config={"scale": 5},
        outputs={"cleaned": out_b},
        cache_options=CacheOptions(cache_hydration="outputs-requested"),
    )
    assert calls == 1
    assert res_b.cache_hit
    assert res_b.run.config_hash == res_a.run.config_hash
    assert res_b.run.signature == res_a.run.signature
    assert out_b.exists()
    assert list(res_b.outputs["cleaned"].as_df()["v"]) == [50]

    # Changing extension (.parquet -> .csv) MUST change config_hash and miss cache
    def produce_any(cleaned: Path, scale: int = 1) -> None:
        nonlocal calls
        calls += 1
        cleaned.write_text(f"v\n{10 * scale}\n", encoding="utf-8")

    res_ext1 = tracker.run(
        fn=produce_any,
        config={"scale": 5},
        outputs={"cleaned": tmp_path / "ext_test.csv"},
    )
    res_ext2 = tracker.run(
        fn=produce_any,
        config={"scale": 5},
        outputs={"cleaned": tmp_path / "ext_test.tsv"},
    )
    assert res_ext1.run.config_hash != res_ext2.run.config_hash
    assert res_ext1.run.signature != res_ext2.run.signature


def test_parameter_path_type_hint_input_binding_inference(tmp_path: Path) -> None:
    """
    Parameters annotated as `: Path` (or `os.PathLike` / `Optional[Path]`) receive
    local `Path` objects while unannotated / DataFrame-annotated parameters receive
    loaded DataFrames when `input_binding` is omitted.
    Explicit `ExecutionOptions(input_binding=...)` overrides inference.
    """
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    raw_path = tmp_path / "raw.csv"
    lookup_path = tmp_path / "lookup.parquet"
    raw_path.write_text("id,val\n1,10\n2,20\n", encoding="utf-8")
    pd.DataFrame({"id": [1, 2], "factor": [2, 3]}).to_parquet(lookup_path)

    seen_types: dict[str, type] = {}

    def mixed_step(raw: Path, lookup: pd.DataFrame, out: Path) -> None:
        seen_types["raw"] = type(raw)
        seen_types["lookup"] = type(lookup)
        raw_df = pd.read_csv(raw)
        merged = raw_df.merge(lookup, on="id")
        merged["result"] = merged["val"] * merged["factor"]
        merged.to_parquet(out)

    out_file = tmp_path / "merged.parquet"
    res = tracker.run(
        fn=mixed_step,
        inputs={"raw": raw_path, "lookup": lookup_path},
        outputs={"out": out_file},
    )
    assert issubclass(seen_types["raw"], Path)
    assert issubclass(seen_types["lookup"], pd.DataFrame)
    assert list(res.outputs["out"].as_df()["result"]) == [20, 60]

    # Explicit input_binding="paths" overrides inference so both arrive as Path
    seen_override: dict[str, type] = {}

    def override_step(raw: Path, lookup: pd.DataFrame, out: Path) -> None:
        seen_override["raw"] = type(raw)
        seen_override["lookup"] = type(lookup)
        pd.DataFrame({"ok": [1]}).to_parquet(out)

    tracker.run(
        fn=override_step,
        inputs={"raw": raw_path, "lookup": lookup_path},
        outputs={"out": tmp_path / "override.parquet"},
        execution_options=ExecutionOptions(input_binding="paths"),
    )
    assert issubclass(seen_override["raw"], Path)
    assert issubclass(seen_override["lookup"], Path)


def test_noop_scenario_supports_symmetric_outputs_and_path_type_hint_binding(
    tmp_path: Path,
) -> None:
    raw_csv = tmp_path / "noop_raw.csv"
    raw_csv.write_text("a\n7\n", encoding="utf-8")
    out_pq = tmp_path / "noop_out" / "result.parquet"

    def step(raw: Path, cleaned: Path) -> None:
        assert isinstance(raw, Path)
        assert isinstance(cleaned, Path)
        df = pd.read_csv(raw)
        df["b"] = df["a"] * 2
        df.to_parquet(cleaned)

    with consist.scenario("noop_pr5", enabled=False) as sc:
        sc.coupler.set("raw", raw_csv)
        res = sc.run(
            fn=step,
            inputs={"raw": sc.coupler.require("raw")},
            outputs={"cleaned": out_pq},
        )
        cleaned_ref = sc.coupler.require("cleaned")
        assert isinstance(cleaned_ref, LocalRef)
        assert cleaned_ref == res.outputs["cleaned"]
        df = cleaned_ref.as_df()
        assert list(df["b"]) == [14]


def test_golden_d2_unified_signature_and_historical_cache_ineligibility(
    tmp_path: Path,
) -> None:
    """
    Golden Test D2:
    1. Tracker.run and Tracker.trace with identical inputs, config, and code identity
       produce identical input_hash and run signature.
    2. Consuming a Kind.HISTORICAL (verifiable=False) V2Artifact records
       mode='historical-v1' in binding identity, marks the run with
       meta['cache_ineligible_reason'] = 'historical_unverified_input', and never
       produces a live cache hit.
    """
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    in_file = tmp_path / "input.csv"
    in_file.write_text("k,v\n1,100\n", encoding="utf-8")
    v2_in = observe(in_file, key="data")

    def compute_fn(data: Path) -> None:
        pass

    run_res = tracker.run(
        fn=compute_fn,
        name="unified_step",
        model="unified_model",
        config={"alpha": 1},
        inputs={"data": v2_in},
        cache_options=CacheOptions(code_identity="repo_git"),
    )

    with tracker.trace(
        name="unified_step",
        model="unified_model",
        config={"alpha": 1},
        inputs={"data": v2_in},
        code_identity="repo_git",
    ) as t:
        pass

    traced_run = t.current_consist.run if t.current_consist else tracker.last_run.run
    assert run_res.run.input_hash == traced_run.input_hash
    assert run_res.run.config_hash == traced_run.config_hash
    assert run_res.run.git_hash == traced_run.git_hash
    assert run_res.run.signature == traced_run.signature

    # Historical artifact (Kind.HISTORICAL, verifiable=False)
    hist_art = V2Artifact(
        id=v2_in.id,
        key="hist_data",
        driver="csv",
        identity=Identity(anchor=Digest("legacy-v1:unverified", v2_in.anchor.hex)),
        hints=v2_in.hints,
    )
    exec_count = 0

    def consume_hist(hist_data: Path, out: Path) -> None:
        nonlocal exec_count
        exec_count += 1
        out.write_text("done", encoding="utf-8")

    hist_res1 = tracker.run(
        fn=consume_hist,
        inputs={"hist_data": hist_art},
        outputs={"out": tmp_path / "hist_out1.txt"},
    )
    assert exec_count == 1
    assert not hist_res1.cache_hit
    assert hist_res1.run.meta.get("cache_ineligible_reason") == (
        "historical_unverified_input"
    )

    # Running a second time with identical inputs MUST NOT hit cache
    hist_res2 = tracker.run(
        fn=consume_hist,
        inputs={"hist_data": hist_art},
        outputs={"out": tmp_path / "hist_out2.txt"},
    )
    assert exec_count == 2
    assert not hist_res2.cache_hit
    assert hist_res2.run.meta.get("cache_ineligible_reason") == (
        "historical_unverified_input"
    )


def test_golden_d5_container_host_mount_virtualization_cache_hit(
    tmp_path: Path,
) -> None:
    """
    Golden Test D5:
    Relocating host mount roots from host_a to host_b while preserving mount names
    on the Tracker produces the exact same ContainerDefinition hashable config and
    container signature, yielding a cache hit.
    """
    db_path = tmp_path / "container_prov.duckdb"
    shared_out = tmp_path / "shared_outputs"
    shared_out.mkdir(parents=True)

    host_a_in = tmp_path / "host_a" / "inputs"
    host_a_in.mkdir(parents=True)

    host_b_in = tmp_path / "host_b" / "inputs"
    host_b_in.mkdir(parents=True)

    (host_a_in / "config.txt").write_text("seed=42\n", encoding="utf-8")
    (host_b_in / "config.txt").write_text("seed=42\n", encoding="utf-8")

    tracker_a = Tracker(
        run_dir=shared_out,
        db_path=db_path,
        mounts={"inputs": str(host_a_in), "outputs": str(shared_out)},
    )

    backend_calls = 0

    def fake_run_a(**kwargs) -> bool:
        nonlocal backend_calls
        backend_calls += 1
        (shared_out / "result.txt").write_text("computed_output\n", encoding="utf-8")
        return True

    with (
        patch(
            "consist.integrations.containers.api.DockerBackend.resolve_image_digest",
            return_value="sha256:deadbeef",
        ),
        patch(
            "consist.integrations.containers.api.DockerBackend.run",
            side_effect=fake_run_a,
        ),
    ):
        res_a = run_container(
            tracker=tracker_a,
            run_id="container_run_a",
            image="alpine:3.19",
            command=["sh", "-c", "echo computed_output > /out/result.txt"],
            volumes={str(host_a_in): "/in", str(shared_out): "/out"},
            volume_modes={str(host_a_in): "ro", str(shared_out): "rw"},
            inputs=[host_a_in / "config.txt"],
            outputs={"result": shared_out / "result.txt"},
        )

    assert backend_calls == 1
    assert not res_a.cache_hit
    assert (shared_out / "result.txt").exists()

    # Now relocate the input mount from host_a_in to host_b_in using a second Tracker
    tracker_b = Tracker(
        run_dir=shared_out,
        db_path=db_path,
        mounts={"inputs": str(host_b_in), "outputs": str(shared_out)},
    )

    with (
        patch(
            "consist.integrations.containers.api.DockerBackend.resolve_image_digest",
            return_value="sha256:deadbeef",
        ),
        patch(
            "consist.integrations.containers.api.DockerBackend.run",
            side_effect=fake_run_a,
        ),
    ):
        res_b = run_container(
            tracker=tracker_b,
            run_id="container_run_b",
            image="alpine:3.19",
            command=["sh", "-c", "echo computed_output > /out/result.txt"],
            volumes={str(host_b_in): "/in", str(shared_out): "/out"},
            volume_modes={str(host_b_in): "ro", str(shared_out): "rw"},
            inputs=[host_b_in / "config.txt"],
            outputs={"result": shared_out / "result.txt"},
        )

    # Backend must NOT be called a second time -> cache hit!
    assert backend_calls == 1
    assert res_b.cache_hit
    assert res_b.cache_source == "container_run_a"


def test_artifact_identity_and_resolved_binding_with_v2_artifacts_and_path_step(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    file_path = tmp_path / "data.csv"
    file_path.write_text("x\n10\n", encoding="utf-8")
    v2_file = observe(file_path, key="raw_file")
    file_identity = ArtifactIdentity.from_artifact(v2_file)
    assert file_identity.kind == "file"
    assert file_identity.digest == v2_file.anchor.hex

    dir_path = tmp_path / "bundle_dir"
    dir_path.mkdir()
    (dir_path / "part.txt").write_text("hello", encoding="utf-8")
    v2_dir = observe(dir_path, key="raw_dir")
    dir_identity = ArtifactIdentity.from_artifact(v2_dir)
    assert dir_identity.kind == "manifest-v1"
    assert dir_identity.digest == v2_dir.anchor.hex

    def check_contract(raw_file: Path, raw_dir: Path) -> None:
        pass

    contract_id = step_contract_identity(check_contract, "check_contract")
    v2_binding = (
        ResolvedBindingBuilder(
            step_name="check_contract",
            step_contract_identity=contract_id,
        )
        .bind_tracked_artifact(parameter="raw_file", artifact=v2_file)
        .bind_tracked_artifact(parameter="raw_dir", artifact=v2_dir)
        .freeze()
    )
    assert v2_binding.artifact_for("raw_file").identity == file_identity
    assert v2_binding.artifact_for("raw_dir").identity == dir_identity

    # Also test ScenarioContext.run with ResolvedBinding and a : Path-annotated step
    # WITHOUT explicit ExecutionOptions(input_binding="paths")
    def produce(raw: Path) -> None:
        raw.write_text("col\n99\n", encoding="utf-8")

    seen_snapshot: list[Path] = []

    def consume(raw: Path, out: Path) -> None:
        seen_snapshot.append(raw)
        df = pd.read_csv(raw)
        df["col2"] = df["col"] + 1
        df.to_parquet(out)

    with tracker.scenario("strict_binding_path_inference") as sc:
        prod_res = sc.run(
            fn=produce,
            outputs={"raw": tmp_path / "produced_raw.csv"},
        )
        tracked_raw = prod_res.outputs["raw"]
        step_id = sc.resolve_step_identity(consume)
        strict_binding = (
            ResolvedBindingBuilder(
                step_name=step_id.name,
                step_contract_identity=step_id.step_contract_identity,
            )
            .bind_tracked_artifact(parameter="raw", artifact=tracked_raw)
            .freeze()
        )
        cons_res = sc.run(
            fn=consume,
            binding=strict_binding,
            step_identity=step_id,
            outputs={"out": tmp_path / "consumed.parquet"},
        )
        assert len(seen_snapshot) == 1
        assert isinstance(seen_snapshot[0], Path)
        assert list(cons_res.outputs["out"].as_df()["col2"]) == [100]


def test_tracker_trace_symmetric_outputs_precreates_dirs_and_does_not_mask_exceptions(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    nested_out = tmp_path / "trace_sub" / "nested" / "out.parquet"
    with tracker.trace("trace_ok", outputs={"out": nested_out}):
        assert nested_out.parent.is_dir()
        pd.DataFrame({"v": [1, 2]}).to_parquet(nested_out)

    assert tracker.last_run is not None
    assert [a.key for a in tracker.last_run.outputs] == ["out"]

    missing_out = tmp_path / "trace_fail" / "never_written.parquet"
    with pytest.raises(RuntimeError, match="step exploded"):
        with tracker.trace("trace_err", outputs={"out": missing_out}):
            assert missing_out.parent.is_dir()
            raise RuntimeError("step exploded")


def test_define_step_inputs_and_path_annotations_in_scenario_run(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    raw_file = tmp_path / "raw_step.csv"
    raw_file.write_text("n\n5\n", encoding="utf-8")
    out_file = tmp_path / "step_out" / "cleaned.parquet"

    @consist.define_step(
        inputs=["raw"],
        outputs={"cleaned": out_file},
    )
    def decorated_step(raw: Path, cleaned: Path) -> None:
        assert isinstance(raw, Path)
        assert isinstance(cleaned, Path)
        df = pd.read_csv(raw)
        df["n2"] = df["n"] * 10
        df.to_parquet(cleaned)

    with tracker.scenario("decorated_scenario") as sc:
        sc.coupler.set("raw", raw_file)
        res = sc.run(fn=decorated_step)
        assert list(res.outputs["cleaned"].as_df()["n2"]) == [50]


def test_decision_d1_directory_and_compound_extensions_change_signature(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")

    def dir_writer(skims: Path) -> None:
        skims.mkdir(parents=True, exist_ok=True)
        (skims / "part.bin").write_bytes(b"123")

    res_zarr = tracker.run(
        fn=dir_writer,
        name="dir_step",
        outputs={"skims": f"{tmp_path / 'out_a.zarr'}/"},
    )
    res_pq_dir = tracker.run(
        fn=dir_writer,
        name="dir_step",
        outputs={"skims": f"{tmp_path / 'out_b.parquet'}/"},
    )
    assert res_zarr.run.config_hash != res_pq_dir.run.config_hash

    def gz_writer(archive: Path) -> None:
        archive.write_bytes(b"compressed")

    res_csv_gz = tracker.run(
        fn=gz_writer,
        name="gz_step",
        outputs={"archive": tmp_path / "data.csv.gz"},
    )
    res_json_gz = tracker.run(
        fn=gz_writer,
        name="gz_step",
        outputs={"archive": tmp_path / "data.json.gz"},
    )
    assert res_csv_gz.run.config_hash != res_json_gz.run.config_hash
