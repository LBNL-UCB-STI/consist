from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import consist
from consist import CacheOptions, ExecutionOptions, Tracker, observe
from consist.artifact import (
    Artifact as V2Artifact,
    MissingOutputError,
)
from consist.core.coupler import Coupler
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


def test_standalone_coupler_supports_unmaterialized_paths_and_existing_files(
    tmp_path: Path,
) -> None:
    from consist.digest import Kind

    nonexistent = tmp_path / "does_not_exist.parquet"
    csv_path = tmp_path / "table.csv"
    csv_path.write_text("col1,col2\n10,20\n", encoding="utf-8")

    coupler = Coupler()
    r1 = coupler.set_from_artifact("out", nonexistent)
    assert isinstance(r1, V2Artifact)
    assert r1.identity.anchor.kind is Kind.HISTORICAL
    assert r1.hints[0].basis == "declared"
    assert not nonexistent.exists()
    assert coupler.require("out") == r1

    r2 = coupler.set("out", csv_path)
    assert isinstance(r2, V2Artifact)
    assert coupler.require("out") == r2
    assert coupler.previous("out") == r1
    assert coupler.history("out") == (r1, r2)
    assert coupler.path("out") == csv_path.resolve()
    df = r2.as_df()
    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0]["col1"] == 10


def test_legacy_artifact_as_df_without_tracker(tmp_path: Path) -> None:
    from dataclasses import replace

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
    assert isinstance(art, V2Artifact)
    detached = replace(art, origin=None)
    loaded = detached.as_df()
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


def test_dbless_tracker_scenario_supports_symmetric_outputs_and_path_type_hint_binding(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)
    raw_csv = tmp_path / "dbless_raw.csv"
    raw_csv.write_text("a\n7\n", encoding="utf-8")
    out_pq = tmp_path / "dbless_out" / "result.parquet"

    def step(raw: Path, cleaned: Path) -> None:
        assert isinstance(raw, Path)
        assert isinstance(cleaned, Path)
        df = pd.read_csv(raw)
        df["b"] = df["a"] * 2
        df.to_parquet(cleaned)

    with tracker.scenario("dbless_pr5") as sc:
        sc.coupler.set("raw", raw_csv)
        res = sc.run(
            fn=step,
            inputs={"raw": sc.coupler.require("raw")},
            outputs={"cleaned": out_pq},
        )
        cleaned_art = sc.coupler.require("cleaned")
        assert cleaned_art == res.outputs["cleaned"]
        df = cleaned_art.as_df()
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


def test_phase3_coupler_set_persists_to_store_ledger_and_snapshots(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "coupler_ledger.duckdb"
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)
    seed_csv = tmp_path / "seed_pop.csv"
    seed_csv.write_text("hh_id,persons\n1,3\n2,4\n", encoding="utf-8")

    init_coupler = Coupler(tracker=tracker)
    seeded_init = init_coupler.set("seed_table", seed_csv)
    assert isinstance(seeded_init, V2Artifact)

    with tracker.scenario("sc_coupler_persist", coupler=init_coupler) as sc:
        sc_run_id = sc.run_id
        seeded = sc.coupler.set("households", seed_csv)
        assert isinstance(seeded, V2Artifact)
        assert tracker.ledger is not None
        loaded = tracker.ledger.load_artifact(seeded.id)
        assert loaded is not None
        assert loaded.identity.anchor == seeded.identity.anchor
        locs = tracker.ledger.locate_current([seeded.identity.anchor])
        assert len(locs) >= 1

        synthetic_path = tmp_path / "synthetic_future.parquet"
        hist_art = sc.coupler.set_from_artifact("future_skims", synthetic_path)
        assert isinstance(hist_art, V2Artifact)
        assert hist_art.identity.verifiable is False
        assert not synthetic_path.exists()

        with sc.trace("consume_coupler_step"):
            pass

    assert tracker.db is not None
    snapshots = tracker.db.get_coupler_snapshots(scenario_run_id=sc_run_id, phase=None)
    assert any(s.phase == "init" and s.role_key == "seed_table" for s in snapshots)
    assert any(s.phase == "post" and s.role_key == "households" for s in snapshots)
    tracker.close()


def test_phase3_output_artifact_spec_and_artifact_vs_dataframe_injection(
    tmp_path: Path,
) -> None:
    from consist.core.run_invocation import OutputArtifactSpec

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "spec.duckdb")
    out_pq = tmp_path / "out_spec.parquet"

    def produce(cleaned: Path) -> None:
        pd.DataFrame({"x": [10, 20], "y": [1.5, 2.5]}).to_parquet(cleaned, index=False)

    prod_res = tracker.run(
        fn=produce,
        outputs={
            "cleaned": OutputArtifactSpec(
                path=out_pq,
                facet={"region": "bay_area", "year": 2026},
                facet_index=True,
                meta={"custom_tag": "v1"},
            )
        },
    )
    art = prod_res.outputs["cleaned"]
    assert isinstance(art, V2Artifact)
    assert art.meta["custom_tag"] == "v1"
    assert art.meta["facet"]["region"] == "bay_area"

    seen: dict[str, object] = {}

    def consume(
        as_art: V2Artifact,
        as_df: pd.DataFrame,
        as_path: Path,
        untyped,
    ) -> None:
        seen["as_art"] = as_art
        seen["as_df"] = as_df
        seen["as_path"] = as_path
        seen["untyped"] = untyped

    tracker.run(
        fn=consume,
        inputs={
            "as_art": art,
            "as_df": art,
            "as_path": art,
            "untyped": art,
        },
    )
    assert isinstance(seen["as_art"], V2Artifact)
    assert isinstance(seen["as_df"], pd.DataFrame)
    assert list(seen["as_df"]["x"]) == [10, 20]
    assert isinstance(seen["as_path"], Path)
    assert seen["as_path"] == out_pq.resolve()
    assert isinstance(seen["untyped"], V2Artifact)
    tracker.close()


def test_phase3_trace_cache_hit_preserves_outputs_and_validates_policy(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "trace_hit.duckdb")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("v\n1\n2\n", encoding="utf-8")
    out_pq = tmp_path / "trace_out.parquet"
    exec_count = 0

    with tracker.trace(
        "trace_step",
        inputs={"raw": in_csv},
        outputs={"out": out_pq},
        config={"factor": 3},
    ) as t1:
        assert not t1.is_cached
        exec_count += 1
        pd.DataFrame({"v": [3, 6]}).to_parquet(out_pq, index=False)

    assert exec_count == 1
    assert tracker.last_run is not None
    assert tracker.last_run.run.status == "completed"
    first_out_id = tracker.last_run.outputs[0].id

    # Second identical trace hits cache cleanly and preserves outputs
    with tracker.trace(
        "trace_step",
        inputs={"raw": in_csv},
        outputs={"out": out_pq},
        config={"factor": 3},
    ) as t2:
        assert t2.is_cached
        assert t2.current_consist is not None
        assert [a.id for a in t2.current_consist.outputs] == [first_out_id]

    # Delete cached output on disk: lazy hits cache, eager misses and re-executes
    out_pq.unlink()
    with tracker.trace(
        "trace_step",
        inputs={"raw": in_csv},
        outputs={"out": out_pq},
        config={"factor": 3},
        validate_cached_outputs="lazy",
    ) as t3:
        assert t3.is_cached

    with tracker.trace(
        "trace_step",
        inputs={"raw": in_csv},
        outputs={"out": out_pq},
        config={"factor": 3},
        validate_cached_outputs="eager",
    ) as t4:
        assert not t4.is_cached
        exec_count += 1
        pd.DataFrame({"v": [3, 6]}).to_parquet(out_pq, index=False)

    assert exec_count == 2
    tracker.close()


def test_phase3_query_df_query_rows_deprecated_views_and_dbless_log_dataframe(
    tmp_path: Path,
) -> None:
    from dataclasses import dataclass
    from pydantic import BaseModel
    from sqlmodel import Field, SQLModel

    class TripSchema(SQLModel, table=True):
        __tablename__ = "phase3_trip_schema"
        trip_id: int = Field(primary_key=True)
        miles: float

    # 1. DB-less log_dataframe(..., schema=...) attaches schema metadata in memory without raising
    dbless_tracker = Tracker(run_dir=tmp_path / "dbless_runs")
    with dbless_tracker.start_run("dbless_df_run", model="demo"):
        df_art = dbless_tracker.log_dataframe(
            pd.DataFrame({"trip_id": [1, 2], "miles": [4.5, 9.0]}),
            key="trips",
            schema=TripSchema,
        )
        assert isinstance(df_art, V2Artifact)
        assert df_art.meta["schema_name"] == "TripSchema"
        loaded_df = df_art.as_df()
        assert list(loaded_df["trip_id"]) == [1, 2]

    # 2. StoreLedger query_df / query_rows + deprecation stubs
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "queries.duckdb")
    with tracker.start_run(
        "q_run_1",
        model="sim",
        config={"alpha": 7},
        facet={"alpha": 7},
        facet_index=True,
    ):
        pass

    @dataclass(frozen=True)
    class _ParamSQL:
        sql: str
        params: tuple[object, ...]

    class _RunProjection(BaseModel):
        id: str
        model_name: str

    df_res = tracker.query_df(
        "SELECT id, model_name FROM main.run WHERE id = ?",
        ["q_run_1"],
    )
    assert list(df_res["id"]) == ["q_run_1"]

    rows_res = tracker.query_rows(
        _ParamSQL(
            sql="SELECT id, model_name FROM main.run WHERE model_name = ?",
            params=("sim",),
        ),
        model=_RunProjection,
    )
    assert len(rows_res) == 1
    assert rows_res[0].id == "q_run_1"

    with pytest.raises(ValueError, match="SELECT"):
        tracker.query_df("DELETE FROM main.run")

    with pytest.raises(ConsistError, match="create_view"):
        tracker.register_views()
    with pytest.raises(ConsistError, match="create_view"):
        tracker.view("trips")

    tracker.close()


def test_phase3_dlt_ingest_records_ctd1_and_db_location_without_meta_pollution(
    tmp_path: Path,
) -> None:
    from sqlmodel import Field, SQLModel

    class HouseholdRow(SQLModel, table=True):
        __tablename__ = "phase3_households"
        hh_id: int = Field(primary_key=True)
        income: int

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "ingest.duckdb")
    pq_file = tmp_path / "households.parquet"
    pd.DataFrame({"hh_id": [1, 2], "income": [50000, 80000]}).to_parquet(
        pq_file, index=False
    )

    with tracker.start_run("ingest_run", model="ingest_demo"):
        art = tracker.log_artifact(pq_file, key="phase3_households", direction="output")
        ingested = tracker.ingest(art, schema=HouseholdRow)
        assert isinstance(ingested, V2Artifact)
        assert "_dlt_table_name" not in ingested.meta
        assert "dlt_table_name" not in ingested.meta
        assert "is_ingested" not in ingested.meta
        assert ingested.is_ingested is True
        assert any(
            d.scheme == "ctd1:sha256"
            for d in (ingested.identity.anchor, *ingested.identity.claims)
        )
        assert any(
            h.location.uri == "db://global_tables/phase3_households"
            for h in ingested.hints
        )

    assert tracker.ledger is not None
    db_locs = tracker.ledger.locate_current([ingested.identity.anchor])
    assert any(r["uri"] == "db://global_tables/phase3_households" for r in db_locs)
    df_hot = tracker.query_df(
        "SELECT hh_id, income FROM global_tables.phase3_households ORDER BY hh_id"
    )
    assert list(df_hot["hh_id"]) == [1, 2]
    tracker.close()


def test_phase3_auto_migrate_and_ibis_cursor_isolation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import duckdb
    from consist.integrations.ibis import ibis_connection
    from consist.store.detect import DatabaseMigrationRequired
    from tests.helpers.v1_db_builder import create_v1_schema

    v1_db = tmp_path / "legacy_v1.duckdb"
    with duckdb.connect(str(v1_db)) as con:
        create_v1_schema(con)

    # Default opens raise DatabaseMigrationRequired
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(run_dir=tmp_path / "runs1", db_path=v1_db)

    # read_only=True always raises DatabaseMigrationRequired even if auto_migrate=True
    with pytest.raises(DatabaseMigrationRequired):
        Tracker(
            run_dir=tmp_path / "runs2",
            db_path=v1_db,
            read_only=True,
            auto_migrate=True,
        )

    # auto_migrate="quick" via CONSIST_AUTO_MIGRATE env var upgrades cleanly to Schema v2
    monkeypatch.setenv("CONSIST_AUTO_MIGRATE", "quick")
    tracker = Tracker(run_dir=tmp_path / "runs3", db_path=v1_db)
    assert tracker.ledger is not None

    try:
        import ibis as _ibis  # noqa: F401
    except ImportError:
        tracker.close()
        return

    backend = ibis_connection(tracker)
    backend.disconnect()
    # Disconnecting the Ibis backend closes only the cloned cursor, not StoreLedger's connection!
    check_df = tracker.query_df("SELECT 1 AS ok")
    assert int(check_df.iloc[0]["ok"]) == 1
    tracker.close()


def test_phase3_runset_snapshot_output_sets_and_command_exec(
    tmp_path: Path,
) -> None:
    import sys
    from consist.core.output_sets import OutputSet
    from consist.core.run_options import ExecutionOptions
    from consist.runset import RunSet

    tracker = Tracker(
        run_dir=tmp_path / "runs", db_path=tmp_path / "phase3_full.duckdb"
    )

    for yr, seed in ((2025, 1), (2030, 2)):
        tracker.run(
            fn=lambda: None,
            name=f"sim_{yr}",
            model="sim_model",
            year=yr,
            config={"seed": seed, "scenario_tag": "baseline"},
            facet={"seed": seed, "scenario_tag": "baseline"},
            facet_index=True,
        )

    rs = RunSet.from_query(tracker, label="base", model="sim_model")
    assert len(rs) == 2
    assert list(rs.filter(seed=2).runs[0].id for _ in [0]) == [
        rs.filter(year=2030)[0].id
    ]
    frame = rs.to_frame()
    assert set(frame["seed"].tolist()) == {1, 2}

    def emit_set(ctx) -> None:
        out_dir = ctx.run_dir / "annual"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "annual_2025.csv").write_text("y,v\n2025,10\n", encoding="utf-8")
        (out_dir / "annual_2030.csv").write_text("y,v\n2030,20\n", encoding="utf-8")

    set_res = tracker.run(
        fn=emit_set,
        output_sets={"annual": OutputSet(root="annual", include="annual_*.csv")},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    parent = set_res.outputs["annual"]
    assert parent.path.is_dir()
    children = tracker.get_child_artifacts(parent)
    assert len(children) == 2

    cmd_out = tmp_path / "cmd_out.txt"
    exec_res = tracker.exec(
        name="cmd_step",
        argv=[
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(cmd_out)!r}).write_text('done\\n')",
        ],
        outputs={"out": cmd_out},
        stream_output=False,
    )
    assert exec_res.run is not None
    assert exec_res.run.status == "completed"
    assert exec_res.outputs["out"].path.read_text(encoding="utf-8") == "done\n"

    snap_dest = tmp_path / "snapshots" / "backup.duckdb"
    snapped = tracker.snapshot_db(snap_dest)
    assert snapped.exists()
    ro_tracker = Tracker(run_dir=tmp_path / "runs_ro", db_path=snapped, read_only=True)
    assert len(ro_tracker.find_runs(model="sim_model")) == 2
    ro_tracker.close()
    tracker.close()


def test_phase3_audit_artifact_kv_and_dbless_facet_runset_types(
    tmp_path: Path,
) -> None:
    """Audit A.1 & A.8: artifact_kv queries work without value_json and DB-less RunSet keeps int vs float."""
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "ledger.duckdb")
    csv_path = tmp_path / "runs" / "metrics.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

    with tracker.start_run("kv_run", model="kv_model"):
        art = tracker.log_artifact(
            csv_path,
            key="metrics",
            direction="output",
            facet={"year": 2030, "ratio": 0.25, "flag": True, "label": "v1"},
            facet_index=True,
        )

    assert tracker.db is not None
    kv_rows = tracker.db.get_artifact_kv(art.id)
    assert {r.key_path for r in kv_rows} >= {"year", "ratio", "flag", "label"}
    bulk = tracker.db.get_artifact_kv_values_bulk(
        artifact_ids=[art.id],
        key_paths=["year", "ratio", "label"],
    )
    assert bulk[art.id]["year"]["value"] == 2030
    assert bulk[art.id]["ratio"]["value"] == 0.25
    assert bulk[art.id]["label"]["value"] == "v1"
    tracker.close()

    # DB-less Tracker (db_path=None): persist_facet populates run.meta["facet"] and RunSet preserves int
    dbless = Tracker(run_dir=tmp_path / "runs_dbless", db_path=None)
    res1 = dbless.run(
        fn=lambda: None,
        name="step_1",
        model="dbless_m",
        facet={"seed": 42, "lr": 0.05},
    )
    res2 = dbless.run(
        fn=lambda: None,
        name="step_2",
        model="dbless_m",
        facet={"seed": 99, "lr": 0.1},
    )
    assert res1.run.meta.get("facet") == {"seed": 42, "lr": 0.05}
    from consist.runset import RunSet

    rs = RunSet(runs=[res1.run, res2.run], label="dbless", _tracker=dbless)
    filtered = rs.filter(seed=42)
    assert [r.id for r in filtered.runs] == [res1.run.id]
    by_seed = rs.split_by("seed")
    assert list(by_seed.keys()) == [42, 99]
    assert all(type(k) is int for k in by_seed.keys())
    facet_vals = rs._load_facet_values(["seed", "lr"])
    assert type(facet_vals[res1.run.id]["seed"]) is int
    assert facet_vals[res1.run.id]["seed"] == 42
    assert type(facet_vals[res1.run.id]["lr"]) is float


def test_phase3_audit_primary_location_ingest_db_hydration_and_archive_move(
    tmp_path: Path,
) -> None:
    """Audit A.2, A.4, A.5, A.7, A.11, A.12: canonical URI priority, DB recovery, archive move, JSON reload."""
    import json
    from consist.models.run import ConsistRecord
    from consist.types import ExecutionOptions

    run_dir = tmp_path / "workspace"
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    tracker = Tracker(
        run_dir=run_dir,
        db_path=tmp_path / "ledger.duckdb",
        mounts={},
        archive_mounts={"cold_archive": str(archive_dir)},
    )

    with tracker.start_run("prod_ingested", model="m"):
        p_file = run_dir / "trips.parquet"
        p_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"trip_id": [1, 2], "dist": [3.5, 7.0]}).to_parquet(
            p_file, index=False
        )
        out_art = tracker.log_artifact(p_file, key="trips", direction="output")

    # Post-hoc ingest updates Coupler + last_run consist.json and preserves workspace:// primary_location
    coupler = Coupler(tracker=tracker)
    coupler.set("trips", out_art)
    ingested_art = tracker.ingest(out_art)
    assert coupler.require("trips").is_ingested is True
    assert ingested_art.primary_location is not None
    assert ingested_art.primary_location.uri.startswith("workspace://")

    run_json_path = run_dir / "consist_runs" / "prod_ingested.json"
    reloaded_rec = ConsistRecord.model_validate_json(
        run_json_path.read_text(encoding="utf-8")
    )
    assert reloaded_rec.outputs[0].is_ingested is True

    # ConsistRecord JSON reload when "locations" is empty and "origin" is present still preserves container_uri
    raw_payload = json.loads(run_json_path.read_text(encoding="utf-8"))
    raw_payload["outputs"][0]["locations"] = []
    reloaded_empty_locs = ConsistRecord.model_validate(raw_payload)
    assert reloaded_empty_locs.outputs[0].primary_location is not None
    assert reloaded_empty_locs.outputs[0].primary_location.uri.startswith(
        "workspace://"
    )

    # Delete cold parquet file on disk; primary_location stays workspace:// so hydrate_run_outputs recovers from DB!
    p_file.unlink()
    reloaded_from_db = tracker.get_artifact(ingested_art.id)
    assert reloaded_from_db is not None
    assert reloaded_from_db.primary_location is not None
    assert reloaded_from_db.primary_location.uri.startswith("workspace://")

    hydrated = tracker.hydrate_run_outputs(
        "prod_ingested",
        target_root=run_dir / "recovered_ws",
    )
    assert hydrated["trips"].status == "materialized_from_db"
    assert hydrated["trips"].path is not None and hydrated["trips"].path.exists()

    # Requested input staging of ingested missing-on-disk parquet succeeds without false sha256:file mismatch
    staged_dest = run_dir / "staged_inputs" / "trips.parquet"

    def _consume_staged() -> None:
        assert staged_dest.exists()
        df_back = pd.read_parquet(staged_dest)
        assert list(df_back["trip_id"]) == [1, 2]

    tracker.run(
        fn=_consume_staged,
        name="consume_ingested_staged",
        inputs={"trips": reloaded_from_db},
        execution_options=ExecutionOptions(
            input_materialization="requested",
            input_paths={"trips": staged_dest},
        ),
    )

    # Archive move preserves workspace:// primary_location and Artifact.locate(mounts=tracker.mounts) still finds archive!
    with tracker.start_run("prod_arch", model="m_arch"):
        f_arch = run_dir / "sub" / "report.txt"
        f_arch.parent.mkdir(parents=True, exist_ok=True)
        f_arch.write_text("archived payload\n", encoding="utf-8")
        arch_art = tracker.log_artifact(f_arch, key="report", direction="output")

    tracker.archive_run_outputs("prod_arch", archive_dir, mode="move")
    assert not f_arch.exists()
    arch_reloaded = tracker.get_artifact(arch_art.id)
    assert arch_reloaded is not None
    arch_reloaded.bind_tracker(tracker)
    assert arch_reloaded.primary_location is not None
    assert arch_reloaded.primary_location.uri.startswith("workspace://")
    located_arch = arch_reloaded.locate(mounts=tracker.mounts)
    assert located_arch is not None and located_arch.exists()
    tracker.close()

    # Read-only StoreLedger rejects dlt ingestion with PermissionError (and Tracker read_only rejects via RuntimeError)
    from consist.integrations.dlt_loader import ingest_artifact

    ro_tracker = Tracker(
        run_dir=run_dir, db_path=tmp_path / "ledger.duckdb", read_only=True
    )
    with pytest.raises(RuntimeError, match="read_only"):
        ro_tracker.ingest(ingested_art, data=[{"trip_id": 3, "dist": 1.0}])
    assert ro_tracker.ledger is not None
    ro_run = ro_tracker.get_run("prod_ingested")
    assert ro_run is not None
    with pytest.raises(PermissionError, match="read-only"):
        ingest_artifact(
            ingested_art,
            ro_run,
            data_iterable=[{"trip_id": 3, "dist": 1.0}],
            ledger=ro_tracker.ledger,
        )
    ro_tracker.close()


def test_phase3_audit_identity_domain_separation_directory_hydration_and_zero_byte_db(
    tmp_path: Path,
) -> None:
    """Audit A.3, A.6, A.9, A.10, A.14, B.1: strict scheme separation, dir manifest hydration, 0-byte DB, seal tx, lazy imports."""
    import subprocess
    import sys
    import uuid
    from consist.artifact import Artifact
    from consist.core.identity import CodeIdentityDescriptor, IdentityManager
    from consist.core.resolved_binding import ArtifactIdentity, ResolvedBindingBuilder
    from consist.core.run_resolution import InputBindingRole
    from consist.digest import Digest, Identity
    from consist.engine import seal
    from consist.store.detect import IncompatibleSchemaError, SchemaState, detect_schema
    from consist.store.ledger import StoreLedger

    # 1. Zero-byte DB handling
    empty_db = tmp_path / "zero_byte.duckdb"
    empty_db.touch()
    assert empty_db.stat().st_size == 0
    assert detect_schema(empty_db) is SchemaState.ABSENT
    with pytest.raises(IncompatibleSchemaError, match="0-byte"):
        StoreLedger(empty_db, read_only=True)
    rw_ledger = StoreLedger(empty_db, read_only=False)
    assert rw_ledger.schema_state is SchemaState.V2
    rw_ledger.close()

    # 2. ArtifactIdentity.from_artifact domain separation (even with identical 64-char hex)
    ctd_art = Artifact(
        id=uuid.uuid4(),
        key="tbl",
        driver="parquet",
        identity=Identity(anchor=Digest("ctd1:sha256", "a" * 64)),
    )
    blake_art = Artifact(
        id=uuid.uuid4(),
        key="raw",
        driver="csv",
        identity=Identity(anchor=Digest("blake3:file", "a" * 64)),
    )
    sha_art = Artifact(
        id=uuid.uuid4(),
        key="raw_sha",
        driver="csv",
        identity=Identity(anchor=Digest("sha256:file", "a" * 64)),
    )
    with pytest.raises(ValueError, match="Unsupported strict-binding identity scheme"):
        ArtifactIdentity.from_artifact(ctd_art)
    with pytest.raises(ValueError, match="Unsupported strict-binding identity scheme"):
        ArtifactIdentity.from_artifact(blake_art)
    assert str(ArtifactIdentity.from_artifact(sha_art)) == f"sha256:file:{'a' * 64}"

    idm = IdentityManager(project_root=str(tmp_path))
    code_desc = CodeIdentityDescriptor(version=1, mode="repo_git", digest="c" * 64)
    role_0 = [InputBindingRole(kind="named", role="inp", input_index=0)]
    act_ctd = idm.compute_action_input_identity(
        inputs=[ctd_art], binding_roles=role_0, code_identity=code_desc
    )
    act_blake = idm.compute_action_input_identity(
        inputs=[blake_art], binding_roles=role_0, code_identity=code_desc
    )
    act_sha = idm.compute_action_input_identity(
        inputs=[sha_art], binding_roles=role_0, code_identity=code_desc
    )
    assert act_ctd.bindings[0].value == f"ctd1:sha256:{'a' * 64}"
    assert act_blake.bindings[0].value == f"blake3:file:{'a' * 64}"
    assert act_sha.bindings[0].value == f"sha256:file:{'a' * 64}"
    assert len({act_ctd.value, act_blake.value, act_sha.value}) == 3

    # 3. Directory manifest hydration + ResolvedBindingBuilder.bind_tracked_artifact + seal() inside transaction + dotted facets
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=tmp_path / "dir_test.duckdb")
    zarr_dir = tmp_path / "ws" / "bundle_dir"
    zarr_dir.mkdir(parents=True, exist_ok=True)
    (zarr_dir / "part_0.txt").write_text("hello\n", encoding="utf-8")
    (zarr_dir / "part_1.txt").write_text("world\n", encoding="utf-8")

    with tracker.start_run(
        "dir_run",
        model="dir_m",
        facet={"a.b": {"c.d": 7}, "top.dot": 11, "str_num": "7"},
    ):
        dir_art = tracker.log_artifact(zarr_dir, key="bundle", direction="output")
        # Exercise engine.seal() multi-output occurrence_index inside active ledger
        extra_p = tmp_path / "ws" / "extra.txt"
        extra_p.write_text("extra\n", encoding="utf-8")
        assert tracker.ledger is not None
        sealed = seal(
            {"extra": extra_p},
            ledger=tracker.ledger,
            run_id="dir_run",
        )
        assert "extra" in sealed

    assert tracker.ledger is not None
    # Literal dotted keys in nested & top-level facet filters + numeric value_type guard
    matched_runs = tracker.ledger.find_runs(
        model="dir_m",
        facet={"a.b": {"c.d": 7}, "top.dot": 11},
    )
    assert [r.id for r in matched_runs] == ["dir_run"]
    assert tracker.ledger.find_runs(model="dir_m", facet={"str_num": 7}) == []

    # Hydrate directory artifact to new workspace and verify detached artifact preserves directory_manifest
    import shutil

    hyd_dir = tracker.hydrate_run_outputs(
        "dir_run", target_root=tmp_path / "ws" / "hydrated_dir_ws"
    )
    assert hyd_dir["bundle"].status == "materialized_directory_from_filesystem"
    detached_dir = hyd_dir["bundle"].artifact
    assert "directory_manifest" in detached_dir.meta

    # Even after cold directory is removed on disk, ResolvedBindingBuilder resolves directory_manifest from StoreLedger content_manifest
    shutil.rmtree(zarr_dir)
    reloaded_dir = tracker.get_artifact(dir_art.id)
    assert reloaded_dir is not None
    assert "directory_manifest" not in reloaded_dir.meta
    binding = (
        ResolvedBindingBuilder(
            step_name="consume_dir",
            step_contract_identity=f"sha256:step-v1:{'d' * 64}",
        )
        .bind_tracked_artifact(parameter="bundle", artifact=reloaded_dir)
        .freeze()
    )
    assert binding.inputs["bundle"].artifact.directory_manifest is not None
    tracker.close()

    # 4. Lazy heavy optional imports in dlt_loader
    lazy_check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys, consist.integrations.dlt_loader; "
                "loaded = [m for m in ('zarr', 'xarray', 'h5py', 'geopandas') if m in sys.modules]; "
                "assert not loaded, f'Eagerly imported: {loaded}'"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert lazy_check.returncode == 0, lazy_check.stderr
