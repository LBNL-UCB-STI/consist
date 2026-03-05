import logging
import importlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import func, select
from sqlmodel import Session, SQLModel

from consist.integrations.beam import BeamConfigAdapter, BeamConfigOverrides
from consist.integrations.beam.config_adapter import _load_config_tree
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from consist.types import CacheOptions, ExecutionOptions
from tests.helpers.beam_fixtures import build_beam_test_configs


def test_beam_models_register_tables(tracker):
    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with tracker.engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        SQLModel.metadata.create_all(
            connection,
            tables=[
                BeamConfigCache.__table__,
                BeamConfigIngestRunLink.__table__,
            ],
        )
    with Session(tracker.engine) as session:
        session.exec(select(func.count()).select_from(BeamConfigCache))


def test_beam_discover_includes_and_hash(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    assert canonical.primary_config == overlay_conf
    assert any(p.name == "common.conf" for p in canonical.config_files)
    assert canonical.content_hash


def test_beam_load_config_tree_applies_env_overrides_without_env_mutation(
    tmp_path: Path,
):
    config_path = tmp_path / "env.conf"
    config_path.write_text("value = ${TEST_OVERRIDE}\n", encoding="utf-8")

    with patch.object(os._Environ, "__setitem__", autospec=True) as setitem_mock:
        with patch.object(os._Environ, "pop", autospec=True) as pop_mock:
            tree = _load_config_tree(
                config_path,
                resolve=True,
                env_overrides={"TEST_OVERRIDE": "scoped-value"},
            )

    assert tree["value"] == "scoped-value"
    assert "TEST_OVERRIDE" not in tree
    assert setitem_mock.call_count == 0
    assert pop_mock.call_count == 0


def test_beam_canonicalize_artifacts_and_rows(tracker, tmp_path: Path, caplog):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_unit", "beam")
    with caplog.at_level(logging.WARNING):
        result = adapter.canonicalize(canonical, run=run, tracker=tracker)
    artifact_keys = {spec.key for spec in result.artifacts}
    assert any("base.conf" in key for key in artifact_keys)
    assert any("sample.csv" in key for key in artifact_keys)
    assert any("missing/does-not-exist.csv" in r.message for r in caplog.records)
    ingest_tables = {spec.table_name for spec in result.ingestables}
    assert "beam_config_cache" in ingest_tables
    assert "beam_config_ingest_run_link" in ingest_tables


def test_beam_materialize_overrides(tracker, tmp_path: Path):
    ConfigFactory = importlib.import_module("pyhocon").ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.75}
    )
    output_dir = tmp_path / "materialized"
    canonical = adapter.materialize(
        [case_dir],
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.75


def test_beam_materialize_from_plan_uses_config_dirs(tracker, tmp_path: Path):
    ConfigFactory = importlib.import_module("pyhocon").ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    plan = tracker.prepare_config(adapter, [case_dir])
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.9}
    )
    output_dir = tmp_path / "materialized_plan"
    canonical = adapter.materialize_from_plan(
        plan,
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.9


def test_beam_run_with_config_overrides_hit_miss_behavior(tracker, tmp_path: Path):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case")

    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert config_dir.is_dir()
        assert (config_dir / "overlay.conf").exists()
        assert (config_dir / "base.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.35}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.35}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.7}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert run_c.cache_hit is False
    assert len(calls) == 2


def test_beam_run_with_config_overrides_additive_identity_inputs_and_auto_metadata(
    tracker, tmp_path: Path
):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_auto")
    manual_dep = tmp_path / "beam_manual_identity.txt"
    manual_dep.write_text("beam=true\n", encoding="utf-8")
    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert (config_dir / "overlay.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="beam_override_auto_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        identity_label="beam_config",
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="beam_override_auto_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        identity_label="beam_config",
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert len(calls) == 1

    for result in (run_a, run_b):
        digest_map = result.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert "manual_dep" in digest_map
        assert "beam_config" in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "auto",
            "adapter": "beam",
            "label": "beam_config",
            "path": metadata["path"],
            "digest": digest_map["beam_config"],
        }
        assert isinstance(metadata["path"], str)
        assert Path(metadata["path"]).is_dir()
        record = tracker.get_run_record(result.run.id)
        assert record is not None
        assert record.run.meta.get("resolved_config_identity") == metadata

    latest_snapshot = tracker.fs.run_dir / "consist.json"
    latest_payload = json.loads(latest_snapshot.read_text(encoding="utf-8"))
    latest_meta = (latest_payload.get("run") or {}).get("meta") or {}
    assert latest_meta.get("resolved_config_identity") == run_b.run.meta.get(
        "resolved_config_identity"
    )


def test_beam_run_with_config_overrides_off_mode_runtime_kwargs_identity_behavior(
    tracker, tmp_path: Path
):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_off")
    manual_dep = tmp_path / "beam_manual_identity_off.txt"
    manual_dep.write_text("beam=off\n", encoding="utf-8")

    runtime_output_a = tmp_path / "beam_runtime_identity_off_a"
    runtime_output_b = tmp_path / "beam_runtime_identity_off_b"
    runtime_output_c = tmp_path / "beam_runtime_identity_on_c"
    runtime_output_d = tmp_path / "beam_runtime_identity_on_d"
    runtime_output_a.mkdir(parents=True)
    runtime_output_b.mkdir(parents=True)
    runtime_output_c.mkdir(parents=True)
    runtime_output_d.mkdir(parents=True)
    (runtime_output_c / "identity.txt").write_text("c\n", encoding="utf-8")
    (runtime_output_d / "identity.txt").write_text("d\n", encoding="utf-8")

    calls: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        calls.append((config_dir, output_dir))
        assert (config_dir / "overlay.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_a},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_b},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_c),
        ],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_c},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_d = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_d),
        ],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_d},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert run_c.cache_hit is False
    assert run_d.cache_hit is False
    assert len(calls) == 3

    for result in (run_a, run_b, run_c, run_d):
        digest_map = result.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert "manual_dep" in digest_map
        assert "beam_config" not in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "off",
            "adapter": "beam",
            "label": "beam_config",
            "path": None,
            "digest": None,
        }


def test_beam_run_with_config_overrides_rejects_multiple_base_selectors(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_dual")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    with pytest.raises(ValueError, match="exactly one base selector"):
        tracker.run_with_config_overrides(
            adapter=adapter,
            base_run_id="baseline",
            base_config_dirs=[case_dir],
            overrides=BeamConfigOverrides(values={}),
            output_dir=tmp_path / "materialized_dual_selector",
            fn=lambda: None,
            name="beam_override_dual_selector",
            model="beam",
        )


def test_beam_run_with_config_overrides_respects_explicit_runtime_kwargs(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_manual")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_manual",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    explicit_config_dir = tmp_path / "manual_runtime_config"
    explicit_config_dir.mkdir(parents=True)
    seen: list[Path] = []

    def step(config_dir: Path) -> None:
        seen.append(config_dir)

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.2}
        ),
        output_dir=tmp_path / "materialized_manual_runtime",
        fn=step,
        name="beam_override_manual_runtime",
        model="beam",
        execution_options=ExecutionOptions(
            runtime_kwargs={"config_dir": explicit_config_dir}
        ),
    )

    assert seen == [explicit_config_dir]


def test_beam_run_with_config_overrides_supports_custom_runtime_kwarg_mapping(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_custom")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_custom",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    seen: list[Path] = []

    def step(config_root: Path) -> None:
        seen.append(config_root)
        assert (config_root / "overlay.conf").exists()

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.15}
        ),
        output_dir=tmp_path / "materialized_custom_runtime",
        fn=step,
        name="beam_override_custom_runtime",
        model="beam",
        override_runtime_kwargs={"config_root": "selected_root_dir"},
    )

    assert len(seen) == 1


def test_beam_run_with_config_overrides_merges_top_level_runtime_kwargs(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_alias")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_alias",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    runtime_output_dir = tmp_path / "runtime_output_dir"
    seen: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        seen.append((config_dir, output_dir))

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.21}
        ),
        output_dir=tmp_path / "materialized_runtime_alias",
        fn=step,
        name="beam_override_runtime_alias",
        model="beam",
        runtime_kwargs={"output_dir": runtime_output_dir},
    )

    assert len(seen) == 1
    assert seen[0][1] == runtime_output_dir
    assert (seen[0][0] / "overlay.conf").exists()


def test_beam_run_with_config_overrides_rejects_dual_runtime_kwargs_sources(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(
        tmp_path / "base_case_runtime_conflict"
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_runtime_conflict",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    with pytest.raises(
        ValueError,
        match="runtime_kwargs= and execution_options.runtime_kwargs=",
    ):
        tracker.run_with_config_overrides(
            adapter=BeamConfigAdapter(),
            base_run_id=base_run.id,
            overrides=BeamConfigOverrides(
                values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.2}
            ),
            output_dir=tmp_path / "materialized_runtime_conflict",
            fn=lambda: None,
            name="beam_override_runtime_conflict",
            model="beam",
            runtime_kwargs={"output_dir": tmp_path / "out_a"},
            execution_options=ExecutionOptions(
                runtime_kwargs={"output_dir": tmp_path / "out_b"}
            ),
        )
