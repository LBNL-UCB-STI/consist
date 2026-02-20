from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlmodel import Session, select

from consist.integrations.activitysim import ActivitySimConfigAdapter, ConfigOverrides
from consist.models.activitysim import ActivitySimConstantsCache
from consist.types import CacheOptions, ExecutionOptions
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs


def _log_mock_output(tracker, run_name: str, tmp_path: Path) -> None:
    output_path = tmp_path / f"{run_name}_summary.csv"
    output_path.write_text("a,b\n1,2\n", encoding="utf-8")
    tracker.log_artifact(
        output_path,
        key=f"outputs:{run_name}",
        direction="output",
    )


def test_activitysim_ingest_and_query_by_config_value(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()

    base_a, overlay_a = build_activitysim_test_configs(tmp_path, sample_rate=0.25)
    run_a = tracker.begin_run(
        "activitysim_run_a",
        "activitysim",
        config={"sample_rate": 0.25},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_a, base_a], ingest=True)
    _log_mock_output(tracker, "activitysim_run_a", tmp_path)
    tracker.end_run()

    base_b, overlay_b = build_activitysim_test_configs(tmp_path, sample_rate=0.5)
    run_b = tracker.begin_run(
        "activitysim_run_b",
        "activitysim",
        config={"sample_rate": 0.5},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_b, base_b], ingest=True)
    _log_mock_output(tracker, "activitysim_run_b", tmp_path)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with tracker.engine.begin() as connection:
        run_rows = connection.exec_driver_sql(
            "SELECT DISTINCT l.run_id "
            "FROM global_tables.activitysim_constants_cache c "
            "JOIN global_tables.activitysim_config_ingest_run_link l "
            "ON l.content_hash = c.content_hash "
            "WHERE l.table_name = 'activitysim_constants_cache' "
            "AND c.key = 'sample_rate' AND c.value_num = 0.5"
        ).fetchall()
        run_ids = [row[0] for row in run_rows]
        assert run_ids

        output_rows = connection.exec_driver_sql(
            "SELECT a.key FROM artifact a "
            "JOIN run_artifact_link r ON a.id = r.artifact_id "
            "WHERE r.direction = 'output' AND r.run_id IN (%s)"
            % ", ".join([f"'{run_id}'" for run_id in run_ids])
        ).fetchall()
        output_keys = [row[0] for row in output_rows]

    assert set(output_keys) == {"outputs:activitysim_run_b"}
    assert run_a.id != run_b.id


def test_activitysim_sqlmodel_query(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()

    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path, sample_rate=0.5)
    tracker.begin_run(
        "activitysim_sqlmodel_query",
        "activitysim",
        config={"sample_rate": 0.5},
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstantsCache)
            .where(ActivitySimConstantsCache.key == "sample_rate")
            .where(ActivitySimConstantsCache.value_num == 0.5)
        ).all()

    assert rows


def test_activitysim_query_coefficients(tracker, tmp_path: Path):
    """Demonstrate querying accessibility coefficients across runs."""
    from consist.models.activitysim import ActivitySimCoefficientsCache

    adapter = ActivitySimConfigAdapter()

    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    tracker.begin_run(
        "activitysim_coeff_query",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    rows = adapter.coefficients_rows(
        coefficient="time",
        file_name="accessibility_coefficients.csv",
        tracker=tracker,
    )

    assert len(rows) > 0, f"Expected rows, got: {rows}"
    # time coefficient should be 1.1 from the test data
    assert any(value == 1.1 for _, value in rows), (
        f"Expected value_num=1.1, got: {[value for _, value in rows]}"
    )
    assert {run_id for run_id, _ in rows} == {"activitysim_coeff_query"}

    from sqlalchemy import Float, cast

    with Session(tracker.engine) as session:
        numeric_rows = session.exec(
            select(ActivitySimCoefficientsCache)
            .where(ActivitySimCoefficientsCache.value_num.is_not(None))
            .where(cast(ActivitySimCoefficientsCache.value_num, Float) > 1.0)
            .where(ActivitySimCoefficientsCache.coefficient_name == "time")
        ).all()

    assert numeric_rows, "Expected numeric comparison on value_num to work."


def test_activitysim_config_plan_apply(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)

    facet_spec = {
        "yaml": {
            "accessibility.yaml": [
                {"key": "CONSTANTS.AUTO_TIME", "alias": "auto_time"}
            ],
        },
        "coefficients": {
            "accessibility_coefficients.csv": [
                {"key": "time", "alias": "access_time_coef"}
            ],
        },
    }
    plan = tracker.prepare_config(
        adapter,
        [overlay_dir, base_dir],
        strict=True,
        facet_spec=facet_spec,
        facet_schema_name="activitysim_config",
        facet_index=True,
    )

    tracker.begin_run("activitysim_plan_apply", "activitysim", cache_mode="overwrite")
    tracker.apply_config_plan(plan, ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstantsCache).where(
                ActivitySimConstantsCache.key == "sample_rate"
            )
        ).all()

    assert rows, "Expected constants to be ingested from config plan."

    matches = tracker.find_runs_by_facet_kv(
        namespace="activitysim",
        key="auto_time",
        value_num=1.5,
    )
    assert matches, "Expected facet lookup to find the run by auto_time."


def test_activitysim_config_plan_run_applies_ingest(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter(root_dirs=[overlay_dir, base_dir])

    tracker.run(
        fn=lambda: None,
        name="activitysim_plan_run",
        model="activitysim",
        adapter=adapter,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimConstantsCache).where(
                ActivitySimConstantsCache.key == "sample_rate"
            )
        ).all()

    assert rows, "Expected config plan ingest during Tracker.run."


def test_activitysim_template_refs_ingested(tracker, tmp_path: Path):
    from consist.models.activitysim import ActivitySimCoefficientTemplateRefsCache

    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)

    tracker.begin_run(
        "activitysim_template_refs",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], ingest=True)
    tracker.end_run()

    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ActivitySimCoefficientTemplateRefsCache).where(
                ActivitySimCoefficientTemplateRefsCache.file_name
                == "tour_mode_choice_coefficients_template.csv"
            )
        ).all()

    assert rows, "Expected template reference rows to be ingested."


def test_run_with_config_overrides_hit_miss_behavior(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case")

    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert config_dir.is_dir()
        assert any(
            (config_dir / file_name).exists()
            for file_name in ("settings.yaml", "settings_local.yaml")
        )

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(
            coefficients={("accessibility_coefficients.csv", "time", ""): 1.1}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="activitysim_override_run",
        model="activitysim",
        config={"calibration_step": "time"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("./settings.yaml"),
        overrides=ConfigOverrides(
            coefficients={("accessibility_coefficients.csv", "time", ""): 1.1}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="activitysim_override_run",
        model="activitysim",
        config={"calibration_step": "time"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(
            coefficients={("accessibility_coefficients.csv", "time", ""): 2.3}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="activitysim_override_run",
        model="activitysim",
        config={"calibration_step": "time"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert run_c.cache_hit is False
    assert len(calls) == 2
    assert run_c.run.meta.get("config_adapter") == "activitysim"
    bundle = tracker.get_config_bundle(run_a.run.id)
    assert bundle is not None
    assert bundle.exists()


def test_run_with_config_overrides_hit_miss_behavior_fast_hashing(
    tracker, tmp_path: Path
):
    tracker.identity.hashing_strategy = "fast"
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case_fast")

    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert config_dir.is_dir()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(
            coefficients={("accessibility_coefficients.csv", "time", ""): 1.1}
        ),
        output_dir=tmp_path / "materialized_overrides_fast",
        fn=step,
        name="activitysim_override_run_fast",
        model="activitysim",
        config={"calibration_step": "time"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(
            coefficients={("accessibility_coefficients.csv", "time", ""): 1.1}
        ),
        output_dir=tmp_path / "materialized_overrides_fast",
        fn=step,
        name="activitysim_override_run_fast",
        model="activitysim",
        config={"calibration_step": "time"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.run.config_hash == run_b.run.config_hash
    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert len(calls) == 1


def test_run_with_config_overrides_rejects_multiple_base_selectors(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case_dual")

    with pytest.raises(ValueError, match="exactly one base selector"):
        tracker.run_with_config_overrides(
            adapter=adapter,
            base_run_id="baseline",
            base_config_dirs=[overlay_dir, base_dir],
            overrides=ConfigOverrides(),
            output_dir=tmp_path / "materialized_dual_selector",
            fn=lambda: None,
            name="activitysim_override_dual_selector",
            model="activitysim",
        )


def test_run_with_config_overrides_rejects_missing_base_primary_config_hint(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(
        tmp_path / "base_case_missing_primary"
    )

    with pytest.raises(
        FileNotFoundError,
        match="base_primary_config was not found under base_config_dirs",
    ):
        tracker.run_with_config_overrides(
            adapter=adapter,
            base_config_dirs=[overlay_dir, base_dir],
            base_primary_config=Path("missing-settings.yaml"),
            overrides=ConfigOverrides(),
            output_dir=tmp_path / "materialized_missing_primary",
            fn=lambda: None,
            name="activitysim_override_missing_primary",
            model="activitysim",
        )


def test_run_with_config_overrides_adds_manual_identity_inputs_and_auto_identity(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case_auto")
    manual_dep = tmp_path / "manual_identity_dep.yaml"
    manual_dep.write_text("mode: auto\n", encoding="utf-8")
    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert config_dir.is_dir()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="activitysim_override_auto_identity",
        model="activitysim",
        identity_inputs=[("manual_dep", manual_dep)],
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="activitysim_override_auto_identity",
        model="activitysim",
        identity_inputs=[("manual_dep", manual_dep)],
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert len(calls) == 1

    for result in (run_a, run_b):
        digest_map = result.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert "manual_dep" in digest_map
        assert "activitysim_config" in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "auto",
            "adapter": "activitysim",
            "label": "activitysim_config",
            "path": metadata["path"],
            "digest": digest_map["activitysim_config"],
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


def test_run_with_config_overrides_respects_explicit_runtime_kwargs(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(
        tmp_path / "base_case_manual"
    )

    base_run = tracker.begin_run(
        "activitysim_override_base_manual",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    explicit_config_dir = tmp_path / "manual_runtime_config"
    explicit_config_dir.mkdir(parents=True)
    seen: list[Path] = []

    def step(config_dir: Path) -> None:
        seen.append(config_dir)

    tracker.run_with_config_overrides(
        adapter=adapter,
        base_run_id=base_run.id,
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_manual_runtime",
        fn=step,
        name="activitysim_override_manual_runtime",
        model="activitysim",
        execution_options=ExecutionOptions(
            runtime_kwargs={"config_dir": explicit_config_dir}
        ),
    )

    assert seen == [explicit_config_dir]


def test_run_with_config_overrides_supports_custom_runtime_kwarg_mapping(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(
        tmp_path / "base_case_custom"
    )

    base_run = tracker.begin_run(
        "activitysim_override_base_custom",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    seen: list[Path] = []

    def step(config_root: Path) -> None:
        seen.append(config_root)
        assert config_root.is_dir()

    tracker.run_with_config_overrides(
        adapter=adapter,
        base_run_id=base_run.id,
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_custom_runtime",
        fn=step,
        name="activitysim_override_custom_runtime",
        model="activitysim",
        override_runtime_kwargs={"config_root": "selected_root_dir"},
    )

    assert len(seen) == 1
    assert any(
        (seen[0] / file_name).exists()
        for file_name in ("settings.yaml", "settings_local.yaml")
    )


def test_run_with_config_overrides_merges_top_level_runtime_kwargs(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case_alias")

    base_run = tracker.begin_run(
        "activitysim_override_base_alias",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    runtime_output_dir = tmp_path / "runtime_output_dir"
    seen: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        seen.append((config_dir, output_dir))

    tracker.run_with_config_overrides(
        adapter=adapter,
        base_run_id=base_run.id,
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_runtime_alias",
        fn=step,
        name="activitysim_override_runtime_alias",
        model="activitysim",
        runtime_kwargs={"output_dir": runtime_output_dir},
    )

    assert len(seen) == 1
    assert seen[0][1] == runtime_output_dir
    assert any(
        (seen[0][0] / file_name).exists()
        for file_name in ("settings.yaml", "settings_local.yaml")
    )


def test_run_with_config_overrides_off_mode_and_runtime_kwargs_identity_behavior(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path / "base_case_off")
    manual_dep = tmp_path / "manual_identity_off.yaml"
    manual_dep.write_text("mode: off\n", encoding="utf-8")

    runtime_output_a = tmp_path / "runtime_identity_off_a"
    runtime_output_b = tmp_path / "runtime_identity_off_b"
    runtime_output_c = tmp_path / "runtime_identity_on_c"
    runtime_output_d = tmp_path / "runtime_identity_on_d"
    runtime_output_a.mkdir(parents=True)
    runtime_output_b.mkdir(parents=True)
    runtime_output_c.mkdir(parents=True)
    runtime_output_d.mkdir(parents=True)
    (runtime_output_c / "identity.txt").write_text("c\n", encoding="utf-8")
    (runtime_output_d / "identity.txt").write_text("d\n", encoding="utf-8")

    calls: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        calls.append((config_dir, output_dir))
        assert config_dir.is_dir()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="activitysim_override_off_identity",
        model="activitysim",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        runtime_kwargs={"output_dir": runtime_output_a},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="activitysim_override_off_identity",
        model="activitysim",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        runtime_kwargs={"output_dir": runtime_output_b},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="activitysim_override_off_identity",
        model="activitysim",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_c),
        ],
        resolved_config_identity="off",
        runtime_kwargs={"output_dir": runtime_output_c},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_d = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[overlay_dir, base_dir],
        base_primary_config=Path("settings.yaml"),
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="activitysim_override_off_identity",
        model="activitysim",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_d),
        ],
        resolved_config_identity="off",
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
        assert "activitysim_config" not in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "off",
            "adapter": "activitysim",
            "label": "activitysim_config",
            "path": None,
            "digest": None,
        }


def test_run_with_config_overrides_rejects_dual_runtime_kwargs_sources(
    tracker, tmp_path: Path
):
    adapter = ActivitySimConfigAdapter()
    base_dir, overlay_dir = build_activitysim_test_configs(
        tmp_path / "base_case_runtime_conflict"
    )

    base_run = tracker.begin_run(
        "activitysim_override_base_runtime_conflict",
        "activitysim",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    with pytest.raises(
        ValueError,
        match="runtime_kwargs= and execution_options.runtime_kwargs=",
    ):
        tracker.run_with_config_overrides(
            adapter=adapter,
            base_run_id=base_run.id,
            overrides=ConfigOverrides(),
            output_dir=tmp_path / "materialized_runtime_conflict",
            fn=lambda: None,
            name="activitysim_override_runtime_conflict",
            model="activitysim",
            runtime_kwargs={"output_dir": tmp_path / "out_a"},
            execution_options=ExecutionOptions(
                runtime_kwargs={"output_dir": tmp_path / "out_b"}
            ),
        )
