from __future__ import annotations

import csv
import logging
from pathlib import Path
from types import SimpleNamespace

import yaml

import pytest

from consist.integrations.activitysim import ActivitySimConfigAdapter, ConfigOverrides
from consist.core.config_canonicalization import ConfigAdapterOptions
from consist.integrations.activitysim.config_adapter import (
    _bundle_configs,
    _digest_path_with_name,
)
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs


def _find_ingestable(ingestables, table_name: str):
    for spec in ingestables:
        if spec.table_name == table_name:
            return spec
    return None


def _append_model(settings_path: Path, model_name: str) -> None:
    lines = settings_path.read_text(encoding="utf-8").splitlines()
    try:
        models_index = next(
            idx for idx, line in enumerate(lines) if line.strip() == "models:"
        )
    except StopIteration:
        lines.append("models:")
        models_index = len(lines) - 1
    insert_at = models_index + 1
    while insert_at < len(lines) and lines[insert_at].lstrip().startswith("- "):
        insert_at += 1
    lines.insert(insert_at, f"  - {model_name}")
    settings_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_discover_resolves_models_and_aliases(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )

    assert canonical.primary_config == overlay_dir / "settings.yaml"

    config_names = {path.name for path in canonical.config_files}
    assert "settings.yaml" in config_names
    assert "accessibility.yaml" in config_names
    assert "tour_scheduling_atwork.yaml" in config_names
    assert "trip_scheduling.yaml" in config_names
    assert "stop_frequency.yaml" in config_names
    assert canonical.content_hash


def test_canonicalize_builds_ingestables_and_constants(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_unit", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "settings.yaml" in artifact_names
    assert "accessibility.yaml" in artifact_names
    assert "stop_frequency_coeffs.csv" in artifact_names
    assert "trip_scheduling_coefficients.csv.gz" in artifact_names
    assert any("config_bundle" in name for name in artifact_names)

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None

    constants_rows = list(constants_spec.rows)
    assert any(
        row["key"] == "sample_rate" and row["value_num"] == 0.25
        for row in constants_rows
    )
    assert any(
        row["key"] == "CONSTANTS.AUTO_TIME" and row["file_name"] == "accessibility.yaml"
        for row in constants_rows
    )
    assert any(
        row["key"] == "CONSTANTS.TRANSIT_MODES" and row["value_type"] == "json"
        for row in constants_rows
    )

    table_names = {spec.table_name for spec in result.ingestables}
    assert table_names == {
        "activitysim_constants_cache",
        "activitysim_coefficients_cache",
        "activitysim_coefficients_template_refs_cache",
        "activitysim_probabilities_cache",
        "activitysim_probabilities_entries_cache",
        "activitysim_probabilities_meta_entries_cache",
        "activitysim_config_ingest_run_link",
    }


def test_digest_includes_file_name(tmp_path: Path):
    file_a = tmp_path / "same_a.csv"
    file_b = tmp_path / "same_b.csv"
    payload = "a,b\n1,2\n"

    file_a.write_text(payload)
    file_b.write_text(payload)

    hash_a = _digest_path_with_name(file_a, tracker=None)
    hash_b = _digest_path_with_name(file_b, tracker=None)

    assert hash_a != hash_b


def test_prepare_config_plan_builds_row_factories(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    plan = tracker.prepare_config(adapter, [overlay_dir, base_dir], strict=True)

    assert plan.identity_hash
    assert plan.adapter_name == "activitysim"
    assert any(spec.rows and callable(spec.rows) for spec in plan.ingestables)


def test_config_plan_dataframes(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    plan = tracker.prepare_config(adapter, [overlay_dir, base_dir], strict=True)

    constants_df = plan.constants_df()
    coeffs_df = plan.coefficients_df()
    probs_df = plan.probabilities_df()

    assert not constants_df.empty
    assert not coeffs_df.empty
    assert not probs_df.empty
    assert "file_name" in constants_df.columns
    assert "coefficient_name" in coeffs_df.columns
    assert "row_index" in probs_df.columns


def test_coefficients_and_probabilities_parsing(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_parse", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    coeff_rows = []
    prob_rows = []
    template_rows = []
    prob_entries = []
    prob_meta_entries = []
    for spec in result.ingestables:
        if spec.table_name == "activitysim_coefficients_cache":
            coeff_rows.extend(list(spec.rows))
        elif spec.table_name == "activitysim_probabilities_cache":
            prob_rows.extend(list(spec.rows))
        elif spec.table_name == "activitysim_coefficients_template_refs_cache":
            template_rows = list(spec.rows)
        elif spec.table_name == "activitysim_probabilities_entries_cache":
            prob_entries.extend(list(spec.rows))
        elif spec.table_name == "activitysim_probabilities_meta_entries_cache":
            prob_meta_entries.extend(list(spec.rows))

    trip_rows = [
        row for row in coeff_rows if row["file_name"].startswith("trip_scheduling")
    ]
    assert any(row["coefficient_name"] == "trip_time" for row in trip_rows)
    assert all(row["source_type"] == "direct" for row in trip_rows)

    dimensional_rows = [
        row
        for row in coeff_rows
        if row["file_name"] == "cdap_interaction_coefficients.csv"
    ]
    assert dimensional_rows
    assert all(row["source_type"] == "dimensional" for row in dimensional_rows)
    assert any(
        row["dims_json"] == '{"activity": "H", "interaction_ptypes": 11}'
        for row in dimensional_rows
    )
    assert template_rows
    assert any(
        row["referenced_coefficient"] == "coef_topology_walk_multiplier_work"
        for row in template_rows
    )
    assert any(row["key"] == "auto" and row["value_num"] == 0.7 for row in prob_entries)
    assert any(
        row["key"] == "depart_range_start" and row["value_num"] == 5.0
        for row in prob_meta_entries
    )

    stop_rows = [
        row for row in coeff_rows if row["file_name"] == "stop_frequency_coeffs.csv"
    ]
    segments = {row["segment"] for row in stop_rows}
    assert segments == {"segment_a", "segment_b"}
    assert all(row["is_constrained"] is True for row in stop_rows)
    assert all(row["source_type"] == "template" for row in stop_rows)

    stop_prob_rows = [
        row for row in prob_rows if row["file_name"] == "stop_frequency_probs.csv"
    ]
    assert stop_prob_rows
    assert stop_prob_rows[0]["dims"] == {"purpose": "work"}
    assert stop_prob_rows[0]["probs"] == {"auto": 0.7, "transit": 0.3}


def test_prepare_config_validation_and_signature(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()
    options = ConfigAdapterOptions(strict=True, bundle=False, ingest=False)

    plan = tracker.prepare_config(
        adapter,
        [overlay_dir, base_dir],
        options=options,
        validate_only=True,
    )

    assert plan.signature == plan.identity_hash
    assert plan.diagnostics is not None
    assert plan.diagnostics.ok
    assert tracker.identity_from_config_plan(plan) == plan.identity_hash


def test_missing_csv_lenient_logs_warning(tracker, tmp_path: Path, caplog):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    missing_yaml = base_dir / "missing_model.yaml"
    missing_yaml.write_text("SPEC: missing_coefficients.csv\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "missing_model")

    adapter = ActivitySimConfigAdapter()
    with caplog.at_level(logging.DEBUG):
        run = tracker.begin_run("activitysim_missing_lenient", "activitysim")
        canonical = adapter.discover(
            [overlay_dir, base_dir], identity=tracker.identity, strict=False
        )
        adapter.canonicalize(canonical, run=run, tracker=tracker, strict=False)
        tracker.end_run()

    assert any("Missing referenced CSV" in record.message for record in caplog.records)


def test_missing_csv_strict_raises(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    missing_yaml = base_dir / "missing_model.yaml"
    missing_yaml.write_text("SPEC: missing_coefficients.csv\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "missing_model")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_missing_strict", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    with pytest.raises(FileNotFoundError):
        adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()


def test_missing_include_settings_warns(tracker, tmp_path: Path, caplog):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    include_yaml = base_dir / "include_missing.yaml"
    include_yaml.write_text(
        "include_settings: missing_settings.yaml\n", encoding="utf-8"
    )
    _append_model(overlay_dir / "settings.yaml", "include_missing")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_missing_include", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    adapter.canonicalize(canonical, run=run, tracker=tracker, strict=False)
    tracker.end_run()

    assert any("include_settings file" in record.message for record in caplog.records)


def test_include_settings_rejects_out_of_root_absolute_include(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    outside_include = tmp_path / "outside_include.yaml"
    outside_include.write_text("CONSTANTS:\n  EXTERNAL_FLAG: 99\n", encoding="utf-8")
    include_yaml = base_dir / "include_external.yaml"
    include_yaml.write_text(
        "include_settings: "
        f"{outside_include.as_posix()}\n"
        "CONSTANTS:\n"
        "  LOCAL_FLAG: 1\n",
        encoding="utf-8",
    )
    _append_model(overlay_dir / "settings.yaml", "include_external")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_include_external_blocked", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None
    rows = list(constants_spec.rows)
    assert any(
        row["file_name"] == "include_external.yaml"
        and row["key"] == "CONSTANTS.LOCAL_FLAG"
        and row["value_num"] == 1.0
        for row in rows
    )
    assert not any(
        row["file_name"] == "include_external.yaml"
        and row["key"] == "CONSTANTS.EXTERNAL_FLAG"
        for row in rows
    )


def test_include_settings_allows_in_root_absolute_include(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    in_root_include = base_dir / "in_root_include.yaml"
    in_root_include.write_text("CONSTANTS:\n  IN_ROOT_FLAG: 42\n", encoding="utf-8")
    include_yaml = base_dir / "include_in_root.yaml"
    include_yaml.write_text(
        f"include_settings: {in_root_include.resolve().as_posix()}\n",
        encoding="utf-8",
    )
    _append_model(overlay_dir / "settings.yaml", "include_in_root")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_include_in_root", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None
    rows = list(constants_spec.rows)
    assert any(
        row["file_name"] == "include_in_root.yaml"
        and row["key"] == "CONSTANTS.IN_ROOT_FLAG"
        and row["value_num"] == 42.0
        for row in rows
    )


def test_inherit_settings_respects_primary_only(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path, sample_rate=0.25)
    overlay_settings = overlay_dir / "settings.yaml"
    overlay_settings.write_text(
        "inherit_settings: false\n"
        "models:\n"
        "  - compute_accessibility\n"
        "  - atwork_subtour_scheduling\n"
        "  - trip_scheduling\n"
        "  - stop_frequency\n"
        "sample_rate: 0.9\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_inherit_false", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None
    constants_rows = list(constants_spec.rows)
    assert any(
        row["key"] == "sample_rate" and row["value_num"] == 0.9
        for row in constants_rows
    )


def test_heuristic_reference_discovery_toggle(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    heuristic_yaml = base_dir / "heuristic_model.yaml"
    heuristic_yaml.write_text("EXTRA_FILE: extra_coeffs.csv\n", encoding="utf-8")
    extra_csv = base_dir / "extra_coeffs.csv"
    extra_csv.write_text("coefficient_name,value\nextra,1.0\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "heuristic_model")

    run = tracker.begin_run("activitysim_heuristic_on", "activitysim")
    adapter = ActivitySimConfigAdapter(allow_heuristic_refs=True)
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "extra_coeffs.csv" in artifact_names

    run = tracker.begin_run("activitysim_heuristic_off", "activitysim")
    adapter = ActivitySimConfigAdapter(allow_heuristic_refs=False)
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "extra_coeffs.csv" not in artifact_names


def test_out_of_root_absolute_csv_reference_is_rejected_by_default(
    tracker, tmp_path: Path
):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    outside_csv = tmp_path / "outside_coefficients.csv"
    outside_csv.write_text("coefficient_name,value\noutside,9.0\n", encoding="utf-8")
    outside_yaml = base_dir / "outside_csv.yaml"
    outside_yaml.write_text(f"SPEC: {outside_csv.as_posix()}\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "outside_csv")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_external_csv_default", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=False)
    tracker.end_run()

    assert outside_csv not in {spec.path for spec in result.artifacts}

    run_strict = tracker.begin_run("activitysim_external_csv_strict", "activitysim")
    strict_canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    with pytest.raises(FileNotFoundError, match="Missing referenced CSV"):
        adapter.canonicalize(
            strict_canonical, run=run_strict, tracker=tracker, strict=True
        )
    tracker.end_run()


def test_out_of_root_absolute_csv_reference_can_be_opted_in(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    outside_csv = tmp_path / "outside_coefficients.csv"
    outside_csv.write_text("coefficient_name,value\noutside,9.0\n", encoding="utf-8")
    outside_yaml = base_dir / "outside_csv_opt_in.yaml"
    outside_yaml.write_text(f"SPEC: {outside_csv.as_posix()}\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "outside_csv_opt_in")

    adapter = ActivitySimConfigAdapter(allow_out_of_root_csv_refs=True)
    run = tracker.begin_run("activitysim_external_csv_opt_in", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    assert outside_csv in {spec.path for spec in result.artifacts}


def test_bundle_artifact_logged(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter(bundle_configs=True)

    run = tracker.begin_run("activitysim_bundle", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_specs = [
        spec for spec in result.artifacts if spec.meta.get("config_role") == "bundle"
    ]
    assert len(bundle_specs) == 1
    assert bundle_specs[0].path.exists()


def test_bundle_cache_dir_reuse(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run_a = tracker.begin_run("activitysim_bundle_a", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result_a = adapter.canonicalize(canonical, run=run_a, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_a = next(
        spec for spec in result_a.artifacts if spec.meta.get("config_role") == "bundle"
    )
    assert bundle_a.path.exists()
    mtime = bundle_a.path.stat().st_mtime

    run_b = tracker.begin_run("activitysim_bundle_b", "activitysim")
    canonical_b = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result_b = adapter.canonicalize(
        canonical_b, run=run_b, tracker=tracker, strict=True
    )
    tracker.end_run()

    bundle_b = next(
        spec for spec in result_b.artifacts if spec.meta.get("config_role") == "bundle"
    )
    assert bundle_b.path == bundle_a.path
    assert bundle_b.path.stat().st_mtime == mtime


def test_bundle_path_sanitizes_model_name_and_stays_under_cache_dir(tmp_path: Path):
    config_root = tmp_path / "config_root"
    config_root.mkdir()
    (config_root / "settings.yaml").write_text("models: []\n", encoding="utf-8")
    cache_dir = tmp_path / "bundle_cache"
    run = SimpleNamespace(model_name="../../../../../tmp/evil//model")
    tracker = SimpleNamespace(run_dir=tmp_path / "run_dir")

    bundle_path = _bundle_configs(
        config_dirs=[config_root],
        run=run,
        tracker=tracker,
        content_hash="0123456789abcdef",
        cache_dir=cache_dir,
    )

    assert bundle_path.resolve().is_relative_to(cache_dir.resolve())
    assert "/" not in bundle_path.name
    assert ".." not in bundle_path.name
    assert bundle_path.name.startswith("tmp_evil_model_config_bundle_")


def test_materialize_applies_overrides(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run = tracker.begin_run("activitysim_materialize_base", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_path = next(
        spec.path
        for spec in result.artifacts
        if spec.meta.get("config_role") == "bundle"
    )
    overrides = ConfigOverrides(
        constants={
            ("settings_local.yaml", "sample_rate"): 0.33,
            ("accessibility.yaml", "CONSTANTS.AUTO_TIME"): 2.5,
        },
        coefficients={
            ("accessibility_coefficients.csv", "time", ""): 5.0,
            ("stop_frequency_coeffs.csv", "coef_1", "segment_a"): 9.0,
        },
    )

    staged_dir = tmp_path / "staged_config"
    staged = adapter.materialize(
        bundle_path,
        overrides,
        output_dir=staged_dir,
        identity=tracker.identity,
    )

    settings_path = next(
        (root / "settings_local.yaml")
        for root in staged.root_dirs
        if (root / "settings_local.yaml").exists()
    )
    settings = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    assert settings["sample_rate"] == 0.33

    access_path = next(
        (root / "accessibility.yaml")
        for root in staged.root_dirs
        if (root / "accessibility.yaml").exists()
    )
    access_yaml = yaml.safe_load(access_path.read_text(encoding="utf-8"))
    assert access_yaml["CONSTANTS"]["AUTO_TIME"] == 2.5

    coeff_path = next(
        (root / "accessibility_coefficients.csv")
        for root in staged.root_dirs
        if (root / "accessibility_coefficients.csv").exists()
    )
    with coeff_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    updated = [row for row in rows if row["coefficient_name"] == "time"]
    assert updated and updated[0]["value"] == "5.0"

    template_path = next(
        (root / "stop_frequency_coeffs.csv")
        for root in staged.root_dirs
        if (root / "stop_frequency_coeffs.csv").exists()
    )
    with template_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    updated = [row for row in rows if row["coefficient_name"] == "coef_1"]
    assert updated and updated[0]["segment_a"] == "9.0"


def test_materialize_hash_changes_with_overrides(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache_hash"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run = tracker.begin_run("activitysim_materialize_hash", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_path = next(
        spec.path
        for spec in result.artifacts
        if spec.meta.get("config_role") == "bundle"
    )

    staged_a = adapter.materialize(
        bundle_path,
        ConfigOverrides(constants={("settings_local.yaml", "sample_rate"): 0.11}),
        output_dir=tmp_path / "staged_a",
        identity=tracker.identity,
    )
    staged_b = adapter.materialize(
        bundle_path,
        ConfigOverrides(constants={("settings_local.yaml", "sample_rate"): 0.22}),
        output_dir=tmp_path / "staged_b",
        identity=tracker.identity,
    )

    assert staged_a.content_hash != staged_b.content_hash


def test_materialize_from_run_resolves_bundle_and_stabilizes_roots(
    tracker, tmp_path: Path
):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_materialize_from_run", "activitysim")
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    staged_a = adapter.materialize_from_run(
        tracker=tracker,
        run_id=run.id,
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_from_run_a",
    )
    staged_b = adapter.materialize_from_run(
        tracker=tracker,
        run_id=run.id,
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "materialized_from_run_b",
    )

    assert [root.name for root in staged_a.root_dirs] == [
        root.name for root in staged_b.root_dirs
    ]
    assert staged_a.primary_config is not None
    primary_root = adapter.select_root_dir(staged_a)
    assert staged_a.primary_config.is_relative_to(primary_root)


def test_get_coefficient_value_supports_bom_headers(tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    coeff_path = config_dir / "bom_coefficients.csv"
    coeff_path.write_text(
        "coefficient_name,value\ntime,1.23\n",
        encoding="utf-8-sig",
    )

    value = adapter.get_coefficient_value(
        config_dirs=[config_dir],
        file_name="bom_coefficients.csv",
        coefficient_name="time",
    )
    assert value == 1.23


def test_get_coefficient_value_supports_run_lookup(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_get_coeff_run", "activitysim")
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    value = adapter.get_coefficient_value(
        run_id=run.id,
        tracker=tracker,
        file_name="accessibility_coefficients.csv",
        coefficient_name="time",
    )
    assert value == 1.1


def test_get_coefficient_value_missing_raises_key_error(tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    coeff_path = config_dir / "coefficients.csv"
    coeff_path.write_text(
        "coefficient_name,value\ntime,1.23\n",
        encoding="utf-8",
    )

    with pytest.raises(KeyError, match="Coefficient not found"):
        adapter.get_coefficient_value(
            config_dirs=[config_dir],
            file_name="coefficients.csv",
            coefficient_name="cost",
        )


def test_get_coefficient_value_non_numeric_raises_value_error(tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    coeff_path = config_dir / "coefficients.csv"
    coeff_path.write_text(
        "coefficient_name,value\ntime,ref_time\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="not numeric"):
        adapter.get_coefficient_value(
            config_dirs=[config_dir],
            file_name="coefficients.csv",
            coefficient_name="time",
        )


def test_materialize_from_run_rejects_non_activitysim_run(tracker, tmp_path: Path):
    adapter = ActivitySimConfigAdapter()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    bundle_path = config_dir / "config_bundle.tar.gz"
    bundle_path.write_text("not-a-bundle", encoding="utf-8")

    run = tracker.begin_run("non_activitysim_run", "other")
    tracker.log_meta(config_adapter="beam")
    tracker.log_artifact(
        bundle_path,
        key="config_bundle",
        direction="input",
        config_role="bundle",
    )
    tracker.end_run()

    with pytest.raises(FileNotFoundError, match="detected run adapter='beam'"):
        adapter.materialize_from_run(
            tracker=tracker,
            run_id=run.id,
            overrides=ConfigOverrides(),
            output_dir=tmp_path / "materialized_non_activitysim",
        )


def test_select_root_dir_supports_required_file_guard(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )

    assert adapter.select_root_dir(canonical) == overlay_dir
    assert (
        adapter.select_root_dir(canonical, required_file="settings_local.yaml")
        == base_dir
    )

    with pytest.raises(FileNotFoundError, match="Required config file"):
        adapter.select_root_dir(canonical, required_file="missing.yaml")
