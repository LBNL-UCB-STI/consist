from __future__ import annotations

import csv
import logging
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

yaml = pytest.importorskip(
    "yaml", reason="ActivitySim adapter tests require the PyYAML dependency"
)

from consist.integrations.activitysim import (  # noqa: E402
    ActivitySimConfigAdapter,
    ConfigOverrides,
)
from consist.core.config_canonicalization import (  # noqa: E402
    CanonicalConfig,
    ConfigAdapterOptions,
)
from consist.integrations.activitysim.config_adapter import (  # noqa: E402
    _bundle_configs,
    _digest_path_with_name,
    _unpack_bundle,
)
from tests.helpers.activitysim_fixtures import (  # noqa: E402
    build_activitysim_test_configs,
)


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
    assert result.canonicalization is not None
    assert (
        tuple(item.reference for item in result.canonicalization.references)
        == result.identity.references
    )
    assert all(
        item.resolved_path is not None for item in result.canonicalization.references
    )
    assert any(not item.artifact_keys for item in result.canonicalization.references)

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


def test_canonicalize_builds_path_stable_identity_manifest(tracker, tmp_path: Path):
    case_a = tmp_path / "case_a"
    base_dir, overlay_dir = build_activitysim_test_configs(case_a)
    case_b = tmp_path / "case_b"
    shutil.copytree(base_dir.parent, case_b)
    clone_base = case_b / "base"
    clone_overlay = case_b / "overlay"

    adapter = ActivitySimConfigAdapter()
    canonical_a = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    canonical_b = adapter.discover(
        [clone_overlay, clone_base], identity=tracker.identity, strict=True
    )

    result_a = adapter.canonicalize(canonical_a, strict=True, plan_only=True)
    result_b = adapter.canonicalize(canonical_b, strict=True, plan_only=True)

    assert result_a.identity.identity_hash == result_b.identity.identity_hash
    assert result_a.identity.primary_config == "root:0:settings.yaml"

    refs = {ref.canonical_value: ref for ref in result_a.identity.references}
    assert refs["root:1:settings_local.yaml"].role == "yaml"
    assert refs["root:1:constants.yaml"].role == "yaml"
    assert refs["root:1:stop_frequency_probs.csv"].role == "csv"
    assert all(ref.hash for ref in refs.values())

    scalars = result_a.identity.scalars
    assert scalars["options"]["allow_heuristic_refs"] is True
    assert "active_yaml_hash" in scalars["component_hashes"]
    assert "referenced_csv_hash" in scalars["component_hashes"]
    assert scalars["roles"]["yaml"]
    assert scalars["roles"]["csv"]
    assert result_a.identity.directory_hash
    assert result_a.identity.directories


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


def test_materialize_from_config_dirs_preserves_non_alphabetical_overlay_precedence(
    tracker, tmp_path: Path
):
    alpha_base, zzz_overlay = build_activitysim_test_configs(tmp_path / "raw")
    alpha_dir = tmp_path / "alpha_base"
    zzz_dir = tmp_path / "zzz_overlay"
    shutil.copytree(alpha_base, alpha_dir)
    shutil.copytree(zzz_overlay, zzz_dir)

    adapter = ActivitySimConfigAdapter()
    staged = adapter.materialize_from_config_dirs(
        base_config_dirs=[zzz_dir, alpha_dir],
        base_primary_config=None,
        overrides=ConfigOverrides(
            constants={("settings_local.yaml", "sample_rate"): 0.42}
        ),
        output_dir=tmp_path / "staged",
        identity=tracker.identity,
    )

    assert [root.name for root in staged.root_dirs] == ["zzz_overlay", "alpha_base"]
    assert staged.primary_config == staged.root_dirs[0] / "settings.yaml"
    assert adapter.select_root_dir(staged) == staged.root_dirs[0]


def test_materialize_from_run_preserves_non_alphabetical_overlay_precedence(
    tracker, tmp_path: Path
):
    alpha_base, zzz_overlay = build_activitysim_test_configs(tmp_path / "raw")
    alpha_dir = tmp_path / "alpha_base"
    zzz_dir = tmp_path / "zzz_overlay"
    shutil.copytree(alpha_base, alpha_dir)
    shutil.copytree(zzz_overlay, zzz_dir)

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_precedence_run", "activitysim")
    tracker.canonicalize_config(adapter, [zzz_dir, alpha_dir], strict=True)
    tracker.end_run()

    staged = adapter.materialize_from_run(
        tracker=tracker,
        run_id=run.id,
        overrides=ConfigOverrides(
            constants={("settings_local.yaml", "sample_rate"): 0.55}
        ),
        output_dir=tmp_path / "staged_from_run",
    )

    assert [root.name for root in staged.root_dirs] == ["zzz_overlay", "alpha_base"]
    assert staged.primary_config == staged.root_dirs[0] / "settings.yaml"
    assert adapter.select_root_dir(staged) == staged.root_dirs[0]


def test_select_root_dir_required_file_preserves_precedence_order(tmp_path: Path):
    zzz_dir = tmp_path / "zzz_overlay"
    alpha_dir = tmp_path / "alpha_base"
    zzz_dir.mkdir()
    alpha_dir.mkdir()
    (zzz_dir / "shared.yaml").write_text("value: 1\n", encoding="utf-8")
    (alpha_dir / "shared.yaml").write_text("value: 2\n", encoding="utf-8")
    (alpha_dir / "settings.yaml").write_text("models: []\n", encoding="utf-8")

    canonical = CanonicalConfig(
        root_dirs=[zzz_dir, alpha_dir],
        primary_config=alpha_dir / "settings.yaml",
        config_files=[
            zzz_dir / "shared.yaml",
            alpha_dir / "shared.yaml",
            alpha_dir / "settings.yaml",
        ],
        external_files=[],
        content_hash="deadbeef",
    )
    adapter = ActivitySimConfigAdapter()
    assert adapter.select_root_dir(canonical, required_file="shared.yaml") == zzz_dir
    assert adapter.select_root_dir(canonical) == alpha_dir


def test_bundle_configs_disambiguates_duplicate_directory_basenames(
    tracker, tmp_path: Path
):
    base_src, overlay_src = build_activitysim_test_configs(tmp_path / "src")
    overlay_configs = tmp_path / "overlay" / "configs"
    base_configs = tmp_path / "base" / "configs"
    shutil.copytree(overlay_src, overlay_configs)
    shutil.copytree(base_src, base_configs)

    run = SimpleNamespace(model_name="activitysim")
    bundle_path = _bundle_configs(
        config_dirs=[overlay_configs, base_configs],
        run=run,
        tracker=tracker,
        content_hash="test_bundle_hash",
        cache_dir=tmp_path / "bundle_cache",
    )

    extracted = _unpack_bundle(
        bundle_path=bundle_path, output_dir=tmp_path / "extracted"
    )
    assert [d.name for d in extracted] == ["configs", "1_configs"]
    assert (extracted[0] / "settings.yaml").exists()
    assert (extracted[1] / "settings_local.yaml").exists()


def test_constants_list_and_probabilities_dlt_ingest(tracker, tmp_path: Path):
    from consist import ParameterizedSQL
    from consist.models.activitysim import (
        ActivitySimProbabilitiesCache,
        ActivitySimProbabilitiesEntriesCache,
        ActivitySimProbabilitiesMetaEntriesCache,
    )

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "models:\n  - stop_frequency\n", encoding="utf-8"
    )
    (config_dir / "stop_frequency.yaml").write_text(
        "PROBS_SPEC: stop_frequency_probs_1.csv\n"
        "CONSTANTS:\n"
        "  auto_modes:\n"
        "    - DRIVEALONEFREE\n"
        "    - SHARED2FREE\n"
        "  nested:\n"
        "    threshold: 3.5\n",
        encoding="utf-8",
    )
    (config_dir / "stop_frequency_probs_1.csv").write_text(
        "purpose_id,outbound,0_stops,1_stop,All\n1,True,0.6,0.4,1.0\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    with tracker.start_run("as_dlt_json_test", "activitysim"):
        tracker.canonicalize_config(adapter, [config_dir], strict=True, ingest=True)

    const_rows = adapter.constants_rows(
        key="CONSTANTS.auto_modes", value_column="value_json", tracker=tracker
    )
    assert len(const_rows) == 1
    assert const_rows[0][1] == ["DRIVEALONEFREE", "SHARED2FREE"]

    prob_cache_rows = tracker.query_rows(
        ParameterizedSQL(
            "SELECT * FROM global_tables.activitysim_probabilities_cache "
            "WHERE file_name = ?",
            ("stop_frequency_probs_1.csv",),
        ),
        model=ActivitySimProbabilitiesCache,
    )
    assert len(prob_cache_rows) == 1
    assert prob_cache_rows[0].dims == {"purpose_id": 1.0, "outbound": True}
    assert prob_cache_rows[0].probs == {"0_stops": 0.6, "1_stop": 0.4}

    prob_rows = adapter.run_link_rows(
        ActivitySimProbabilitiesEntriesCache,
        columns=[ActivitySimProbabilitiesEntriesCache.value_num],
        where={
            "file_name": "stop_frequency_probs_1.csv",
            "key": "0_stops",
        },
        tracker=tracker,
    )
    assert len(prob_rows) == 1
    assert prob_rows[0] == pytest.approx(0.6)

    all_rows = adapter.run_link_rows(
        ActivitySimProbabilitiesEntriesCache,
        columns=[ActivitySimProbabilitiesEntriesCache.value_num],
        where={
            "file_name": "stop_frequency_probs_1.csv",
            "key": "All",
        },
        tracker=tracker,
    )
    assert all_rows == []

    meta_rows = adapter.run_link_rows(
        ActivitySimProbabilitiesMetaEntriesCache,
        columns=[ActivitySimProbabilitiesMetaEntriesCache.value_num],
        where={
            "file_name": "stop_frequency_probs_1.csv",
            "key": "purpose_id",
        },
        tracker=tracker,
    )
    assert len(meta_rows) == 1
    assert meta_rows[0] == pytest.approx(1.0)


def test_multi_root_csv_only_overlay_identity(tracker, tmp_path: Path):
    base_dir = tmp_path / "common"
    overlay_dir = tmp_path / "resident"
    base_dir.mkdir()
    overlay_dir.mkdir()

    (base_dir / "settings.yaml").write_text(
        "inherit_settings: true\nmodels:\n  - stop_frequency\n",
        encoding="utf-8",
    )
    (base_dir / "stop_frequency.yaml").write_text(
        "COEFFICIENTS: stop_frequency_coefficients.csv\n",
        encoding="utf-8",
    )
    (base_dir / "stop_frequency_coefficients.csv").write_text(
        "coefficient_name,value\ntime,-0.02\n",
        encoding="utf-8",
    )
    # Overlay has ONLY a CSV file and no settings.yaml
    (overlay_dir / "stop_frequency_coefficients.csv").write_text(
        "coefficient_name,value\ntime,-0.05\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    captured_dirs: list[list[Path]] = []
    captured_configs_dir: list[list[Path]] = []
    captured_var_kwargs: list[list[str]] = []
    captured_both: list[tuple[Path, list[Path] | None]] = []

    def step_with_config_dirs(config_dirs: list[Path]) -> None:
        captured_dirs.append(list(config_dirs))

    def step_with_configs_dir(configs_dir: list[Path]) -> None:
        captured_configs_dir.append(list(configs_dir))

    def step_with_var_kwargs(**kwargs) -> None:
        captured_var_kwargs.append(sorted(kwargs.keys()))

    def step_with_both(config_dir: Path, config_dirs: list[Path] | None = None) -> None:
        captured_both.append((config_dir, config_dirs))

    res1 = adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[overlay_dir, base_dir],
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "staged_1",
        fn=step_with_config_dirs,
        name="as_multi_root_step",
        model="activitysim",
    )
    assert len(captured_dirs) == 1
    assert len(captured_dirs[0]) == 2
    digest_1 = res1.run.meta["resolved_config_identity"]["digest"]
    root_digests_1 = res1.run.meta["resolved_config_identity"]["root_digests"]
    assert len(root_digests_1) == 2

    # Mutate CSV in overlay_dir (which has no settings.yaml)
    (overlay_dir / "stop_frequency_coefficients.csv").write_text(
        "coefficient_name,value\ntime,-0.09\n",
        encoding="utf-8",
    )

    res2 = adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[overlay_dir, base_dir],
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "staged_2",
        fn=step_with_config_dirs,
        name="as_multi_root_step",
        model="activitysim",
    )
    digest_2 = res2.run.meta["resolved_config_identity"]["digest"]
    root_digests_2 = res2.run.meta["resolved_config_identity"]["root_digests"]
    assert digest_2 != digest_1
    assert root_digests_2[0] != root_digests_1[0]
    assert root_digests_2[1] == root_digests_1[1]
    assert res2.run.config_hash != res1.run.config_hash

    # Verify configs_dir injection, **kwargs fallback, and config_dir precedence
    adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[overlay_dir, base_dir],
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "staged_configs_dir",
        fn=step_with_configs_dir,
        name="as_configs_dir_step",
        model="activitysim",
        config={"case": "configs_dir"},
    )
    assert len(captured_configs_dir) == 1
    assert len(captured_configs_dir[0]) == 2

    adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[overlay_dir, base_dir],
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "staged_var_kwargs",
        fn=step_with_var_kwargs,
        name="as_var_kwargs_step",
        model="activitysim",
        config={"case": "var_kwargs"},
    )
    assert captured_var_kwargs == [["config_dir"]]

    adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[overlay_dir, base_dir],
        overrides=ConfigOverrides(),
        output_dir=tmp_path / "staged_both",
        fn=step_with_both,
        name="as_both_step",
        model="activitysim",
        config={"case": "both"},
    )
    assert len(captured_both) == 1
    assert captured_both[0][1] is None


def test_spec_none_and_logsum_settings_discovery(tracker, tmp_path: Path):
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "use_shadow_pricing: true\n"
        "models:\n"
        "  - compute_disaggregate_accessibility\n"
        "  - parking_location\n"
        "  - work_from_home\n",
        encoding="utf-8",
    )
    (config_dir / "constants.yaml").write_text(
        "walk_speed: 3.0\n",
        encoding="utf-8",
    )
    (config_dir / "shadow_pricing.yaml").write_text(
        "SHADOW_PRICE_MAX_ITERATIONS: 5\n",
        encoding="utf-8",
    )
    (config_dir / "disaggregate_accessibility.yaml").write_text(
        "LOGSUM_SETTINGS: tour_mode_choice_logsum.yaml\n",
        encoding="utf-8",
    )
    (config_dir / "tour_mode_choice_logsum.yaml").write_text(
        "COEFFICIENTS: tour_mode_choice_coefficients.csv\n",
        encoding="utf-8",
    )
    (config_dir / "tour_mode_choice_coefficients.csv").write_text(
        "coefficient_name,value\nivt,-0.02\n",
        encoding="utf-8",
    )
    (config_dir / "parking_location_choice.yaml").write_text(
        "SPECIFICATION: parking_spec.csv\nALTS: parking_alts.csv\n",
        encoding="utf-8",
    )
    (config_dir / "parking_spec.csv").write_text(
        "Label,Description,Expression\nutil_park,Park,1\n",
        encoding="utf-8",
    )
    (config_dir / "parking_alts.csv").write_text(
        "alt,id\npark1,1\n",
        encoding="utf-8",
    )
    (config_dir / "work_from_home.yaml").write_text(
        "SPEC: None\nCONSTANTS:\n  wfh_flag: 1\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    plan = tracker.prepare_config(adapter, [config_dir], strict=True)
    yaml_names = {p.name for p in plan.canonical.config_files}
    assert "constants.yaml" in yaml_names
    assert "shadow_pricing.yaml" in yaml_names
    assert "disaggregate_accessibility.yaml" in yaml_names
    assert "tour_mode_choice_logsum.yaml" in yaml_names
    assert "parking_location_choice.yaml" in yaml_names
    assert "work_from_home.yaml" in yaml_names

    artifact_names = {spec.path.name for spec in plan.artifacts}
    assert "tour_mode_choice_coefficients.csv" in artifact_names
    assert "parking_spec.csv" in artifact_names
    assert "parking_alts.csv" in artifact_names


def test_infix_coefficients_and_probabilities_files_ingested(tracker, tmp_path: Path):
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "models:\n  - stop_frequency\n  - cdap\n",
        encoding="utf-8",
    )
    (config_dir / "stop_frequency.yaml").write_text(
        "SPEC_SEGMENTS:\n"
        "  - WORK:\n"
        "      COEFFICIENTS: stop_frequency_coefficients_work.csv\n"
        "PROBS_SPEC: stop_frequency_probs_1.csv\n"
        "COEFFICIENT_TEMPLATE: tour_mode_choice_coefficients_template_estb.csv\n",
        encoding="utf-8",
    )
    # SFBay Description column should NOT become a dimensional segment
    (config_dir / "stop_frequency_coefficients_work.csv").write_text(
        "Description,value,coefficient_name\nWork intercept,-1.25,asc_work\n",
        encoding="utf-8",
    )
    (config_dir / "stop_frequency_probs_1.csv").write_text(
        "purpose_id,0_stops,1_stop\n1,0.7,0.3\n",
        encoding="utf-8",
    )
    (config_dir / "tour_mode_choice_coefficients_template_estb.csv").write_text(
        "coefficient_name,work\ncoef_ivt,coef_ivt_work\n",
        encoding="utf-8",
    )
    # Expression spec table named *_coefficients.csv should be skipped as coefficients
    (config_dir / "cdap.yaml").write_text(
        "INDIV_AND_HH_SPEC: cdap_indiv_and_hh_coefficients.csv\n",
        encoding="utf-8",
    )
    (config_dir / "cdap_indiv_and_hh_coefficients.csv").write_text(
        "Description,Expression,M,N,H\nWork,ptype == 1,1.0,0.0,0.0\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    with tracker.start_run("as_infix_test", "activitysim"):
        tracker.canonicalize_config(adapter, [config_dir], strict=True, ingest=True)

    from consist.models.activitysim import (
        ActivitySimCoefficientsCache,
        ActivitySimCoefficientTemplateRefsCache,
    )

    coeff_rows = adapter.coefficients_rows(
        file_name="stop_frequency_coefficients_work.csv",
        coefficient="asc_work",
        columns=[
            ActivitySimCoefficientsCache.segment,
            ActivitySimCoefficientsCache.source_type,
            ActivitySimCoefficientsCache.value_num,
        ],
        tracker=tracker,
    )
    assert len(coeff_rows) == 1
    assert coeff_rows[0][0] == ""
    assert coeff_rows[0][1] == "direct"
    assert coeff_rows[0][2] == pytest.approx(-1.25)

    # Verify cdap_indiv_and_hh_coefficients.csv was skipped
    cdap_rows = adapter.coefficients_rows(
        file_name="cdap_indiv_and_hh_coefficients.csv",
        tracker=tracker,
    )
    assert cdap_rows == []

    # Verify infix template refs were ingested
    tmpl_rows = adapter.run_link_rows(
        ActivitySimCoefficientTemplateRefsCache,
        columns=[
            ActivitySimCoefficientTemplateRefsCache.coefficient_name,
            ActivitySimCoefficientTemplateRefsCache.segment,
            ActivitySimCoefficientTemplateRefsCache.referenced_coefficient,
        ],
        where={"file_name": "tour_mode_choice_coefficients_template_estb.csv"},
        tracker=tracker,
    )
    assert len(tmpl_rows) == 1
    assert tuple(tmpl_rows[0]) == ("coef_ivt", "work", "coef_ivt_work")


def test_dimensional_coefficients_unique_primary_keys(tracker, tmp_path: Path):
    from consist.models.activitysim import ActivitySimCoefficientsCache

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "models:\n  - accessibility\n  - parking_location\n",
        encoding="utf-8",
    )
    (config_dir / "accessibility.yaml").write_text(
        "COEFFICIENTS: accessibility_coefficients.csv\n",
        encoding="utf-8",
    )
    (config_dir / "accessibility_coefficients.csv").write_text(
        "coefficient_name,purpose,value,constrain\n"
        "decay_exp,work,-0.05,T\n"
        "decay_exp,retail,-0.08,F\n",
        encoding="utf-8",
    )
    (config_dir / "parking_location_choice.yaml").write_text(
        "COEFFICIENTS: parking_location_coefficients.csv\n",
        encoding="utf-8",
    )
    (config_dir / "parking_location_coefficients.csv").write_text(
        "purpose,income_group,coefficient\nwork,low,-1.5\nwork,high,-0.5\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    with tracker.start_run("as_dim_coeffs", "activitysim"):
        tracker.canonicalize_config(adapter, [config_dir], strict=True, ingest=True)

    acc_rows = adapter.coefficients_rows(
        file_name="accessibility_coefficients.csv",
        coefficient="decay_exp",
        columns=[
            ActivitySimCoefficientsCache.segment,
            ActivitySimCoefficientsCache.source_type,
            ActivitySimCoefficientsCache.value_num,
            ActivitySimCoefficientsCache.dims_json,
        ],
        tracker=tracker,
    )
    assert len(acc_rows) == 2
    segments = {row[0] for row in acc_rows}
    assert segments == {'{"purpose": "retail"}', '{"purpose": "work"}'}
    assert all(row[1] == "dimensional" for row in acc_rows)
    assert all(row[0] == row[3] for row in acc_rows)

    park_rows = adapter.coefficients_rows(
        file_name="parking_location_coefficients.csv",
        coefficient="coefficient",
        columns=[
            ActivitySimCoefficientsCache.segment,
            ActivitySimCoefficientsCache.source_type,
            ActivitySimCoefficientsCache.value_num,
            ActivitySimCoefficientsCache.dims_json,
        ],
        tracker=tracker,
    )
    assert len(park_rows) == 2
    park_segments = {row[0] for row in park_rows}
    assert len(park_segments) == 2
    assert all(row[1] == "dimensional" for row in park_rows)


def test_inherit_settings_filename_and_nondefault_primary_config(
    tracker, tmp_path: Path
):
    common_dir = tmp_path / "common"
    ext_dir = tmp_path / "ext"
    common_dir.mkdir()
    ext_dir.mkdir()

    (common_dir / "settings.yaml").write_text(
        "households_sample_size: 1000\nmodels:\n  - initialize_households\n",
        encoding="utf-8",
    )
    (common_dir / "initialize_households.yaml").write_text(
        "CONSTANTS:\n  base_const: 10\n  override_const: 1\n",
        encoding="utf-8",
    )
    (ext_dir / "settings_mp.yaml").write_text(
        "inherit_settings: settings.yaml\nhouseholds_sample_size: 250\n",
        encoding="utf-8",
    )
    (ext_dir / "initialize_households.yaml").write_text(
        "inherit_settings: true\nCONSTANTS:\n  override_const: 2\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter(primary_config=Path("settings_mp.yaml"))
    with tracker.start_run("as_inherit_mp", "activitysim"):
        tracker.canonicalize_config(
            adapter, [ext_dir, common_dir], strict=True, ingest=True
        )

    sample_rows = adapter.constants_rows(
        file_name="settings_mp.yaml",
        key="households_sample_size",
        value_column="value_num",
        tracker=tracker,
    )
    assert len(sample_rows) == 1
    assert sample_rows[0][1] == pytest.approx(250.0)

    base_const_rows = adapter.constants_rows(
        file_name="initialize_households.yaml",
        key="CONSTANTS.base_const",
        value_column="value_num",
        tracker=tracker,
    )
    assert len(base_const_rows) == 1
    assert base_const_rows[0][1] == pytest.approx(10.0)

    override_const_rows = adapter.constants_rows(
        file_name="initialize_households.yaml",
        key="CONSTANTS.override_const",
        value_column="value_num",
        tracker=tracker,
    )
    assert len(override_const_rows) == 1
    assert override_const_rows[0][1] == pytest.approx(2.0)

    # Also verify run_with_config_overrides propagates base_primary_config
    captured_primary: list[Path | None] = []

    def check_step(config_dirs: list[Path]) -> None:
        captured_primary.append(config_dirs[0] / "settings_mp.yaml")

    adapter.run_with_config_overrides(
        tracker=tracker,
        base_config_dirs=[ext_dir, common_dir],
        base_primary_config=Path("settings_mp.yaml"),
        overrides=ConfigOverrides(
            constants={("settings_mp.yaml", "households_sample_size"): 500}
        ),
        output_dir=tmp_path / "staged_mp",
        fn=check_step,
        name="as_mp_override_step",
        model="activitysim",
    )
    assert captured_primary[0] is not None and captured_primary[0].exists()


def test_sandag_template_placeholder_detection_and_rendering(tracker, tmp_path: Path):
    from consist.models.activitysim import ActivitySimConstantsCache

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "models:\n  - route_stops\n",
        encoding="utf-8",
    )
    (config_dir / "constants.yaml").write_text(
        "year: {year:}\nSeniorAge: 65\nTurnOnTelecommute: {Activate Telecommute?:}\n",
        encoding="utf-8",
    )
    (config_dir / "route_stops.yaml").write_text(
        "location_settings_estb:\n  CONSTANTS:\n    sample_size: 20\n",
        encoding="utf-8",
    )

    # 1. strict=True without template_values raises ValueError
    strict_adapter = ActivitySimConfigAdapter()
    with pytest.raises(ValueError, match="Unrendered template placeholder"):
        tracker.prepare_config(strict_adapter, [config_dir], strict=True)

    # 2. strict=False without template_values emits value_type="unrendered_template"
    with tracker.start_run("as_unrendered_non_strict", "activitysim"):
        tracker.canonicalize_config(
            strict_adapter, [config_dir], strict=False, ingest=True
        )

    unrendered_rows = strict_adapter.constants_rows(
        file_name="constants.yaml",
        key="CONSTANTS.year",
        columns=[
            ActivitySimConstantsCache.value_type,
            ActivitySimConstantsCache.value_str,
        ],
        tracker=tracker,
    )
    assert len(unrendered_rows) == 1
    assert tuple(unrendered_rows[0]) == ("unrendered_template", "{year:}")

    unrendered_q_rows = strict_adapter.constants_rows(
        file_name="constants.yaml",
        key="CONSTANTS.TurnOnTelecommute",
        columns=[
            ActivitySimConstantsCache.value_type,
            ActivitySimConstantsCache.value_str,
        ],
        tracker=tracker,
    )
    assert len(unrendered_q_rows) == 1
    assert tuple(unrendered_q_rows[0]) == (
        "unrendered_template",
        "{Activate Telecommute?:}",
    )

    # 3. With template_values provided, renders cleanly in strict=True and materializes rendered YAML
    rendered_adapter = ActivitySimConfigAdapter(
        template_values={"year": 2022, "Activate Telecommute?": 1}
    )
    with tracker.start_run("as_rendered_strict", "activitysim"):
        tracker.canonicalize_config(
            rendered_adapter, [config_dir], strict=True, ingest=True
        )

    year_rows = rendered_adapter.constants_rows(
        file_name="constants.yaml",
        key="CONSTANTS.year",
        columns=[
            ActivitySimConstantsCache.value_type,
            ActivitySimConstantsCache.value_num,
        ],
        where={"run_id": "as_rendered_strict"},
        tracker=tracker,
    )
    assert len(year_rows) == 1
    assert year_rows[0][0] == "num"
    assert year_rows[0][1] == pytest.approx(2022.0)

    estb_rows = rendered_adapter.constants_rows(
        file_name="route_stops.yaml",
        key="CONSTANTS.location_settings_estb.sample_size",
        value_column="value_num",
        where={"run_id": "as_rendered_strict"},
        tracker=tracker,
    )
    assert len(estb_rows) == 1
    assert estb_rows[0][1] == pytest.approx(20.0)

    # Verify materialize_from_config_dirs writes rendered YAML to staged directory
    staged = rendered_adapter.materialize_from_config_dirs(
        base_config_dirs=[config_dir],
        base_primary_config=None,
        overrides=ConfigOverrides(
            constants={("constants.yaml", "CONSTANTS.SeniorAge"): 67}
        ),
        output_dir=tmp_path / "staged_rendered",
        identity=tracker.identity,
    )
    staged_constants_text = (staged.root_dirs[0] / "constants.yaml").read_text(
        encoding="utf-8"
    )
    assert "{year:}" not in staged_constants_text
    assert "2022" in staged_constants_text
    assert "67" in staged_constants_text
