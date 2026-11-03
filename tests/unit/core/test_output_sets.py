from pathlib import Path
import uuid

import pytest

from consist import (
    EnumCapture,
    FilenameCapture,
    FilenamePattern,
    IntCapture,
    OutputSet,
    StringCapture,
)
from consist.api import StringCapture as ApiStringCapture
from consist.core.output_sets import (
    build_output_set_manifest,
    discover_output_set_members,
    _manifest_identity_hash,
    output_set_child_destinations,
    resolve_output_set_expected_members,
    validate_cached_output_sets,
)
from consist.models.artifact import Artifact


def test_discover_output_set_members_are_filtered_and_sorted(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "chunk_10.csv").write_text("id\n10\n")
    (root / "chunk_02.csv").write_text("id\n2\n")
    (root / "chunk_ignore.tmp").write_text("ignore\n")
    (root / "nested").mkdir()
    (root / "nested" / "chunk_01.csv").write_text("id\n1\n")

    output_set = OutputSet(
        root=root,
        include=["chunk_*.csv", "nested/*.csv"],
        exclude=["chunk_10.csv"],
        recursive=True,
    )

    members = discover_output_set_members(output_set)

    assert [member.relative_path for member in members] == [
        "chunk_02.csv",
        "nested/chunk_01.csv",
    ]


def test_recursive_glob_includes_root_level_and_nested_files(tmp_path: Path) -> None:
    """A recursive glob treats ``**/`` as matching zero directory levels too."""
    root = tmp_path / "annual"
    root.mkdir()
    (root / "root.csv").write_text("id\n0\n")
    (root / "nested").mkdir()
    (root / "nested" / "nested.csv").write_text("id\n1\n")
    outside = tmp_path / "outside.csv"
    outside.write_text("id\n2\n")

    members = discover_output_set_members(OutputSet(root=root, include="**/*"))

    assert [member.relative_path for member in members] == [
        "nested/nested.csv",
        "root.csv",
    ]
    assert all(member.path != outside for member in members)


def test_recursive_glob_exclude_matches_root_level_file(tmp_path: Path) -> None:
    """A zero-depth ``**/`` exclusion applies to files at the set root."""
    root = tmp_path / "annual"
    root.mkdir()
    (root / "root.csv").write_text("id\n0\n")
    (root / "nested").mkdir()
    (root / "nested" / "nested.csv").write_text("id\n1\n")

    members = discover_output_set_members(
        OutputSet(root=root, include="**/*", exclude="**/root.csv")
    )

    assert [member.relative_path for member in members] == ["nested/nested.csv"]


def test_discover_output_set_members_rejects_symlinked_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "outputs"
    root.mkdir()
    external_file = tmp_path / "external.csv"
    external_file.write_text("secret\n")
    symlink_path = root / "leak.csv"
    try:
        symlink_path.symlink_to(external_file)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        discover_output_set_members(OutputSet(root=root, include="*.csv"))


def test_discover_output_set_members_prunes_excluded_zarr_subtrees(
    tmp_path: Path,
) -> None:
    """Excluded directory trees are not traversed before file filtering."""
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "keep.csv").write_text("id\n1\n")
    zarr_root = root / "skims.zarr"
    zarr_root.mkdir()
    external_file = tmp_path / "external.csv"
    external_file.write_text("id\n2\n")
    symlink_path = zarr_root / "unexpected-symlink.csv"
    try:
        symlink_path.symlink_to(external_file)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    members = discover_output_set_members(
        OutputSet(root=root, include="**/*", exclude="**/*.zarr/**")
    )

    assert [member.relative_path for member in members] == ["keep.csv"]


def test_discover_output_set_members_prunes_excluded_subtrees_when_recursive(
    tmp_path: Path,
) -> None:
    """Explicit recursion prunes excluded trees before checking their members."""
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "keep.csv").write_text("id\n1\n")
    zarr_root = root / "skims.zarr"
    zarr_root.mkdir()
    external_file = tmp_path / "external.csv"
    external_file.write_text("id\n2\n")
    symlink_path = zarr_root / "unexpected-symlink.csv"
    try:
        symlink_path.symlink_to(external_file)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    members = discover_output_set_members(
        OutputSet(
            root=root,
            include="*.csv",
            exclude="**/*.zarr/**",
            recursive=True,
        )
    )

    assert [member.relative_path for member in members] == ["keep.csv"]


def test_output_set_child_destinations_rejects_traversal_metadata() -> None:
    parent_id = uuid.uuid4()
    parent = Artifact(
        id=parent_id,
        key="annual",
        container_uri="inputs://annual",
        driver="artifact_set",
    )
    child = Artifact(
        key="annual__escape",
        container_uri="inputs://annual/../escape.csv",
        driver="csv",
        parent_artifact_id=parent_id,
        meta={
            "output_set_member": True,
            "output_set_relative_path": "../escape.csv",
        },
    )

    with pytest.raises(ValueError, match="unsafe output-set member path"):
        output_set_child_destinations(
            parent=parent,
            children=[child],
            destination_root=Path("/tmp/hydrated"),
        )


def test_output_set_child_destinations_rejects_absolute_metadata() -> None:
    parent_id = uuid.uuid4()
    parent = Artifact(
        id=parent_id,
        key="annual",
        container_uri="inputs://annual",
        driver="artifact_set",
    )
    child = Artifact(
        key="annual__absolute",
        container_uri="inputs://annual/escape.csv",
        driver="csv",
        parent_artifact_id=parent_id,
        meta={
            "output_set_member": True,
            "output_set_relative_path": "/tmp/escape.csv",
        },
    )

    with pytest.raises(ValueError, match="unsafe output-set member path"):
        output_set_child_destinations(
            parent=parent,
            children=[child],
            destination_root=Path("/tmp/hydrated"),
        )


def test_validate_cached_output_sets_rejects_traversal_metadata() -> None:
    parent_id = uuid.uuid4()
    parent = Artifact(
        id=parent_id,
        key="annual",
        container_uri="inputs://annual",
        driver="artifact_set",
    )
    child = Artifact(
        key="annual__escape",
        container_uri="inputs://annual/../escape.csv",
        driver="csv",
        parent_artifact_id=parent_id,
        meta={
            "output_set_member": True,
            "output_set_relative_path": "../escape.csv",
        },
    )

    with pytest.raises(ValueError, match="unsafe output-set member path"):
        validate_cached_output_sets(
            outputs={"annual": parent, "annual__escape": child},
            output_sets={"annual": OutputSet(root="annual", include="*.csv")},
            config={},
        )


def test_resolve_output_set_expected_members_validates_count_and_names(
    tmp_path: Path,
) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "annual_2030.csv").write_text("year\n2030\n")
    output_set = OutputSet(
        root=root,
        include="annual_*.csv",
        expected_count=lambda config: len(config["years"]),
        expected_members=lambda config: [
            f"annual_{year}.csv" for year in config["years"]
        ],
    )
    members = discover_output_set_members(output_set)

    with pytest.raises(ValueError, match="missing expected members"):
        resolve_output_set_expected_members(
            key="annual_outputs",
            output_set=output_set,
            members=members,
            config={"years": [2030, 2035]},
        )


def test_build_output_set_manifest_is_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "annual_2035.csv").write_text("year\n2035\n")
    (root / "annual_2030.csv").write_text("year\n2030\n")
    output_set = OutputSet(
        root=root,
        include="annual_*.csv",
        kind="tabular-partitioned",
        expected_members=["annual_2030.csv", "annual_2035.csv"],
        member_facets=lambda path, relpath, config: {
            "year": int(Path(relpath).stem.removeprefix("annual_"))
        },
    )
    members = discover_output_set_members(output_set)

    manifest = build_output_set_manifest(
        key="annual_outputs",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert manifest["output_set_key"] == "annual_outputs"
    assert manifest["kind"] == "tabular-partitioned"
    assert manifest["expected"]["members"] == ["annual_2030.csv", "annual_2035.csv"]
    assert [member["relative_path"] for member in manifest["members"]] == [
        "annual_2030.csv",
        "annual_2035.csv",
    ]
    assert manifest["members"][0]["facets"] == {
        "output_set_key": "annual_outputs",
        "year": 2030,
    }
    assert "validation" not in manifest


def test_filename_pattern_manifest_identity_is_order_independent(
    tmp_path: Path,
) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "output_2030_home.csv").write_text("year,purpose\n2030,home\n")

    pattern_a = FilenamePattern.glob("output_*_*.csv").with_captures(
        IntCapture(name="year", wildcard=1),
        EnumCapture(name="purpose", allowed={"home", "work"}, wildcard=2),
    )
    pattern_b = FilenamePattern.glob("output_*_*.csv").with_captures(
        EnumCapture(name="purpose", allowed={"work", "home"}, wildcard=2),
        IntCapture(name="year", wildcard=1),
    )

    output_set_a = OutputSet(root=root, include=pattern_a)
    output_set_b = OutputSet(root=root, include=pattern_b)

    members_a = discover_output_set_members(output_set_a)
    members_b = discover_output_set_members(output_set_b)

    manifest_a = build_output_set_manifest(
        key="annual_outputs",
        output_set=output_set_a,
        members=members_a,
        config={},
        logged_members=[],
    )
    manifest_b = build_output_set_manifest(
        key="annual_outputs",
        output_set=output_set_b,
        members=members_b,
        config={},
        logged_members=[],
    )

    assert _manifest_identity_hash(manifest_a) == _manifest_identity_hash(manifest_b)


def test_filename_pattern_capture_extracts_typed_facets(tmp_path: Path) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "output_2030.parquet").write_text("year\n2030\n")
    (root / "output_2035.parquet").write_text("year\n2035\n")

    output_set = OutputSet(
        root=root,
        include=FilenamePattern.glob("output_*.parquet").with_captures(
            IntCapture(name="year", wildcard=1)
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="annual_outputs",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member.relative_path for member in members] == [
        "output_2030.parquet",
        "output_2035.parquet",
    ]
    assert [member["facets"]["year"] for member in manifest["members"]] == [2030, 2035]
    assert all(
        isinstance(member["facets"]["year"], int) for member in manifest["members"]
    )


def test_filename_pattern_exclude_precedes_typed_capture_validation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "output_final.parquet").write_text("final\n")

    output_set = OutputSet(
        root=root,
        include=FilenamePattern.glob("output_*.parquet").with_captures(
            IntCapture(name="year", wildcard=1)
        ),
        exclude=["output_final.parquet"],
    )

    members = discover_output_set_members(output_set)

    assert members == []


def test_filename_pattern_rejects_double_star() -> None:
    with pytest.raises(ValueError, match="\\*\\*"):
        FilenamePattern.glob("output_**.parquet")


def test_filename_pattern_capture_extracts_repeated_relative_path_facet(
    tmp_path: Path,
) -> None:
    root = tmp_path / "output"
    (root / "IT.3").mkdir(parents=True)
    (root / "IT.3" / "3.events.parquet").write_text("event\n")

    output_set = OutputSet(
        root=root,
        recursive=True,
        include=FilenamePattern.glob("IT.*/*.events.parquet").with_captures(
            IntCapture(name="iteration", wildcard=1),
            IntCapture(name="iteration", wildcard=2),
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="events",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member.relative_path for member in members] == ["IT.3/3.events.parquet"]
    assert manifest["members"][0]["facets"] == {
        "output_set_key": "events",
        "iteration": 3,
    }


def test_filename_pattern_rejects_repeated_capture_value_mismatch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "output"
    (root / "IT.3").mkdir(parents=True)
    (root / "IT.3" / "4.events.parquet").write_text("event\n")

    output_set = OutputSet(
        root=root,
        recursive=True,
        include=FilenamePattern.glob("IT.*/*.events.parquet").with_captures(
            IntCapture(name="iteration", wildcard=1),
            IntCapture(name="iteration", wildcard=2),
        ),
    )

    with pytest.raises(ValueError, match="repeated capture"):
        discover_output_set_members(output_set)


def test_filename_pattern_rejects_capture_binding_errors() -> None:
    with pytest.raises(ValueError, match="out of range"):
        FilenamePattern.glob("output_*.parquet").with_captures(
            IntCapture(name="year", wildcard=2)
        )

    with pytest.raises(ValueError, match="duplicate wildcard index"):
        FilenamePattern.glob("output_*_*.parquet").with_captures(
            IntCapture(name="year", wildcard=1),
            IntCapture(name="iteration", wildcard=1),
        )

    with pytest.raises(ValueError, match="incompatible repeated capture"):
        FilenamePattern.glob("output_*_*.parquet").with_captures(
            IntCapture(name="purpose", wildcard=1),
            EnumCapture(name="purpose", allowed={"home"}, wildcard=2),
        )


def test_filename_pattern_rejects_missing_typed_captures(tmp_path: Path) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "output_2030.parquet").write_text("year\n2030\n")
    (root / "output_final.parquet").write_text("final\n")

    output_set = OutputSet(
        root=root,
        include=FilenamePattern.glob("output_*.parquet").with_captures(
            IntCapture(name="year", wildcard=1)
        ),
    )

    with pytest.raises(ValueError, match="failed typed capture"):
        discover_output_set_members(output_set)


def test_filename_pattern_extracts_enum_facets(tmp_path: Path) -> None:
    root = tmp_path / "trips"
    root.mkdir()
    (root / "trip_home.csv").write_text("purpose\nhome\n")
    (root / "trip_homeoffice.csv").write_text("purpose\nhomeoffice\n")

    output_set = OutputSet(
        root=root,
        include=FilenamePattern.glob("trip_*.csv").with_captures(
            EnumCapture(
                name="purpose",
                allowed={"home", "homeoffice"},
                wildcard=1,
            )
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="trip_outputs",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member["facets"]["purpose"] for member in manifest["members"]] == [
        "home",
        "homeoffice",
    ]


def test_string_capture_public_imports() -> None:
    assert StringCapture is ApiStringCapture
    capture = StringCapture(name="event_type", wildcard=1)
    assert isinstance(capture, FilenameCapture.__args__)


def test_filename_pattern_extracts_string_facets(tmp_path: Path) -> None:
    root = tmp_path / "events"
    root.mkdir()
    (root / "0.events.PathTraversal.parquet").write_text("event\n1\n")
    (root / "0.events.ModeChoice.parquet").write_text("event\n2\n")
    (root / "0.events.actend.parquet").write_text("event\n3\n")

    output_set = OutputSet(
        root=root,
        include=FilenamePattern.glob("0.events.*.parquet").with_captures(
            StringCapture(name="event_type", wildcard=1)
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="events_by_type",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member.relative_path for member in members] == [
        "0.events.ModeChoice.parquet",
        "0.events.PathTraversal.parquet",
        "0.events.actend.parquet",
    ]
    assert [member["facets"]["event_type"] for member in manifest["members"]] == [
        "ModeChoice",
        "PathTraversal",
        "actend",
    ]
    assert manifest["include"] == {
        "pattern": "0.events.*.parquet",
        "captures": [
            {
                "kind": "string",
                "name": "event_type",
                "wildcard": 1,
                "pattern": r"[A-Za-z0-9_.-]+",
            }
        ],
    }


def test_filename_pattern_combines_int_and_string_captures(tmp_path: Path) -> None:
    root = tmp_path / "output"
    (root / "ITERS" / "it.0").mkdir(parents=True)
    (root / "ITERS" / "it.2").mkdir(parents=True)
    (root / "ITERS" / "it.0" / "0.events.PathTraversal.parquet").write_text("e\n")
    (root / "ITERS" / "it.2" / "0.events.ModeChoice.parquet").write_text("e\n")

    output_set = OutputSet(
        root=root,
        recursive=True,
        include=FilenamePattern.glob("ITERS/it.*/0.events.*.parquet").with_captures(
            IntCapture(name="iteration", wildcard=1),
            StringCapture(name="event_type", wildcard=2),
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="events",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member.relative_path for member in members] == [
        "ITERS/it.0/0.events.PathTraversal.parquet",
        "ITERS/it.2/0.events.ModeChoice.parquet",
    ]
    assert [member["facets"] for member in manifest["members"]] == [
        {
            "output_set_key": "events",
            "iteration": 0,
            "event_type": "PathTraversal",
        },
        {
            "output_set_key": "events",
            "iteration": 2,
            "event_type": "ModeChoice",
        },
    ]


def test_filename_pattern_string_capture_on_directory_wildcard(tmp_path: Path) -> None:
    root = tmp_path / "output"
    (root / "scenarios" / "baseline").mkdir(parents=True)
    (root / "scenarios" / "high_transit").mkdir(parents=True)
    (root / "scenarios" / "baseline" / "0.events.PathTraversal.parquet").write_text(
        "e\n"
    )
    (root / "scenarios" / "high_transit" / "0.events.ModeChoice.parquet").write_text(
        "e\n"
    )

    output_set = OutputSet(
        root=root,
        recursive=True,
        include=FilenamePattern.glob("scenarios/*/0.events.*.parquet").with_captures(
            StringCapture(name="scenario", wildcard=1),
            StringCapture(name="event_type", wildcard=2),
        ),
    )

    members = discover_output_set_members(output_set)
    manifest = build_output_set_manifest(
        key="scenario_events",
        output_set=output_set,
        members=members,
        config={},
        logged_members=[],
    )

    assert [member.relative_path for member in members] == [
        "scenarios/baseline/0.events.PathTraversal.parquet",
        "scenarios/high_transit/0.events.ModeChoice.parquet",
    ]
    assert [member["facets"] for member in manifest["members"]] == [
        {
            "output_set_key": "scenario_events",
            "scenario": "baseline",
            "event_type": "PathTraversal",
        },
        {
            "output_set_key": "scenario_events",
            "scenario": "high_transit",
            "event_type": "ModeChoice",
        },
    ]


def test_string_capture_custom_pattern_validates_tokens(tmp_path: Path) -> None:
    root = tmp_path / "events"
    root.mkdir()
    (root / "0.events.PathTraversal.parquet").write_text("event\n")
    (root / "0.events.bad_123.parquet").write_text("event\n")

    pattern = FilenamePattern.glob("0.events.*.parquet").with_captures(
        StringCapture(name="event_type", wildcard=1, pattern=r"[A-Za-z]+")
    )

    with pytest.raises(ValueError, match="failed typed capture"):
        discover_output_set_members(OutputSet(root=root, include=pattern))

    members = discover_output_set_members(
        OutputSet(
            root=root,
            include=pattern,
            exclude=["0.events.bad_123.parquet"],
        )
    )
    assert [member.relative_path for member in members] == [
        "0.events.PathTraversal.parquet"
    ]


def test_string_capture_adjacent_wildcards_disambiguated_with_delimiter_free_pattern(
    tmp_path: Path,
) -> None:
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "north_bay_transit.csv").write_text("value\n1\n")

    default_pattern = FilenamePattern.glob("*_*.csv").with_captures(
        StringCapture(name="region", wildcard=1),
        StringCapture(name="mode", wildcard=2),
    )
    default_manifest = build_output_set_manifest(
        key="greedy_split",
        output_set=OutputSet(root=root, include=default_pattern),
        members=discover_output_set_members(
            OutputSet(root=root, include=default_pattern)
        ),
        config={},
        logged_members=[],
    )
    assert default_manifest["members"][0]["facets"] == {
        "output_set_key": "greedy_split",
        "region": "north_bay",
        "mode": "transit",
    }

    delimited_pattern = FilenamePattern.glob("*_*_*.csv").with_captures(
        StringCapture(name="subregion", wildcard=1, pattern=r"[A-Za-z0-9-]+"),
        StringCapture(name="corridor", wildcard=2, pattern=r"[A-Za-z0-9-]+"),
        StringCapture(name="mode", wildcard=3, pattern=r"[A-Za-z0-9-]+"),
    )
    delimited_manifest = build_output_set_manifest(
        key="explicit_split",
        output_set=OutputSet(root=root, include=delimited_pattern),
        members=discover_output_set_members(
            OutputSet(root=root, include=delimited_pattern)
        ),
        config={},
        logged_members=[],
    )
    assert delimited_manifest["members"][0]["facets"] == {
        "output_set_key": "explicit_split",
        "subregion": "north",
        "corridor": "bay",
        "mode": "transit",
    }


def test_string_capture_validation_errors() -> None:
    with pytest.raises(ValueError, match="1-indexed"):
        StringCapture(name="event_type", wildcard=0)

    with pytest.raises(ValueError, match="safe identifier"):
        StringCapture(name="", wildcard=1)

    with pytest.raises(ValueError, match="reserved"):
        StringCapture(name="output_set_key", wildcard=1)

    with pytest.raises(ValueError, match="non-empty string"):
        StringCapture(name="event_type", wildcard=1, pattern="")

    with pytest.raises(ValueError, match="invalid string capture regex"):
        StringCapture(name="event_type", wildcard=1, pattern="[unclosed")

    with pytest.raises(ValueError, match="empty strings"):
        StringCapture(name="event_type", wildcard=1, pattern=r"[A-Za-z]*")

    with pytest.raises(ValueError, match="path separators"):
        StringCapture(name="event_type", wildcard=1, pattern=r".+")

    with pytest.raises(ValueError, match="path separators"):
        StringCapture(name="event_type", wildcard=1, pattern=r"[A-Za-z/]+")

    with pytest.raises(ValueError, match="path separators"):
        StringCapture(name="event_type", wildcard=1, pattern=r"[A-Za-z\\]+")

    with pytest.raises(ValueError, match="capturing groups"):
        StringCapture(name="event_type", wildcard=1, pattern=r"(foo|bar)")

    # Non-capturing groups remain allowed
    sc = StringCapture(name="event_type", wildcard=1, pattern=r"(?:foo|bar)")
    assert sc.pattern == r"(?:foo|bar)"

    with pytest.raises(ValueError, match="incompatible repeated capture"):
        FilenamePattern.glob("output_*_*.parquet").with_captures(
            StringCapture(name="token", wildcard=1, pattern=r"[a-z]+"),
            StringCapture(name="token", wildcard=2, pattern=r"[A-Z]+"),
        )


def test_output_set_rejects_unimplemented_validation_modes(tmp_path: Path) -> None:
    root = tmp_path / "annual"
    root.mkdir()
    (root / "annual_2030.csv").write_text("year\n2030\n")
    output_set = OutputSet(root=root, include="*.csv", validate="hashes")
    members = discover_output_set_members(output_set)

    with pytest.raises(ValueError, match="not implemented"):
        resolve_output_set_expected_members(
            key="annual_outputs",
            output_set=output_set,
            members=members,
            config={},
        )
