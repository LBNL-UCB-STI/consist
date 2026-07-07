from pathlib import Path
import uuid

import pytest

from consist import EnumCapture, FilenamePattern, IntCapture, OutputSet
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


def test_output_set_child_destinations_rejects_traversal_metadata() -> None:
    parent_id = uuid.uuid4()
    parent = Artifact(id=parent_id, key="annual", driver="artifact_set")
    child = Artifact(
        key="annual__escape",
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
    parent = Artifact(id=parent_id, key="annual", driver="artifact_set")
    child = Artifact(
        key="annual__absolute",
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
    parent = Artifact(id=parent_id, key="annual", driver="artifact_set")
    child = Artifact(
        key="annual__escape",
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
