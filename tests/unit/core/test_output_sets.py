from pathlib import Path
import uuid

import pytest

from consist import OutputSet
from consist.core.output_sets import (
    build_output_set_manifest,
    discover_output_set_members,
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
