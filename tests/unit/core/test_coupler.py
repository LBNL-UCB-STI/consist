from __future__ import annotations

from pathlib import Path
import warnings
from typing import cast

import pytest

from consist.core.coupler import (
    Coupler,
    CouplerSchemaBase,
    coupler_schema,
)
from consist.models.artifact import Artifact


def _artifact(*, key: str, uri: str = "workspace://dummy.csv") -> Artifact:
    return Artifact(key=key, uri=uri, driver="csv")


def test_coupler_set_get_update_mapping_protocol() -> None:
    coupler = Coupler()
    art_a = _artifact(key="a")
    art_b = _artifact(key="b", uri="workspace://b.csv")
    art_c = _artifact(key="c", uri="workspace://c.csv")

    assert coupler.get("a") is None
    assert "a" not in coupler

    coupler.set("a", art_a)
    assert coupler.get("a") == art_a
    assert coupler["a"] == art_a
    assert "a" in coupler

    coupler["b"] = art_b
    assert coupler.get("b") == art_b

    coupler.update({"c": art_c})
    assert coupler.get("c") == art_c

    coupler.update(d=art_a)
    assert coupler.get("d") == art_a


def test_coupler_require_raises_with_available_keys() -> None:
    coupler = Coupler()
    coupler.set("persons", _artifact(key="persons"))

    with pytest.raises(KeyError) as excinfo:
        coupler.require("households")

    msg = str(excinfo.value)
    assert "missing key" in msg.lower()
    assert "households" in msg
    assert "persons" in msg


def test_coupler_path_requires_tracker() -> None:
    coupler = Coupler()
    coupler.set("data", _artifact(key="data", uri="inputs://data.csv"))

    with pytest.raises(RuntimeError) as excinfo:
        coupler.path("data")
    assert "no tracker" in str(excinfo.value).lower()


def test_coupler_path_resolves_without_mutating_artifact() -> None:
    art = _artifact(key="data", uri="inputs://data.csv")
    coupler = Coupler()
    coupler.tracker = type(
        "TrackerStub",
        (),
        {"resolve_uri": lambda _self, uri: "/abs/data.csv"},
    )()
    coupler.set("data", art)

    assert art.abs_path is None
    resolved = coupler.path("data")
    assert resolved == Path("/abs/data.csv")
    assert art.abs_path is None


def test_coupler_declare_outputs_tracks_missing_required() -> None:
    coupler = Coupler()
    coupler.declare_outputs("zarr", "csv", required={"zarr": True, "csv": False})

    assert coupler.missing_declared_outputs() == ["zarr"]

    coupler.set("zarr", _artifact(key="zarr"))
    assert coupler.missing_declared_outputs() == []


def test_coupler_missing_declared_outputs_treats_none_as_missing() -> None:
    coupler = Coupler()
    coupler.declare_outputs("required", required=True)

    coupler.set("required", cast(Artifact, None))
    assert coupler.missing_declared_outputs() == ["required"]


def test_coupler_schema_wraps_attribute_access() -> None:
    @coupler_schema
    class WorkflowCoupler(CouplerSchemaBase):
        a: Artifact
        b: Artifact

    coupler = Coupler()
    schema = WorkflowCoupler(coupler)
    artifact = _artifact(key="a")

    schema.a = artifact
    assert schema.a == artifact
    assert coupler.require("a") == artifact


def test_coupler_warns_on_undocumented_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undocumented=True)

    with pytest.warns(UserWarning, match="undocumented"):
        coupler.set("unknown", _artifact(key="unknown"))


def test_coupler_update_warns_on_undocumented_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undocumented=True)

    with pytest.warns(UserWarning, match="undocumented"):
        coupler.update({"unknown": _artifact(key="unknown")})


def test_coupler_set_from_artifact_warns_on_undocumented_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undocumented=True)

    with pytest.warns(UserWarning, match="undocumented"):
        coupler.set_from_artifact("unknown", _artifact(key="unknown"))


def test_coupler_warns_only_once_per_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undocumented=True)
    art = _artifact(key="unknown")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        coupler.set("unknown", art)
        coupler.set("unknown", art)

    assert len(captured) == 1


def test_coupler_require_outputs_defaults_required() -> None:
    coupler = Coupler()
    coupler.require_outputs("required")

    assert coupler.missing_declared_outputs() == ["required"]


def test_coupler_set_from_artifact_with_real_artifact() -> None:
    """Test set_from_artifact with a real Artifact object."""
    coupler = Coupler()
    art = _artifact(key="data")

    result = coupler.set_from_artifact("data", art)

    assert result == art
    assert coupler.get("data") == art


def test_coupler_set_from_artifact_with_artifact_like() -> None:
    """Test set_from_artifact with an artifact-like object (has .path and .uri)."""
    from consist.core.noop import NoopArtifact

    coupler = Coupler()
    noop_art = NoopArtifact(
        key="persons",
        path=Path("workspace/persons.parquet"),
        uri="workspace://persons.parquet",
    )

    result = coupler.set_from_artifact("persons", noop_art)

    assert result == noop_art
    assert coupler.get("persons") == noop_art


def test_coupler_set_from_artifact_with_path() -> None:
    """Test set_from_artifact with a Path object."""
    coupler = Coupler()
    path = Path("workspace/data.csv")

    result = coupler.set_from_artifact("data", path)

    assert result == path
    assert coupler.get("data") == path


def test_coupler_set_from_artifact_with_string_path() -> None:
    """Test set_from_artifact with a string path."""
    coupler = Coupler()
    path_str = "workspace/data.csv"

    result = coupler.set_from_artifact("data", path_str)

    assert result == path_str
    assert coupler.get("data") == path_str


def test_coupler_set_from_artifact_works_with_schema() -> None:
    """Test that set_from_artifact works through CouplerSchemaBase."""

    @coupler_schema
    class WorkflowCoupler(CouplerSchemaBase):
        persons: Artifact

    coupler = Coupler()
    schema = WorkflowCoupler(coupler)
    art = _artifact(key="persons")

    # Should be able to call set_from_artifact on schema too
    result = schema.set_from_artifact("persons", art)

    assert result == art
    assert schema.persons == art
