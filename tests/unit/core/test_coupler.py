from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import warnings
from typing import cast

import pytest

from consist.artifact import Artifact
from consist.core.coupler import Coupler
from consist.digest import Kind
from tests.helpers.artifact_factory import make_artifact


@dataclass(frozen=True)
class _DummyArtifactLike:
    key: str
    path: Path
    container_uri: str


def _artifact(*, key: str, container_uri: str = "workspace://dummy.csv") -> Artifact:
    return make_artifact(key=key, container_uri=container_uri, driver="csv")


def test_coupler_set_get_update_mapping_protocol() -> None:
    coupler = Coupler()
    art_a = _artifact(key="a")
    art_b = _artifact(key="b", container_uri="workspace://b.csv")
    art_c = _artifact(key="c", container_uri="workspace://c.csv")

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
    coupler.set("data", _artifact(key="data", container_uri="inputs://data.csv"))

    with pytest.raises(RuntimeError) as excinfo:
        coupler.path("data")
    assert "no tracker" in str(excinfo.value).lower()


def test_coupler_path_resolves_without_mutating_artifact() -> None:
    art = _artifact(key="data", container_uri="inputs://data.csv")
    coupler = Coupler()
    coupler.tracker = type(
        "TrackerStub",
        (),
        {"resolve_uri": lambda _self, uri: "/abs/data.csv"},
    )()
    coupler.set("data", art)

    assert art.locate() is None
    resolved = coupler.path("data")
    assert resolved == Path("/abs/data.csv")
    assert art.locate() is None


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


def test_coupler_collect_by_keys_updates_coupler() -> None:
    coupler = Coupler()
    outputs = {"a": _artifact(key="a"), "b": _artifact(key="b")}

    collected = coupler.collect_by_keys(outputs, "a", prefix="2024_")

    assert "2024_a" in coupler
    assert collected == {"2024_a": outputs["a"]}


def test_coupler_warns_on_undefined_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undefined=True)

    with pytest.warns(UserWarning, match="undefined"):
        coupler.set("unknown", _artifact(key="unknown"))


def test_coupler_update_warns_on_undefined_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undefined=True)

    with pytest.warns(UserWarning, match="undefined"):
        coupler.update({"unknown": _artifact(key="unknown")})


def test_coupler_set_from_artifact_warns_on_undefined_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undefined=True)

    with pytest.warns(UserWarning, match="undefined"):
        coupler.set_from_artifact("unknown", _artifact(key="unknown"))


def test_coupler_warns_only_once_per_key() -> None:
    coupler = Coupler()
    coupler.require_outputs("known", warn_undefined=True)
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
    """Test set_from_artifact with an artifact-like object (has .path and .container_uri)."""
    coupler = Coupler()
    dummy_art = _DummyArtifactLike(
        key="persons",
        path=Path("workspace/persons.parquet"),
        container_uri="workspace://persons.parquet",
    )

    result = coupler.set_from_artifact("persons", dummy_art)

    assert result == dummy_art
    assert coupler.get("persons") == dummy_art


def test_coupler_set_from_artifact_with_path() -> None:
    """Test set_from_artifact with a Path object wraps in a Kind.HISTORICAL Artifact."""
    coupler = Coupler()
    path = Path("workspace/data.csv")

    result = coupler.set_from_artifact("data", path)

    assert isinstance(result, Artifact)
    assert result.anchor.kind is Kind.HISTORICAL
    assert coupler.get("data") == result


def test_coupler_set_from_artifact_with_string_path() -> None:
    """Test set_from_artifact with a string path wraps in a Kind.HISTORICAL Artifact."""
    coupler = Coupler()
    path_str = "workspace/data.csv"

    result = coupler.set_from_artifact("data", path_str)

    assert isinstance(result, Artifact)
    assert result.anchor.kind is Kind.HISTORICAL
    assert coupler.get("data") == result


def test_coupler_view_sets_namespaced_keys_and_preserves_global_access() -> None:
    coupler = Coupler()
    beam = coupler.view("beam")
    art = _artifact(key="plans_in")

    beam.set("plans_in", art)

    assert beam.require("plans_in") == art
    assert coupler.require("beam/plans_in") == art
    assert "plans_in" in beam
    assert "beam/plans_in" in coupler
    assert set(beam.keys()) == {"plans_in"}
    assert dict(beam.items()) == {"plans_in": art}


def test_coupler_view_nested_namespace_is_composable() -> None:
    coupler = Coupler()
    nested = coupler.view("/beam/").view("postprocess")
    art = _artifact(key="linkstats")

    nested["linkstats"] = art

    assert nested.qualify("linkstats") == "beam/postprocess/linkstats"
    assert coupler.require("beam/postprocess/linkstats") == art
    assert nested.require("linkstats") == art


def test_coupler_view_require_outputs_tracks_namespaced_contracts() -> None:
    coupler = Coupler()
    beam = coupler.view("beam")

    beam.require_outputs("plans")

    assert coupler.missing_declared_outputs() == ["beam/plans"]
    assert beam.missing_declared_outputs() == ["plans"]

    beam.set("plans", _artifact(key="plans"))

    assert coupler.missing_declared_outputs() == []
    assert beam.missing_declared_outputs() == []


def test_coupler_view_path_resolves_tracker() -> None:
    coupler = Coupler()
    coupler.tracker = type(
        "TrackerStub",
        (),
        {"resolve_uri": lambda _self, uri: "/abs/beam/data.csv"},
    )()
    beam = coupler.view("beam")
    beam.set("data", _artifact(key="data", container_uri="inputs://beam/data.csv"))

    assert beam.path("data") == Path("/abs/beam/data.csv")


def test_coupler_view_rejects_empty_namespace() -> None:
    coupler = Coupler()

    with pytest.raises(ValueError, match="cannot be empty"):
        coupler.view("///")


def test_coupler_pop() -> None:
    coupler = Coupler()
    art_a = _artifact(key="a")
    coupler.set("a", art_a)

    # Pop existing
    popped = coupler.pop("a")
    assert popped == art_a
    assert "a" not in coupler
    assert coupler.get("a") is None

    # Pop missing returns default (None)
    assert coupler.pop("a") is None
    sentinel = _artifact(key="sentinel")
    assert coupler.pop("missing", sentinel) == sentinel

    # Invalid key triggers validation
    with pytest.raises(ValueError):
        coupler.pop("")
    with pytest.raises(ValueError):
        coupler.pop("   ")


def test_coupler_view_pop() -> None:
    coupler = Coupler()
    beam = coupler.view("beam")
    plans = _artifact(key="plans")
    beam.set("plans", plans)

    assert "beam/plans" in coupler
    assert "plans" in beam

    popped = beam.pop("plans")
    assert popped == plans
    assert "plans" not in beam
    assert "beam/plans" not in coupler

    # Pop missing returns default
    assert beam.pop("plans") is None
    default_art = _artifact(key="default")
    assert beam.pop("missing", default_art) == default_art

    # Invalid key triggers validation
    with pytest.raises(ValueError):
        beam.pop("")


def test_coupler_path_supports_path_string_and_artifact_like() -> None:
    coupler = Coupler()
    coupler.set_from_artifact("as_path", Path("workspace/data.csv"))
    coupler.set_from_artifact("as_str", "workspace/other.csv")
    coupler.set_from_artifact(
        "as_dummy",
        _DummyArtifactLike(
            key="as_dummy",
            path=Path("workspace/persons.parquet"),
            container_uri="workspace://persons.parquet",
        ),
    )

    assert coupler.path("as_path") == Path("workspace/data.csv")
    assert coupler.path("as_str") == Path("workspace/other.csv")
    assert coupler.path("as_dummy") == Path("workspace/persons.parquet")

    coupler.tracker = type(
        "TrackerStub",
        (),
        {"resolve_uri": lambda _self, uri: f"/resolved/{uri.split('://', 1)[1]}"},
    )()
    coupler.set_from_artifact("as_uri_str", "inputs://table.parquet")
    assert coupler.path("as_uri_str") == Path("/resolved/table.parquet")
