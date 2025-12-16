from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.coupler import Coupler
from consist.models.artifact import Artifact


class FakeTracker:
    def __init__(
        self,
        *,
        cached_outputs: dict[str, Artifact] | None = None,
        resolved_by_uri: dict[str, str] | None = None,
    ) -> None:
        self._cached_outputs = cached_outputs or {}
        self._resolved_by_uri = resolved_by_uri or {}

    def cached_output(self, key: str | None = None) -> Artifact | None:
        if not self._cached_outputs:
            return None
        if key is None:
            return next(iter(self._cached_outputs.values()))
        return self._cached_outputs.get(key)

    def resolve_uri(self, uri: str) -> str:
        return self._resolved_by_uri.get(uri, uri)


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
    tracker = FakeTracker(resolved_by_uri={"inputs://data.csv": "/abs/data.csv"})
    coupler = Coupler(tracker)
    coupler.set("data", art)

    assert art.abs_path is None
    resolved = coupler.path("data")
    assert resolved == Path("/abs/data.csv")
    assert art.abs_path is None


def test_coupler_adopt_cached_output_sets_state() -> None:
    cached_persons = _artifact(key="persons", uri="workspace://persons.parquet")
    tracker = FakeTracker(cached_outputs={"persons": cached_persons})
    coupler = Coupler(tracker)

    assert coupler.get("persons") is None
    adopted = coupler.adopt_cached_output("persons")
    assert adopted == cached_persons
    assert coupler.get("persons") == cached_persons


def test_coupler_cached_aliases_delegate_to_adopt_cached_output() -> None:
    cached = _artifact(key="persons", uri="workspace://persons.parquet")
    tracker = FakeTracker(cached_outputs={"persons": cached})
    coupler = Coupler(tracker)

    adopted = coupler.get_cached("persons")
    assert adopted == cached
    assert coupler.get("persons") == cached

    coupler.pop("persons")
    adopted2 = coupler.get_cached_output("persons")
    assert adopted2 == cached
    assert coupler.get("persons") == cached


def test_coupler_adopt_cached_output_without_tracker_is_noop() -> None:
    coupler = Coupler()
    assert coupler.adopt_cached_output("anything") is None
