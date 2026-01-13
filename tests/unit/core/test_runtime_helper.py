import pytest

import consist


def test_create_tracker_disabled_returns_noop() -> None:
    tracker = consist.create_tracker(enabled=False)
    assert isinstance(tracker, consist.NoopTracker)


def test_create_tracker_requires_factory_when_enabled() -> None:
    with pytest.raises(ValueError, match="requires tracker_factory"):
        consist.create_tracker(enabled=True)


def test_create_tracker_fallback_on_error() -> None:
    def factory():
        raise RuntimeError("boom")

    tracker = consist.create_tracker(enabled=True, tracker_factory=factory)
    assert isinstance(tracker, consist.NoopTracker)
