import pytest
from pathlib import Path

from consist.core.tracker import Tracker
from consist.integrations.containers.api import run_container
from consist.integrations.containers.backends import DockerBackend


def test_container_cache_reuse_and_miss_for_new_outputs(
    tracker: Tracker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """
    Uses the public run_container API and a mocked backend to verify:
    - First execution runs.
    - Second execution with same signature and outputs reuses cached artifacts (no execution).
    - Third execution with additional outputs re-executes.
    """
    exec_counter = {"count": 0}

    def fake_resolve(self, image: str):
        return "sha256:dummy"

    def fake_run(self, image, command, volumes, env, working_dir):
        exec_counter["count"] += 1
        return True

    def fake_init(self, pull_latest: bool = False):
        self.pull_latest = pull_latest
        self.client = None

    monkeypatch.setattr(DockerBackend, "__init__", fake_init, raising=False)
    monkeypatch.setattr(
        DockerBackend, "resolve_image_digest", fake_resolve, raising=False
    )
    monkeypatch.setattr(DockerBackend, "run", fake_run, raising=False)

    base_out = tracker.run_dir / "out_table.txt"
    extra_out = tracker.run_dir / "out_extra.txt"

    # Ensure files exist so artifact logging succeeds
    base_out.write_text("hello")
    extra_out.write_text("extra")

    # First run executes
    result1 = run_container(
        tracker=tracker,
        run_id="container_run_1",
        image="alpine:3.18",
        command=["echo", "hi"],
        volumes={},  # no mounts
        inputs=[],
        outputs=[str(base_out)],
        environment={},
        working_dir=None,
        backend_type="docker",
        pull_latest=False,
        strict_mounts=False,
    )
    assert exec_counter["count"] == 1
    assert result1.cache_hit is False

    # Second run should reuse cache (no new execution)
    result2 = run_container(
        tracker=tracker,
        run_id="container_run_2",
        image="alpine:3.18",
        command=["echo", "hi"],
        volumes={},
        inputs=[],
        outputs=[str(base_out)],
        environment={},
        working_dir=None,
        backend_type="docker",
        pull_latest=False,
        strict_mounts=False,
    )
    assert exec_counter["count"] == 1  # no increment on cache hit
    assert result2.cache_hit is True

    run2 = tracker.get_run("container_run_2")
    assert run2.meta.get("cache_hit") is True
    assert run2.meta.get("cache_source") == "container_run_1"
    assert run2.meta.get("materialized_outputs") is not None

    # Third run asks for additional output -> should execute again
    result3 = run_container(
        tracker=tracker,
        run_id="container_run_3",
        image="alpine:3.18",
        command=["echo", "hi"],
        volumes={},
        inputs=[],
        outputs=[str(base_out), str(extra_out)],
        environment={},
        working_dir=None,
        backend_type="docker",
        pull_latest=False,
        strict_mounts=False,
    )
    assert exec_counter["count"] == 2
    assert result3.cache_hit is False

    run3 = tracker.get_run("container_run_3")
    assert run3.meta.get("cache_hit") is False
    assert run3.meta.get("declared_outputs") == [str(base_out), str(extra_out)]
