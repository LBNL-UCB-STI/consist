from __future__ import annotations

from consist.core.context import get_active_tracker


def test_scenario_run_step_skips_callable_on_cache_hit(tracker):
    calls: list[str] = []

    def write_output() -> None:
        calls.append("called")
        t = get_active_tracker()
        (t.run_dir / "out.txt").write_text(f"calls={len(calls)}\n")

    with tracker.scenario("scen_run_step_A") as sc:
        outs = sc.run_step(
            "produce",
            write_output,
            config={"step": "produce"},
            output_paths={"out": "out.txt"},
        )
        assert outs["out"].key == "out"
        assert "out" in sc.coupler

    assert calls == ["called"]
    assert (tracker.run_dir / "out.txt").read_text() == "calls=1\n"

    with tracker.scenario("scen_run_step_B") as sc:
        outs = sc.run_step(
            "produce",
            write_output,
            config={"step": "produce"},
            output_paths={"out": "out.txt"},
        )
        assert outs["out"].key == "out"
        assert "out" in sc.coupler

    # Cache hit should skip the callable, so the file should not be overwritten.
    assert calls == ["called"]
    assert (tracker.run_dir / "out.txt").read_text() == "calls=1\n"
