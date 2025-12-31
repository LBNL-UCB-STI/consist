from consist.core.tracker import Tracker


def test_define_step_metadata_applied(tracker: Tracker) -> None:
    @tracker.define_step(outputs=["out"], tags=["demo"], description="demo step")
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    result = tracker.run(
        fn=step,
        output_paths={"out": "out.txt"},
        inject_context="ctx",
    )

    assert "out" in result.outputs
    assert result.outputs["out"].path.exists()
    assert "demo" in tracker.last_run.run.tags
    assert tracker.last_run.run.description == "demo step"
