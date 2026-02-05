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


def test_define_step_name_template_and_callable_metadata(tracker: Tracker) -> None:
    @tracker.define_step(
        name_template="{func_name}__y{year}__phase_{phase}",
        tags=lambda ctx: [f"phase:{ctx.phase}"],
        description=lambda ctx: f"year:{ctx.year}",
    )
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.run_dir / "out.txt"
        out_path.write_text("ok")

    tracker.run(
        fn=step,
        year=2030,
        phase="demand",
        output_paths={"out": "out.txt"},
        inject_context="ctx",
    )

    run = tracker.last_run.run
    assert run.model_name == "step__y2030__phase_demand"
    assert run.tags == ["phase:demand"]
    assert run.description == "year:2030"
