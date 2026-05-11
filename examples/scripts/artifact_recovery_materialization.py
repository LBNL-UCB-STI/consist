from pathlib import Path
import shutil

from consist import Tracker, output_path


def main() -> None:
    base_dir = Path("examples/runs/artifact_recovery_materialization")
    if base_dir.exists():
        shutil.rmtree(base_dir)

    tracker = Tracker(
        run_dir=base_dir / "workspace",
        db_path=base_dir / "consist.duckdb",
        project_root=".",
        allow_external_paths=True,
    )

    with tracker.trace("produce_summary", run_id="source_run"):
        summary = output_path("outputs/summary", ext="csv")
        summary.write_text("metric,value\nvmt,12345\n", encoding="utf-8")
        tracker.log_output(summary, key="summary")

    artifact = tracker.get_run_outputs("source_run")["summary"]

    archive_root = base_dir / "archive"
    tracker.archive_artifact(artifact, archive_root, mode="copy")

    original_path = artifact.as_path()
    original_path.unlink()

    fresh_workspace = base_dir / "fresh_workspace"
    result = tracker.materialize_artifact(
        artifact,
        target_root=fresh_workspace,
        preserve_existing=True,
    )

    print(f"status: {result.status}")
    print(f"path: {result.path}")
    print(f"artifact.as_path(): {result.artifact.as_path()}")


if __name__ == "__main__":
    main()
