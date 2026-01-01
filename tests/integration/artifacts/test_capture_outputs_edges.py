from pathlib import Path


def test_capture_outputs_missing_directory_is_noop(tracker, tmp_path: Path) -> None:
    """
    Missing capture directories should be created and result in no artifacts.
    """
    missing_dir = tmp_path / "missing_outputs"
    with tracker.start_run("missing_dir", model="model"):
        with tracker.capture_outputs(missing_dir, pattern="*.csv") as capture:
            pass

    assert missing_dir.exists()
    assert capture.artifacts == []


def test_capture_outputs_recursive_pattern_filters(tracker, tmp_path: Path) -> None:
    """
    Recursive capture should include nested matches and ignore unrelated files.
    """
    output_root = tmp_path / "outputs"
    nested_dir = output_root / "nested"
    nested_dir.mkdir(parents=True, exist_ok=True)
    existing = output_root / "existing.csv"
    existing.write_text("id,val\n1,2\n", encoding="utf-8")

    with tracker.start_run("recursive_capture", model="model"):
        with tracker.capture_outputs(
            output_root, pattern="*.csv", recursive=True
        ) as capture:
            nested_csv = nested_dir / "nested.csv"
            nested_csv.write_text("id,val\n3,4\n", encoding="utf-8")
            ignored = nested_dir / "ignored.txt"
            ignored.write_text("ignore", encoding="utf-8")

        captured_keys = {art.key for art in capture.artifacts}
        captured_paths = {
            Path(art.abs_path or art.uri).name for art in capture.artifacts
        }

    assert "nested" in captured_keys
    assert "nested.csv" in captured_paths
    assert "ignored.txt" not in captured_paths
