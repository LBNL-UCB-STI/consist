from __future__ import annotations

import csv
import shutil
from pathlib import Path

import consist
from consist import ExecutionOptions, Tracker


HERE = Path(__file__).resolve().parent
GENERATED_DIR = HERE / ".generated"
RUN_DIR = GENERATED_DIR / "runs"
DB_PATH = GENERATED_DIR / "provenance.duckdb"


def clean_data() -> Path:
    out = consist.output_path("cleaned", ext="csv")
    rows = [
        {"value": 9, "status": "kept"},
        {"value": 16, "status": "kept"},
    ]
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["value", "status"])
        writer.writeheader()
        writer.writerows(rows)
    return out


def summarize(cleaned_path: Path) -> Path:
    with cleaned_path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    values = [int(row["value"]) for row in rows]
    mean = sum(values) / len(values)

    out = consist.output_path("summary", ext="txt")
    out.write_text(f"rows={len(rows)}\nmean={mean:.1f}\n")
    return out


def main() -> None:
    if GENERATED_DIR.exists():
        shutil.rmtree(GENERATED_DIR)
    RUN_DIR.mkdir(parents=True)

    tracker = Tracker(run_dir=RUN_DIR, db_path=DB_PATH)
    clean_result = tracker.run(
        fn=clean_data,
        name="clean_data",
        model="clean_data",
        run_id="demo_clean_data",
        config={"threshold": 8},
        outputs=["cleaned"],
    )
    tracker.run(
        fn=clean_data,
        name="clean_data",
        model="clean_data",
        run_id="demo_clean_data_repeat",
        config={"threshold": 8, "repeat": True},
        outputs=["cleaned"],
    )
    tracker.run(
        fn=summarize,
        name="summarize",
        model="summarize",
        run_id="demo_summarize",
        config={"source": "cleaned"},
        inputs={"cleaned_path": consist.ref(clean_result, key="cleaned")},
        outputs=["summary"],
        execution_options=ExecutionOptions(input_binding="paths"),
    )


if __name__ == "__main__":
    main()
