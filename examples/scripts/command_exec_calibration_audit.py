"""Showcase: Tracking Black-Box CLI Commands with `tracker.exec` / `consist exec`.

This example demonstrates how Consist supervises external CLI commands
and non-Python pipelines:
1. Previewing boundaries and discovering undeclared inputs with `dry_run=True`.
2. Running baseline calibrations with automatic `--outputs-dir` artifact scanning.
3. Detecting parameter drift via automatic baseline diffs (`baseline_comparison`).
4. Catching the "silent-success trap" when a tool exits 0 without writing outputs.

Run from repo root:
    uv run python examples/scripts/command_exec_calibration_audit.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from consist import Tracker

console = Console()
EXAMPLE_ROOT = Path("examples/runs/command_exec_audit")


def _write_mock_simulator_cli(script_path: Path) -> None:
    """Create a tiny standalone CLI script representing a legacy simulator binary."""
    script_path.write_text(
        """#!/usr/bin/env python3
import argparse, csv, json, sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--zones", required=True, type=Path)
parser.add_argument("--params", required=True, type=Path)
parser.add_argument("--out-dir", required=True, type=Path)
parser.add_argument("--skip-flows", action="store_true")
args = parser.parse_args()

params = json.loads(args.params.read_text())
alpha = float(params["gravity_alpha"])
mode_bias = float(params["transit_bias"])

with args.zones.open() as f:
    zones = list(csv.DictReader(f))

args.out_dir.mkdir(parents=True, exist_ok=True)

# 1. Write deterministic convergence report (depends only on zones count)
(args.out_dir / "convergence.txt").write_text(
    f"converged=true\\nzones_processed={len(zones)}\\n"
)

# 2. Write transit flows (unless --skip-flows simulates a buggy silent exit 0)
if not args.skip_flows:
    flows_path = args.out_dir / "corridor_flows.csv"
    with flows_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["zone_id", "transit_trips"])
        for row in zones:
            pop = float(row["population"])
            dist = float(row["distance_km"])
            trips = round((pop * mode_bias) / (dist ** alpha), 2)
            writer.writerow([row["zone_id"], trips])

print(f"[sim_cli] Completed simulation for {len(zones)} zones (alpha={alpha})")
""",
        encoding="utf-8",
    )


def main() -> None:
    if EXAMPLE_ROOT.exists():
        shutil.rmtree(EXAMPLE_ROOT)
    EXAMPLE_ROOT.mkdir(parents=True, exist_ok=True)

    sim_cli = EXAMPLE_ROOT / "sim_cli.py"
    _write_mock_simulator_cli(sim_cli)

    zones_csv = EXAMPLE_ROOT / "zones.csv"
    zones_csv.write_text(
        "zone_id,population,distance_km\nZ1,1200,2.5\nZ2,3400,5.0\nZ3,850,1.2\n",
        encoding="utf-8",
    )

    params_json = EXAMPLE_ROOT / "calibration.json"
    params_json.write_text(
        json.dumps({"gravity_alpha": 1.2, "transit_bias": 0.35}, indent=2),
        encoding="utf-8",
    )

    out_dir = EXAMPLE_ROOT / "outputs"
    tracker = Tracker(
        run_dir=EXAMPLE_ROOT / ".consist" / "runs",
        db_path=EXAMPLE_ROOT / ".consist" / "provenance.duckdb",
    )

    cmd = [
        sys.executable,
        str(sim_cli),
        "--zones",
        str(zones_csv),
        "--params",
        str(params_json),
        "--out-dir",
        str(out_dir),
    ]

    # -------------------------------------------------------------------------
    # Act 1: Dry-Run & Automatic Input Suggestion
    # -------------------------------------------------------------------------
    console.print(Panel("[bold cyan]Act 1: Dry-Run & Input Discovery[/bold cyan]"))
    dry = tracker.exec(
        name="corridor_calibration",
        argv=cmd,
        inputs={"params": params_json},  # Intentionally omitted --zones
        outputs={"flows": out_dir / "corridor_flows.csv"},
        dry_run=True,
    )
    assert dry.plan is not None
    console.print("Declared inputs:", list(dry.plan["inputs"].keys()))
    console.print(
        "Suggested (undeclared) inputs found in argv:",
        dry.plan["suggested_inputs"],
    )

    # -------------------------------------------------------------------------
    # Act 2: Baseline Execution (with accept_suggested + outputs_dir)
    # -------------------------------------------------------------------------
    console.print(Panel("[bold green]Act 2: Baseline Calibration Run[/bold green]"))
    run1 = tracker.exec(
        name="corridor_calibration",
        argv=cmd,
        inputs={"params": params_json},
        outputs={"flows": out_dir / "corridor_flows.csv"},
        outputs_dir=out_dir,  # Automatically registers convergence.txt
        accept_suggested=True,
    )
    console.print(f"Run 1 status: {run1.run.status} (exit={run1.exit_code})")
    console.print(f"Logged inputs:  {list(run1.inputs.keys())}")
    console.print(f"Logged outputs: {list(run1.outputs.keys())}")

    # -------------------------------------------------------------------------
    # Act 3: Parameter Drift & Automatic Baseline Diff
    # -------------------------------------------------------------------------
    console.print(Panel("[bold yellow]Act 3: Detecting Parameter Drift[/bold yellow]"))
    params_json.write_text(
        json.dumps({"gravity_alpha": 1.45, "transit_bias": 0.35}, indent=2),
        encoding="utf-8",
    )
    run2 = tracker.exec(
        name="corridor_calibration",
        argv=cmd,
        inputs={"params": params_json},
        outputs={"flows": out_dir / "corridor_flows.csv"},
        outputs_dir=out_dir,
        accept_suggested=True,
    )
    assert run2.run is not None
    diff = run2.run.meta["command"]["baseline_comparison"]
    console.print(f"Compared against baseline run: {diff['baseline_run_id']}")
    console.print(f"Inputs modified:   {diff['inputs']['modified']}")
    console.print(f"Inputs unchanged:  {diff['inputs']['unchanged']}")
    console.print(f"Outputs modified:  {diff['outputs']['modified']}")
    console.print(f"Outputs unchanged: {diff['outputs']['unchanged']}")

    # -------------------------------------------------------------------------
    # Act 4: Catching a "Silent Success" (Exit 0 but Missing Output)
    # -------------------------------------------------------------------------
    console.print(Panel("[bold red]Act 4: Catching Silent Missing Outputs[/bold red]"))
    missing_target = out_dir / "missing_report.csv"
    run3 = tracker.exec(
        name="corridor_calibration",
        argv=[*cmd, "--skip-flows"],
        inputs={"params": params_json},
        outputs={"missing_report": missing_target},
        accept_suggested=True,
    )
    assert run3.run is not None
    console.print(
        f"Process exited 0, but Consist marked status={run3.run.status!r}, "
        f"exit_code={run3.exit_code}, error={run3.error!r}"
    )


if __name__ == "__main__":
    main()
