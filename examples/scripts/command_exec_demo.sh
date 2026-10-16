#!/usr/bin/env bash
# Showcase: Language-Agnostic Provenance with `consist init` & `consist exec`
#
# Run from repo root:
#   bash examples/scripts/command_exec_demo.sh
# or:
#   uv run bash examples/scripts/command_exec_demo.sh

set -euo pipefail

if command -v consist >/dev/null 2>&1; then
  CONSIST_CMD="consist"
else
  CONSIST_CMD="python -m consist.cli"
fi

DEMO_DIR="examples/runs/command_exec_demo"
rm -rf "$DEMO_DIR"
mkdir -p "$DEMO_DIR/inputs" "$DEMO_DIR/outputs"
cd "$DEMO_DIR"

# ---------------------------------------------------------------------------
# 0. Create a legacy POSIX shell + awk gravity model (no Python anywhere!)
# ---------------------------------------------------------------------------
cat << 'EOF' > run_gravity_model.sh
#!/usr/bin/env sh
set -eu

PARAMS_ENV="$1"
ZONES_CSV="$2"
OUT_DIR="$3"
SKIP_FLOWS="${4:-false}"

# Source calibration coefficients (ALPHA, TRANSIT_BIAS)
. "$PARAMS_ENV"

mkdir -p "$OUT_DIR"

# 1. Write solver convergence log (depends only on zone count)
ZONE_COUNT=$(awk 'NR > 1 {count++} END {print count+0}' "$ZONES_CSV")
printf "converged=true\nzones_processed=%s\n" "$ZONE_COUNT" > "$OUT_DIR/convergence.log"

# 2. Compute corridor transit flows using awk
if [ "$SKIP_FLOWS" != "--skip-flows" ]; then
  awk -F',' -v alpha="$ALPHA" -v bias="$TRANSIT_BIAS" '
    BEGIN { OFS=","; print "zone_id,transit_trips" }
    NR > 1 {
      trips = ($2 * bias) / ($3 ^ alpha)
      printf "%s,%.2f\n", $1, trips
    }
  ' "$ZONES_CSV" > "$OUT_DIR/corridor_flows.csv"
fi

echo "[gravity_model.sh] Solved $ZONE_COUNT zones (ALPHA=$ALPHA, TRANSIT_BIAS=$TRANSIT_BIAS)"
EOF
chmod +x run_gravity_model.sh

cat << 'EOF' > inputs/zones.csv
zone_id,population,distance_km
Z101,1200,2.5
Z102,3400,5.0
Z103,850,1.2
EOF

cat << 'EOF' > inputs/calibration.env
ALPHA=1.20
TRANSIT_BIAS=0.35
EOF

# ---------------------------------------------------------------------------
# 1. Initialize Consist in this workspace
# ---------------------------------------------------------------------------
echo "=== 1. Initializing Consist workspace ==="
$CONSIST_CMD init

# ---------------------------------------------------------------------------
# 2. Dry-Run: Let Consist inspect the command and suggest undeclared inputs
# ---------------------------------------------------------------------------
echo ""
echo "=== 2. Dry-Run: Previewing boundary & suggested inputs ==="
# Notice we only declared `params`, omitting `inputs/zones.csv` on the CLI:
$CONSIST_CMD exec \
  --name gravity_calibration \
  --input params=inputs/calibration.env \
  --output flows=outputs/corridor_flows.csv \
  --dry-run \
  -- ./run_gravity_model.sh inputs/calibration.env inputs/zones.csv outputs

# ---------------------------------------------------------------------------
# 3. Baseline Run: Accept suggested inputs + watch `outputs/` directory
# ---------------------------------------------------------------------------
echo ""
echo "=== 3. Running Baseline Calibration ==="
$CONSIST_CMD exec \
  --name gravity_calibration \
  --input params=inputs/calibration.env \
  --output flows=outputs/corridor_flows.csv \
  --outputs-dir outputs \
  --accept-suggested \
  -- ./run_gravity_model.sh inputs/calibration.env inputs/zones.csv outputs

# ---------------------------------------------------------------------------
# 4. Parameter Drift: Tweak `calibration.env` and re-run
# ---------------------------------------------------------------------------
echo ""
echo "=== 4. Tweaking ALPHA=1.45 in calibration.env and re-running ==="
cat << 'EOF' > inputs/calibration.env
ALPHA=1.45
TRANSIT_BIAS=0.35
EOF

# Consist automatically diffs against Run #1:
# - inputs_modified:   [params]
# - inputs_unchanged:  [zones]
# - outputs_modified:  [flows]
# - outputs_unchanged: [convergence]
$CONSIST_CMD exec \
  --name gravity_calibration \
  --input params=inputs/calibration.env \
  --output flows=outputs/corridor_flows.csv \
  --outputs-dir outputs \
  --accept-suggested \
  -- ./run_gravity_model.sh inputs/calibration.env inputs/zones.csv outputs

# ---------------------------------------------------------------------------
# 5. Inspect recorded runs & artifacts using standard Consist CLI
# ---------------------------------------------------------------------------
echo ""
echo "=== 5. Querying Provenance DB with \`consist runs\` & \`consist preview\` ==="
$CONSIST_CMD runs --limit 5
$CONSIST_CMD preview flows --rows 5
