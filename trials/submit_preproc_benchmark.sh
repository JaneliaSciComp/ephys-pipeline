#!/usr/bin/env bash
# Submit a preprocessing-speed benchmark to the LSF cluster.
#
# Runs trials/preproc_speed_benchmark.py which times two approaches:
#   A — two-pass: detect_saturation_periods + remove_saturation_artifacts
#   B — single-pass: SaturationArtifactRemover (lazy preprocessor)
#
# Usage:
#   ./trials/submit_preproc_benchmark.sh <day_dir> <probe> <shank_num> <config.yaml>
#
# Example:
#   ./trials/submit_preproc_benchmark.sh /data/2026_03_05 a 0 \
#       trials/runs/preproc_speed_test.yaml
#
# Environment overrides:
#   BENCH_PYTHON    path to python binary (bypasses apptainer container)
#   SPIKENV_SIF     container image (default: shared spikenv411.sif)
#   BENCH_QUEUE     LSF queue (default: normal — CPU only, no GPU needed)
#   BENCH_CORES     CPU cores (default: 12)
#   BENCH_WALLTIME  HH:MM (default: 3:00)
#   CLUSTER_HOST    SSH host when bsub is not available locally

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_dir> <probe> <shank_num> <config.yaml>
  probe     : a | b
  shank_num : 0 | 1 | 2 | 3
EOF
}

if [ $# -ne 4 ]; then
  usage >&2; exit 1
fi

DAY_DIR="$1"
PROBE="$2"
SHANK_NUM="$3"

if [ -d "$(dirname "$4")" ]; then
  CONFIG_YAML="$(cd "$(dirname "$4")" && pwd)/$(basename "$4")"
else
  CONFIG_YAML="$4"
fi

if [[ ! "$PROBE" =~ ^[ab]$ ]]; then
  echo "ERROR: probe must be 'a' or 'b'" >&2; exit 2
fi
if [[ ! "$SHANK_NUM" =~ ^[0123]$ ]]; then
  echo "ERROR: shank_num must be 0–3" >&2; exit 2
fi
if [ ! -f "$CONFIG_YAML" ]; then
  echo "ERROR: config file not found: $CONFIG_YAML" >&2; exit 2
fi

# ── Resolve paths ──────────────────────────────────────────────────────────────
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SHARED_SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
: "${SPIKENV_SIF:="$SHARED_SCRIPT_DIR/containers/spikenv411.sif"}"
: "${LOCAL_CODE_DIR:="$(cd "$(dirname "$0")/.." && pwd)"}"
: "${SYNC_CODE:=1}"

user="${USER:-$(whoami)}"
STAGED_SCRIPT_DIR="/groups/voigts/voigtslab/${user}/ephys-pipeline-dev"

# ── Remote fallback: SSH + rsync when bsub is not available locally ────────────
: "${CLUSTER_HOST:=}"

if ! command -v bsub >/dev/null 2>&1; then
  if [ -z "$CLUSTER_HOST" ]; then
    echo "ERROR: bsub not found and CLUSTER_HOST is not set." >&2
    echo "  e.g.: export CLUSTER_HOST=login1.cluster.hhmi.org" >&2
    exit 2
  fi

  if command -v rsync >/dev/null 2>&1; then
    echo "Syncing code: $LOCAL_CODE_DIR → ${CLUSTER_HOST}:${STAGED_SCRIPT_DIR}"
    ssh "$CLUSTER_HOST" "mkdir -p '$STAGED_SCRIPT_DIR'"
    rsync -a --delete --ignore-errors \
      --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
      --exclude='.nfs*' --exclude='output' --exclude='*.sif' \
      --exclude='*.bin' --exclude='*.npy' \
      "$LOCAL_CODE_DIR/" "${CLUSTER_HOST}:${STAGED_SCRIPT_DIR}/" || true
    echo "Sync done."
  fi

  CONFIG_REL="${CONFIG_YAML#$LOCAL_CODE_DIR/}"
  CLUSTER_CONFIG="$STAGED_SCRIPT_DIR/$CONFIG_REL"

  echo "Submitting via SSH to $CLUSTER_HOST"
  exec ssh "$CLUSTER_HOST" \
    SYNC_CODE=0 \
    CLUSTER_EMAIL="${CLUSTER_EMAIL:-}" \
    BENCH_QUEUE="${BENCH_QUEUE:-normal}" \
    BENCH_CORES="${BENCH_CORES:-12}" \
    BENCH_WALLTIME="${BENCH_WALLTIME:-3:00}" \
    SPIKENV_SIF="${SPIKENV_SIF:-}" \
    BENCH_PYTHON="${BENCH_PYTHON:-}" \
    bash "$STAGED_SCRIPT_DIR/trials/submit_preproc_benchmark.sh" \
      "$DAY_DIR" "$PROBE" "$SHANK_NUM" "$CLUSTER_CONFIG"
fi

# ── On cluster: pick script dir ───────────────────────────────────────────────
if [ "${SYNC_CODE}" = "0" ]; then
  SCRIPT_DIR="$STAGED_SCRIPT_DIR"
else
  SCRIPT_DIR="$SHARED_SCRIPT_DIR"
fi

# ── Read config ────────────────────────────────────────────────────────────────
_py3() { python3 -c "$1" 2>/dev/null || true; }

TRIAL_NAME="$(_py3 "import yaml; print(yaml.safe_load(open('$CONFIG_YAML'))['trial_name'])")"
DURATION_H="$(_py3 "import yaml; print(yaml.safe_load(open('$CONFIG_YAML')).get('duration_hours', 1.0))")"
MARGIN_MS="$(_py3  "import yaml; print(yaml.safe_load(open('$CONFIG_YAML')).get('margin_ms', 500.0))")"
THRESHOLD="$(_py3  "import yaml; print(yaml.safe_load(open('$CONFIG_YAML')).get('abs_threshold', 1500))")"
N_JOBS="$(_py3     "import yaml; print(yaml.safe_load(open('$CONFIG_YAML')).get('n_jobs', 12))")"
OUTPUT_DIR="$(_py3 "import yaml; print(yaml.safe_load(open('$CONFIG_YAML')).get('output_dir', ''))")"

if [ -z "$TRIAL_NAME" ]; then
  echo "ERROR: could not read trial_name from $CONFIG_YAML" >&2; exit 2
fi

if [ -z "$OUTPUT_DIR" ]; then
  OUTPUT_DIR="$DAY_DIR/output/${PROBE}/shank_${SHANK_NUM}/preproc_speed/${TRIAL_NAME}"
fi

mkdir -p "$OUTPUT_DIR"

: "${BENCH_QUEUE:=normal}"
: "${BENCH_CORES:=12}"
: "${BENCH_WALLTIME:=3:00}"

if [ -n "${BENCH_PYTHON:-}" ]; then
  PYTHON_CMD="$BENCH_PYTHON"
else
  PYTHON_CMD="apptainer exec --bind /groups $SPIKENV_SIF python"
fi

email="${CLUSTER_EMAIL:-${user}@${CLUSTER_DOMAIN:-}}"
JOB_NAME="preproc_speed_${TRIAL_NAME}_${PROBE}${SHANK_NUM}"

echo "Submitting: $JOB_NAME"
echo "  Day      : $DAY_DIR"
echo "  Probe    : $PROBE  shank $SHANK_NUM"
echo "  Duration : ${DURATION_H}h"
echo "  Output   : $OUTPUT_DIR"
echo "  Queue    : $BENCH_QUEUE  cores=$BENCH_CORES  walltime=$BENCH_WALLTIME"
echo ""

bsub \
  -J "$JOB_NAME" \
  -n "$BENCH_CORES" \
  -q "$BENCH_QUEUE" \
  -W "$BENCH_WALLTIME" \
  -N -u "$email" \
  -oo "$OUTPUT_DIR/${JOB_NAME}.%J.out" \
  -eo "$OUTPUT_DIR/${JOB_NAME}.%J.err" \
  $PYTHON_CMD "$SCRIPT_DIR/trials/preproc_speed_benchmark.py" \
    --data_dir      "$DAY_DIR" \
    --probe         "$PROBE" \
    --shank         "$SHANK_NUM" \
    --output_dir    "$OUTPUT_DIR" \
    --duration_hours "$DURATION_H" \
    --margin_ms     "$MARGIN_MS" \
    --abs_threshold "$THRESHOLD" \
    --n_jobs        "$N_JOBS"

echo "Results will appear in: $OUTPUT_DIR/preproc_speed_results.json"
