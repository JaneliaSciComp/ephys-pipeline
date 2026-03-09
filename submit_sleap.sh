#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_directory> <large|box|minimaze>
Example: $0 /data/2025_12_02_square_arena_02 large
EOF
}

if [ $# -ne 2 ]; then
  usage >&2
  exit 1
fi

DAY_DIR="$1"
MAZE="$2"

if [ ! -d "$DAY_DIR" ]; then
  echo "ERROR: Day directory does not exist: $DAY_DIR" >&2
  exit 2
fi
if [ ! -d "$DAY_DIR/data" ]; then
  echo "ERROR: Missing data directory: $DAY_DIR/data" >&2
  exit 2
fi

case "$MAZE" in
  large|box|minimaze) ;;
  *)
    echo "ERROR: Invalid maze '$MAZE'. Use one of: large, box, minimaze." >&2
    exit 2
    ;;
esac

if ! command -v bsub >/dev/null 2>&1; then
  echo "ERROR: 'bsub' command not found in PATH." >&2
  exit 2
fi
if ! command -v apptainer >/dev/null 2>&1; then
  echo "ERROR: 'apptainer' command not found in PATH." >&2
  exit 2
fi

BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
RUNNER_SCRIPT="$SCRIPT_DIR/run_sleap.sh"
: "${SLEAP_SIF:="$SCRIPT_DIR/containers/sleap.sif"}"

if [ ! -f "$RUNNER_SCRIPT" ]; then
  echo "ERROR: run_sleap.sh not found: $RUNNER_SCRIPT" >&2
  exit 2
fi
if [ ! -f "$SLEAP_SIF" ]; then
  echo "ERROR: sleap.sif not found: $SLEAP_SIF" >&2
  exit 2
fi

extract_job_id() {
  local bsub_output="$1"
  if [[ "$bsub_output" =~ Job[[:space:]]*\<([0-9]+)\> ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

DIR_NAME="$(basename "$DAY_DIR")"
LOG_DIR="$DAY_DIR/sleap_output"
mkdir -p "$LOG_DIR"

SLEAP_QUEUE="${SLEAP_QUEUE:-gpu_a100}"
SLEAP_CORES="${SLEAP_CORES:-12}"
SLEAP_WALLTIME="${SLEAP_WALLTIME:-36:00}"
SLEAP_GPU="${SLEAP_GPU:-1}"

JOB_NAME="sleap_${DIR_NAME}"
echo "Submitting SLEAP job: $JOB_NAME"
submit_output="$(bsub -J "$JOB_NAME" \
    -q "$SLEAP_QUEUE" \
    -gpu "num=${SLEAP_GPU}" \
    -n "$SLEAP_CORES" \
    -W "$SLEAP_WALLTIME" \
    -oo "$LOG_DIR/${JOB_NAME}.%J.out" \
    -eo "$LOG_DIR/${JOB_NAME}.%J.err" \
    bash -c "SLEAP_SIF='$SLEAP_SIF' bash '$RUNNER_SCRIPT' '$DAY_DIR' '$MAZE'")"

printf '%s\n' "$submit_output"

sleap_job_id="$(extract_job_id "$submit_output")" || {
  echo "ERROR: Failed to parse LSF job ID from SLEAP submission output." >&2
  exit 3
}

echo "SLEAP_JOB_ID=$sleap_job_id"
