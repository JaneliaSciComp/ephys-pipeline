#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_directory> <large|box|minimaze>
Example: $0 /groups/.../2025_12_02_square_arena_02 large
EOF
}

if [ $# -ne 2 ]; then
  usage >&2
  exit 1
fi

DAY_DIR="$1"
if [ ! -d "$DAY_DIR" ]; then
  echo "ERROR: Directory does not exist: $DAY_DIR" >&2
  exit 2
fi
if [ ! -d "$DAY_DIR/data" ]; then
  echo "WARNING: $DAY_DIR/data not found. Exiting" >&2
  exit 2
fi

MAZE="$2"
case "$MAZE" in
  large|box|minimaze) ;;
  *)
    echo "ERROR: Invalid maze '$MAZE'. Use one of: large, box, minimaze." >&2
    exit 2
    ;;
esac

DIR_NAME="$(basename "$DAY_DIR")"
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
CONTAINERS_DIR="$SCRIPT_DIR/containers"
SPIKENV_SIF="$CONTAINERS_DIR/spikenv411.sif"
SLEAP_SIF="$CONTAINERS_DIR/sleap.sif"
COMBINER_WORKERS="${COMBINER_WORKERS:-16}"

if [ ! -f "$SCRIPT_DIR/submit_ephys.sh" ]; then
  echo "ERROR: submit_ephys.sh not found: $SCRIPT_DIR/submit_ephys.sh" >&2
  exit 2
fi
if [ ! -f "$SCRIPT_DIR/submit_sleap.sh" ]; then
  echo "ERROR: submit_sleap.sh not found: $SCRIPT_DIR/submit_sleap.sh" >&2
  exit 2
fi
if [ ! -f "$SCRIPT_DIR/submit_combiner.sh" ]; then
  echo "ERROR: submit_combiner.sh not found: $SCRIPT_DIR/submit_combiner.sh" >&2
  exit 2
fi
if [ ! -f "$SPIKENV_SIF" ]; then
  echo "ERROR: spikenv411.sif not found: $SPIKENV_SIF" >&2
  exit 2
fi
if [ ! -f "$SLEAP_SIF" ]; then
  echo "ERROR: sleap.sif not found: $SLEAP_SIF" >&2
  exit 2
fi
if ! command -v bsub >/dev/null 2>&1; then
  echo "ERROR: 'bsub' command not found in PATH." >&2
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

# -----------------------------
# SUBMIT KILOSORT
# -----------------------------
mkdir -p "$DAY_DIR/output"
echo "Submitting Kilosort jobs via submit_ephys.sh"
ephys_output="$(SPIKENV_SIF="$SPIKENV_SIF" bash "$SCRIPT_DIR/submit_ephys.sh" --emit-job-ids "$DAY_DIR")"
printf '%s\n' "$ephys_output"

job_ids_line="$(printf '%s\n' "$ephys_output" | awk -F= '/^JOB_IDS=/{print $2}' | tail -n 1)"
if [ -z "$job_ids_line" ]; then
  echo "ERROR: submit_ephys.sh did not emit a JOB_IDS=... line." >&2
  exit 3
fi

read -r -a ks_job_ids <<< "$job_ids_line"
if [ "${#ks_job_ids[@]}" -eq 0 ]; then
  echo "ERROR: No Kilosort job IDs parsed from submit_ephys.sh output." >&2
  exit 3
fi

for job_id in "${ks_job_ids[@]}"; do
  if ! [[ "$job_id" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Non-numeric Kilosort job ID parsed: '$job_id'" >&2
    exit 3
  fi
done

# -----------------------------
# SUBMIT SLEAP
# -----------------------------
mkdir -p "$DAY_DIR/sleap_output"
SLEAP_JOB_NAME="sleap_${DIR_NAME}"
echo "Submitting SLEAP job: $SLEAP_JOB_NAME"
sleap_submit_output="$(bsub -J "$SLEAP_JOB_NAME" \
     -q gpu_a100 \
     -gpu "num=1" \
     -n 12 \
     -oo "$DAY_DIR/sleap_output/${SLEAP_JOB_NAME}.%J.out" \
     -eo "$DAY_DIR/sleap_output/${SLEAP_JOB_NAME}.%J.err" \
     -W 36:00 \
     bash -c "cd '$DAY_DIR' && SLEAP_SIF='$SLEAP_SIF' bash '$SCRIPT_DIR/submit_sleap.sh' '$MAZE'")"
printf '%s\n' "$sleap_submit_output"

sleap_job_id="$(extract_job_id "$sleap_submit_output")" || {
  echo "ERROR: Failed to parse LSF job ID from SLEAP submission output." >&2
  exit 3
}

dependency_expr=""
for job_id in "${ks_job_ids[@]}" "$sleap_job_id"; do
  term="done($job_id)"
  if [ -z "$dependency_expr" ]; then
    dependency_expr="$term"
  else
    dependency_expr="${dependency_expr} && ${term}"
  fi
done

if [ -z "$dependency_expr" ]; then
  echo "ERROR: Empty combiner dependency expression." >&2
  exit 3
fi

echo "Submitting combiner job with dependency: $dependency_expr"
combiner_submit_output="$(bash "$SCRIPT_DIR/submit_combiner.sh" \
    "$DAY_DIR" \
    --workers "$COMBINER_WORKERS" \
    --wait "$dependency_expr")"
printf '%s\n' "$combiner_submit_output"

combiner_job_id="$(printf '%s\n' "$combiner_submit_output" | awk -F= '/^COMBINER_JOB_ID=/{print $2}' | tail -n 1)"
if ! [[ "${combiner_job_id:-}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: Failed to parse COMBINER_JOB_ID from submit_combiner.sh output." >&2
  exit 3
fi

echo "Summary:"
echo "  Kilosort job IDs: ${ks_job_ids[*]}"
echo "  SLEAP job ID: $sleap_job_id"
echo "  Combiner job ID: $combiner_job_id"
