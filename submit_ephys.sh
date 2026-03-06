#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [--emit-job-ids] <day_directory>
Example: $0 /groups/.../2025_12_02_square_arena_02
EOF
}

EMIT_JOB_IDS=false
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --emit-job-ids)
      EMIT_JOB_IDS=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "ERROR: Unknown option '$1'" >&2
      usage >&2
      exit 2
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [ "${#POSITIONAL[@]}" -ne 1 ]; then
  usage >&2
  exit 1
fi

DAY_DIR="${POSITIONAL[0]}"
if [ ! -d "$DAY_DIR" ]; then
  echo "ERROR: Directory does not exist: $DAY_DIR" >&2
  exit 2
fi
if [ ! -d "$DAY_DIR/data" ]; then
  echo "WARNING: $DAY_DIR/data not found. Exiting" >&2
  exit 2
fi
if ! command -v bsub >/dev/null 2>&1; then
  echo "ERROR: 'bsub' command not found in PATH." >&2
  exit 2
fi
if ! command -v apptainer >/dev/null 2>&1; then
  echo "ERROR: 'apptainer' command not found in PATH." >&2
  exit 2
fi

DIR_NAME="$(basename "$DAY_DIR")"
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
# SPIKENV_SIF is passed in from submit_a_day.sh via the environment
: "${SPIKENV_SIF:="$SCRIPT_DIR/containers/spikenv411.sif"}"

user="${USER:-$(whoami)}"
email="${user}@janelia.hhmi.org"
job_ids=()

extract_job_id() {
  local bsub_output="$1"
  if [[ "$bsub_output" =~ Job[[:space:]]*\<([0-9]+)\> ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

mkdir -p "$DAY_DIR/output"

# -----------------------------
# SUBMIT KILOSORT
# -----------------------------
for probe in a b; do
  for shank_num in 0 1 2 3; do
    JOB_NAME="ks_${DIR_NAME}_${probe}_shank${shank_num}"
    echo "Submitting job: $JOB_NAME"
    submit_output="$(bsub -J "$JOB_NAME" \
         -n 8 \
         -gpu "num=1" \
         -q gpu_l4 \
         -W 8:00 \
         -N -u "$email" \
         -oo "$DAY_DIR/output/${JOB_NAME}.%J.out" \
         -eo "$DAY_DIR/output/${JOB_NAME}.%J.err" \
         apptainer exec --nv --bind /groups "$SPIKENV_SIF" \
             python "$SCRIPT_DIR/run_pipeline.py" "$DAY_DIR" "$probe" "$shank_num")"
    echo "$submit_output"

    job_id="$(extract_job_id "$submit_output")" || {
      echo "ERROR: Failed to parse LSF job ID from bsub output for $JOB_NAME" >&2
      exit 3
    }
    job_ids+=("$job_id")
  done
done

if [ "${#job_ids[@]}" -ne 8 ]; then
  echo "ERROR: Expected 8 Kilosort jobs, parsed ${#job_ids[@]}." >&2
  exit 3
fi

echo "All jobs submitted!"
echo "Will email ${email} upon job completion/error."

if [ "$EMIT_JOB_IDS" = true ]; then
  echo "JOB_IDS=${job_ids[*]}"
fi
