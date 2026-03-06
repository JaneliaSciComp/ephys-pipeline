#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_directory> [--workers 16] [--wait "<lsf_dependency_expression>"]
Example:
  $0 /groups/.../2025_12_02_square_arena_02 --workers 16 --wait "done(123) && done(124)"
EOF
}

DAY_DIR=""
WORKERS=16
WAIT_EXPR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --workers requires an integer value." >&2
        exit 2
      fi
      WORKERS="$2"
      shift 2
      ;;
    --wait)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --wait requires a dependency expression." >&2
        exit 2
      fi
      WAIT_EXPR="$2"
      shift 2
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
      if [ -n "$DAY_DIR" ]; then
        echo "ERROR: Multiple day directories provided." >&2
        usage >&2
        exit 2
      fi
      DAY_DIR="$1"
      shift
      ;;
  esac
done

if [ -z "$DAY_DIR" ]; then
  usage >&2
  exit 1
fi
if [ ! -d "$DAY_DIR" ]; then
  echo "ERROR: Directory does not exist: $DAY_DIR" >&2
  exit 2
fi
if [ ! -d "$DAY_DIR/data" ]; then
  echo "ERROR: $DAY_DIR/data not found." >&2
  exit 2
fi
if ! [[ "$WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: --workers must be a positive integer, got '$WORKERS'." >&2
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

BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
CONTAINERS_DIR="$SCRIPT_DIR/containers"
COMBINER_SIF="$CONTAINERS_DIR/combiner.sif"
COMBINER_SCRIPT="$SCRIPT_DIR/combiner_pipeline.py"

if [ ! -f "$COMBINER_SIF" ]; then
  echo "ERROR: combiner.sif not found: $COMBINER_SIF" >&2
  exit 2
fi
if [ ! -f "$COMBINER_SCRIPT" ]; then
  echo "ERROR: combiner pipeline script not found: $COMBINER_SCRIPT" >&2
  exit 2
fi

if [ -n "$WAIT_EXPR" ] && [[ "$WAIT_EXPR" != *done\(*\)* ]]; then
  echo "ERROR: --wait expression must contain done(<jobid>) terms." >&2
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
JOB_NAME="combiner_${DIR_NAME}"
LOG_DIR="$DAY_DIR/output"
mkdir -p "$LOG_DIR"

COMBINER_QUEUE="${COMBINER_QUEUE:-}"
COMBINER_WALLTIME="${COMBINER_WALLTIME:-04:00}"
COMBINER_MEM_MB="${COMBINER_MEM_MB:-16000}"

run_cmd="apptainer exec --bind /groups \"$COMBINER_SIF\" python \"$COMBINER_SCRIPT\" \"$DAY_DIR\" --workers \"$WORKERS\" --plot true"

bsub_args=(
  -J "$JOB_NAME"
  -n "$WORKERS"
  -W "$COMBINER_WALLTIME"
  -R "rusage[mem=${COMBINER_MEM_MB}]"
  -oo "$LOG_DIR/${JOB_NAME}.%J.out"
  -eo "$LOG_DIR/${JOB_NAME}.%J.err"
)

if [ -n "$COMBINER_QUEUE" ]; then
  bsub_args+=(-q "$COMBINER_QUEUE")
fi
if [ -n "$WAIT_EXPR" ]; then
  bsub_args+=(-w "$WAIT_EXPR")
fi

submit_output="$(bsub "${bsub_args[@]}" bash -c "$run_cmd")"

printf '%s\n' "$submit_output"
combiner_job_id="$(extract_job_id "$submit_output")" || {
  echo "ERROR: Failed to parse LSF job ID from combiner submission output." >&2
  exit 3
}

echo "Submitted combiner job '$JOB_NAME' as job ID $combiner_job_id"
if [ -n "$WAIT_EXPR" ]; then
  echo "Combiner dependency expression: $WAIT_EXPR"
fi
echo "COMBINER_JOB_ID=$combiner_job_id"
