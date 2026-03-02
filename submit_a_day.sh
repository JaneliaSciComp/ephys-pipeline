#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <day_directory>" >&2
  echo "Example: $0 /groups/.../2025_12_02_square_arena_02" >&2
  exit 1
fi

# check that the directory exists, and that it has a 'data' subdirectory
DAY_DIR="$1"
if [ ! -d "$DAY_DIR" ]; then
  echo "ERROR: Directory does not exist: $DAY_DIR" >&2
  exit 2
fi
if [ ! -d "$DAY_DIR/data" ]; then
  echo "WARNING: $DAY_DIR/data not found. Exiting" >&2
  exit 2
fi

DIR_NAME="$(basename "$DAY_DIR")"
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline" # where submit_ephys.sh and submit_sleap.sh live
SLEAP_ENV_BIN="$BASE_DIR/envs/sleap/bin"


if [ ! -f "$SCRIPT_DIR/submit_ephys.sh" ]; then
  echo "ERROR: submit_ephys.sh not found next to this script: $SCRIPT_DIR/submit_ephys.sh" >&2
  exit 2
fi

if [ ! -f "$SCRIPT_DIR/submit_sleap.sh" ]; then
  echo "ERROR: submit_sleap.sh not found next to this script: $SCRIPT_DIR/submit_sleap.sh" >&2
  exit 2
fi

user="${USER:-$(whoami)}"
email="${user}@janelia.hhmi.org"

# -----------------------------
# SUBMIT KILOSORT
# -----------------------------
mkdir -p "$DAY_DIR/output"
NPX_SUBMIT_JOB_NAME="submit_npx_${DIR_NAME}"
echo "Submitting NPX job-submitter: $NPX_SUBMIT_JOB_NAME"
bsub -J "$NPX_SUBMIT_JOB_NAME" \
     -q short \
     -n 1 \
     -W 00:30 \
     -R "rusage[mem=2000]" \
     -oo "$DAY_DIR/output/${NPX_SUBMIT_JOB_NAME}.%J.out" \
     -eo "$DAY_DIR/output/${NPX_SUBMIT_JOB_NAME}.%J.err" \
     bash -lc "cd '$SCRIPT_DIR' && bash '$SCRIPT_DIR/submit_ephys.sh' '$DAY_DIR'"


# -----------------------------
# SUBMIT SLEAP
# -----------------------------
mkdir -p "$DAY_DIR/sleap_output"
SLEAP_JOB_NAME="sleap_${DIR_NAME}"
echo "Submitting SLEAP job: $SLEAP_JOB_NAME"
bsub -J "$SLEAP_JOB_NAME" \
     -q gpu_l4 \
     -gpu "num=1" \
     -n 5 \
     -R "rusage[mem=16000]" \
     -oo "$DAY_DIR/sleap_output/${SLEAP_JOB_NAME}.%J.out" \
     -eo "$DAY_DIR/sleap_output/${SLEAP_JOB_NAME}.%J.err" \
     -W 36:00 \
     bash -lc "cd '$DAY_DIR' && SLEAP_ENV_BIN='$SLEAP_ENV_BIN' bash '$SCRIPT_DIR/submit_sleap.sh'"
