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
SCRIPT_DIR="/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/" # where submit_ks and submit_sleap live; this should be stable, and we can drop in developed stuff
SLEAP_ENV_ACTIVATE="$SCRIPT_DIR/envs/sleap/bin/activate"


if [ ! -f "$SCRIPT_DIR/ephys-pipeline/submit_all.sh" ]; then
  echo "ERROR: submit_ks.sh not found next to this script: $SCRIPT_DIR/ephys-pipeline/submit_all.sh" >&2
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
     bash -lc "cd '$SCRIPT_DIR/ephys-pipeline' && bash '$SCRIPT_DIR/submit_all.sh' '$DAY_DIR'"


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
     bash -lc "cd '$DAY_DIR' && source '$SLEAP_ENV_ACTIVATE' && bash '$SCRIPT_DIR/submit_sleap.sh'" 
