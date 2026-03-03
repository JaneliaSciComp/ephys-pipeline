#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <day_directory>" >&2
  echo "Example: $0 /groups/.../2025_12_02_square_arena_02" >&2
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

DIR_NAME="$(basename "$DAY_DIR")"
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
ENV_BIN="$SCRIPT_DIR/envs/spikenv411/bin"

user="${USER:-$(whoami)}"
email="${user}@janelia.hhmi.org"

mkdir -p "$DAY_DIR/output"

# -----------------------------
# SUBMIT KILOSORT
# -----------------------------
for probe in a b; do
  for shank_num in 0 1 2 3; do
    JOB_NAME="ks_${DIR_NAME}_${probe}_shank${shank_num}"
    echo "Submitting job: $JOB_NAME"
    bsub -J "$JOB_NAME" \
         -n 12 \
         -gpu "num=1" \
         -q gpu_a100 \
         -W 4:00 \
         -N -u "$email" \
         -oo "$DAY_DIR/output/${JOB_NAME}.%J.out" \
         -eo "$DAY_DIR/output/${JOB_NAME}.%J.err" \
         bash -lc "PATH='$ENV_BIN:\$PATH' python -u '$SCRIPT_DIR/run_pipeline.py' '$DAY_DIR' '$probe' '$shank_num'"
  done
done

echo "All jobs submitted!"
echo "Will email ${email} upon job completion/error."
