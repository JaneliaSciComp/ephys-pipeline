#!/usr/bin/env bash
# Submit a single preprocessing trial to the LSF cluster.
#
# Usage:
#   ./trials/submit_trial.sh <day_dir> <probe> <shank_num> <config.yaml>
#
# Example:
#   ./trials/submit_trial.sh /data/2026_03_05_npx11_large_maze a 0 trials/my_trial.yaml
#
# The job writes output to:
#   <day_dir>/processing_pipeline_trials/<trial_name>/
# and logs to:
#   <day_dir>/processing_pipeline_trials/logs/<trial_name>.<jobid>.{out,err}
#
# Code syncing:
#   By default the script rsyncs your local working copy of the pipeline code
#   to a personal staging directory on the shared filesystem before submitting,
#   so the cluster job always runs your latest local changes without needing a
#   git push/pull.  The staging location is:
#     /groups/voigts/voigtslab/<your-username>/ephys-pipeline-dev/
#   Set SYNC_CODE=0 to skip the rsync and use the shared team copy instead.
#
# Environment variables (override defaults):
#   LOCAL_CODE_DIR  root of your local pipeline checkout
#                   (default: directory containing this script's parent)
#   SYNC_CODE       1 = rsync local code before submitting (default), 0 = skip
#   SPIKENV_SIF     path to the spikenv411.sif container
#   TRIAL_QUEUE     LSF queue name           (default: gpu_a100)
#   TRIAL_CORES     number of CPU cores      (default: 12)
#   TRIAL_WALLTIME  walltime HH:MM           (default: 6:00)

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_dir> <probe> <shank_num> <config.yaml>
  probe     : a | b
  shank_num : 0 | 1 | 2 | 3
EOF
}

# ── Parse arguments ────────────────────────────────────────────────────────────
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

# Validate
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

# Local code directory: the repo root (one level up from this script)
: "${LOCAL_CODE_DIR:="$(cd "$(dirname "$0")/.." && pwd)"}"
: "${SYNC_CODE:=1}"

user="${USER:-$(whoami)}"
STAGED_SCRIPT_DIR="/groups/voigts/voigtslab/${user}/ephys-pipeline-dev"

# ── If bsub isn't available, rsync then SSH to the cluster to submit ──────────
: "${CLUSTER_HOST:=}"

if ! command -v bsub >/dev/null 2>&1; then
  if [ -z "$CLUSTER_HOST" ]; then
    echo "ERROR: bsub not found and CLUSTER_HOST is not set." >&2
    echo "  Set it in your shell or ~/.bashrc, e.g.:" >&2
    echo "    export CLUSTER_HOST=login1.cluster.hhmi.org" >&2
    exit 2
  fi

  if command -v rsync >/dev/null 2>&1; then
    echo "Syncing code: $LOCAL_CODE_DIR → $STAGED_SCRIPT_DIR"
    ssh "$CLUSTER_HOST" "mkdir -p '$STAGED_SCRIPT_DIR'"
    rsync -a --delete \
      --exclude='.git' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      --exclude='processing_pipeline_trials' \
      --exclude='output' \
      --exclude='*.sif' \
      --exclude='*.bin' \
      --exclude='*.npy' \
      "$LOCAL_CODE_DIR/" "${CLUSTER_HOST}:${STAGED_SCRIPT_DIR}/"
    echo "Sync done."
  else
    echo "WARNING: rsync not found — cluster will use shared team copy" >&2
  fi

  # Translate local config path to its cluster equivalent under STAGED_SCRIPT_DIR
  CONFIG_REL="${CONFIG_YAML#$LOCAL_CODE_DIR/}"
  CLUSTER_CONFIG="$STAGED_SCRIPT_DIR/$CONFIG_REL"

  echo "Submitting via SSH to $CLUSTER_HOST"
  exec ssh "$CLUSTER_HOST" \
    SYNC_CODE=0 \
    CLUSTER_EMAIL="${CLUSTER_EMAIL:-}" \
    TRIAL_QUEUE="${TRIAL_QUEUE:-gpu_a100}" \
    TRIAL_CORES="${TRIAL_CORES:-12}" \
    TRIAL_WALLTIME="${TRIAL_WALLTIME:-6:00}" \
    SPIKENV_SIF="${SPIKENV_SIF:-}" \
    bash "$STAGED_SCRIPT_DIR/trials/submit_trial.sh" \
      "$DAY_DIR" "$PROBE" "$SHANK_NUM" "$CLUSTER_CONFIG"
fi

# ── When running on the cluster, SYNC_CODE=0 so just set SCRIPT_DIR ──────────
if [ "${SYNC_CODE}" = "0" ]; then
  SCRIPT_DIR="$STAGED_SCRIPT_DIR"
else
  SCRIPT_DIR="$SHARED_SCRIPT_DIR"
  echo "SYNC_CODE=0: using shared copy at $SCRIPT_DIR"
fi

# Extract trial_name from the YAML (used for job name and log files)
TRIAL_NAME="$(python3 -c "import yaml,sys; cfg=yaml.safe_load(open('$CONFIG_YAML')); print(cfg['trial_name'])" 2>/dev/null || true)"
if [ -z "$TRIAL_NAME" ]; then
  echo "ERROR: could not read trial_name from $CONFIG_YAML" >&2; exit 2
fi

DIR_NAME="$(basename "$DAY_DIR")"
JOB_NAME="$TRIAL_NAME"

LOG_DIR="$DAY_DIR/processing_pipeline_trials/$TRIAL_NAME"
# log dir lives on the cluster filesystem — create it only if accessible
mkdir -p "$LOG_DIR" 2>/dev/null || true

# ── Cluster settings ───────────────────────────────────────────────────────────
: "${TRIAL_QUEUE:=gpu_l4}"
: "${TRIAL_CORES:=12}"
: "${TRIAL_WALLTIME:=6:00}"   # longer than a normal KS job: includes postproc

email="${CLUSTER_EMAIL:-${user}@${CLUSTER_DOMAIN:-}}"

# ── Submit ─────────────────────────────────────────────────────────────────────
echo "Submitting trial: $TRIAL_NAME"
echo "  Day    : $DAY_DIR"
echo "  Probe  : $PROBE  shank $SHANK_NUM"
echo "  Config : $CONFIG_YAML"
echo "  Job    : $JOB_NAME"
echo "  Queue  : $TRIAL_QUEUE  cores=$TRIAL_CORES  walltime=$TRIAL_WALLTIME"
echo ""

bsub \
  -J "$JOB_NAME" \
  -n "$TRIAL_CORES" \
  -gpu "num=1" \
  -q "$TRIAL_QUEUE" \
  -W "$TRIAL_WALLTIME" \
  -N -u "$email" \
  -oo "$LOG_DIR/job.%J.out" \
  -eo "$LOG_DIR/job.%J.err" \
  apptainer exec --nv --bind /groups "$SPIKENV_SIF" \
    python "$SCRIPT_DIR/trials/run_trial.py" \
      "$DAY_DIR" "$PROBE" "$SHANK_NUM" "$CONFIG_YAML"
