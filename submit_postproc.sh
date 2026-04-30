#!/usr/bin/env bash
# Submit post-processing (SortingAnalyzer + UnitRefine) to the cluster.
#
# Usage:
#   ./submit_postproc.sh <day_dir> <probe> <shank_num> [--shank_folder <path>]
#
# Examples:
#   # production output (standard path):
#   ./submit_postproc.sh /groups/.../2026_03_05 a 0
#
#   # trial sort output (custom path):
#   ./submit_postproc.sh /groups/.../2026_03_05 a 0 \
#       --shank_folder /groups/.../2026_03_05/output/trials/14_check_ks/probe_a/shank_0
#
# Environment overrides:
#   SPIKENV_SIF    container image (default: shared spikenv411.sif)
#   POST_CORES     CPU cores (default: 12)
#   POST_WALLTIME  HH:MM (default: 4:00)
#   CLUSTER_HOST   SSH host when bsub is not available locally

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_dir> <probe> <shank_num> [--shank_folder <path>]
  probe     : a | b
  shank_num : 0 | 1 | 2 | 3
EOF
}

if [ $# -lt 3 ]; then
  usage >&2; exit 1
fi

DAY_DIR="$1"
PROBE="$2"
SHANK_NUM="$3"
SHANK_FOLDER_ARG=""

shift 3
while [[ $# -gt 0 ]]; do
  case "$1" in
    --shank_folder)
      SHANK_FOLDER_ARG="--shank_folder $2"; shift 2 ;;
    *)
      echo "ERROR: Unknown argument '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ! "$PROBE" =~ ^[ab]$ ]]; then
  echo "ERROR: probe must be 'a' or 'b'" >&2; exit 2
fi
if [[ ! "$SHANK_NUM" =~ ^[0123]$ ]]; then
  echo "ERROR: shank_num must be 0–3" >&2; exit 2
fi

# ── Resolve paths ──────────────────────────────────────────────────────────────
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
SHARED_SCRIPT_DIR="$BASE_DIR/ephys-pipeline"
: "${SPIKENV_SIF:="$SHARED_SCRIPT_DIR/containers/spikenv411.sif"}"
: "${LOCAL_CODE_DIR:="$(cd "$(dirname "$0")" && pwd)"}"
: "${SYNC_CODE:=1}"

user="${USER:-$(whoami)}"
STAGED_SCRIPT_DIR="/groups/voigts/voigtslab/${user}/ephys-pipeline-dev"

# ── Remote fallback ────────────────────────────────────────────────────────────
: "${CLUSTER_HOST:=}"

if ! command -v bsub >/dev/null 2>&1; then
  if [ -z "$CLUSTER_HOST" ]; then
    echo "ERROR: bsub not found and CLUSTER_HOST is not set." >&2
    echo "  e.g.: export CLUSTER_HOST=login1.int.janelia.org" >&2
    exit 2
  fi

  if command -v rsync >/dev/null 2>&1; then
    echo "Syncing code: $LOCAL_CODE_DIR → ${CLUSTER_HOST}:${STAGED_SCRIPT_DIR}"
    ssh "$CLUSTER_HOST" "mkdir -p '$STAGED_SCRIPT_DIR'"
    rsync -a --delete --force --ignore-errors \
      --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
      --exclude='.nfs*' --exclude='output' --exclude='*.sif' \
      --exclude='*.bin' --exclude='*.npy' \
      "$LOCAL_CODE_DIR/" "${CLUSTER_HOST}:${STAGED_SCRIPT_DIR}/" || true
    echo "Sync done."
  fi

  echo "Submitting via SSH to $CLUSTER_HOST"
  exec ssh "$CLUSTER_HOST" \
    SYNC_CODE=0 \
    CLUSTER_EMAIL="${CLUSTER_EMAIL:-}" \
    POST_CORES="${POST_CORES:-12}" \
    POST_WALLTIME="${POST_WALLTIME:-4:00}" \
    SPIKENV_SIF="${SPIKENV_SIF:-}" \
    bash "$STAGED_SCRIPT_DIR/submit_postproc.sh" \
      "$DAY_DIR" "$PROBE" "$SHANK_NUM" $SHANK_FOLDER_ARG
fi

# ── On cluster: pick script dir ───────────────────────────────────────────────
if [ "${SYNC_CODE}" = "0" ]; then
  SCRIPT_DIR="$STAGED_SCRIPT_DIR"
else
  SCRIPT_DIR="$SHARED_SCRIPT_DIR"
fi

if [ ! -f "$SPIKENV_SIF" ]; then
  echo "ERROR: container image not found: $SPIKENV_SIF" >&2; exit 2
fi

: "${POST_CORES:=12}"
: "${POST_WALLTIME:=4:00}"

email="${CLUSTER_EMAIL:-${user}@${CLUSTER_DOMAIN:-}}"
JOB_NAME="postproc_${PROBE}${SHANK_NUM}"
OUTPUT_DIR="$DAY_DIR/output/${PROBE}/shank_${SHANK_NUM}"

echo "Submitting: $JOB_NAME"
echo "  Day   : $DAY_DIR"
echo "  Probe : $PROBE  shank $SHANK_NUM"
if [ -n "$SHANK_FOLDER_ARG" ]; then
  echo "  Shank folder override: ${SHANK_FOLDER_ARG#--shank_folder }"
fi
echo ""

mkdir -p "$OUTPUT_DIR"

bsub \
  -J "$JOB_NAME" \
  -n "$POST_CORES" \
  -W "$POST_WALLTIME" \
  -N -u "$email" \
  -oo "$OUTPUT_DIR/${JOB_NAME}.%J.out" \
  -eo "$OUTPUT_DIR/${JOB_NAME}.%J.err" \
  apptainer exec --cleanenv --bind /groups "$SPIKENV_SIF" \
    python -s "$SCRIPT_DIR/pipeline/postproc.py" \
      "$DAY_DIR" "$PROBE" "$SHANK_NUM" \
      $SHANK_FOLDER_ARG
