#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <day_directory> <large|box|minimaze>
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
  large)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_150146.centroid.n=2228"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_164053.centered_instance.n=2228"
    ;;
  box)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_124156.centroid.n=169"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_130544.centered_instance.n=169"
    ;;
  minimaze)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze260309_124154.centroid.n=248"
    INSTANCE_MODEL=INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze260309_130927.centered_instance.n=248"
    ;;
  *)
    echo "ERROR: Invalid maze '$MAZE'. Use one of: large, box, minimaze." >&2
    usage >&2
    exit 2
    ;;
esac

OUTPUT_DIR="$DAY_DIR/sleap_output"
INPUT_DIR="$DAY_DIR/data"

# SLEAP_SIF is passed in from submit_sleap.sh via the environment
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
: "${SLEAP_SIF:="$BASE_DIR/ephys-pipeline/containers/sleap.sif"}"
if [ ! -f "$SLEAP_SIF" ]; then
  echo "ERROR: sleap container not found: $SLEAP_SIF" >&2
  exit 2
fi
if ! command -v apptainer >/dev/null 2>&1; then
  echo "ERROR: 'apptainer' command not found in PATH." >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"
shopt -s nullglob
mp4_files=("$INPUT_DIR"/compressed*.mp4)
shopt -u nullglob
if [ "${#mp4_files[@]}" -eq 0 ]; then
  echo "ERROR: No files matching $INPUT_DIR/compressed*.mp4" >&2
  exit 2
fi

for mp4_file in "${mp4_files[@]}"; do
  base_name=$(basename "$mp4_file" .mp4)
  output_path="$OUTPUT_DIR/${base_name}.slp"
  analysis_path="$OUTPUT_DIR/${base_name}.analysis.h5"

  echo "Processing $mp4_file..."
  if [ -f "$output_path" ]; then
    echo "Output file $output_path already exists. Skipping..."
    continue
  fi

  apptainer exec --nv --bind /groups \
    --env LD_LIBRARY_PATH=/opt/sleap/lib \
    "$SLEAP_SIF" sleap-track \
    "$mp4_file" \
    -m "$CENTROID_MODEL" \
    -m "$INSTANCE_MODEL" \
    -o "$output_path" \
    --verbosity json \
    --batch_size 8 \
    --max_instances 1

  echo "Converting $output_path to $analysis_path..."
  apptainer exec --nv --bind /groups \
    --env LD_LIBRARY_PATH=/opt/sleap/lib \
    "$SLEAP_SIF" sleap-convert \
    "$output_path" \
    --format analysis \
    -o "$analysis_path"
done

echo "Changing permissions on output directory..."
chmod -R 777 "$OUTPUT_DIR"
