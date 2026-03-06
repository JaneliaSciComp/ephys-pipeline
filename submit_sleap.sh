#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 <day_directory> [large|box|minimaze]
  $0 [large|box|minimaze]            # legacy mode: run from within day_directory
EOF
}

DAY_DIR=""
MAZE="large"

if [ $# -eq 0 ]; then
  DAY_DIR="$PWD"
elif [ $# -eq 1 ]; then
  case "$1" in
    large|box|minimaze)
      DAY_DIR="$PWD"
      MAZE="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      DAY_DIR="$1"
      ;;
  esac
elif [ $# -eq 2 ]; then
  DAY_DIR="$1"
  MAZE="$2"
elif [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  usage
  exit 0
else
  echo "ERROR: Invalid arguments." >&2
  usage >&2
  exit 2
fi

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
    # this one works!
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_150146.centroid.n=2228"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_164053.centered_instance.n=2228"
    # testing new one: doesnt really work..
    # CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_152533.centroid.n=2228"
    # INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_163925.centered_instance.n=2228"
    ;;
  box)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_124156.centroid.n=169"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/260305_130544.centered_instance.n=169"
    ;;
  minimaze)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze251217_172518.centroid.n=62"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze251217_174931.centered_instance.n=62"
    ;;
  *)
    echo "Error: invalid maze '$MAZE'"
    usage >&2
    exit 2
    ;;
esac

OUTPUT_DIR="$DAY_DIR/sleap_output"
INPUT_DIR="$DAY_DIR/data"

# SLEAP_SIF is passed in from submit_a_day.sh via the environment
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

echo "changing permissions on output directory..."
chmod -R 777 "$OUTPUT_DIR"
