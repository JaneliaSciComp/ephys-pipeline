set -euo pipefail

MAZE="${1:-large}"

case "$MAZE" in
  large)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_150146.centroid.n=2228"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_164053.centered_instance.n=2228"
    ;;
  box)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/square_arena251217_163935.centroid.n=60"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/square_arena251217_170608.centered_instance.n=60"
    ;;
  minimaze)
    CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze251217_172518.centroid.n=62"
    INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/minimaze251217_174931.centered_instance.n=62"
    ;;
  -h|--help)
    echo "Usage: $0 [large|box|minimaze]"
    echo "Defaults to 'large' if omitted."
    exit 0
    ;;
  *)
    echo "Error: invalid maze '$MAZE'"
    echo "Usage: $0 [large|box|minimaze]"
    exit 2
    ;;
esac

OUTPUT_DIR="./sleap_output"
INPUT_DIR="./data"

# SLEAP_SIF is passed in from submit_a_day.sh via the environment
BASE_DIR="/groups/voigts/voigtslab/submit_a_day"
: "${SLEAP_SIF:="$BASE_DIR/ephys-pipeline/containers/sleap.sif"}"

mkdir -p "$OUTPUT_DIR"

for mp4_file in "$INPUT_DIR"/compressed*.mp4; do
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
