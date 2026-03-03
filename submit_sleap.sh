#!/bin/bash

# SQUARE ARENA / BOX IN BIG MAZE
CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/square_arena251217_163935.centroid.n=60"
INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/square_arena251217_170608.centered_instance.n=60"

# # BIG MAZE
# CENTROID_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_150146.centroid.n=2228"
# INSTANCE_MODEL="/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_164053.centered_instance.n=2228"

# MINIMAZE
##"/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_150146.centroid.n=2228"
##"/groups/voigts/voigtslab/animal_tracking/sleap/models/251205_164053.centered_instance.n=2228"

OUTPUT_DIR="./sleap_output"
INPUT_DIR="./data"

# Activate conda env without needing conda on PATH
export CONDA_PREFIX="$(dirname "$SLEAP_ENV_BIN")"
export PATH="$SLEAP_ENV_BIN:$PATH"
for f in "$CONDA_PREFIX/etc/conda/activate.d"/*.sh; do [ -f "$f" ] && source "$f"; done

mkdir -p "$OUTPUT_DIR"

chmod -R 777 "$OUTPUT_DIR"

for mp4_file in "$INPUT_DIR"/compressed*.mp4; do
    base_name=$(basename "$mp4_file" .mp4)
    output_path="$OUTPUT_DIR/${base_name}.slp"
    analysis_path="$OUTPUT_DIR/${base_name}.analysis.h5"
    
    echo "Processing $mp4_file..."
    if [ -f "$output_path" ]; then
        echo "Output file $output_path already exists. Skipping..."
        continue
    fi
    "$SLEAP_ENV_BIN/python" "$SLEAP_ENV_BIN/sleap-track" \
        "$mp4_file" \
        -m "$CENTROID_MODEL" \
        -m "$INSTANCE_MODEL" \
        -o "$output_path" \
        --verbosity json \
        --batch_size 4 \
        --max_instances 1

    echo "Converting $output_path to $analysis_path..."
    "$SLEAP_ENV_BIN/python" "$SLEAP_ENV_BIN/sleap-convert" \
        "$output_path" \
        --format analysis \
        -o "$analysis_path"
done

echo "changing permissions on output directory..."
chmod -R 777 "$OUTPUT_DIR"
