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
    sleap-track \
        "$mp4_file" \
        -m "$CENTROID_MODEL" \
        -m "$INSTANCE_MODEL" \
        -o "$output_path" \
        --verbosity json \
        --batch_size 4 \
        --max_instances 1 
        # \
        # --verbosity json #\ # ac edit dec 12
        # i think tracking is unnecessary cause it's identity assignment
        # --tracking.tracker simple \
        # --tracking.max_tracks 1 \
        # --tracking.similarity instance \
        # --tracking.match hungarian

    echo "Converting $output_path to $analysis_path..."
    sleap-convert \
        "$output_path" \
        --format analysis \
        -o "$analysis_path"
done

echo "changing permissions on output directory..."
chmod -R 777 "$OUTPUT_DIR"

# running training on a single video
# /npx06/2025_06_09_npx06_day1/sleap_output$ sleap-convert "compressed_video_2025-06-09T13_12_48.slp" --format analysis -o "compressed_video_2025-06-09T13_12_48.analysis.h5"

# converting a single video
# /npx06/2025_06_09_npx06_day1$ sleap-track "/mnt/v/neuropixels_2025/npx06/2025_06_09_npx06_day1/data/compressed_video_2025-06-09T13_12_48.mp4" -m "/mnt/v/neuropixels_2025/sleap/models/250625_210613.centroid.n=369" -m "/mnt/v/neuropixels_2025/sleap/models/250625_213824.centered_instance.n=369" -o "./sleap_output/compressed_video_2025-06-09T13_12_48.slp" --max_instances 1 --tracking.max_tracks 2