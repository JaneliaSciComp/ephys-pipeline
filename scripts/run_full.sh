#!/bin/bash

# Define paths
CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_07_31_npx_test_01/"
DATAPATH="data"
CODEPATH="$HOME/ephys-pipeline/pipeline"
LOG_DIR="$HOME/ephys-pipeline/logs/$(basename "$FILEPATH")2"
PROBE="probe_b"
SHANK="4"

# Create the log folder if it doesn't exist
mkdir -p "$HOME/ephys-pipeline/logs/"
mkdir -p "$LOG_DIR"

source ~/.bashrc && conda activate $CONDA_ENV

# Iterate over probes and shanks, then dynamically process chunks
for shank in 1 2 3 4; do
    echo "Running detection for probe $PROBE shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1

    KS_JOB_ID=$(bsub -n 16 -gpu "num=1" -q "gpu_tesla" \
                -o "$LOG_DIR/detection_${PROBE}_shank_${shank}.log" \
                "python ${CODEPATH}/detection.py $FILEPATH $shank $PROBE $DATAPATH" | awk '{print $2}' | tr -d '<>')

    if [ -z "$KS_JOB_ID" ]; then
        echo "Failed to submit detection job for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
        exit 1
done
