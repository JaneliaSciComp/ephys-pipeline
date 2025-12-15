#!/bin/bash

# Define paths
CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_09_04_npx_3dmaze/"
DATAPATH="data"
CODEPATH="$HOME/ephys-pipeline/pipeline"
LOG_DIR="$HOME/ephys-pipeline/logs_chunked/$(basename "$FILEPATH")2"
CHUNK=true

# Create the log folder if it doesn't exist
mkdir -p "$HOME/ephys-pipeline/logs_chunked/"
mkdir -p "$LOG_DIR"

source ~/.bashrc && conda activate $CONDA_ENV

# Iterate over probes and shanks, then dynamically process chunks
for probe in a; do
    for shank in 4; do
        for chunk_idx in 0; do
            for dredge in true; do
                echo "Running detection for probe $probe shank $shank chunk $chunk_idx dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1

                KS_JOB_ID=$(bsub -n 12 -gpu "num=1" -q "gpu_tesla" \
                            -o "$LOG_DIR/detection_${probe}_shank_${shank}_chunk_${chunk_idx}_dredge_${dredge}_${chunk_idx}.log" \
                            "python ${CODEPATH}/detection.py $FILEPATH $dredge $probe $shank $DATAPATH $chunk_idx" | awk '{print $2}' | tr -d '<>')

                if [ -z "$KS_JOB_ID" ]; then
                    echo "Failed to submit detection job for probe $probe shank $shank chunk $chunk_idx dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1
                    exit 1
                fi
            done
        done
    done
done
