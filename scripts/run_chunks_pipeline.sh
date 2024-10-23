#!/bin/bash

# Define paths
CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_09_04_npx_3dmaze/"
DATAPATH="data"
CODEPATH="$HOME/ephys-pipeline/pipeline"
LOG_DIR="$HOME/ephys-pipeline/logs/$(basename "$FILEPATH")"
CHUNK=true

# Create the log folder if it doesn't exist
mkdir -p "$HOME/ephys-pipeline/logs/"
mkdir -p "$LOG_DIR"

echo "Merging recordings and splitting by shank" >> "$LOG_DIR/pipeline.log" 2>&1
source ~/.bashrc && conda activate $CONDA_ENV

# Submit merge_and_split job and capture job ID and number of chunks
MERGE_LOG_FILE="$LOG_DIR/merge_and_split.log"
MERGE_JOB_ID=$(bsub -n 32 -o "$MERGE_LOG_FILE" "python ${CODEPATH}/split.py $FILEPATH $DATAPATH $CHUNK" | awk '{print $2}' | tr -d '<>')

# Check if merge job submitted successfully
if [ -z "$MERGE_JOB_ID" ]; then
    echo "Failed to submit merge_and_split job" >> "$LOG_DIR/pipeline.log" 2>&1
    exit 1
fi

# Wait for the job to finish and then extract the number of chunks from the log
bwait -w "done($MERGE_JOB_ID)" # Wait for the merge job to finish

# Assuming that split.py prints the number of chunks to its log, extract that value
N_CHUNKS=$(grep -oP 'Chunks Processed: \K\d+' "$MERGE_LOG_FILE")

if [ -z "$N_CHUNKS" ]; then
    echo "Failed to determine number of chunks from merge_and_split job" >> "$LOG_DIR/pipeline.log" 2>&1
    exit 1
fi

echo "Detected $N_CHUNKS chunks for each shank" >> "$LOG_DIR/pipeline.log" 2>&1

# Iterate over probes and shanks, then dynamically process chunks
for probe in a b; do
    for shank in 1 2 3 4; do
        for chunk_idx in $(seq 0 $(($N_CHUNKS - 1))); do
            echo "Preprocessing for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
    
            # Submit preprocess job dependent on merge_and_split job completion
            PROC_JOB_ID=$(bsub -w "done($MERGE_JOB_ID)" -n 32 -o "$LOG_DIR/preprocess_${probe}_shank_${shank}_${chunk_idx}.log" \
                        "python ${CODEPATH}/preprocess.py $FILEPATH $probe $shank $DATAPATH $chunk_idx" | awk '{print $2}' | tr -d '<>')
    
            if [ -z "$PROC_JOB_ID" ]; then
                echo "Failed to submit preprocess job for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
                exit 1
            fi
    
            echo "Running motion correction for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
    
            # Submit motion job dependent on preprocess job completion
            DREDGE_JOB_ID=$(bsub -w "done($PROC_JOB_ID)" -n 32 -o "$LOG_DIR/motion_${probe}_shank_${shank}_${chunk_idx}.log" \
                            "python ${CODEPATH}/motion.py $FILEPATH $probe $shank $DATAPATH $chunk_idx" | awk '{print $2}' | tr -d '<>')
    
            if [ -z "$DREDGE_JOB_ID" ]; then
                echo "Failed to submit motion job for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
                exit 1
            fi
    

            for dredge in true false; do
                echo "Running detection for probe $probe shank $shank chunk $chunk_idx dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1

                # Submit detection job dependent on motion job completion
                KS_JOB_ID=$(bsub -w "done($DREDGE_JOB_ID)" -n 12 -gpu "num=1" -q "gpu_tesla" \
                            -o "$LOG_DIR/detection_${probe}_shank_${shank}_chunk_${chunk_idx}_dredge_${dredge}_${chunk_idx}.log" \
                            "python ${CODEPATH}/detection.py $FILEPATH $dredge $probe $shank $chunk_idx $DATAPATH $chunk_idx" | awk '{print $2}' | tr -d '<>')

                if [ -z "$KS_JOB_ID" ]; then
                    echo "Failed to submit detection job for probe $probe shank $shank chunk $chunk_idx dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1
                    exit 1
                fi
            done
        done
    done
done
