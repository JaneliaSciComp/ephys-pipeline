#!/bin/bash

# Define paths
CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_09_04_npx_3dmaze/"
DATAPATH="data"
CODEPATH="$HOME/ephys-pipeline/pipeline"
LOG_DIR="$HOME/ephys-pipeline/logs/$(basename "$FILEPATH")"
CHUNK=false

# Create the log folder if it doesn't exist
mkdir -p "$HOME/ephys-pipeline/logs/"
mkdir -p "$LOG_DIR"

echo "Merging recordings and splitting by shank" >> "$LOG_DIR/pipeline.log" 2>&1
source ~/.bashrc && conda activate $CONDA_ENV

# Submit merge_and_split job and capture job ID
MERGE_JOB_ID=$(bsub -n 32 -o "$LOG_DIR/merge_and_split.log" "python ${CODEPATH}/split.py $FILEPATH $DATAPATH" | awk '{print $2}' | tr -d '<>')

if [ -z "$MERGE_JOB_ID" ]; then
    echo "Failed to submit merge_and_split job" >> "$LOG_DIR/pipeline.log" 2>&1
    exit 1
fi

for probe in a b; do
    for shank in 1 2 3 4; do
        echo "Preprocessing for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1

        # Submit preprocess job dependent on merge_and_split job completion
        PROC_JOB_ID=$(bsub -w "done($MERGE_JOB_ID)" -n 32 -o "$LOG_DIR/preprocess_${probe}_shank_${shank}.log" \
                      "python ${CODEPATH}/preprocess.py $FILEPATH $probe $shank $DATAPATH" | awk '{print $2}' | tr -d '<>')

        if [ -z "$PROC_JOB_ID" ]; then
            echo "Failed to submit preprocess job for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
            exit 1
        fi

        echo "Running motion correction for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1

        # Submit motion job dependent on preprocess job completion
        DREDGE_JOB_ID=$(bsub -w "done($PROC_JOB_ID)" -n 32 -o "$LOG_DIR/motion_${probe}_shank_${shank}.log" \
                         "python ${CODEPATH}/motion.py $FILEPATH $probe $shank $DATAPATH" | awk '{print $2}' | tr -d '<>')

        if [ -z "$DREDGE_JOB_ID" ]; then
            echo "Failed to submit motion job for probe $probe shank $shank" >> "$LOG_DIR/pipeline.log" 2>&1
            exit 1
        fi

        for dredge in true false; do
            echo "Running detection for probe $probe shank $shank dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1

            # Submit detection job dependent on motion job completion
            KS_JOB_ID=$(bsub -w "done($DREDGE_JOB_ID)" -n 12 -gpu "num=1" -q "gpu_tesla" \
                        -o "$LOG_DIR/detection_${probe}_shank_${shank}_dredge_${dredge}.log" \
                        "python ${CODEPATH}/detection.py $FILEPATH $dredge $probe $shank $DATAPATH $CHUNK" | awk '{print $2}' | tr -d '<>')

            if [ -z "$KS_JOB_ID" ]; then
                echo "Failed to submit detection job for probe $probe shank $shank dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1
                exit 1
            fi

            echo "Running analysis for probe $probe shank $shank dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1

            # Submit analysis job dependent on detection job completion
            ANALYSIS_JOB_ID=$(bsub -w "done($KS_JOB_ID)" -n 16 -o "$LOG_DIR/analysis_${probe}_shank_${shank}_dredge_${dredge}.log" \
                              "python ${CODEPATH}/analysis.py $FILEPATH $dredge $probe $shank $DATAPATH" | awk '{print $2}' | tr -d '<>')

            if [ -z "$ANALYSIS_JOB_ID" ]; then
                echo "Failed to submit analysis job for probe $probe shank $shank dredge $dredge" >> "$LOG_DIR/pipeline.log" 2>&1
                exit 1
            fi
        done
    done
done
