#!/bin/bash

CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_09_06_npx_3dmaze/"
DATAPATH="/data_test/"

LOG_DIR="logs_test/$(basename "$FILEPATH")"

# Create the log folder if it doesn't exist
mkdir -p "logs_test"
mkdir -p "$LOG_DIR"

echo "Merging recordings and splitting by shank"
source ~/.bashrc && conda activate $CONDA_ENV
python split.py "$FILEPATH" "$DATAPATH" > "$LOG_DIR/merge_and_split.log"
if [ $? -ne 0 ]; then
    echo "merge_and_split failed"
    exit 1
fi

for probe in a b; do
    for shank in 1 2 3 4; do
        echo "Preprocessing for probe $probe shank $shank"
        python preprocess.py "$FILEPATH" "$probe" "$shank" "$DATAPATH" > "$LOG_DIR/preprocess_${probe}_shank_${shank}.log"
        if [ $? -ne 0 ]; then
            echo "preprocess failed for probe $probe shank $shank"
            exit 1
        fi

        echo "Running motion correction for probe $probe shank $shank"
        python motion.py "$FILEPATH" "$probe" "$shank" "$DATAPATH" > "$LOG_DIR/motion_${probe}_shank_${shank}.log"
        if [ $? -ne 0 ]; then
            echo "motion failed for probe $probe shank $shank"
            exit 1
        fi

        for dredge in true false; do
            echo "Running detection for probe $probe shank $shank dredge $dredge"
            python detection.py "$FILEPATH" "$dredge" "$probe" "$shank" "$DATAPATH" > "$LOG_DIR/detection_${probe}_shank_${shank}_dredge_${dredge}.log"
            if [ $? -ne 0 ]; then
                echo "detection failed for probe $probe shank $shank dredge $dredge"
                exit 1
            fi

            echo "Running analysis for probe $probe shank $shank dredge $dredge"
            python analysis.py "$FILEPATH" "$dredge" "$probe" "$shank" "$DATAPATH" > "$LOG_DIR/analysis_${probe}_shank_${shank}_dredge_${dredge}.log"
            if [ $? -ne 0 ]; then
                echo "analysis failed for probe $probe shank $shank dredge $dredge"
                exit 1
            fi
        done
    done
done
