#!/bin/bash

CONDA_ENV="spikenv"
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_09_06_npx_3dmaze/"
DATAPATH="/data_test/"

LOG_DIR="logs_test/$(basename "$FILEPATH")"

# Create the log folder if it doesn't exist
mkdir -p "logs_test"
mkdir -p "$LOG_DIR"

function check_failure_and_kill() {
    JOB_ID=$1
    if [ "$(bjobs -noheader -o stat $JOB_ID)" == "EXIT" ]; then
        echo "Job ${JOB_ID} failed. Killing all dependent jobs."
        bkill $(bjobs -noheader -o jobid -w "ended(${JOB_ID})")
        exit 1
    fi
}

# Submit merge_and_split job and capture the job ID
echo "Merging recordings and splitting by shank"
MERGE_JOB_ID=$(bsub -J "merge_and_split_$(basename "$FILEPATH")" \
                    -o "${LOG_DIR}/merge_and_split.log" \
                    -n 32 \
                    "bash -c 'source ~/.bashrc && conda activate ${CONDA_ENV} && python split.py ${FILEPATH} ${DATAPATH}'" \
                    | awk '{print $2}' | tr -d '<>')

# Wait for the merge_and_split job to finish before proceeding
echo "Waiting for merge_and_split to finish (Job ID: $MERGE_JOB_ID)"

for probe in a b; do
    for shank in 1 2 3 4; do
        echo "Submitting DREDge and artifact correction for probe ${probe} shank ${shank}"

        # Submit the first processing job
        PROC_JOB_ID=$(bsub -w "done(${MERGE_JOB_ID})" \
                         -J "preprocess_${probe}_shank_${shank}_$(basename "$FILEPATH")" \
                         -o "${LOG_DIR}/preprocess_${probe}_shank_${shank}.log" \
                         -n 32 \
                         "bash -c 'source ~/.bashrc && conda activate ${CONDA_ENV} && python preprocess.py ${FILEPATH} ${probe} ${shank} ${DATAPATH}'" \
                         | awk '{print $2}' | tr -d '<>')

        # Wait for the process job to complete
        wait $PROC_JOB_ID

        # Check for failure in processing and kill dependent jobs if any
        check_failure_and_kill $PROC_JOB_ID

        # Submit DREDge job if processing is successful
        DREDGE_JOB_ID=$(bsub -w "done(${PROC_JOB_ID})" \
                         -J "motion_${probe}_shank_${shank}_$(basename "$FILEPATH")" \
                         -o "${LOG_DIR}/motion_${probe}_shank_${shank}.log" \
                         -n 32 \
                         "bash -c 'source ~/.bashrc && conda activate ${CONDA_ENV} && python motion.py ${FILEPATH} ${probe} ${shank} ${DATAPATH}'" \
                         | awk '{print $2}' | tr -d '<>')

        # Wait for the DREDge job to complete
        wait $DREDGE_JOB_ID

        # Check for failure in DREDge job and kill dependent jobs if any
        check_failure_and_kill $DREDGE_JOB_ID

        for dredge in true false; do

            # Submit KS sorting job and capture the job ID
            KS_JOB_ID=$(bsub -w "done(${DREDGE_JOB_ID})" \
                             -J "detection_${probe}_shank_${shank}_$(basename "$FILEPATH")" \
                             -o "${LOG_DIR}/detection_${probe}_shank_${shank}_dredge_${dredge}.log" \
                             -n 12 \
                             -gpu "num=1" \
                             -q "gpu_tesla" \
                             "bash -c 'source ~/.bashrc && conda activate ${CONDA_ENV} && python detection.py ${FILEPATH} ${dredge} ${probe} ${shank} ${DATAPATH}'" \
                             | awk '{print $2}' | tr -d '<>')

            # Wait for the KS job to complete
            wait $KS_JOB_ID

            # Check for failure in KS job and kill dependent jobs if any
            check_failure_and_kill $KS_JOB_ID

            echo "Submitting recording creation for probe ${probe} shank ${shank}, after KS job ${KS_JOB_ID}"

            # Submit create_recordings.py job with a dependency on KS job completion
            bsub -w "done(${KS_JOB_ID})" \
                 -J "analysis_${probe}_shank_${shank}_$(basename "$FILEPATH")" \
                 -o "${LOG_DIR}/analysis_${probe}_shank_${shank}_dredge_${dredge}.log" \
                 -n 16 \
                 "bash -c 'source ~/.bashrc && conda activate ${CONDA_ENV} && python analysis.py ${FILEPATH} ${dredge} ${probe} ${shank} ${DATAPATH}')"
                 done
    done
done

