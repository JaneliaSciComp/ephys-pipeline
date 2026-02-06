#!/bin/bash

# Check if an argument was provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <day_directory>"
    echo "Example: $0 /path/to/day/directory"
    exit 1
fi

# Set the day directory from the first argument
day_dir="$1"

# Check if the day directory exists
if [ ! -d "$day_dir" ]; then
    echo "Directory does not exist: $day_dir"
    exit 1
fi

# user will get emailed here
user="${USER:-$(whoami)}"
email="${user}@janelia.hhmi.org"

for probe in a b; do
    for shank_num in 0 1 2 3; do
        dir_name=$(basename "$day_dir")
        output_file="output/output_${dir_name}_${probe}_shank${shank_num}.log"

        echo "Submitting job for ${day_dir}, probe ${probe}, shank ${shank_num}"
        
        bsub -n 12 -gpu "num=1" -q gpu_a100 \
                -o "$output_file" -N -u "$email" \
                bash -c "source ~/.bashrc && conda activate spikenv411 && python -u run_shank.py '${day_dir}' '${probe}' '${shank_num}' && python postproc.py '${day_dir}' '${probe}' '${shank_num}' && python extract_unit_data.py --data_dir '${day_dir}' --probe '${probe}' --shank '${shank_num}'"
        
        if [ $? -eq 0 ]; then
            echo "Job submitted successfully for probe ${probe}, shank ${shank_num}"
        else
            echo "Failed to submit job for probe ${probe}, shank ${shank_num}"
        fi
    done
done

echo "All jobs submitted!"
echo "Will email ${email} upon job completion/error."
