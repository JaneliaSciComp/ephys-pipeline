#!/bin/bash

# Set the base directory from the first argument
base_dir="$1"

# Check if the base directory exists
if [ ! -d "$base_dir" ]; then
    echo "Directory does not exist: $base_dir"
    exit 1
fi

# Loop through each subdirectory (ending in /)
for day_dir in "$base_dir"*/; do
    for probe in a b; do
        dir_name=$(basename "$day_dir")
        output_file="output_${dir_name}_${probe}.log"

        bsub -n 8 -gpu "num=1" -q gpu_l4 \
             -o "$output_file" -N -u sheppardj@janelia.hhmi.org \
             bash -c "source ~/.bashrc && conda activate spikenv && python -u run_probe.py '${day_dir}' '${probe}' 'npx07'"
    done
done
