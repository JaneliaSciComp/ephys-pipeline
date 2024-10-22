
# Spike Sorting Pipeline

This repository contains scripts and setup for running a spike sorting pipeline, including data preprocessing and analysis for our electrophysiology data.

## Getting Started

### Cloning the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/jvoigts/spikesorting.git
cd spikesorting
```

Then switch to simple_templates branch

```bash
git checkout simple_templates
```

### Creating the Conda Environment

To create the environment necessary for running the pipeline, use the provided `sorting_env.yml` file:

```bash
conda env create -f sorting_env.yml
```

### Modifying File Locations

Open the `run_spike_pipe.sh` script and update the `FILEPATH` variable with the correct file location for your data:

```bash
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_08_06_npx_long_test/"
```

### Setting Permissions

Before running the scripts, ensure the necessary files are executable by setting the correct permissions:

```bash
chmod +x run_dredge.py run_ks.py run_movement_analysis.py run_spike_pipe.sh create_recordings.py
```

### Running the Spike Sorting Pipeline

Once everything is set up, you can run the spike sorting pipeline with:

```bash
./run_spike_pipe.sh
```
