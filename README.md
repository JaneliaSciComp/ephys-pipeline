
# Spike Sorting Pipeline

This repository contains scripts and setup for running a spike sorting pipeline, including data preprocessing and analysis for our electrophysiology data.

## Getting Started

### Cloning the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/joannercsheppard/ephys-pipeline.git
```

### Creating the Conda Environment

To create the environment necessary for running the pipeline, use the provided `sorting_env.yml` file:

```bash
conda env create -f sorting_env.yml
```

### Run Locally

```bash
conda activate spikenv
run_shank.py {path} {probe} {shank_num}
```

Example:

```bash
run_shank.py /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 b 0
```

### Run Cluster

If first time using on Linux:

```bash
cd ephys-pipeline
chmod +x submit_all.sh run_shank.py
```

Then in terminal: 

```bash
./submit_all.sh {path}
```

Ex.

```bash
./submit_all.sh /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```