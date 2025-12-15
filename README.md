
# Spike Sorting Pipeline

This repository contains scripts and setup for running a spike sorting pipeline, including data preprocessing and analysis for our electrophysiology data.
This was created based on voigtslab ephys-pipeline-all repo, which was forked from sci comp git - given the permissions, made a new repo for shared lab work.

## Getting Started

### Cloning the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/voigtslab/ephys-pipeline-lab.git
```

### Creating the Conda Environment

To create the environment necessary for running the pipeline, use the provided `sorting_env.yml` file, or just install the basics yourself:

```bash
conda env create -f sorting_env.yml
```

Recommend updating to KS 4.1.1, which worked well with the initial commit in this repo

### Chunking or Total
In the /scripts directory there are two versions of the script that can be run from the cluster
`run_pipeline.sh` that runs the pipeline for the full session
`run_chunks_pipeline.sh` that will segment the clips into 1 hour chunks (default and can be changed in [config.py](https://github.com/joannercsheppard/ephys-pipeline/blob/main/pipeline/config.py)).

at time of making this repo, currenly we're running shank by shank with submit_all.sh

### Modifying File Locations

Open the [`run_pipeline.sh`](https://github.com/joannercsheppard/ephys-pipeline/blob/fc7846cfe5814c7210c156a436b7ca04fc4b8339/scripts/run_pipeline.sh#L5) or [`run_chunks_pipeline.sh`](https://github.com/joannercsheppard/ephys-pipeline/blob/fc7846cfe5814c7210c156a436b7ca04fc4b8339/scripts/run_chunks_pipeline.sh#L5) script and update the `FILEPATH` variable with the correct file location for your data:

```bash
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_08_06_npx_long_test/"
```

Or if using submit_all.sh just run it from the repo and provide the daily data folder path as user arg input when running the script

### Running the Spike Sorting Pipeline

Once everything is set up, you can run the spike sorting pipeline with:

```bash
ephys-pipeline/run_pipeline.sh
```

or rather as something like so for shank based sorting:

```bash
./submit_all.sh /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```

### Issues
### BEFORE FIRST RUN: Setting Permissions, LF line endings for running on linux

As the cluster is Linux-based, when first running the script you may need to change the file permissions using `chmod +x <filename>`. Changing the permissions will ensure the necessary files are executable:

```bash
cd ephys-pipeline/pipeline/
chmod +x analysis.py config.py detection.py motion.py preprocess.py split.py utils.py
cd ..
cd scripts
chmod +x run_pipeline.sh run_chunk_pipeline.sh
cd ..
cd ..
```

note we can probably set up git attributes to keep line endings consistent in longer run, and automate lf instead of crlf endings for linux compatability
