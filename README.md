
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

### Chunking or Total
In the /scripts directory there are two versions of the script that can be run from the cluster
`run_pipeline.sh` that runs the pipeline for the full session
`run_chunks_pipeline.sh` that will segment the clips into 1 hour chunks (default and can be changed in [config.py](https://github.com/joannercsheppard/ephys-pipeline/blob/main/pipeline/config.py)).


### Modifying File Locations

Open the [`run_pipeline.sh`](https://github.com/joannercsheppard/ephys-pipeline/blob/fc7846cfe5814c7210c156a436b7ca04fc4b8339/scripts/run_pipeline.sh#L5) or [`run_chunks_pipeline.sh`](https://github.com/joannercsheppard/ephys-pipeline/blob/fc7846cfe5814c7210c156a436b7ca04fc4b8339/scripts/run_chunks_pipeline.sh#L5) script and update the `FILEPATH` variable with the correct file location for your data:

```bash
FILEPATH="/groups/voigts/voigtslab/neuropixels_tests_aug_2024/2024_08_06_npx_long_test/"
```

### Running the Spike Sorting Pipeline

Once everything is set up, you can run the spike sorting pipeline with:

```bash
./run_pipeline.sh
```

### Issues
### BEFORE FIRST RUN: Setting Permissions

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
