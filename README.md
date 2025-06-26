# Spike Sorting Pipeline

A comprehensive pipeline for processing and analyzing electrophysiology data from Neuropixels recordings using Kilosort and SpikeInterface.

## Overview

This repository contains a complete spike sorting pipeline designed for processing Neuropixels data. The pipeline includes:

- **Data preprocessing**: Binary file loading, probe configuration, and signal filtering
- **Spike sorting**: Kilosort-based spike detection and clustering
- **Analysis tools**: Jupyter notebooks for exploring results and motion correction
- **Batch processing**: Automated submission scripts for processing multiple recordings

## Prerequisites

- Linux system with CUDA support
- Conda package manager
- Access to a computing cluster with GPU resources (for batch processing)
- Neuropixels data in binary format with probe configuration files

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/joannercsheppard/ephys-pipeline.git
cd ephys-pipeline
```

### 2. Create the Conda Environment

The pipeline requires a specific conda environment with all necessary dependencies:

```bash
conda env create -f sorting_env.yml
```

This creates an environment named `spikenv` with:
- Python 3.9
- PyTorch with CUDA support
- SpikeInterface for data handling
- Kilosort for spike sorting
- Jupyter for analysis notebooks

### 3. Activate the Environment

```bash
conda activate spikenv
```

## Data Structure

The pipeline expects data organized in the following structure:

```
animal_folder/
├── day_folder/
│   ├── data/
│   │   ├── np2-a-amp-*.bin    # Probe A binary files
│   │   └── np2-b-amp-*.bin    # Probe B binary files
│   ├── a_probe_conf.json      # Probe A configuration
│   ├── b_probe_conf.json      # Probe B configuration
│   └── output/                # Generated output folder
└── ...
```

## Usage

### Single Recording Processing

To process a single recording for one probe:

```bash
bsub -n 12 -gpu "num=1" -q gpu_h200 -o "output.log" -N \
bash -c "source ~/.bashrc && conda activate spikenv && python -u run_probe.py '/path/to/recording/folder/' 'probe_letter'"
```

**Parameters:**
- `recording_folder`: Path to the day folder containing the data
- `probe_letter`: Either 'a' or 'b' for the respective probe

**Example:**
```bash
bsub -n 12 -gpu "num=1" -q gpu_h200 -o "output.log" -N \
bash -c "source ~/.bashrc && conda activate spikenv && python -u run_probe.py '/groups/voigts/voigtslab/neuropixels_2025/npx06/2025_06_12_npx06_day4/' 'a'"
```

### Batch Processing

To process all recordings for an animal across multiple days:

```bash
./submit_all.sh /path/to/animal/folder/
```

**Example:**
```bash
./submit_all.sh /groups/voigts/voigtslab/neuropixels_2025/npx07/
```

This script will:
- Automatically detect all day folders
- Submit jobs for both probes (a and b) in each day
- Use GPU L4 queue with appropriate resource allocation
- Generate separate log files for each job

## Pipeline Components

### Core Processing (`run_probe.py`)

The main processing script performs:

1. **Probe Configuration Loading**: Reads probe geometry and channel mapping from JSON files
2. **Data Collection**: Gathers all binary files for the specified probe
3. **Signal Preprocessing**:
   - Phase shift correction for Neuropixels
   - Bandpass filtering (300-7500 Hz)
   - Common reference subtraction
4. **Kilosort Execution**: Runs spike detection and clustering
5. **Output Generation**: Saves sorted spike data and quality metrics

### Analysis Tools

#### Exploration Notebooks (`explore/`)
- `ksoutput.ipynb`: Analyze Kilosort output and spike quality
- `motion_corr.ipynb`: Motion correction analysis and visualization

#### Analysis Notebooks (`analysis/`)
- `place.ipynb`: Place cell analysis and spatial coding

#### Main Exploration (`explore.ipynb`)
- Comprehensive data exploration and visualization

## Output Structure

After processing, the output folder contains:

```
output/
└── probe_letter/
    ├── probe_letter.bin       # Preprocessed binary data
    ├── probe.prb             # Probe configuration for Kilosort
    ├── rez.mat               # Kilosort results
    ├── spike_times.npy       # Spike timestamps
    ├── spike_clusters.npy    # Cluster assignments
    └── cluster_info.csv      # Cluster quality metrics
```

## Configuration

### Cluster Settings

The pipeline is configured for Janelia's computing cluster:

- **GPU Queue**: `gpu_h200` (single recording) or `gpu_l4` (batch processing)
- **CPU Cores**: 8-12 cores per job
- **Memory**: Automatically allocated based on queue settings

### Probe Configuration

Probe configurations are stored in JSON files with:
- Channel positions and IDs
- Shank information
- Active channel mapping
- Device-specific parameters

## Troubleshooting

### Common Issues

1. **Empty Recording Files**: The pipeline will skip empty files and continue processing
2. **Memory Issues**: Reduce the number of CPU cores if encountering memory errors
3. **GPU Queue**: Ensure you have access to the specified GPU queues

### Log Files

- Single recording: Check `output.log` for processing details
- Batch processing: Check `output_YYYY_MM_DD_probe.log` files

## Contributing

To contribute to this pipeline:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions or issues, please contact the development team or create an issue in the repository.