# Ephys Pipeline

An end-to-end pipeline for spike sorting and pose estimation of Neuropixels recordings acquired with the OpenEphys system. Jobs are submitted to an LSF cluster via `bsub`.

---

## Features

| | |
|---|---|
| **Spike sorting** | Kilosort4 with SpikeInterface preprocessing, run independently per shank |
| **Artifact removal** | Curated cleaning for motion artifacts in freely behaving animals |
| **Unit curation** | UnitRefine labels units as SUA, MUA, or noise automatically |
| **Unit tracking** | Waveforms exported in UnitMatch format for cross-session matching |
| **Pose estimation** | SLEAP extracts animal pose trajectories from synchronized video |
| **Multimodal fusion** | Neural, pose, and BNO (IMU) data aligned to a single Parquet file |
| **Cluster execution** | LSF job submission via `bsub` with automatic dependency management |

---

## How It Works

```
  .raw .prb files (neural)           .mp4 files (video)      .csv /.raw files (BNO + NP clock)
        │                                  │                                │
        ▼                                  ▼                                │
  ┌─────────────────────────┐    ┌──────────────────────┐                   │
  │   Ephys  (per shank,    │    │        SLEAP         │                   │
  │   run in parallel)      │    │                      │                   │
  │                         │    │  1. Pose estimation  │                   │
  │  1. Preproc (SI)        │    │     on video frames  │                   │
  │  2. Artifact removal    │    │                      │                   │
  │  3. Kilosort4           │    └──────────┬───────────┘                   │
  │  4. Quality metrics     │               │                               │
  │  5. UnitRefine curation │               │                               │
  │  6. Waveform export     │               │                               │
  │     (UnitMatch format)  │               │                               │
  └───────────┬─────────────┘               │                               │
              │                             │                               │
              └─────────────────────────────┬───────────────────────────────┘ 
                                            │               
                                            ▼
                            ┌──────────────────────────────┐
                            │           Combiner           │
                            │                              │
                            │  Synchronise all streams     │
                            │  using NP clock timestamps   │
                            │                              │
                            │  Neural + Pose + BNO (IMU)   │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
                              processed_data-<timestamp>/
                              final_df_<recording_id>.parquet
```

## Prerequisites

- LSF cluster with GPU nodes
- [Apptainer](https://apptainer.org) (for container builds) **or** conda
- NVIDIA GPU with CUDA 11.8+ support

---

## Inputs Required

Before running, make sure you have the following for each recording day:

| Input | Description |
|-------|-------------|
| `.bin` files | Binary neural data from OpenEphys, one per probe/shank |
| Probe file | Probe geometry file (`.prb` or equivalent) |
| `.mp4` files | Behavioural video files for SLEAP pose estimation |
| `.csv` files (BNO) | IMU data capturing head orientation and movement |
| `.raw` files (NP clock) | Neuropixels clock timestamps for synchronisation |

---

## Setup

Clone the repo and set two shell variables used throughout these instructions — no manual paths needed.

```bash
git clone https://github.com/JaneliaSciComp/ephys-pipeline.git
cd ephys-pipeline
REPO=$(pwd)
ENVS=$REPO/envs
```

### Option A: Apptainer containers (recommended)

Containers isolate each pipeline stage and are the most reproducible option. Each one requires a packed conda environment followed by an Apptainer build — run these once.

**Spike sorting (`spikenv411.sif`)**

```bash
conda create -y -p $ENVS/spikenv411 python=3.11
PYTHONNOUSERSITE=1 $ENVS/spikenv411/bin/pip install --no-user \
  spikeinterface==0.103.2 kilosort==4.1.1 numba==0.60.0 pandas==2.2.3 \
  huggingface_hub==1.4.0 skops==0.13.0
$ENVS/spikenv411/bin/pip install conda-pack
$ENVS/spikenv411/bin/conda-pack \
  --prefix $ENVS/spikenv411 --ignore-missing-files \
  -o $REPO/containers/spikenv411_env.tar.gz

cd $REPO/containers && apptainer build --ignore-fakeroot-command spikenv411.sif spikenv411.def
```

**SLEAP (`sleap.sif`)**

```bash
conda create -y -n sleap python=3.8
# install SLEAP into the env — see https://sleap.ai/installation.html
conda activate sleap && pip install conda-pack
conda-pack -n sleap -o $REPO/containers/sleap_env.tar.gz

cd $REPO/containers && apptainer build --ignore-fakeroot-command sleap.sif sleap.def
```

**Combiner (`combiner.sif`)**

```bash
conda create -y -p $ENVS/combiner python=3.11
$ENVS/combiner/bin/pip install -r $REPO/containers/combiner_requirements.txt
$ENVS/combiner/bin/pip install conda-pack
$ENVS/combiner/bin/conda-pack \
  --prefix $ENVS/combiner --ignore-missing-files \
  -o $REPO/containers/combiner_env.tar.gz

cd $REPO/containers && apptainer build --ignore-fakeroot-command combiner.sif combiner.def
```

### Option B: Conda environments only

If Apptainer is unavailable, environments can be used directly:

```bash
# Spike sorting
conda env create -f $REPO/sorting_env.yml

# SLEAP — see https://sleap.ai/installation.html
conda create -n sleap ...

# Combiner
conda create -y -p $ENVS/combiner python=3.11
$ENVS/combiner/bin/pip install -r $REPO/containers/combiner_requirements.txt
```

### Permissions

If scripts are not executable after cloning:

```bash
chmod +x $REPO/submit_a_day.sh $REPO/submit_ephys.sh $REPO/submit_sleap.sh \
         $REPO/run_sleap.sh $REPO/submit_combiner.sh
```

---

## Usage

### Run the full pipeline

This is the normal entry point. It submits spike sorting, SLEAP, and the combiner in one go, with the combiner automatically held until the other jobs finish.

```bash
bash $REPO/submit_a_day.sh <day_directory> <large|box|minimaze>
```

The arena argument (`large`, `box`, or `minimaze`) selects the SLEAP model to use.

### Run individual stages

You can also submit each stage independently:

```bash
# Spike sorting (Kilosort4) only
bash $REPO/submit_ephys.sh <day_directory>

# SLEAP only
bash $REPO/submit_sleap.sh <day_directory> <large|box|minimaze>

# Combiner only
bash $REPO/submit_combiner.sh <day_directory> --workers 16

# Combiner with an explicit LSF job dependency
bash $REPO/submit_combiner.sh <day_directory> --workers 16 --wait "done(12345) && done(12346)"
```

---


`submit_a_day.sh` submits all three stages and wires them together via LSF job dependencies — the combiner stays `PEND` until every upstream job succeeds:

```
done(<ks_job_1>) && done(<ks_job_2>) && ... && done(<sleap_job>)
```

---

## Monitoring and Recovery

Check job status by name:

```bash
bjobs -J "ks_*"       # spike sorting jobs
bjobs -J "sleap_*"    # SLEAP job
bjobs -J "combiner_*" # combiner job
```

**If a Kilosort job fails:** the combiner will not run (LSF dependency is not met). Fix the issue, re-submit only the failed shank(s) using `submit_ephys.sh`, then submit the combiner manually once all shank outputs are present.


## Output Structure

### Spike sorting (per shank)

Written to `<day_directory>/output/<probe>/shank_<N>/kilosort4/`:

```
kilosort4/
├── spike_times.npy               # spike times for all clusters
├── spike_clusters.npy            # cluster assignment per spike
├── amplitudes.npy                # spike amplitudes
├── spike_positions.npy           # spike positions on probe
├── cluster_KSLabel.tsv           # Kilosort quality labels
├── sorting_analyzer.zarr/        # SpikeInterface SortingAnalyzer with computed extensions
├── unitrefine_input_metrics.tsv  # quality + template metrics fed to UnitRefine
├── unit_labels.tsv               # UnitRefine predictions (SUA / MUA / noise)
└── RawWaveforms/
    ├── Unit0_RawSpikes.npy       # median waveform per unit (shape: 61 × 96 channels × 2 halves)
    ├── Unit1_RawSpikes.npy
    └── ...
```

If a shank has no usable spikes, a `NO_GOOD_SPIKES` sentinel file is written and that shank is skipped by the combiner.

### Combiner

Written to `<day_directory>/processed_data-<YYYY_MM_DD_HH_MM_SS>/`:

```
processed_data-<timestamp>/
├── final_df_<recording_id>.parquet      # aligned neural + pose + BNO data
├── neural_labels_<recording_id>.parquet # SUA / MUA / noise unit classifications
├── metadata.json                        # recording_id, data masks, loader config, data paths
├── process_<recording_name>.log         # processing log with timestamps
├── data_existence_over_time.png         # temporal availability of each data modality
├── pose_cleaner_QC.png                  # pose cleaning QC plot
├── pose_cleaner_QC_jumps.png            # jump-artifact QC plot
└── raw_data/                            # pre-processing intermediates (if save_raw=True)
    ├── raw_pose_<index>.parquet
    ├── raw_bno_<index>.parquet
    └── raw_neural_<dataset_name>.parquet
```

The output directory is timestamped to avoid overwriting previous runs. `recording_id` is derived from the last two components of the input path joined with `_`.
