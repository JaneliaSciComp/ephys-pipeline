
# Ephys Pipeline

Scripts for spike sorting and pose estimation for Neuropixels recordings. Designed for use on an HPC cluster with GPU nodes and [Apptainer](https://apptainer.org/) containers, using the [IBM LSF](https://www.ibm.com/docs/en/spectrum-lsf) job scheduler (`bsub`).

## Workflow Overview

The typical entry point is `submit_a_day.sh`, which submits both Kilosort spike sorting and SLEAP pose estimation for a full recording day.

```
submit_a_day.sh <day_directory> <maze>
  ├── submit_ephys.sh  → one bsub job per probe/shank (Kilosort)
  └── submit_sleap.sh  → one bsub job for pose tracking
```

Each Kilosort job runs `run_pipeline.py`, which calls three stages in sequence:
1. `run_shank.py` — preprocessing and Kilosort 4
2. `postproc.py` — UnitRefine SUA/MUA and noise/neural classification
3. `extract_unitmatch_data.py` — raw waveform extraction for UnitMatch

## Usage

### Submit everything for a day

```bash
bash /path/to/ephys-pipeline/submit_a_day.sh <day_directory> <large|box|minimaze>
```

The second argument selects the SLEAP pose estimation model by arena type. The day directory must contain a `data/` subdirectory. Output logs go to `<day_dir>/output/` and `<day_dir>/sleap_output/`.

### Submit spike sorting only

```bash
bash /path/to/ephys-pipeline/submit_ephys.sh <day_directory>
```

Submits one `bsub` job per probe (`a`, `b`) × shank (`0`–`3`), each running `run_pipeline.py`.

### Submit SLEAP only

```bash
cd <day_directory>
bash /path/to/ephys-pipeline/submit_sleap.sh <large|box|minimaze>
```

Tracks all `data/compressed*.mp4` files and writes `.slp` and `.analysis.h5` files to `sleap_output/`.

### Check pipeline completion

```bash
python check_unit_labels.py <day_directory>
```

Reports which shanks have finished post-processing (i.e. have `unit_labels.tsv`).

## Repository Structure

```
ephys-pipeline/
├── submit_a_day.sh            # top-level entry point: submits KS + SLEAP for a day
├── submit_ephys.sh            # submits per-shank Kilosort jobs
├── submit_sleap.sh            # submits SLEAP tracking job
├── run_pipeline.py            # per-shank pipeline orchestrator (called by submit_ephys.sh)
├── run_shank.py               # stage 1: preprocessing + Kilosort 4
├── postproc.py                # stage 2: UnitRefine classification
├── extract_unitmatch_data.py  # stage 3: raw waveform extraction for UnitMatch
├── check/
│   └── check_unit_labels.py   # utility: check pipeline completion for a day
├── pipeline/
│   ├── probe_utils.py         # probe config loading
│   └── get_artifacts.py       # saturation artifact detection
├── containers/
│   ├── spikenv411.def         # Apptainer definition for spike sorting env
│   └── sleap.def              # Apptainer definition for SLEAP env
└── sorting_env.yml            # conda environment spec
```

## Environment

Pre-built Apptainer containers (`.sif` files) are required to run the pipeline on the cluster. Build them from the `.def` files in `containers/`:

```bash
apptainer build containers/spikenv411.sif containers/spikenv411.def
apptainer build containers/sleap.sif containers/sleap.def
```

To recreate the spike sorting environment locally with conda instead:
```bash
conda env create -f sorting_env.yml
```

## Cluster Notes

The submission scripts are written for [IBM LSF](https://www.ibm.com/docs/en/spectrum-lsf) (`bsub`) and will need to be adapted for other schedulers (SLURM, PBS, etc.). Hardcoded cluster paths in the scripts will also need to be updated for your site.

| Task | Cores | GPU | Wall time |
|---|---|---|---|
| KS submitter | 1 | — | 30 min |
| Spike sorting (per shank) | 8 | 1 | 4 hr |
| SLEAP tracking | 12 | 1 | 36 hr |

### First-time setup

If scripts aren't executable after cloning:
```bash
chmod +x submit_a_day.sh submit_ephys.sh submit_sleap.sh
```

If you see Windows line ending issues on Linux:
```bash
dos2unix submit_a_day.sh submit_ephys.sh submit_sleap.sh
```
