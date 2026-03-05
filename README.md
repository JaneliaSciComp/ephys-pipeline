
# Ephys Pipeline

Scripts for spike sorting and pose estimation for neuropixels recordings. The repo lives on the cluster at `/groups/voigts/voigtslab/submit_a_day/ephys-pipeline` and is what gets called when you submit a day.

## Workflow Overview

The typical entry point is `submit_a_day.sh`, which submits both the Kilosort spike sorting and SLEAP pose estimation jobs for a full recording day.

```
submit_a_day.sh <day_directory>
  ├── submit_all.sh     → bsub jobs per probe/shank (Kilosort, GPU a100)
  └── submit_sleap.sh   → bsub job for pose tracking (GPU l4)
```

## Usage

### Submitting a full day on cluster

```bash
bash /groups/voigts/voigtslab/ephys-pipeline/submit_a_day.sh /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```

This expects the day directory to have a `data/` subdirectory. It will:
1. Submit 8 Kilosort jobs (probes a/b × shanks 0–3) via `submit_all.sh`
2. Submit a SLEAP tracking job via `submit_sleap.sh`

Output logs go to `<day_dir>/output/` and `<day_dir>/sleap_output/`.

### Submitting spike sorting only

```bash
bash /groups/voigts/voigtslab/ephys-pipeline/submit_ephys.sh /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```

Submits one `bsub` job per probe (`a`, `b`) per shank (`0`–`3`), each running `run_pipeline.py`. Jobs run on `gpu_a100`, 12 cores, 4 hour wall time. You'll get an email at `$USER@janelia.hhmi.org` on completion or error.

### Submitting SLEAP only

```bash
cd <day_directory>
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_sleap.sh
```

Tracks all `data/compressed*.mp4` files using the square arena SLEAP models. Output `.slp` and `.analysis.h5` files go to `sleap_output/`. To use a different arena model (big maze, minimaze), edit the model paths at the top of `submit_sleap.sh`.

## Repository Structure

```
ephys-pipeline/
├── submit_a_day.sh      # top-level entry point: submits KS + SLEAP for a day
├── submit_all.sh        # submits per-shank Kilosort jobs
├── submit_sleap.sh      # submits SLEAP tracking job
├── submit_postproc.sh   # post-processing submission
├── run_pipeline.py      # per-shank Kilosort pipeline (called by submit_all.sh)
├── run_shank.py         # single-shank runner
├── postproc.py          # post-processing
├── probe_utils.py       # probe geometry utilities
├── sorting_env.yml      # conda environment spec
└── ...
```

## Environment

The spike sorting environment lives at:
```
/groups/voigts/voigtslab/submit_a_day/envs/spikenv411/
```

The SLEAP environment lives at:
```
/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/sleap/
```

To recreate the spike sorting environment locally:
```bash
conda env create -f sorting_env.yml
```

Kilosort 4.1.1 is recommended.

## Cluster Notes

- Jobs run on the Janelia LSF cluster via `bsub`
- Spike sorting: `gpu_a100` queue, 1 GPU, 12 cores, 4 hr wall time
- SLEAP: `gpu_l4` queue, 1 GPU, 5 cores, 36 hr wall time
- The submitter job itself runs on the `short` queue (30 min, 1 core)

### First-time setup / permissions

If scripts aren't executable after cloning:
```bash
chmod +x submit_a_day.sh submit_all.sh submit_sleap.sh submit_postproc.sh
chmod +x run_pipeline.py run_shank.py postproc.py
```

If you see Windows line endings causing issues on Linux:
```bash
dos2unix submit_a_day.sh submit_all.sh submit_sleap.sh
```