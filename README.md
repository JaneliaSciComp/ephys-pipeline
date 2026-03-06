# Ephys Pipeline

Scripts for spike sorting and pose estimation for Neuropixels recordings.

Cluster location:
`/groups/voigts/voigtslab/submit_a_day/ephys-pipeline`

## Quick Commands

### Full day submit (Kilosort + SLEAP + dependency-gated combiner)

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_a_day.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  large
```

### Ephys only

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_ephys.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```

Optional machine-readable IDs:

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_ephys.sh \
  --emit-job-ids \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02
```

### SLEAP only (submit mode)

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_sleap.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  large
```

### Combiner only

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_combiner.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  --workers 16
```

With explicit dependency wait:

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_combiner.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  --workers 16 \
  --wait "done(12345) && done(12346)"
```

## Workflow Overview

`submit_a_day.sh` orchestrates the full pipeline:

```text
submit_a_day.sh <day_directory> <large|box|minimaze>
  ├── submit_ephys.sh                    → submits 8 Kilosort jobs (LSF)
  ├── submit_sleap.sh                    → submits 1 SLEAP job (LSF)
  └── submit_combiner.sh --wait "<expr>" → submits 1 combiner job (LSF dependency)
```

Combiner submission uses an LSF dependency expression:

`done(<ks_job_1>) && done(<ks_job_2>) && ... && done(<sleap_job>)`

So combiner remains `PEND` until all required upstream jobs finish successfully.

## Script Notes

### `submit_a_day.sh`

- Validates inputs and required scripts/containers.
- Submits Kilosort jobs via `submit_ephys.sh --emit-job-ids`.
- Submits one SLEAP job via `submit_sleap.sh <day_dir> <maze>`.
- Submits one combiner job with dependency on all Kilosort + SLEAP job IDs.
- Prints parsed job IDs for traceability.

### `submit_ephys.sh`

- Submits one job per probe/shank (`a|b`, `0..3`) using `run_pipeline.py`.
- Supports `--emit-job-ids` to return `JOB_IDS=...`.
- Strictly parses `bsub` output and exits on parse errors.

### `submit_sleap.sh`

- Submit-only script: schedules one SLEAP LSF job for a day/maze.
- Interface: `submit_sleap.sh <day_dir> <large|box|minimaze>`.
- Emits machine-readable `SLEAP_JOB_ID=<id>`.

### `run_sleap.sh`

- Internal SLEAP runner invoked by `submit_sleap.sh` inside the LSF job.
- Runs `sleap-track` and `sleap-convert` over `data/compressed*.mp4`.
- Writes outputs to `<day_dir>/sleap_output`.

### `submit_combiner.sh`

- Submits combiner job independently or with optional `--wait`.
- Defaults: `--workers 16`, `--plot true`.
- Uses `combiner.sif` and runs `combiner_pipeline.py`.
- Optional env overrides:
  - `COMBINER_QUEUE`
  - `COMBINER_WALLTIME`
  - `COMBINER_MEM_MB`

## Combiner Container Setup

Combiner uses:
`/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/containers/combiner.sif`

Runtime command inside job:
`python /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/combiner_pipeline.py <day_dir> --workers 16 --plot true`

### 1) Create/update and pack the env on cluster

```bash
conda create -y -p /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/combiner python=3.11
/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/combiner/bin/pip install \
  -r /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/containers/combiner_requirements.txt
/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/combiner/bin/pip install conda-pack

/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/combiner/bin/conda-pack \
  --prefix /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/combiner \
  --ignore-missing-files \
  -o /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/containers/combiner_env.tar.gz
```

### 2) Build `combiner.sif`

```bash
cd /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/containers
apptainer build --ignore-fakeroot-command combiner.sif combiner.def
```

## Monitoring and Recovery

### Monitor jobs

```bash
bjobs -J "ks_*"
bjobs -J "sleap_*"
bjobs -J "combiner_*"
```

Combiner should stay pending until dependencies are `DONE`.

### If one Kilosort job fails

- The dependency expression uses `done(jobid)`, so combiner will not run.
- Re-submit failed shank(s), submit combiner directly when outputs are ready.

## Environment

Spike sorting env:
`/groups/voigts/voigtslab/submit_a_day/envs/spikenv411/`

SLEAP env:
`/groups/voigts/voigtslab/submit_a_day/ephys-pipeline/envs/sleap/`

To recreate spike-sorting env locally:

```bash
conda env create -f sorting_env.yml
```

Kilosort 4.1.1 is recommended.

## Repository Structure

```text
ephys-pipeline/
├── submit_a_day.sh
├── submit_ephys.sh
├── submit_sleap.sh
├── run_sleap.sh
├── submit_combiner.sh
├── run_pipeline.py
├── run_shank.py
├── postproc.py
├── probe_utils.py
├── sorting_env.yml
└── containers/
    ├── spikenv411.def
    ├── sleap.def
    ├── combiner.def
    └── combiner_requirements.txt
```

## Cluster Notes

- Jobs run on Janelia LSF via `bsub`.
- Ephys shank jobs: `gpu_l4`, 1 GPU, 8 CPU cores, 8 hour wall time.
- SLEAP day job: `gpu_a100`, 1 GPU, 12 CPU cores, 36 hour wall time.
- Combiner queue defaults to cluster default unless `COMBINER_QUEUE` is set.

## First-time Setup / Permissions

If scripts are not executable after cloning:

```bash
chmod +x submit_a_day.sh submit_ephys.sh submit_sleap.sh run_sleap.sh submit_combiner.sh
chmod +x run_pipeline.py run_shank.py postproc.py
```

If you hit Windows line-ending issues on Linux:

```bash
dos2unix submit_a_day.sh submit_ephys.sh submit_sleap.sh run_sleap.sh submit_combiner.sh
```

## Reliability Notes

- All submit scripts use `set -euo pipefail`.
- Submit scripts fail fast on missing files/tools/paths.
- Job ID parsing is strict. If `bsub` output is unexpected, scripts exit with an error.
