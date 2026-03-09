# Ephys Pipeline

Scripts for spike sorting and pose estimation for Neuropixels recordings.

## Getting Started

Two options for setting up the environments. Both require the repo to be cloned and `containers/` to be present.

### Option A: Apptainer containers (recommended on cluster)

Build all three `.sif` containers once — see [Container Setup](#container-setup) below for full steps. In short:

```bash
cd <repo_root>/containers
apptainer build --ignore-fakeroot-command spikenv411.sif spikenv411.def
apptainer build --ignore-fakeroot-command sleap.sif sleap.def
apptainer build --ignore-fakeroot-command combiner.sif combiner.def
```

### Option B: Conda environments

```bash
# Spike sorting
conda env create -f sorting_env.yml

# SLEAP — install separately into its own env
conda create -n sleap ...  # see sleap.def comments for packages

# Combiner
conda create -y -p envs/combiner python=3.11
envs/combiner/bin/pip install -r containers/combiner_requirements.txt
```

### Run the pipeline

Once environments are ready, submit a full day:

```bash
bash <repo_root>/submit_a_day.sh <day_directory> <large|box|minimaze>
```

---

## Quick Commands

### Full day submit (Kilosort + SLEAP + dependency-gated combiner)

```bash
bash <repo_root>/submit_a_day.sh <day_directory> large
```

### Ephys only

```bash
bash <repo_root>/submit_ephys.sh <day_directory>
```

Optional machine-readable IDs:

```bash
bash <repo_root>/submit_ephys.sh --emit-job-ids <day_directory>
```

### SLEAP only (submit mode)

```bash
bash <repo_root>/submit_sleap.sh <day_directory> <large|box|minimaze>
```

### Combiner only

```bash
bash <repo_root>/submit_combiner.sh <day_directory> --workers 16
```

With explicit dependency wait:

```bash
bash <repo_root>/submit_combiner.sh <day_directory> --workers 16 --wait "done(12345) && done(12346)"
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

## Container Setup

All three containers follow the same pattern: pack the conda environment with `conda-pack`, then build the Apptainer `.sif` from the `.def` file in `containers/`.

### Spike-sorting container (`spikenv411.sif`)

#### 1) Install/update packages and pack the env

```bash
PYTHONNOUSERSITE=1 <env_root>/spikenv411/bin/pip install --no-user \
  spikeinterface==0.103.2 kilosort==4.1.1 numba==0.60.0 pandas==2.2.3 \
  huggingface_hub==1.4.0 skops==0.13.0
<env_root>/spikenv411/bin/pip install conda-pack

<env_root>/spikenv411/bin/conda-pack \
  --prefix <env_root>/spikenv411 \
  --ignore-missing-files \
  -o <repo_root>/containers/spikenv411_env.tar.gz
```

#### 2) Build `spikenv411.sif`

```bash
cd <repo_root>/containers
apptainer build --ignore-fakeroot-command spikenv411.sif spikenv411.def
```

---

### SLEAP container (`sleap.sif`)

#### 1) Pack the env

```bash
<env_root>/sleap/bin/pip install conda-pack

<env_root>/sleap/bin/conda-pack \
  --prefix <env_root>/sleap \
  -o <repo_root>/containers/sleap_env.tar.gz
```

#### 2) Build `sleap.sif`

```bash
cd <repo_root>/containers
apptainer build --ignore-fakeroot-command sleap.sif sleap.def
```

---

### Combiner container (`combiner.sif`)

#### 1) Create/update and pack the env

```bash
conda create -y -p <env_root>/combiner python=3.11
<env_root>/combiner/bin/pip install -r <repo_root>/containers/combiner_requirements.txt
<env_root>/combiner/bin/pip install conda-pack

<env_root>/combiner/bin/conda-pack \
  --prefix <env_root>/combiner \
  --ignore-missing-files \
  -o <repo_root>/containers/combiner_env.tar.gz
```

#### 2) Build `combiner.sif`

```bash
cd <repo_root>/containers
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

Spike sorting env: `<env_root>/spikenv411/`

SLEAP env: `<env_root>/sleap/`

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

- Jobs run on an LSF cluster via `bsub`.
- Ephys shank jobs: 1 GPU, 8 CPU cores, 8 hour wall time.
- SLEAP day job: 1 GPU, 12 CPU cores, 36 hour wall time.
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
