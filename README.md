# Ephys Pipeline

Scripts for spike sorting and pose estimation for Neuropixels recordings. Runs on an LSF cluster via `bsub`.

## Prerequisites

- LSF cluster with GPU nodes
- [Apptainer](https://apptainer.org) (for container builds) **or** conda
- NVIDIA GPU with CUDA 11.8+ support

## Setup

Clone the repo, then `cd` into it. All commands below use `$REPO` and `$ENVS` which are set from your current directory — no paths to fill in.

```bash
git clone https://github.com/JaneliaSciComp/ephys-pipeline.git
cd ephys-pipeline
REPO=$(pwd)
ENVS=$REPO/envs
```

### Option A: Apptainer containers (recommended)

Each container needs a packed conda env first, then an Apptainer build. Run once per container.

**Spike-sorting (`spikenv411.sif`)**

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

## Usage

### Full pipeline (Kilosort + SLEAP + combiner)

```bash
bash $REPO/submit_a_day.sh <day_directory> <large|box|minimaze>
```

### Ephys (Kilosort) only

```bash
bash $REPO/submit_ephys.sh <day_directory>
```

### SLEAP only

```bash
bash $REPO/submit_sleap.sh <day_directory> <large|box|minimaze>
```

### Combiner only

```bash
bash $REPO/submit_combiner.sh <day_directory> --workers 16
```

With explicit job dependency:

```bash
bash $REPO/submit_combiner.sh <day_directory> --workers 16 --wait "done(12345) && done(12346)"
```

## How It Works

`submit_a_day.sh` orchestrates the full pipeline:

```
submit_a_day.sh <day_directory> <large|box|minimaze>
  ├── submit_ephys.sh                    → 8 Kilosort jobs (one per probe/shank)
  ├── submit_sleap.sh                    → 1 SLEAP job
  └── submit_combiner.sh --wait "<expr>" → 1 combiner job, held until above finish
```

The combiner uses an LSF dependency expression so it stays `PEND` until all upstream jobs are `DONE`:

```
done(<ks_job_1>) && done(<ks_job_2>) && ... && done(<sleap_job>)
```

## Monitoring and Recovery

```bash
bjobs -J "ks_*"
bjobs -J "sleap_*"
bjobs -J "combiner_*"
```

If a Kilosort job fails, the combiner will not run. Re-submit the failed shank(s), then submit the combiner directly once outputs are ready.

## Repository Structure

```
ephys-pipeline/
├── submit_a_day.sh         # full pipeline orchestrator
├── submit_ephys.sh         # submits Kilosort jobs
├── submit_sleap.sh         # submits SLEAP job
├── run_sleap.sh            # SLEAP runner (called inside LSF job)
├── submit_combiner.sh      # submits combiner job
├── run_pipeline.py         # per-shank pipeline entry point
├── run_shank.py            # shank-level spike sorting
├── postproc.py             # post-processing
├── probe_utils.py          # probe/shank utilities
├── sorting_env.yml         # conda env spec for spike sorting
└── containers/
    ├── spikenv411.def       # Apptainer def for spike sorting
    ├── sleap.def            # Apptainer def for SLEAP
    ├── combiner.def         # Apptainer def for combiner
    └── combiner_requirements.txt
```

## Cluster Resource Defaults

| Job | GPUs | CPUs | Wall time |
|-----|------|------|-----------|
| Kilosort (per shank) | 1 | 8 | 8 h |
| SLEAP | 1 | 12 | 36 h |
| Combiner | — | 16 | cluster default |

Combiner queue can be overridden with env vars: `COMBINER_QUEUE`, `COMBINER_WALLTIME`, `COMBINER_MEM_MB`.
