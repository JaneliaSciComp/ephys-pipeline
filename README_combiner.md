# Ephys Pipeline

Scripts for spike sorting and pose estimation for neuropixels recordings. The cluster copy lives at:

`/groups/voigts/voigtslab/submit_a_day/ephys-pipeline`

## Workflow Overview

`submit_a_day.sh` is the top-level entrypoint and now chains a combiner job automatically.

```text
submit_a_day.sh <day_directory> <large|box|minimaze>
  ├── submit_ephys.sh                    → submits 8 Kilosort jobs (LSF)
  ├── submit_sleap.sh                    → submits 1 SLEAP job (LSF)
  └── submit_combiner.sh --wait "<expr>" → submits 1 combiner job (LSF dependency)
```

The combiner job is submitted with an LSF dependency expression:

`done(<ks_job_1>) && done(<ks_job_2>) && ... && done(<sleap_job>)`

That means combiner stays pending until all upstream jobs finish successfully.

## Usage

### Submit full day (KS + SLEAP + automatic combiner)

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_a_day.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  large
```

This submits:
1. 8 Kilosort jobs (`submit_ephys.sh`)
2. 1 SLEAP job (`submit_sleap.sh`)
3. 1 combiner job (`submit_combiner.sh`) with `done(...)` dependency on all of the above

`submit_a_day.sh` prints all parsed job IDs for traceability.

### Submit spike sorting only

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

### Submit SLEAP only

```bash
cd <day_directory>
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_sleap.sh <large|box|minimaze>
```

### Submit combiner only (manual recovery)

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_combiner.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  --workers 16
```

If you need to wait on explicit upstream jobs:

```bash
bash /groups/voigts/voigtslab/submit_a_day/ephys-pipeline/submit_combiner.sh \
  /groups/voigts/voigtslab/neuropixels_2025/npx08/2025_12_02_square_arena_02 \
  --workers 16 \
  --wait "done(12345) && done(12346)"
```

## Combiner Container Setup

The combiner runs in `containers/combiner.sif` and executes:

`python /groups/voigts/voigtslab/submit_a_day/voigts/data_loading/combiner_pipeline.py <day_dir> --workers 16 --plot true`

### 1) Create and pack the env on cluster

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

## Monitoring

```bash
bjobs -J "ks_*"
bjobs -J "sleap_*"
bjobs -J "combiner_*"
```

Combiner should show as `PEND` until all dependency jobs are `DONE`.

## Repository Structure

```text
ephys-pipeline/
├── submit_a_day.sh
├── submit_ephys.sh
├── submit_sleap.sh
├── submit_combiner.sh
├── run_pipeline.py
├── run_shank.py
├── postproc.py
└── containers/
    ├── spikenv411.def
    ├── sleap.def
    ├── combiner.def
    └── combiner_requirements.txt
```

## Notes

- All submit scripts use `set -euo pipefail` and fail fast on missing inputs/tools/files.
- Job ID parsing is strict. If `bsub` output cannot be parsed, scripts exit with an error.
- Combiner defaults:
  - workers: `16`
  - plot: `true`
  - dependency policy: success-only (`done(jobid)`)
  - queue: cluster default unless `COMBINER_QUEUE` is set
