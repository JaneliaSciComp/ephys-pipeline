#!/usr/bin/env python3
"""
Trial runner for preprocessing pipeline experiments.

Accepts a YAML config that defines an ordered list of preprocessing steps,
runs the pipeline on a time-sliced excerpt of one shank, saves output to
  <day_dir>/processing_pipeline_trials/<trial_name>/
and writes metrics.json + trial_info.json for later comparison.

Usage:
    python run_trial.py <day_dir> <probe> <shank_num> <config.yaml>
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

import spikeinterface.full as si
import spikeinterface.extractors as se
import spikeinterface.preprocessing as spre
from kilosort import io, run_kilosort
from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts

from utils.get_artifacts import detect_saturation_periods, remove_saturation_artifacts
from utils.probe_utils import load_probe

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384
TRIALS_DIR = "processing_pipeline_trials"

# ── Default pipeline (reproduces the current run_shank.py behaviour exactly) ──
DEFAULT_PIPELINE = [
    {"step": "slice_time",                     "start_time_s": 0, "duration_s": 3600},
    {"step": "remove_artifacts",               "threshold": 3900, "ms_before": 50, "ms_after": 50},
    {"step": "highpass_filter",                "ftype": "bessel"},
    {"step": "phase_shift"},
    {"step": "split_shank"},                   # must come after phase_shift
    {"step": "global_cmr",                     "operator": "median"},
    {"step": "detect_interpolate_bad_channels","methods": ["coherence+psd", "mad"]},
    {"step": "local_cmr"},
    {"step": "bandpass_filter",                "freq_min": 300.0, "freq_max": 7500.0},
]
DEFAULT_KS_BATCH_SIZE = 30000

UNITREFINE_MODELS = [
    "SpikeInterface/UnitRefine_sua_mua_classifier_lightweight",
    "SpikeInterface/UnitRefine_noise_neural_classifier_lightweight",
]


# ── Config loading ─────────────────────────────────────────────────────────────

def load_config(yaml_path: Path) -> dict:
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)
    if not cfg.get("trial_name"):
        raise ValueError("Config must have a non-empty 'trial_name'")
    cfg.setdefault("pipeline", DEFAULT_PIPELINE)
    cfg.setdefault("ks_batch_size", DEFAULT_KS_BATCH_SIZE)
    cfg.setdefault("ks_extra_settings", {})
    cfg.setdefault("description", "")
    cfg.setdefault("n_jobs", 12)
    return cfg


def compute_diff(cfg: dict) -> dict:
    """Compare trial config against defaults; return human-readable diff."""
    default_by_name = {s["step"]: s for s in DEFAULT_PIPELINE}
    trial_by_name   = {s["step"]: s for s in cfg["pipeline"]}

    default_order = [s["step"] for s in DEFAULT_PIPELINE]
    trial_order   = [s["step"] for s in cfg["pipeline"]]

    diff: dict = {}

    added   = [s for s in trial_order   if s not in default_by_name]
    removed = [s for s in default_order if s not in trial_by_name]
    if added:
        diff["steps_added"] = added
    if removed:
        diff["steps_removed"] = removed
    if trial_order != default_order and not added and not removed:
        diff["step_order_changed"] = {"default": default_order, "trial": trial_order}

    param_diffs: dict = {}
    for name, trial_step in trial_by_name.items():
        default_step = default_by_name.get(name, {})
        for key, val in trial_step.items():
            if key == "step":
                continue
            if key not in default_step or default_step[key] != val:
                param_diffs.setdefault(name, {})[key] = {
                    "default": default_step.get(key, "<not in default>"),
                    "trial": val,
                }
    if cfg["ks_batch_size"] != DEFAULT_KS_BATCH_SIZE:
        param_diffs["kilosort"] = {"ks_batch_size": {
            "default": DEFAULT_KS_BATCH_SIZE, "trial": cfg["ks_batch_size"],
        }}
    if param_diffs:
        diff["param_changes"] = param_diffs

    return diff


def get_git_hash() -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).parent, capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


# ── Pipeline executor ──────────────────────────────────────────────────────────

def run_pipeline(
    recording: si.BaseRecording,
    steps: list[dict],
    probe_data,
    shank_num: str,
    shank_probe,
    n_jobs: int,
    trial_dir: Path | None = None,
) -> si.BaseRecording:
    """Execute the ordered list of preprocessing steps defined in the config."""

    step_names = [s["step"] for s in steps]
    if "split_shank" not in step_names:
        raise ValueError("Pipeline must include a 'split_shank' step")
    if "phase_shift" in step_names:
        if step_names.index("split_shank") < step_names.index("phase_shift"):
            raise ValueError("'split_shank' must come after 'phase_shift'")

    has_split = False

    for step_cfg in steps:
        name = step_cfg["step"]
        print(f"  [{name}]", end="  ", flush=True)

        if name == "slice_time":
            start_s    = step_cfg.get("start_time_s", 0)
            duration_s = step_cfg.get("duration_s", 3600)
            f0 = int(start_s * SAMPLE_RATE)
            f1 = min(int((start_s + duration_s) * SAMPLE_RATE), recording.get_num_frames())
            recording = recording.frame_slice(start_frame=f0, end_frame=f1)
            print(f"{start_s:.0f}–{f1/SAMPLE_RATE:.0f} s  ({recording.get_total_duration():.1f} s)")

        elif name == "remove_artifacts":
            sat_periods = detect_saturation_periods(
                recording,
                abs_threshold=step_cfg.get("threshold", 3900),
                direction="upper",
                chunk_size=30000 * 10,
                n_jobs=n_jobs,
            )
            n_events = sum(len(s) // 2 for s in sat_periods)
            print(f"{n_events} saturation events")
            recording = remove_saturation_artifacts(
                recording, sat_periods,
                ms_before=step_cfg.get("ms_before", 50),
                ms_after=step_cfg.get("ms_after", 50),
                mode=step_cfg.get("mode", "zeros"),
            )

        elif name == "highpass_filter":
            ftype = step_cfg.get("ftype", "bessel")
            recording = si.highpass_filter(recording, ftype=ftype, dtype="float32")
            print(f"ftype={ftype}")

        elif name == "phase_shift":
            shifts = get_neuropixels_sample_shifts(N_CHANNELS_PROBE, 16, 16)
            recording = spre.phase_shift(recording, inter_sample_shift=shifts)
            recording.set_property("group", probe_data.shank_ids)
            print("done")

        elif name == "split_shank":
            rec_split = recording.split_by("group")
            shank_key = str(shank_num)
            if shank_key not in rec_split:
                shank_key = int(shank_num)
            if shank_key not in rec_split:
                raise KeyError(
                    f"Shank {shank_num!r} not in recording groups: {list(rec_split.keys())}"
                )
            recording = rec_split[shank_key]
            recording = recording.set_probe(shank_probe, in_place=True)
            recording.set_channel_gains(1)
            recording.set_channel_offsets(0)
            has_split = True
            print(f"{recording.get_num_channels()} channels")

        elif name == "global_cmr":
            operator = step_cfg.get("operator", "median")
            recording = si.common_reference(recording, operator=operator, reference="global")
            print(f"operator={operator}")

        elif name == "highpass_spatial_filter":
            recording = si.highpass_spatial_filter(recording, dtype="int16", n_jobs=n_jobs)
            print("done")

        elif name == "correct_motion":
            preset = step_cfg.get("preset", "nonrigid_accurate")
            estimate_motion_kwargs = step_cfg.get("estimate_motion_kwargs", {})
            extra = f"  estimate_motion_kwargs={estimate_motion_kwargs}" if estimate_motion_kwargs else ""
            print(f"preset={preset}  n_jobs={n_jobs}{extra}  (estimating motion, this takes a while...)")
            n_frames_before = recording.get_num_frames()
            motion_folder = (trial_dir / "motion") if trial_dir else None
            try:
                kwargs = {"preset": preset, "n_jobs": n_jobs}
                if estimate_motion_kwargs:
                    kwargs["estimate_motion_kwargs"] = estimate_motion_kwargs
                if motion_folder:
                    kwargs["folder"] = motion_folder
                result = spre.correct_motion(recording, **kwargs)
                recording = result[0] if isinstance(result, tuple) else result
            except AttributeError:
                raise RuntimeError(
                    "spre.correct_motion not found — check your SpikeInterface version. "
                    "Available in spikeinterface >= 0.100."
                )
            # MotionCorrectedRecording can silently expand to the full parent
            # duration — re-apply the original frame slice if that happened.
            if recording.get_num_frames() != n_frames_before:
                recording = recording.frame_slice(start_frame=0, end_frame=n_frames_before)
                print(f"  (re-applied frame slice: {n_frames_before/SAMPLE_RATE:.0f} s)")
            # Generate KS4-style drift plots from the saved motion data
            if motion_folder and motion_folder.exists():
                try:
                    sys.path.insert(0, str(Path(__file__).parent))
                    from make_dredge_plots import _load_motion_arrays, plot_drift_amount, plot_drift_scatter
                    motion_arr, t_bins, sp_bins = _load_motion_arrays(motion_folder)
                    ks_dir = trial_dir / "kilosort4"
                    ks_dir.mkdir(exist_ok=True)
                    plot_drift_amount(motion_arr, t_bins, sp_bins,
                                      f"DREDge motion ({preset})", ks_dir / "drift_amount.png")
                    plot_drift_scatter(motion_folder, t_bins,
                                       f"DREDge scatter ({preset})", ks_dir / "drift_scatter.png")
                    print("  Saved DREDge drift_amount.png + drift_scatter.png")
                except Exception as e:
                    print(f"  Warning: could not save drift plots: {e}")

        elif name == "detect_interpolate_bad_channels":
            methods = step_cfg.get("methods", ["coherence+psd", "mad"])
            bad_ids: list = []
            for method in methods:
                _, labels = si.detect_bad_channels(recording, method=method, seed=42)
                if method == "coherence+psd":
                    mask = labels == "dead"
                elif method == "mad":
                    mask = labels == "noise"
                else:
                    mask = (labels != "good") & (labels != "out")
                ids = recording.get_channel_ids()[mask]
                bad_ids.extend(ids.tolist())
                print(f"\n    {method}: {mask.sum()} bad", end="  ")
            bad_ids_arr = np.unique(bad_ids)
            if len(bad_ids_arr) > 0:
                recording = si.interpolate_bad_channels(recording, bad_ids_arr)
            print(f"→ {len(bad_ids_arr)} interpolated")

        elif name == "local_cmr":
            recording = si.common_reference(recording, reference="local")
            print("done")

        elif name == "bandpass_filter":
            freq_min = step_cfg.get("freq_min", 300.0)
            freq_max = step_cfg.get("freq_max", 7500.0)
            recording = spre.bandpass_filter(
                recording, freq_min=freq_min, freq_max=freq_max, dtype="int16"
            )
            print(f"{freq_min}–{freq_max} Hz")

        else:
            raise ValueError(f"Unknown pipeline step: {name!r}")

    return recording


# ── Postprocessing (re-uses pipeline/postproc.py functions) ───────────────────

def run_postproc(trial_dir: Path, shank_probe, n_channels_shank: int, n_jobs: int):
    sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
    from postproc import (
        compute_sorting_analyzer,
        save_unitrefine_dataset,
        apply_unitrefine_classification,
        save_results,
    )
    import pandas as pd

    recording = se.read_binary(
        str(trial_dir / "shank_recording.bin"),
        sampling_frequency=SAMPLE_RATE,
        num_channels=n_channels_shank,
        dtype="int16",
        offset_to_uV=0.0,
        gain_to_uV=1.0,
    )
    recording = recording.set_probe(shank_probe)
    sorting = se.read_phy(folder_path=str(trial_dir / "kilosort4"))

    print(f"  recording: {recording.get_num_channels()} ch  {recording.get_total_duration():.1f} s")
    print(f"  sorting:   {len(sorting.unit_ids)} units")

    analyzer = compute_sorting_analyzer(recording, sorting, n_jobs=n_jobs)
    save_unitrefine_dataset(analyzer, trial_dir)

    all_labels = []
    for model_id in UNITREFINE_MODELS:
        labels, _ = apply_unitrefine_classification(analyzer, model_id)
        all_labels.append(labels)

    save_results(analyzer, pd.concat(all_labels, axis=1), trial_dir, models_to_run=UNITREFINE_MODELS)
    return analyzer


# ── Metrics extraction ─────────────────────────────────────────────────────────

def extract_metrics(trial_dir: Path, analyzer: si.SortingAnalyzer) -> dict:
    import pandas as pd

    metrics: dict = {}

    labels_path = trial_dir / "kilosort4" / "unit_labels.tsv"
    labels_df = None
    if labels_path.exists():
        labels_df = pd.read_csv(labels_path, index_col=0)
        metrics["n_units_total"] = len(labels_df)

        if "noise_prediction" in labels_df.columns:
            neural_mask = labels_df["noise_prediction"] == "neural"
            metrics["n_neural"] = int(neural_mask.sum())

        sua_mask = None
        if {"sua_prediction", "noise_prediction"} <= set(labels_df.columns):
            sua_mask = (labels_df["sua_prediction"] == "sua") & (labels_df["noise_prediction"] == "neural")
            mua_mask = (labels_df["sua_prediction"] == "mua") & (labels_df["noise_prediction"] == "neural")
            metrics["n_sua"] = int(sua_mask.sum())
            metrics["n_mua"] = int(mua_mask.sum())

        # Presence ratio and firing rate for SUA units (proxy for tracking duration)
        try:
            qm = analyzer.get_extension("quality_metrics").get_data()
            if sua_mask is not None and sua_mask.sum() > 0:
                sua_unit_ids = labels_df.index[sua_mask]
                qm_sua = qm.loc[qm.index.isin(sua_unit_ids)]
                if "presence_ratio" in qm_sua.columns:
                    metrics["mean_presence_ratio_sua"]   = round(float(qm_sua["presence_ratio"].mean()), 4)
                    metrics["median_presence_ratio_sua"] = round(float(qm_sua["presence_ratio"].median()), 4)
                if "firing_rate" in qm_sua.columns:
                    metrics["mean_firing_rate_sua_hz"] = round(float(qm_sua["firing_rate"].mean()), 4)
        except Exception as e:
            print(f"  Warning: could not read quality metrics: {e}")

    # Drift range from Kilosort4 ops (dshift = estimated displacement per block/time-bin)
    ops_path = trial_dir / "kilosort4" / "ops.npy"
    if ops_path.exists():
        try:
            ops = np.load(ops_path, allow_pickle=True).item()
            if "dshift" in ops:
                dshift = np.asarray(ops["dshift"])
                metrics["drift_range_um"] = round(float(np.ptp(dshift)), 2)
                metrics["drift_std_um"]   = round(float(np.std(dshift)), 2)
        except Exception as e:
            print(f"  Warning: could not read drift from ops.npy: {e}")

    return metrics


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Run a preprocessing pipeline trial.")
    parser.add_argument("day_dir",   type=Path)
    parser.add_argument("probe",     choices=["a", "b"])
    parser.add_argument("shank_num", choices=["0", "1", "2", "3"])
    parser.add_argument("config",    type=Path, help="Trial config YAML")
    args = parser.parse_args()

    t0_wall = time.time()
    cfg = load_config(args.config)
    trial_name = cfg["trial_name"]

    trial_dir = args.day_dir / TRIALS_DIR / trial_name
    if trial_dir.exists():
        # Wipe known outputs but leave log files so LSF's open fd stays valid
        for name in ["shank_recording.bin", "trial_info.json", "metrics.json", "config.yaml"]:
            p = trial_dir / name
            if p.exists():
                p.unlink()
        for subdir in ["kilosort4", "sorting_analyzer", "unitrefine_dataset"]:
            p = trial_dir / subdir
            if p.exists():
                shutil.rmtree(p)
        print(f"Cleared existing outputs in {trial_dir}")
    trial_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Trial : {trial_name}")
    print(f"Probe : {args.probe}  shank {args.shank_num}")
    print(f"Output: {trial_dir}")
    if cfg["description"]:
        print(f"Note  : {cfg['description']}")
    print(f"{'='*60}\n")

    # Load probe
    probe_file = args.day_dir / f"{args.probe}_probe_conf.json"
    probe_data, _           = load_probe(probe_file)
    shank_probe, n_ch_shank = load_probe(probe_file, args.shank_num)
    if not n_ch_shank:
        print(f"ERROR: Shank {args.shank_num} has zero channels.")
        sys.exit(1)

    # Load + concatenate raw recording files
    data_dir = args.day_dir / "data"
    rec_files = sorted(glob.glob(str(data_dir / f"np2-{args.probe}-ephys*")))
    if not rec_files:
        print(f"ERROR: No files matching np2-{args.probe}-ephys* in {data_dir}")
        sys.exit(1)
    print(f"Loading {len(rec_files)} recording file(s)...")
    recs = [
        se.read_binary(rf, dtype="int16", sampling_frequency=SAMPLE_RATE, num_channels=N_CHANNELS_PROBE)
        for rf in rec_files
        if se.read_binary(rf, dtype="int16", sampling_frequency=SAMPLE_RATE,
                          num_channels=N_CHANNELS_PROBE).get_num_frames() > 0
    ]
    recording = si.concatenate_recordings(recs)
    recording.set_probe(probe_data)
    print(f"Total recording: {recording.get_total_duration():.1f} s\n")

    # Run preprocessing pipeline
    print("Preprocessing steps:")
    final_rec = run_pipeline(
        recording, cfg["pipeline"], probe_data, args.shank_num, shank_probe, cfg["n_jobs"],
        trial_dir=trial_dir,
    )
    print(f"\nPreprocessed: {final_rec.get_num_channels()} ch  "
          f"{final_rec.get_total_duration():.1f} s\n")

    # Export to binary
    if final_rec.get_dtype() != np.dtype("int16"):
        print(f"  Casting {final_rec.get_dtype()} → int16 before export")
        final_rec = final_rec.astype("int16")
    # Sanity-check a single chunk so we can see in the logs if data looks dead
    _chunk = final_rec.get_traces(start_frame=0, end_frame=min(30000, final_rec.get_num_frames()))
    print(f"  Data range check (first 1s): min={_chunk.min():.1f}  max={_chunk.max():.1f}  std={_chunk.std():.2f}")
    del _chunk
    print("Exporting to binary...")
    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        final_rec, trial_dir,
        data_name="shank_recording.bin", dtype=np.int16,
        chunksize=30000 * 8, export_probe=True, probe_name="probe.prb",
        max_workers=cfg["n_jobs"],
    )
    kilosort_probe = io.load_probe(probe_path)

    # Run Kilosort4
    ks_settings = {"fs": fs, "n_chan_bin": c, "batch_size": cfg["ks_batch_size"]}
    ks_settings.update(cfg.get("ks_extra_settings", {}))
    print(f"\nRunning Kilosort4  (settings: {ks_settings})...")
    ops, *_ = run_kilosort(settings=ks_settings, probe=kilosort_probe, filename=filename)
    print("Kilosort done.")

    # Fix hardcoded dat_path for Phy portability
    params_py = filename.parent / "kilosort4" / "params.py"
    if params_py.exists():
        text = params_py.read_text()
        text = re.sub(r"^dat_path\s*=.*$", "dat_path = '../shank_recording.bin'",
                      text, flags=re.MULTILINE)
        params_py.write_text(text)

    # Postprocessing
    print("\nRunning postprocessing (SortingAnalyzer + UnitRefine)...")
    analyzer = run_postproc(trial_dir, shank_probe, n_ch_shank, cfg["n_jobs"])

    # Metrics
    print("\nExtracting metrics...")
    metrics = extract_metrics(trial_dir, analyzer)
    metrics["trial_name"]        = trial_name
    metrics["recording_duration_s"] = round(final_rec.get_total_duration(), 1)
    metrics["wall_time_s"]       = round(time.time() - t0_wall, 1)

    (trial_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"Metrics: {metrics}")

    # Trial info (config + diff + provenance)
    trial_info = {
        "trial_name":        trial_name,
        "description":       cfg.get("description", ""),
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "git_hash":          get_git_hash(),
        "day_dir":           str(args.day_dir),
        "probe":             args.probe,
        "shank_num":         args.shank_num,
        "config":            cfg,
        "diff_from_defaults": compute_diff(cfg),
    }
    (trial_dir / "trial_info.json").write_text(json.dumps(trial_info, indent=2))

    # Copy the original YAML into the output directory so the full config is
    # human-readable alongside the results without needing to parse trial_info.json
    shutil.copy2(args.config, trial_dir / "config.yaml")

    subprocess.run(["chmod", "-R", "777", str(trial_dir)], check=False)
    print(f"\nTrial '{trial_name}' complete in {metrics['wall_time_s']:.0f} s")
    print(f"Output: {trial_dir}")


if __name__ == "__main__":
    main()
