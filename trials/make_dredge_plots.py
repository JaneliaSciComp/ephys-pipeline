#!/usr/bin/env python3
"""
Generate drift_amount.png and drift_scatter.png (matching KS4 style) for
completed trials that have a motion/ folder from SpikeInterface DREDge correction.

Usage:
    python trials/make_dredge_plots.py <day_dir>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SAMPLE_RATE = 30000


def _load_motion_arrays(motion_folder: Path):
    """Return (motion_arr, t_bins, sp_bins) or raise FileNotFoundError."""
    inner = motion_folder / "motion"
    for motion_f, t_f, sp_f in [
        (inner / "displacement_seg0.npy", inner / "temporal_bins_s_seg0.npy", inner / "spatial_bins_um.npy"),
        (inner / "motion_array.npy",      inner / "temporal_bins_s.npy",      inner / "spatial_bins_um.npy"),
        (inner / "motion.npy",            inner / "temporal_bins_s.npy",      inner / "spatial_bins_um.npy"),
    ]:
        if motion_f.exists() and t_f.exists():
            sp_bins = np.load(sp_f) if sp_f.exists() else None
            return np.load(motion_f), np.load(t_f), sp_bins
    files = [f.name for f in sorted(motion_folder.iterdir())]
    inner_files = [f.name for f in sorted(inner.iterdir())] if inner.exists() else []
    raise FileNotFoundError(
        f"motion/ contents: {files}\nmotion/motion/ contents: {inner_files}"
    )


def plot_drift_amount(motion_arr: np.ndarray, t_bins: np.ndarray,
                      sp_bins, title: str, out_path: Path) -> None:
    """Replicate KS4 drift_amount.png: dark background, drift per probe section over time."""
    fig, ax = plt.subplots(figsize=(10, 10), facecolor="black")
    ax.set_facecolor("black")

    arr = motion_arr if motion_arr.ndim == 2 else motion_arr[:, np.newaxis]
    n_spatial = arr.shape[1]

    ax.plot(t_bins, arr.mean(axis=1), color="#4C9BE8", lw=1.2)

    ax.set_xlabel("Time (s)", color="white")
    ax.set_ylabel("Depth drift (µm)", color="white")
    ax.set_title(title, color="white", fontsize=10)
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_edgecolor("white")

    fig.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="black")
    plt.close(fig)


def plot_drift_scatter(motion_folder: Path, t_bins: np.ndarray,
                       title: str, out_path: Path) -> None:
    """Replicate KS4 drift_scatter.png: spike amplitude across time and depth."""
    peaks_path = motion_folder / "peaks.npy"
    locs_path  = motion_folder / "peak_locations.npy"
    if not peaks_path.exists() or not locs_path.exists():
        return

    peaks = np.load(peaks_path)
    locs  = np.load(locs_path)

    # peaks is a structured array: sample_index, amplitude, channel_index, segment_index
    # locs  is a structured array: x, y  (y = depth in µm)
    times_s  = peaks["sample_index"].astype(float) / SAMPLE_RATE
    depths   = locs["y"].astype(float)
    amps     = np.abs(peaks["amplitude"].astype(float))

    # Subsample if huge (>500k peaks gets slow)
    if len(times_s) > 500_000:
        idx = np.random.choice(len(times_s), 500_000, replace=False)
        times_s, depths, amps = times_s[idx], depths[idx], amps[idx]

    # Clip amplitudes for colour scaling (ignore top 1%)
    vmax = np.percentile(amps, 99)
    amps_clipped = np.clip(amps, 0, vmax) / vmax

    fig, ax = plt.subplots(figsize=(10, 10), facecolor="black")
    ax.set_facecolor("black")

    ax.scatter(times_s, depths, c=amps_clipped, cmap="Greys",
               s=1, alpha=0.5, linewidths=0, rasterized=True)

    ax.set_xlim(times_s.min(), times_s.max())
    ax.set_xlabel("Time (s)", color="white")
    ax.set_ylabel("Depth (µm)", color="white")
    ax.set_title(title, color="white", fontsize=10)
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_edgecolor("white")

    fig.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="black")
    plt.close(fig)


def make_plots(trial_dir: Path) -> bool:
    motion_folder = trial_dir / "motion"
    if not motion_folder.exists():
        return False

    try:
        motion_arr, t_bins, sp_bins = _load_motion_arrays(motion_folder)
    except FileNotFoundError as e:
        print(f"    {e}")
        return False

    info_path  = trial_dir / "trial_info.json"
    trial_name = trial_dir.name
    preset     = ""
    if info_path.exists():
        info = json.loads(info_path.read_text())
        trial_name = info.get("trial_name", trial_name)
        for step in info.get("config", {}).get("pipeline", []):
            if step.get("step") == "correct_motion":
                preset = step.get("preset", "")

    title = f"DREDge motion — {trial_name}" + (f" ({preset})" if preset else "")
    ks_dir = trial_dir / "kilosort4"
    ks_dir.mkdir(exist_ok=True)

    plot_drift_amount(motion_arr, t_bins, sp_bins, title, ks_dir / "drift_amount.png")
    plot_drift_scatter(motion_folder, t_bins, title, ks_dir / "drift_scatter.png")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("day_dir", type=Path)
    args = parser.parse_args()

    trials_root = args.day_dir / "processing_pipeline_trials"
    if not trials_root.exists():
        print(f"No trials directory at {trials_root}")
        return

    found = 0
    for trial_dir in sorted(trials_root.iterdir()):
        if not trial_dir.is_dir() or trial_dir.name in ("logs", "plots"):
            continue
        if make_plots(trial_dir):
            print(f"  Saved {trial_dir.name}/kilosort4/drift_amount.png + drift_scatter.png")
            found += 1
        else:
            print(f"  Skipped {trial_dir.name} (no motion/ data)")

    print(f"\nDone — {found} trial(s) plotted.")


if __name__ == "__main__":
    main()
