#!/usr/bin/env python3
"""
Plot 100 raw waveforms per good (neural) cluster for one or more trials.

Each cluster gets its own panel. Panels are arranged in a grid (N_COLS per row)
and wrap automatically. One figure is saved per trial.

Usage:
    python trials/plot_waveforms.py <day_dir> --trial no_artifacts filter_first
    python trials/plot_waveforms.py <day_dir>          # all completed trials
    python trials/plot_waveforms.py <day_dir> --probe b --shank 0
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SAMPLE_RATE  = 30000
WIN_PRE      = 30    # samples before trough
WIN_POST     = 40    # samples after trough
N_WAVEFORMS  = 100   # raw waveforms to overlay per cluster
def n_cols_for(n_units: int) -> int:
    if n_units <= 16:  return 4
    if n_units <= 48:  return 8
    if n_units <= 120: return 12
    return 16

CATEGORY_COLOURS = {
    "sua_neural": "#2196F3",
    "mua_neural": "#FF9800",
    "noise":      "#9E9E9E",
}
CATEGORY_LABELS = {
    "sua_neural": "SUA neural",
    "mua_neural": "MUA neural",
    "noise":      "Noise",
}
CATEGORIES = ["sua_neural", "mua_neural", "noise"]


# ── Helpers ────────────────────────────────────────────────────────────────────

def read_n_channels(ks_dir: Path) -> int | None:
    params = ks_dir / "params.py"
    if not params.exists():
        return None
    for line in params.read_text().splitlines():
        m = re.match(r"n_channels_dat\s*=\s*(\d+)", line)
        if m:
            return int(m.group(1))
    return None


def load_labels(ks_dir: Path) -> pd.DataFrame | None:
    path = ks_dir / "unit_labels.tsv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0)
    if len(df.columns) < 2:
        df = pd.read_csv(path, sep="\t", index_col=0)
    return df


def assign_category(labels_df: pd.DataFrame) -> pd.Series:
    cat = pd.Series("noise", index=labels_df.index)
    if "noise_prediction" in labels_df.columns and "sua_prediction" in labels_df.columns:
        neural = labels_df["noise_prediction"] == "neural"
        cat[neural & (labels_df["sua_prediction"] == "sua")] = "sua_neural"
        cat[neural & (labels_df["sua_prediction"] == "mua")] = "mua_neural"
    return cat


def best_channel_per_unit(ks_dir: Path, unit_ids: np.ndarray) -> dict[int, int]:
    """Return {unit_id: best_channel_index} using KS4 templates."""
    templates_path      = ks_dir / "templates.npy"
    spike_clusters_path = ks_dir / "spike_clusters.npy"
    spike_templates_path = ks_dir / "spike_templates.npy"
    if not all(p.exists() for p in [templates_path, spike_clusters_path, spike_templates_path]):
        return {}
    templates       = np.load(templates_path)
    spike_clusters  = np.load(spike_clusters_path).ravel()
    spike_templates = np.load(spike_templates_path).ravel()
    result = {}
    for uid in unit_ids:
        mask = spike_clusters == uid
        if not mask.any():
            continue
        tmpl_ids, counts = np.unique(spike_templates[mask], return_counts=True)
        best_tmpl = int(tmpl_ids[np.argmax(counts)])
        if best_tmpl < len(templates):
            result[uid] = int(np.argmax(np.ptp(templates[best_tmpl], axis=0)))
    return result


def extract_raw_waveforms(
    bin_path: Path,
    n_channels: int,
    spike_times: np.ndarray,
    best_ch: int,
    n_waveforms: int = N_WAVEFORMS,
) -> np.ndarray | None:
    """Return (n_waveforms, WIN_PRE+WIN_POST) array of raw traces on best_ch."""
    try:
        data = np.memmap(bin_path, dtype="int16", mode="r").reshape(-1, n_channels)
    except Exception:
        return None

    n_total = len(data)
    valid = spike_times[
        (spike_times >= WIN_PRE) & (spike_times + WIN_POST < n_total)
    ]
    if len(valid) == 0:
        return None

    rng = np.random.default_rng(42)
    chosen = rng.choice(valid, size=min(n_waveforms, len(valid)), replace=False)
    chosen.sort()

    wfs = np.stack([data[t - WIN_PRE : t + WIN_POST, best_ch] for t in chosen])
    return wfs.astype(float)


# ── Per-trial figure ───────────────────────────────────────────────────────────

def _save_category_figure(
    unit_ids: np.ndarray,
    cat: str,
    cat_series: pd.Series,
    spike_times: np.ndarray,
    spike_clusters: np.ndarray,
    best_channels: dict,
    bin_path: Path,
    n_channels: int,
    trial_name: str,
    out_path: Path,
) -> None:
    colour  = CATEGORY_COLOURS[cat]
    n_units = len(unit_ids)
    if n_units == 0:
        return

    n_cols = n_cols_for(n_units)
    n_rows = (n_units + n_cols - 1) // n_cols
    t_axis = np.arange(WIN_PRE + WIN_POST) / SAMPLE_RATE * 1000  # ms

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(n_cols * 1.8, n_rows * 2.2),
        squeeze=False,
    )
    fig.suptitle(
        f"{trial_name} — {CATEGORY_LABELS[cat]}  (n={n_units})",
        fontsize=10, fontweight="bold",
    )

    for idx, uid in enumerate(unit_ids):
        row, col = divmod(idx, n_cols)
        ax = axes[row, col]

        mask     = spike_clusters == uid
        sp_times = spike_times[mask]
        best_ch  = best_channels.get(uid, 0)
        wfs      = extract_raw_waveforms(bin_path, n_channels, sp_times, best_ch)

        if wfs is not None and len(wfs) > 0:
            for wf in wfs:
                ax.plot(t_axis, wf, color=colour, alpha=0.15, lw=0.5)
            ax.plot(t_axis, wfs.mean(axis=0), color=colour, lw=1.5)

        ax.axvline(WIN_PRE / SAMPLE_RATE * 1000, color="white", lw=0.4, alpha=0.3)
        ax.set_title(f"#{uid}  n={mask.sum()}", fontsize=6, pad=2)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.spines[["top", "right", "left", "bottom"]].set_visible(False)
        ax.set_facecolor("#111111")

    for idx in range(n_units, n_rows * n_cols):
        row, col = divmod(idx, n_cols)
        axes[row, col].set_visible(False)

    fig.patch.set_facecolor("#1a1a1a")
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close(fig)


def plot_trial_waveforms(trial_dir: Path, out_dir: Path) -> bool:
    ks_dir   = trial_dir / "kilosort4"
    bin_path = trial_dir / "shank_recording.bin"

    if not ks_dir.exists() or not bin_path.exists():
        return False

    labels_df  = load_labels(ks_dir)
    times_path = ks_dir / "spike_times.npy"
    clus_path  = ks_dir / "spike_clusters.npy"

    if labels_df is None or not times_path.exists() or not clus_path.exists():
        return False

    n_channels = read_n_channels(ks_dir)
    if n_channels is None:
        return False

    info_path  = trial_dir / "trial_info.json"
    trial_name = trial_dir.name
    if info_path.exists():
        trial_name = json.loads(info_path.read_text()).get("trial_name", trial_name)

    cat_series     = assign_category(labels_df)
    all_ids        = labels_df.index.to_numpy()
    best_channels  = best_channel_per_unit(ks_dir, all_ids)
    spike_times    = np.load(times_path).ravel().astype(int)
    spike_clusters = np.load(clus_path).ravel()

    out_dir.mkdir(parents=True, exist_ok=True)

    for cat in CATEGORIES:
        unit_ids = cat_series[cat_series == cat].index.to_numpy()
        # Sort by depth
        unit_ids = sorted(unit_ids, key=lambda u: best_channels.get(u, 0))
        unit_ids = np.array(unit_ids)
        _save_category_figure(
            unit_ids, cat, cat_series,
            spike_times, spike_clusters, best_channels,
            bin_path, n_channels, trial_name,
            out_dir / f"{cat}.png",
        )
        print(f"    {cat}: {len(unit_ids)} units → {cat}.png")

    return True


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("day_dir", type=Path)
    parser.add_argument("--trial", nargs="*", dest="trials", default=None,
                        help="Trial name(s) to plot (default: all completed)")
    parser.add_argument("--probe", default=None, choices=["a", "b"])
    parser.add_argument("--shank", default=None, choices=["0", "1", "2", "3"])
    args = parser.parse_args()

    trials_root = args.day_dir / "processing_pipeline_trials"
    out_dir     = trials_root / "plots"
    out_dir.mkdir(exist_ok=True)

    done = 0
    for trial_dir in sorted(trials_root.iterdir()):
        if not trial_dir.is_dir() or trial_dir.name in ("logs", "plots"):
            continue
        info_path = trial_dir / "trial_info.json"
        if not info_path.exists():
            continue
        info = json.loads(info_path.read_text())
        name = info.get("trial_name", trial_dir.name)
        if args.trials and name not in args.trials:
            continue
        if args.probe and info.get("probe") != args.probe:
            continue
        if args.shank and info.get("shank_num") != args.shank:
            continue

        trial_out = out_dir / f"waveforms_{name}"
        print(f"  {name} →  {trial_out.name}/")
        if plot_trial_waveforms(trial_dir, trial_out):
            done += 1
        else:
            print("    skipped (missing data)")

    print(f"\nDone — {done} figure(s) saved to {out_dir}")


if __name__ == "__main__":
    main()
