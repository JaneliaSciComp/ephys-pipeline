#!/usr/bin/env python3
"""
Visual comparison of preprocessing trial results.

Saves three figures to processing_pipeline_trials/plots/:
  drift_comparison.png     — KS4 drift_amount + drift_scatter per trial
  waveforms_comparison.png — mean waveforms by unit category (SUA / MUA / noise)
  amplitudes_comparison.png — spike amplitude over time coloured by category

Requires only numpy, pandas, matplotlib — no SpikeInterface.

Usage:
    python trials/plot_comparisons.py <day_dir> --probe b --shank 0
    python trials/plot_comparisons.py <day_dir> --probe b --shank 0 --trials default artifact100
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

SAMPLE_RATE = 30000

# Category colours (consistent across all plots)
CATEGORY_COLOURS = {
    "sua_neural": "#2196F3",   # blue
    "mua_neural": "#FF9800",   # orange
    "noise":      "#9E9E9E",   # grey
}
CATEGORY_LABELS = {
    "sua_neural": "SUA (neural)",
    "mua_neural": "MUA (neural)",
    "noise":      "Noise",
}


# ── Data loading ───────────────────────────────────────────────────────────────

def load_labels(ks_dir: Path) -> pd.DataFrame | None:
    path = ks_dir / "unit_labels.tsv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0)
    if len(df.columns) < 2:
        df = pd.read_csv(path, sep="\t", index_col=0)
    return df


def assign_category(labels_df: pd.DataFrame) -> pd.Series:
    """Map each unit to sua_neural / mua_neural / noise."""
    cat = pd.Series("noise", index=labels_df.index)
    if "noise_prediction" in labels_df.columns and "sua_prediction" in labels_df.columns:
        neural = labels_df["noise_prediction"] == "neural"
        cat[neural & (labels_df["sua_prediction"] == "sua")] = "sua_neural"
        cat[neural & (labels_df["sua_prediction"] == "mua")] = "mua_neural"
    return cat


def load_trial(trial_dir: Path) -> dict | None:
    info_path = trial_dir / "trial_info.json"
    if not info_path.exists():
        return None
    info = json.loads(info_path.read_text())
    ks_dir = trial_dir / "kilosort4"
    if not ks_dir.exists():
        return None
    return {"name": info["trial_name"], "dir": trial_dir, "ks": ks_dir,
            "desc": info.get("description", ""), "diff": info.get("diff_from_defaults", {})}


# ── Figure 1: Drift ────────────────────────────────────────────────────────────

def plot_drift_comparison(trials: list[dict], out_path: Path) -> None:
    n = len(trials)
    fig, axes = plt.subplots(2, n, figsize=(4 * n, 8))
    if n == 1:
        axes = axes.reshape(2, 1)

    row_labels = ["drift amount", "drift scatter"]
    png_names  = ["drift_amount.png", "drift_scatter.png"]

    for col, trial in enumerate(trials):
        for row, (png_name, row_label) in enumerate(zip(png_names, row_labels)):
            ax = axes[row, col]
            png_path = trial["ks"] / png_name
            if png_path.exists():
                img = plt.imread(str(png_path))
                ax.imshow(img)
            else:
                ax.text(0.5, 0.5, f"{png_name}\nnot found",
                        ha="center", va="center", transform=ax.transAxes, color="red")
            ax.axis("off")
            if row == 0:
                ax.set_title(trial["name"], fontsize=9, fontweight="bold")
            if col == 0:
                ax.set_ylabel(row_label, fontsize=8)

    fig.suptitle("Drift — per trial", fontsize=11)
    plt.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path.name}")


# ── Figure 2: Waveforms ────────────────────────────────────────────────────────

def get_best_channel_waveforms(ks_dir: Path, unit_ids: np.ndarray) -> dict[int, np.ndarray]:
    """Return {unit_id: waveform_on_best_channel} using KS4 templates.npy."""
    templates_path = ks_dir / "templates.npy"
    spike_clusters_path = ks_dir / "spike_clusters.npy"
    spike_templates_path = ks_dir / "spike_templates.npy"

    if not all(p.exists() for p in [templates_path, spike_clusters_path, spike_templates_path]):
        return {}

    templates = np.load(templates_path)          # (n_templates, n_times, n_channels)
    spike_clusters  = np.load(spike_clusters_path).ravel()
    spike_templates = np.load(spike_templates_path).ravel()

    waveforms = {}
    for uid in unit_ids:
        mask = spike_clusters == uid
        if not mask.any():
            continue
        # Most common template for this cluster
        tmpl_ids, counts = np.unique(spike_templates[mask], return_counts=True)
        best_tmpl = int(tmpl_ids[np.argmax(counts)])
        if best_tmpl >= len(templates):
            continue
        wf = templates[best_tmpl]                # (n_times, n_channels)
        best_ch = int(np.argmax(np.ptp(wf, axis=0)))
        waveforms[uid] = wf[:, best_ch]
    return waveforms


def plot_waveforms_comparison(trials: list[dict], out_path: Path,
                              max_units_per_cat: int = 15) -> None:
    cats = ["sua_neural", "mua_neural", "noise"]
    n = len(trials)
    fig, axes = plt.subplots(n, 3, figsize=(12, 3.5 * n), squeeze=False)

    for row, trial in enumerate(trials):
        labels_df = load_labels(trial["ks"])
        if labels_df is None:
            for col in range(3):
                axes[row, col].text(0.5, 0.5, "no labels", ha="center", va="center",
                                    transform=axes[row, col].transAxes)
            continue

        cat_series = assign_category(labels_df)
        all_unit_ids = labels_df.index.to_numpy()
        waveforms = get_best_channel_waveforms(trial["ks"], all_unit_ids)

        for col, cat in enumerate(cats):
            ax = axes[row, col]
            unit_ids = cat_series[cat_series == cat].index.to_numpy()
            colour = CATEGORY_COLOURS[cat]

            if len(unit_ids) == 0:
                ax.text(0.5, 0.5, "none", ha="center", va="center",
                        transform=ax.transAxes, color="grey")
            else:
                # Sample up to max_units_per_cat
                sample = unit_ids[np.random.choice(len(unit_ids),
                                  min(max_units_per_cat, len(unit_ids)), replace=False)]
                valid_wfs = [waveforms[u] for u in sample if u in waveforms]

                if valid_wfs:
                    wf_arr = np.array(valid_wfs)
                    # Normalise each waveform to [-1, 1] for shape comparison
                    ptp = np.ptp(wf_arr, axis=1, keepdims=True)
                    ptp[ptp == 0] = 1
                    wf_norm = wf_arr / ptp

                    t = np.arange(wf_norm.shape[1])
                    for wf in wf_norm:
                        ax.plot(t, wf, color=colour, alpha=0.25, lw=0.8)
                    ax.plot(t, wf_norm.mean(axis=0), color=colour, lw=2)

            ax.set_yticks([])
            ax.spines[["top", "right", "left"]].set_visible(False)

            if row == 0:
                n_units = int((cat_series == cat).sum())
                ax.set_title(f"{CATEGORY_LABELS[cat]}\n(n={n_units})", fontsize=9)
            if col == 0:
                ax.set_ylabel(trial["name"], fontsize=8, rotation=0,
                              ha="right", va="center", labelpad=60)

    fig.suptitle("Waveforms by unit category — normalised to unit peak", fontsize=11)
    plt.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path.name}")


# ── Figure 3: Amplitudes over time ────────────────────────────────────────────

def plot_amplitudes_comparison(trials: list[dict], out_path: Path,
                               bin_s: float = 60.0) -> None:
    n = len(trials)
    fig, axes = plt.subplots(n, 1, figsize=(16, 4 * n), squeeze=False)

    for row, trial in enumerate(trials):
        ax = axes[row, 0]
        ks_dir = trial["ks"]

        amp_path   = ks_dir / "amplitudes.npy"
        times_path = ks_dir / "spike_times.npy"
        clus_path  = ks_dir / "spike_clusters.npy"
        labels_df  = load_labels(ks_dir)

        if not all(p.exists() for p in [amp_path, times_path, clus_path]) \
                or labels_df is None:
            ax.text(0.5, 0.5, "data missing", ha="center", va="center",
                    transform=ax.transAxes)
            ax.set_title(trial["name"])
            continue

        amplitudes = np.load(amp_path).ravel().astype(float)
        spike_times_s = np.load(times_path).ravel().astype(float) / SAMPLE_RATE
        spike_clusters = np.load(clus_path).ravel()
        cat_series = assign_category(labels_df)

        duration_s = spike_times_s.max()
        n_bins = max(1, int(duration_s / bin_s))
        bin_edges = np.linspace(0, duration_s, n_bins + 1)
        bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2

        # Draw categories from back (noise) to front (sua) so SUA is on top
        for cat in ["noise", "mua_neural", "sua_neural"]:
            colour = CATEGORY_COLOURS[cat]
            unit_ids = cat_series[cat_series == cat].index.to_numpy()
            alpha = 0.15 if cat == "noise" else (0.45 if cat == "mua_neural" else 0.85)
            lw    = 0.6  if cat == "noise" else (0.9  if cat == "mua_neural" else 1.2)

            for uid in unit_ids:
                mask = spike_clusters == uid
                if not mask.any():
                    continue
                t_spk = spike_times_s[mask]
                a_spk = amplitudes[mask]
                # Median amplitude per bin
                bin_meds = np.full(n_bins, np.nan)
                for b in range(n_bins):
                    in_bin = (t_spk >= bin_edges[b]) & (t_spk < bin_edges[b + 1])
                    if in_bin.sum() >= 3:
                        bin_meds[b] = np.median(a_spk[in_bin])
                # Only plot if unit has spikes in >20% of bins (filters out sparse noise)
                valid = (~np.isnan(bin_meds)).mean()
                if cat == "noise" and valid < 0.4:
                    continue
                ax.plot(bin_centres, bin_meds, color=colour, alpha=alpha,
                        lw=lw, solid_capstyle="round")

        # Legend
        for cat in ["sua_neural", "mua_neural", "noise"]:
            n_units = int((cat_series == cat).sum())
            ax.plot([], [], color=CATEGORY_COLOURS[cat], lw=2,
                    label=f"{CATEGORY_LABELS[cat]} (n={n_units})")
        ax.legend(fontsize=7, loc="upper right", framealpha=0.7)

        ax.set_xlim(0, duration_s)
        ax.set_ylabel("Amplitude (AU)", fontsize=8)
        ax.set_title(trial["name"], fontsize=9, fontweight="bold")
        ax.spines[["top", "right"]].set_visible(False)

    axes[-1, 0].set_xlabel("Time (s)", fontsize=9)
    fig.suptitle(f"Spike amplitude over time  ({bin_s:.0f}s median bins)", fontsize=11)
    plt.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path.name}")


# ── Figure 4: Raster + waveforms ──────────────────────────────────────────────

def get_unit_depth(ks_dir: Path, unit_ids: np.ndarray) -> dict[int, float]:
    """Return {unit_id: best_channel_index} as a depth proxy (higher index = deeper)."""
    templates_path = ks_dir / "templates.npy"
    spike_clusters_path = ks_dir / "spike_clusters.npy"
    spike_templates_path = ks_dir / "spike_templates.npy"
    if not all(p.exists() for p in [templates_path, spike_clusters_path, spike_templates_path]):
        return {uid: i for i, uid in enumerate(unit_ids)}
    templates = np.load(templates_path)
    spike_clusters  = np.load(spike_clusters_path).ravel()
    spike_templates = np.load(spike_templates_path).ravel()
    depths = {}
    for uid in unit_ids:
        mask = spike_clusters == uid
        if not mask.any():
            continue
        tmpl_ids, counts = np.unique(spike_templates[mask], return_counts=True)
        best_tmpl = int(tmpl_ids[np.argmax(counts)])
        if best_tmpl < len(templates):
            depths[uid] = int(np.argmax(np.ptp(templates[best_tmpl], axis=0)))
    return depths


def plot_raster_comparison(trials: list[dict], out_path: Path) -> None:
    neural_cats = ["sua_neural", "mua_neural"]
    n = len(trials)
    fig = plt.figure(figsize=(18, 5 * n))
    outer = gridspec.GridSpec(n, 1, figure=fig, hspace=0.45)

    for row, trial in enumerate(trials):
        ks_dir = trial["ks"]
        labels_df = load_labels(ks_dir)
        times_path = ks_dir / "spike_times.npy"
        clus_path  = ks_dir / "spike_clusters.npy"

        inner = gridspec.GridSpecFromSubplotSpec(
            1, 2, subplot_spec=outer[row], width_ratios=[5, 1], wspace=0.04
        )
        ax_raster = fig.add_subplot(inner[0])
        ax_wf     = fig.add_subplot(inner[1])

        if labels_df is None or not times_path.exists() or not clus_path.exists():
            ax_raster.text(0.5, 0.5, "data missing", ha="center", va="center",
                           transform=ax_raster.transAxes)
            ax_raster.set_title(trial["name"], fontsize=9, fontweight="bold")
            ax_wf.axis("off")
            continue

        cat_series = assign_category(labels_df)
        neural_ids = cat_series[cat_series.isin(neural_cats)].index.to_numpy()

        spike_times_s  = np.load(times_path).ravel().astype(float) / SAMPLE_RATE
        spike_clusters = np.load(clus_path).ravel()

        # Sort units by depth (best channel index)
        depths = get_unit_depth(ks_dir, neural_ids)
        sorted_ids = sorted(neural_ids, key=lambda u: depths.get(u, 0))

        waveforms = get_best_channel_waveforms(ks_dir, neural_ids)

        for y_pos, uid in enumerate(sorted_ids):
            mask   = spike_clusters == uid
            t_spk  = spike_times_s[mask]
            cat    = cat_series.get(uid, "noise")
            colour = CATEGORY_COLOURS.get(cat, "#9E9E9E")
            ax_raster.vlines(t_spk, y_pos - 0.4, y_pos + 0.4,
                             color=colour, lw=0.3, alpha=0.6)

        ax_raster.set_xlim(0, spike_times_s.max())
        ax_raster.set_ylim(-0.5, max(len(sorted_ids) - 0.5, 0.5))
        ax_raster.set_xlabel("Time (s)", fontsize=8)
        ax_raster.set_ylabel("Unit (sorted by depth)", fontsize=8)
        ax_raster.set_title(
            f"{trial['name']}   neural: {len(sorted_ids)} "
            f"(SUA={int((cat_series=='sua_neural').sum())}  "
            f"MUA={int((cat_series=='mua_neural').sum())})",
            fontsize=9, fontweight="bold"
        )
        ax_raster.spines[["top", "right"]].set_visible(False)

        # Waveforms panel — one small trace per unit, aligned to raster y-axis
        n_units = len(sorted_ids)
        TRIM = 10   # samples each side of the trough
        for y_pos, uid in enumerate(sorted_ids):
            if uid not in waveforms:
                continue
            wf = waveforms[uid].astype(float)
            ptp = np.ptp(wf)
            if ptp == 0:
                continue
            # Crop to a window around the trough
            trough = int(np.argmin(wf))
            lo = max(0, trough - TRIM)
            hi = min(len(wf), trough + TRIM)
            wf_crop = wf[lo:hi]
            wf_norm = wf_crop / ptp * 0.8
            t_wf    = np.linspace(0, 1, len(wf_norm))
            cat     = cat_series.get(uid, "noise")
            colour  = CATEGORY_COLOURS.get(cat, "#9E9E9E")
            ax_wf.plot(t_wf, wf_norm + y_pos, color=colour, lw=0.8, alpha=0.9)

        ax_wf.set_xlim(0, 1)
        ax_wf.set_ylim(-0.5, max(n_units - 0.5, 0.5))
        ax_wf.axis("off")

        # Legend patches
        for cat in neural_cats:
            ax_raster.plot([], [], color=CATEGORY_COLOURS[cat], lw=2,
                           label=CATEGORY_LABELS[cat])
        ax_raster.legend(fontsize=7, loc="upper right", framealpha=0.6)

    fig.suptitle("Spike raster — good units sorted by depth", fontsize=12)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path.name}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Plot visual comparison of trial results.")
    parser.add_argument("day_dir", type=Path)
    parser.add_argument("--probe",  default=None, choices=["a", "b"])
    parser.add_argument("--shank",  default=None, choices=["0", "1", "2", "3"])
    parser.add_argument("--trials", nargs="*", default=None,
                        help="Subset of trial names to include (default: all completed)")
    args = parser.parse_args()

    trials_root = args.day_dir / "processing_pipeline_trials"
    if not trials_root.exists():
        print(f"No trials directory at {trials_root}")
        return

    # Load all completed trials
    all_trials = []
    for d in sorted(trials_root.iterdir()):
        if not d.is_dir() or d.name == "logs" or d.name == "plots":
            continue
        t = load_trial(d)
        if t is None:
            continue
        info = json.loads((d / "trial_info.json").read_text())
        if args.probe and info.get("probe") != args.probe:
            continue
        if args.shank and info.get("shank_num") != args.shank:
            continue
        all_trials.append(t)

    if args.trials:
        all_trials = [t for t in all_trials if t["name"] in args.trials]

    if not all_trials:
        print("No completed trials found.")
        return

    print(f"Plotting {len(all_trials)} trial(s): {[t['name'] for t in all_trials]}")

    out_dir = trials_root / "plots"
    out_dir.mkdir(exist_ok=True)

    plot_drift_comparison(all_trials,      out_dir / "drift_comparison.png")
    plot_waveforms_comparison(all_trials,  out_dir / "waveforms_comparison.png")
    plot_amplitudes_comparison(all_trials, out_dir / "amplitudes_comparison.png")
    plot_raster_comparison(all_trials,     out_dir / "raster_comparison.png")

    print(f"\nAll plots saved to {out_dir}")


if __name__ == "__main__":
    main()
