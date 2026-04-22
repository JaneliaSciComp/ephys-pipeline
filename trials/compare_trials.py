#!/usr/bin/env python3
"""
Compare metrics across all completed trials for a given day / probe / shank.

Usage:
    python compare_trials.py <day_dir> [--probe a] [--shank 0]

Output: a table with one row per trial showing key metrics and what was changed
vs the default pipeline.  Trials without metrics.json (still running or failed)
are listed at the bottom.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


METRIC_COLS = [
    ("n_sua",                    "SUA units"),
    ("n_mua",                    "MUA units"),
    ("n_neural",                 "Neural units"),
    ("n_units_total",            "Total units"),
    ("mean_presence_ratio_sua",  "Presence ratio\n(SUA mean)"),
    ("median_presence_ratio_sua","Presence ratio\n(SUA median)"),
    ("mean_firing_rate_sua_hz",  "Firing rate\n(SUA mean Hz)"),
    ("drift_range_um",           "Drift range\n(µm)"),
    ("drift_std_um",             "Drift std\n(µm)"),
    ("recording_duration_s",     "Duration (s)"),
    ("wall_time_s",              "Wall time (s)"),
]


def fmt(val) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.3f}" if val < 100 else f"{val:.1f}"
    return str(val)


def summarise_diff(diff: dict) -> str:
    if not diff:
        return "baseline (no changes)"
    parts = []
    if "steps_added" in diff:
        parts.append(f"added: {', '.join(diff['steps_added'])}")
    if "steps_removed" in diff:
        parts.append(f"removed: {', '.join(diff['steps_removed'])}")
    if "step_order_changed" in diff:
        parts.append("order changed")
    if "param_changes" in diff:
        for step, changes in diff["param_changes"].items():
            for param, vals in changes.items():
                parts.append(f"{step}.{param}: {vals['default']} → {vals['trial']}")
    return " | ".join(parts)


def load_trial(trial_dir: Path, probe_filter: str | None, shank_filter: str | None):
    info_path    = trial_dir / "trial_info.json"
    metrics_path = trial_dir / "metrics.json"

    if not info_path.exists():
        return None  # not a trial directory

    info = json.loads(info_path.read_text())

    if probe_filter and info.get("probe") != probe_filter:
        return None
    if shank_filter and info.get("shank_num") != shank_filter:
        return None

    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else None

    return {
        "name":        info["trial_name"],
        "description": info.get("description", ""),
        "timestamp":   info.get("timestamp", "")[:19].replace("T", " "),
        "git_hash":    info.get("git_hash", ""),
        "probe":       info.get("probe", ""),
        "shank":       info.get("shank_num", ""),
        "diff":        summarise_diff(info.get("diff_from_defaults", {})),
        "metrics":     metrics,
    }


def print_table(trials: list[dict]) -> None:
    completed = [t for t in trials if t["metrics"] is not None]
    pending   = [t for t in trials if t["metrics"] is None]

    if not completed and not pending:
        print("No trials found.")
        return

    # ── Completed trials table ─────────────────────────────────────────────────
    if completed:
        # Column widths
        name_w = max(len(t["name"]) for t in completed)
        name_w = max(name_w, 12)
        diff_w = max(len(t["diff"]) for t in completed)
        diff_w = max(diff_w, 20)
        metric_w = 10

        header_sep = "-" * (name_w + diff_w + len(METRIC_COLS) * (metric_w + 3) + 8)

        print("\n" + "=" * len(header_sep))
        print("COMPLETED TRIALS")
        print("=" * len(header_sep))

        # Header row
        header = f"{'Trial':<{name_w}}  {'What changed':<{diff_w}}"
        for _, col_label in METRIC_COLS:
            short = col_label.split("\n")[0]
            header += f"  {short:>{metric_w}}"
        print(header)
        print(header_sep)

        # Find best values for highlighting
        best: dict = {}
        for key, _ in METRIC_COLS:
            vals = [t["metrics"].get(key) for t in completed if t["metrics"].get(key) is not None]
            if not vals:
                continue
            if key in ("wall_time_s", "drift_range_um", "drift_std_um"):
                best[key] = min(vals)
            elif key not in ("recording_duration_s",):
                best[key] = max(vals)

        for t in sorted(completed, key=lambda x: x["timestamp"]):
            row = f"{t['name']:<{name_w}}  {t['diff']:<{diff_w}}"
            for key, _ in METRIC_COLS:
                val = t["metrics"].get(key)
                cell = fmt(val)
                # Mark best value with *
                if val is not None and key in best and val == best[key]:
                    cell = f"*{cell}"
                row += f"  {cell:>{metric_w}}"
            row += f"    {t['timestamp']}  git:{t['git_hash']}"
            print(row)

        print(header_sep)
        print("* = best value in column")

    # ── Pending / failed trials ────────────────────────────────────────────────
    if pending:
        print("\nNOT YET COMPLETE (no metrics.json):")
        for t in pending:
            print(f"  {t['name']}  —  {t['diff']}  [{t['timestamp']}]")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare trial metrics.")
    parser.add_argument("day_dir", type=Path)
    parser.add_argument("--probe",  default=None, choices=["a", "b"],
                        help="Filter to a specific probe")
    parser.add_argument("--shank",  default=None, choices=["0", "1", "2", "3"],
                        help="Filter to a specific shank")
    args = parser.parse_args()

    trials_root = args.day_dir / "processing_pipeline_trials"
    if not trials_root.exists():
        print(f"No trials directory found at {trials_root}")
        return

    trials = []
    for trial_dir in sorted(trials_root.iterdir()):
        if not trial_dir.is_dir() or trial_dir.name == "logs":
            continue
        entry = load_trial(trial_dir, args.probe, args.shank)
        if entry is not None:
            trials.append(entry)

    if not trials:
        print(f"No trials found in {trials_root}"
              + (f" (probe={args.probe})" if args.probe else "")
              + (f" (shank={args.shank})" if args.shank else ""))
        return

    print(f"\nDay : {args.day_dir}")
    if args.probe:
        print(f"Probe: {args.probe}  Shank: {args.shank or 'all'}")
    print(f"Found {len(trials)} trial(s)\n")

    print_table(trials)

    # ── Per-trial diff detail ──────────────────────────────────────────────────
    print("PARAMETER DIFFS (vs default pipeline):")
    for t in sorted(trials, key=lambda x: x["timestamp"]):
        print(f"\n  [{t['name']}]  {t['diff']}")


if __name__ == "__main__":
    main()
