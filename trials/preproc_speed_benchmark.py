#!/usr/bin/env python3
"""Benchmark: two-pass artifact removal vs lazy SaturationArtifactRemover.

Approach A (current):
    highpass → phase_shift → detect_saturation_periods (full scan)
                           → remove_saturation_artifacts (lazy)
                           → split_by_shank → save

Approach B (new):
    highpass → phase_shift → SaturationArtifactRemover (lazy)
                           → split_by_shank → save

Both save the per-shank float32 binary to a temp folder so the full lazy
chain is materialised exactly once.  Timing is wall-clock via time.perf_counter.

Usage:
    python trials/preproc_speed_benchmark.py \\
        --data_dir /path/to/day_dir --probe a --shank 0 \\
        --output_dir /path/to/bench_output

    # or via the submit script:
    ./trials/submit_preproc_benchmark.sh /path/to/day_dir a 0 \\
        trials/runs/preproc_speed_test.yaml
"""

from __future__ import annotations

import argparse
import glob
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import spikeinterface.extractors as se
import spikeinterface.full as si
import spikeinterface.preprocessing as spre
from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts

from utils.get_artifacts import (
    SaturationArtifactRemover,
    detect_saturation_periods,
    remove_saturation_artifacts,
)
from utils.probe_utils import load_probe

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384


def _get_sample_shifts() -> np.ndarray:
    return get_neuropixels_sample_shifts(384, 16, 16)


def _load_raw(data_dir: Path, probe: str, duration_hours: float, n_jobs: int):
    """Return concatenated raw recording trimmed to duration_hours, plus probe data."""
    probe_name = f"np2-{probe}-ephys"
    recording_files = sorted(glob.glob(str(data_dir / "data" / f"{probe_name}*")))
    if not recording_files:
        raise FileNotFoundError(f"No files matching {probe_name}* in {data_dir / 'data'}")
    print(f"Found {len(recording_files)} file(s) for probe {probe}")

    recs = []
    for rf in recording_files:
        r = se.read_binary(
            rf, dtype="int16", sampling_frequency=SAMPLE_RATE,
            num_channels=N_CHANNELS_PROBE,
        )
        if r.get_num_frames() > 0:
            recs.append(r)

    rec = si.concatenate_recordings(recs)
    n_frames_target = int(duration_hours * 3600 * SAMPLE_RATE)
    if rec.get_num_frames() < n_frames_target:
        print(
            f"Warning: recording is only {rec.get_total_duration() / 3600:.2f} h; "
            f"benchmarking on full length."
        )
    else:
        rec = rec.frame_slice(0, n_frames_target)

    print(f"Benchmark duration: {rec.get_total_duration() / 3600:.3f} h  "
          f"({rec.get_num_frames():,} frames)")
    return rec


def _base_chain(raw_rec, probe_data):
    """Highpass + phase shift — shared by both approaches."""
    rec = si.concatenate_recordings([raw_rec]) if raw_rec.get_num_segments() > 1 else raw_rec
    rec.set_probe(probe_data)
    rec = si.highpass_filter(rec, ftype="bessel", dtype="float32")
    rec = spre.phase_shift(rec, inter_sample_shift=_get_sample_shifts())
    return rec


def _split_shank(rec, probe_data, shank_num, shank_probe):
    """Split a full-probe recording to a single shank."""
    rec.set_property("group", probe_data.shank_ids)
    rec_split = rec.split_by("group")
    key = str(shank_num)
    if key not in rec_split:
        key = int(shank_num)
    return rec_split[key].set_probe(shank_probe)


def run_approach_a(raw_rec, probe_data, shank_num, shank_probe,
                   out_dir: Path, n_jobs: int, abs_threshold: int) -> dict:
    """Two-pass: detect then remove (current approach)."""
    rec = _base_chain(raw_rec, probe_data)

    t0 = time.perf_counter()
    saturation_idx = detect_saturation_periods(
        rec, abs_threshold=abs_threshold, direction="upper",
        chunk_size=SAMPLE_RATE * 10, n_jobs=n_jobs,
    )
    t_detect = time.perf_counter() - t0
    print(f"[A] detect_saturation_periods: {t_detect:.1f}s")

    rec = remove_saturation_artifacts(
        rec, list_periods=saturation_idx, ms_before=10, ms_after=10, mode="linear",
    )
    rec = _split_shank(rec, probe_data, shank_num, shank_probe)

    save_dir = out_dir / "approach_a_save"
    t0 = time.perf_counter()
    rec.save(folder=str(save_dir), overwrite=True, n_jobs=n_jobs)
    t_save = time.perf_counter() - t0
    print(f"[A] save: {t_save:.1f}s")

    shutil.rmtree(save_dir, ignore_errors=True)
    return {"detect_s": t_detect, "save_s": t_save, "total_s": t_detect + t_save}


def run_approach_b(raw_rec, probe_data, shank_num, shank_probe,
                   out_dir: Path, n_jobs: int, abs_threshold: int,
                   margin_ms: float) -> dict:
    """Single-pass: SaturationArtifactRemover (new approach)."""
    rec = _base_chain(raw_rec, probe_data)
    rec = SaturationArtifactRemover(
        rec, abs_threshold=abs_threshold, direction="upper",
        ms_before=10, ms_after=10, mode="linear", margin_ms=margin_ms,
    )
    rec = _split_shank(rec, probe_data, shank_num, shank_probe)

    save_dir = out_dir / "approach_b_save"
    t0 = time.perf_counter()
    rec.save(folder=str(save_dir), overwrite=True, n_jobs=n_jobs)
    t_save = time.perf_counter() - t0
    print(f"[B] save (detect + remove inline): {t_save:.1f}s")

    shutil.rmtree(save_dir, ignore_errors=True)
    return {"detect_s": 0.0, "save_s": t_save, "total_s": t_save}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark two-pass vs lazy artifact removal"
    )
    parser.add_argument("--data_dir",       required=True)
    parser.add_argument("--probe",          required=True, choices=["a", "b"])
    parser.add_argument("--shank",          required=True, choices=["0", "1", "2", "3"])
    parser.add_argument("--output_dir",     required=True)
    parser.add_argument("--duration_hours", type=float, default=1.0)
    parser.add_argument("--margin_ms",      type=float, default=500.0,
                        help="Margin around each chunk for approach B (ms)")
    parser.add_argument("--abs_threshold",  type=int,   default=1500)
    parser.add_argument("--n_jobs",         type=int,   default=12)
    args = parser.parse_args()

    data_dir   = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    probe_file  = data_dir / f"{args.probe}_probe_conf.json"
    probe_data, _ = load_probe(probe_file)
    shank_probe, n_ch = load_probe(probe_file, args.shank)
    if not n_ch:
        print(f"ERROR: shank {args.shank} has no channels"); return 1

    raw_rec = _load_raw(data_dir, args.probe, args.duration_hours, args.n_jobs)

    print("\n" + "=" * 60)
    print("APPROACH A — two-pass (detect then remove)")
    print("=" * 60)
    result_a = run_approach_a(
        raw_rec, probe_data, args.shank, shank_probe,
        output_dir, args.n_jobs, args.abs_threshold,
    )

    print("\n" + "=" * 60)
    print("APPROACH B — single-pass SaturationArtifactRemover")
    print("=" * 60)
    result_b = run_approach_b(
        raw_rec, probe_data, args.shank, shank_probe,
        output_dir, args.n_jobs, args.abs_threshold, args.margin_ms,
    )

    speedup = result_a["total_s"] / result_b["total_s"] if result_b["total_s"] > 0 else float("nan")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Duration benchmarked : {args.duration_hours:.2f} h")
    print(f"  [A] detect           : {result_a['detect_s']:.1f}s")
    print(f"  [A] save             : {result_a['save_s']:.1f}s")
    print(f"  [A] total            : {result_a['total_s']:.1f}s")
    print(f"  [B] save (combined)  : {result_b['save_s']:.1f}s")
    print(f"  [B] total            : {result_b['total_s']:.1f}s")
    print(f"  Speedup B vs A       : {speedup:.2f}x")

    summary = {
        "duration_hours": args.duration_hours,
        "probe": args.probe, "shank": args.shank,
        "abs_threshold": args.abs_threshold,
        "margin_ms": args.margin_ms,
        "n_jobs": args.n_jobs,
        "approach_a": result_a,
        "approach_b": result_b,
        "speedup_b_over_a": speedup,
    }
    out_path = output_dir / "preproc_speed_results.json"
    out_path.write_text(json.dumps(summary, indent=2))
    print(f"\nResults saved to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
