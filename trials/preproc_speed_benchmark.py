#!/usr/bin/env python3
"""Run one preprocessing trial and record per-step timing.

Each trial is defined by a YAML config that controls which pipeline
variant to use and its parameters.  Run different configs to compare
approaches; results land in output_dir/preproc_timing.json.

Usage:
    python trials/preproc_speed_benchmark.py \\
        --data_dir /path/to/day_dir --probe a --shank 0 \\
        --config trials/runs/baseline.yaml \\
        --output_dir /path/to/results

    # or via the submit script:
    ./trials/submit_preproc_benchmark.sh /path/to/day_dir a 0 \\
        trials/runs/baseline.yaml

Steps timed
-----------
artifact_detect : detect_saturation_periods scan (two_pass only; 0 for lazy)
dredge          : motion estimation via DREDge
bad_channels    : both detect_bad_channels calls
save            : materialise full chain → binary on disk
total           : sum of above
"""

from __future__ import annotations

import argparse
import glob
import json
import shutil
import sys
import time
from pathlib import Path

import yaml

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sample_shifts() -> np.ndarray:
    return get_neuropixels_sample_shifts(384, 16, 16)


def _load_raw(data_dir: Path, probe: str, duration_hours: float) -> si.BaseRecording:
    probe_name = f"np2-{probe}-ephys"
    files = sorted(glob.glob(str(data_dir / "data" / f"{probe_name}*")))
    if not files:
        raise FileNotFoundError(f"No files matching {probe_name}* in {data_dir / 'data'}")
    print(f"Found {len(files)} file(s) for probe {probe}")

    recs = [
        se.read_binary(f, dtype="int16", sampling_frequency=SAMPLE_RATE,
                       num_channels=N_CHANNELS_PROBE)
        for f in files if se.read_binary(
            f, dtype="int16", sampling_frequency=SAMPLE_RATE,
            num_channels=N_CHANNELS_PROBE).get_num_frames() > 0
    ]
    rec = si.concatenate_recordings(recs)

    target = int(duration_hours * 3600 * SAMPLE_RATE)
    if rec.get_num_frames() >= target:
        rec = rec.frame_slice(0, target)
    else:
        print(f"Warning: recording shorter than {duration_hours}h; "
              f"using full {rec.get_total_duration() / 3600:.3f}h")
    print(f"Benchmark duration: {rec.get_total_duration() / 3600:.3f}h "
          f"({rec.get_num_frames():,} frames)")
    return rec


def _tick(label: str, t0: float) -> float:
    elapsed = time.perf_counter() - t0
    print(f"  {label}: {elapsed:.1f}s")
    return elapsed


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_trial(cfg: dict, data_dir: Path, probe: str, shank: str,
              output_dir: Path, scratch_dir: Path | None = None) -> dict:
    timings: dict[str, float] = {}

    probe_file = data_dir / f"{probe}_probe_conf.json"
    probe_data, _ = load_probe(probe_file)
    shank_probe, n_ch = load_probe(probe_file, shank)
    if not n_ch:
        raise ValueError(f"Shank {shank} has no channels")

    raw = _load_raw(data_dir, probe, cfg.get("duration_hours", 1.0))
    n_jobs        = cfg.get("n_jobs", 12)
    chunk_duration = cfg.get("chunk_duration", "1s")

    # ── Highpass + phase shift (lazy) ────────────────────────────────────────
    hp = cfg.get("highpass", {})
    rec = raw
    rec.set_probe(probe_data)
    rec = si.highpass_filter(rec, ftype=hp.get("ftype", "bessel"), dtype="float32")
    rec = spre.phase_shift(rec, inter_sample_shift=_sample_shifts())

    # ── Artifact removal ─────────────────────────────────────────────────────
    art = cfg.get("artifact", {})
    method      = art.get("method", "two_pass")
    threshold   = art.get("abs_threshold", 1500)
    direction   = art.get("direction", "upper")
    ms_before   = art.get("ms_before", 10.0)
    ms_after    = art.get("ms_after", 10.0)
    margin_ms   = art.get("margin_ms", 500.0)

    if method == "two_pass":
        t0 = time.perf_counter()
        sat_idx = detect_saturation_periods(
            rec, abs_threshold=threshold, direction=direction,
            chunk_duration=chunk_duration, n_jobs=n_jobs,
        )
        timings["artifact_detect"] = _tick("artifact_detect", t0)
        rec = remove_saturation_artifacts(
            rec, list_periods=sat_idx,
            ms_before=ms_before, ms_after=ms_after, mode="linear",
        )
    elif method == "lazy":
        timings["artifact_detect"] = 0.0
        rec = SaturationArtifactRemover(
            rec, abs_threshold=threshold, direction=direction,
            ms_before=ms_before, ms_after=ms_after, mode="linear",
            margin_ms=margin_ms,
        )
    else:
        raise ValueError(f"Unknown artifact method: {method!r} "
                         "(expected 'two_pass' or 'lazy')")

    # ── Split to shank (lazy) ────────────────────────────────────────────────
    rec.set_property("group", probe_data.shank_ids)
    rec_split = rec.split_by("group")
    key = str(shank)
    if key not in rec_split:
        key = int(shank)
    rec = rec_split[key].set_probe(shank_probe)

    # ── Intermediate save (materialise filter chain once before DREDge) ──────
    int_save = cfg.get("intermediate_save", {})
    if int_save.get("enabled", False):
        cache_base = scratch_dir if scratch_dir else output_dir
        int_save_dir = cache_base / "intermediate_cache"
        t0 = time.perf_counter()
        rec = rec.save(folder=str(int_save_dir), overwrite=True, n_jobs=n_jobs,
                       chunk_duration=chunk_duration)
        timings["intermediate_save"] = _tick("intermediate_save", t0)
    else:
        timings["intermediate_save"] = 0.0

    # ── DREDge ───────────────────────────────────────────────────────────────
    dr = cfg.get("dredge", {})
    if dr.get("enabled", True):
        t0 = time.perf_counter()
        n_before = rec.get_num_frames()
        rec = spre.correct_motion(
            rec, preset=dr.get("preset", "dredge"),
            estimate_motion_kwargs={
                "win_step_um":   dr.get("win_step_um",   100),
                "win_scale_um":  dr.get("win_scale_um",  100),
                "win_margin_um": dr.get("win_margin_um", -15),
            },
            n_jobs=n_jobs,
        )
        if rec.get_num_frames() != n_before:
            rec = rec.frame_slice(0, n_before)
        timings["dredge"] = _tick("dredge", t0)
    else:
        timings["dredge"] = 0.0
        print("  dredge: skipped")

    # ── Global CMR (lazy) ────────────────────────────────────────────────────
    cmr = cfg.get("cmr", {})
    rec = si.common_reference(rec, operator=cmr.get("global_operator", "median"),
                               reference="global")

    # ── Bad channel detection ────────────────────────────────────────────────
    rec.set_channel_gains(1)
    rec.set_channel_offsets(0)

    t0 = time.perf_counter()
    _, dead_labels = si.detect_bad_channels(rec, method="coherence+psd", seed=42)
    _, noise_labels = si.detect_bad_channels(rec, method="mad", seed=42)
    timings["bad_channels"] = _tick("bad_channels", t0)

    dead_ids  = rec.get_channel_ids()[dead_labels  == "dead"]
    noise_ids = rec.get_channel_ids()[noise_labels == "noise"]
    print(f"    dead={len(dead_ids)}  noise={len(noise_ids)}")

    bad_ids = np.concatenate([dead_ids, noise_ids])
    if bad_ids.size > 0:
        rec = si.interpolate_bad_channels(rec, bad_ids)

    # ── Local CMR + bandpass (lazy) ──────────────────────────────────────────
    rec = si.common_reference(rec, reference=cmr.get("local_reference", "local"))
    bp = cfg.get("bandpass", {})
    rec = spre.bandpass_filter(rec,
                                freq_min=bp.get("freq_min", 300.0),
                                freq_max=bp.get("freq_max", 7500.0),
                                dtype="int16")

    # ── Save (materialises full chain) ───────────────────────────────────────
    cache_base = scratch_dir if scratch_dir else output_dir
    save_dir = cache_base / "recording_cache"
    t0 = time.perf_counter()
    rec.save(folder=str(save_dir), overwrite=True, n_jobs=n_jobs,
             chunk_duration=chunk_duration)
    timings["save"] = _tick("save", t0)
    shutil.rmtree(save_dir, ignore_errors=True)
    shutil.rmtree(cache_base / "intermediate_cache", ignore_errors=True)

    timings["total"] = sum(timings.values())
    return timings


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one preprocessing trial and record timing"
    )
    parser.add_argument("--data_dir",   required=True)
    parser.add_argument("--probe",      required=True, choices=["a", "b"])
    parser.add_argument("--shank",      required=True, choices=["0", "1", "2", "3"])
    parser.add_argument("--config",     required=True, help="YAML trial config")
    parser.add_argument("--output_dir",  required=True)
    parser.add_argument("--scratch_dir", default=None,
                        help="Local scratch dir for temp caches (default: output_dir)")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 60}")
    print(f"Trial : {cfg.get('trial_name', Path(args.config).stem)}")
    print(f"Probe : {args.probe}  shank {args.shank}")
    print(f"{'=' * 60}\n")

    scratch_dir = Path(args.scratch_dir) if args.scratch_dir else None
    if scratch_dir:
        scratch_dir.mkdir(parents=True, exist_ok=True)
        print(f"Scratch : {scratch_dir}")

    timings = run_trial(cfg, Path(args.data_dir), args.probe, args.shank,
                        output_dir, scratch_dir=scratch_dir)

    print(f"\n{'=' * 60}")
    print("TIMING SUMMARY")
    print(f"{'=' * 60}")
    for k, v in timings.items():
        print(f"  {k:<20}: {v:.1f}s")

    result = {
        "trial_name": cfg.get("trial_name", Path(args.config).stem),
        "probe": args.probe,
        "shank": args.shank,
        "config": cfg,
        "timings": timings,
    }
    out_path = output_dir / "preproc_timing.json"
    out_path.write_text(json.dumps(result, indent=2))
    print(f"\nSaved to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
