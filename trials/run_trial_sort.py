#!/usr/bin/env python3
"""Run preprocessing + KS4 for a trial config, saving output for Phy.

Preprocessing is driven by the same YAML config as preproc_speed_benchmark.py.
Output mirrors the production pipeline: shank_recording.bin + kilosort4/ folder.

Usage:
    python trials/run_trial_sort.py \\
        --data_dir /path/to/day_dir --probe a --shank 0 \\
        --config trials/runs/13_dredge.yaml \\
        --output_dir /path/to/output
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import spikeinterface.extractors as se
import spikeinterface.full as si
import spikeinterface.preprocessing as spre
from kilosort import io, run_kilosort
from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts

from utils.get_artifacts import (
    SaturationArtifactRemover,
    detect_saturation_periods,
    merge_artifact_logs,
    remove_saturation_artifacts,
    save_artifact_periods,
)
from utils.probe_utils import load_probe

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384


def _sample_shifts() -> np.ndarray:
    return get_neuropixels_sample_shifts(384, 16, 16)


def _load_raw(data_dir: Path, probe: str, duration_hours: float) -> si.BaseRecording:
    probe_name = f"np2-{probe}-ephys"
    files = sorted(glob.glob(str(data_dir / "data" / f"{probe_name}*")))
    if not files:
        raise FileNotFoundError(f"No files matching {probe_name}* in {data_dir / 'data'}")
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
    print(f"Duration: {rec.get_total_duration() / 3600:.3f}h ({rec.get_num_frames():,} frames)")
    return rec


def get_git_hash() -> str:
    try:
        r = subprocess.run(
            ["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"],
            capture_output=True, text=True,
        )
        return r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def run_sort(cfg: dict, data_dir: Path, probe: str, shank: str,
             output_dir: Path, scratch_dir: Path | None = None) -> None:

    probe_file = data_dir / f"{probe}_probe_conf.json"
    probe_data, _ = load_probe(probe_file)
    shank_probe, n_ch = load_probe(probe_file, shank)
    if not n_ch:
        raise ValueError(f"Shank {shank} has no channels")

    n_jobs         = cfg.get("n_jobs", 12)
    chunk_duration = cfg.get("chunk_duration", "1s")

    raw = _load_raw(data_dir, probe, cfg.get("duration_hours", 1.0))

    # ── Phase shift on full recording (lazy; 384 shifts indexed correctly before split) ──
    hp = cfg.get("highpass", {})
    rec = raw
    rec.set_probe(probe_data)
    rec = spre.phase_shift(rec, inter_sample_shift=_sample_shifts())

    # ── Split to shank — all further processing on shank channels only ───────
    rec.set_property("group", probe_data.shank_ids)
    rec_split = rec.split_by("group")
    key = str(shank)
    if key not in rec_split:
        key = int(shank)
    rec = rec_split[key].set_probe(shank_probe)

    # ── Artifact removal + highpass (order configurable) ─────────────────────
    art = cfg.get("artifact", {})
    threshold       = art.get("abs_threshold", 1500)
    direction       = art.get("direction", "upper")
    ms_before       = art.get("ms_before", 150.0)
    ms_after        = art.get("ms_after",  150.0)
    before_highpass = art.get("before_highpass", True)

    periods_path = output_dir / "artifact_periods.json"
    use_existing = art.get("use_existing", False) and periods_path.exists()
    lazy         = art.get("lazy", False)
    art_log_dir  = output_dir / "artifact_log"  # populated by lazy remover, merged after intermediate save

    def _detect_and_remove(r):
        if use_existing:
            print(f"Loading existing artifact periods from {periods_path}")
            d = json.loads(periods_path.read_text())
            sat_periods = []
            for seg in d["segments"]:
                flat = []
                for ev in seg:
                    flat.append(int(ev["start_frame"]))
                    flat.append(int(ev["end_frame"]))
                sat_periods.append(flat)
            print(f"  {d['n_artifacts']} artifact periods ({d['total_duration_sec']:.2f}s total)")
            return remove_saturation_artifacts(
                r, list_periods=sat_periods,
                ms_before=ms_before, ms_after=ms_after, mode=art.get("mode", "linear"),
            )
        if lazy:
            print("Using lazy SaturationArtifactRemover (single-pass detect+remove).")
            return SaturationArtifactRemover(
                r, abs_threshold=threshold, direction=direction,
                ms_before=ms_before, ms_after=ms_after, mode=art.get("mode", "linear"),
                margin_ms=art.get("margin_ms", 500.0),
                log_dir=art_log_dir,
            )
        print("Detecting saturation artifacts...")
        sat_periods = detect_saturation_periods(
            r, abs_threshold=threshold, direction=direction,
            chunk_duration=chunk_duration, n_jobs=n_jobs,
        )
        save_artifact_periods(
            sat_periods, r.get_sampling_frequency(), periods_path,
        )
        return remove_saturation_artifacts(
            r, list_periods=sat_periods,
            ms_before=ms_before, ms_after=ms_after, mode=art.get("mode", "linear"),
        )

    if before_highpass:
        rec = _detect_and_remove(rec)
        rec = si.highpass_filter(rec, ftype=hp.get("ftype", "bessel"), dtype="float32")
    else:
        rec = si.highpass_filter(rec, ftype=hp.get("ftype", "bessel"), dtype="float32")
        rec = _detect_and_remove(rec)

    # ── Intermediate save ────────────────────────────────────────────────────
    int_save = cfg.get("intermediate_save", {})
    if int_save.get("enabled", False):
        if scratch_dir is None:
            lsb_jobid = os.environ.get("LSB_JOBID")
            user = os.environ.get("USER") or os.environ.get("USERNAME")
            if lsb_jobid and user:
                scratch_dir = Path(f"/scratch/{user}/{lsb_jobid}")
            else:
                print("WARNING: LSB_JOBID not set — intermediate cache will write to output_dir (network storage, slower).")
                scratch_dir = output_dir
        scratch_dir.mkdir(parents=True, exist_ok=True)
        int_save_dir = scratch_dir / "intermediate_cache"
        print("Saving intermediate cache...")
        t0 = time.perf_counter()
        rec = rec.save(folder=str(int_save_dir), overwrite=True,
                       n_jobs=n_jobs, chunk_duration=chunk_duration)
        print(f"  intermediate_save: {time.perf_counter() - t0:.1f}s")

        # Merge per-chunk artifact logs (only populated if lazy=true)
        if lazy and art_log_dir.exists():
            print("Merging artifact logs...")
            merge_artifact_logs(
                art_log_dir, fs=rec.get_sampling_frequency(),
                output_path=periods_path, n_segments=rec.get_num_segments(),
            )
            import shutil as _sh
            _sh.rmtree(art_log_dir, ignore_errors=True)

    # ── DREDge ───────────────────────────────────────────────────────────────
    dr = cfg.get("dredge", {})
    if dr.get("enabled", True):
        print("Running DREDge...")
        t0 = time.perf_counter()
        n_before = rec.get_num_frames()
        rec = spre.correct_motion(
            rec, preset=dr.get("preset", "dredge"),
            estimate_motion_kwargs={
                "win_step_um":   dr.get("win_step_um",   150),
                "win_scale_um":  dr.get("win_scale_um",  150),
                "win_margin_um": dr.get("win_margin_um", -75),
            },
            n_jobs=n_jobs,
        )
        if rec.get_num_frames() != n_before:
            rec = rec.frame_slice(0, n_before)
        print(f"  dredge: {time.perf_counter() - t0:.1f}s")

    # ── Global CMR + bad channels ────────────────────────────────────────────
    cmr = cfg.get("cmr", {})
    rec = si.common_reference(rec, operator=cmr.get("global_operator", "median"),
                               reference="global")
    rec.set_channel_gains(1)
    rec.set_channel_offsets(0)

    _, dead_labels  = si.detect_bad_channels(rec, method="coherence+psd", seed=42)
    _, noise_labels = si.detect_bad_channels(rec, method="mad", seed=42)
    dead_ids  = rec.get_channel_ids()[dead_labels  == "dead"]
    noise_ids = rec.get_channel_ids()[noise_labels == "noise"]
    print(f"  bad channels — dead={len(dead_ids)}  noise={len(noise_ids)}")
    bad_ids = np.concatenate([dead_ids, noise_ids])
    if bad_ids.size > 0:
        rec = si.interpolate_bad_channels(rec, bad_ids)

    # ── Local CMR + bandpass ─────────────────────────────────────────────────
    rec = si.common_reference(rec, reference=cmr.get("local_reference", "local"))
    bp = cfg.get("bandpass", {})
    rec = spre.bandpass_filter(rec,
                                freq_min=bp.get("freq_min", 300.0),
                                freq_max=bp.get("freq_max", 7500.0),
                                dtype="int16")

    # ── Save binary for KS4 ──────────────────────────────────────────────────
    print("Saving shank_recording.bin...")
    t0 = time.perf_counter()
    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        rec, output_dir, data_name="shank_recording.bin", dtype=np.int16,
        chunksize=int(float(chunk_duration.rstrip("s")) * SAMPLE_RATE),
        export_probe=True, probe_name="probe.prb",
        max_workers=n_jobs,
    )
    print(f"  save: {time.perf_counter() - t0:.1f}s  →  {filename}")

    # Clean up intermediate cache now that save is done
    if int_save.get("enabled", False):
        import shutil
        shutil.rmtree(int_save_dir, ignore_errors=True)

    # ── KS4 ──────────────────────────────────────────────────────────────────
    assert probe_path is not None, "No probe exported"
    kilosort_probe = io.load_probe(probe_path)
    ks = cfg.get("kilosort", {})
    settings = {"fs": fs, "n_chan_bin": c,
                "batch_size": ks.get("batch_size", 30000),
                "nblocks":    ks.get("nblocks", 0)}

    print("Running Kilosort4...")
    t0 = time.perf_counter()
    run_kilosort(settings=settings, probe=kilosort_probe, filename=filename)
    print(f"  kilosort: {time.perf_counter() - t0:.1f}s")

    # Fix dat_path for Phy
    params_py = filename.parent / "kilosort4" / "params.py"
    if params_py.exists():
        text = params_py.read_text()
        text = re.sub(r"^dat_path\s*=.*$", "dat_path = '../shank_recording.bin'",
                      text, flags=re.MULTILINE)
        params_py.write_text(text)
        print(f"Rewrote dat_path in {params_py}")

    # ── Metadata ─────────────────────────────────────────────────────────────
    info = {
        "git_hash":      get_git_hash(),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "trial_name":    cfg.get("trial_name", ""),
        "config":        cfg,
        "data_dir":      str(data_dir),
        "probe":         probe,
        "shank":         shank,
    }
    (output_dir / "pipeline_info.json").write_text(json.dumps(info, indent=2))
    subprocess.run(["chmod", "-R", "777", str(output_dir)], check=True)
    print(f"\nDone. Output: {output_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Preprocess + KS4 for a trial config")
    parser.add_argument("--data_dir",   required=True)
    parser.add_argument("--probe",      required=True, choices=["a", "b"])
    parser.add_argument("--shank",      required=True, choices=["0", "1", "2", "3"])
    parser.add_argument("--config",     required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--scratch_dir", default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(args.scratch_dir) if args.scratch_dir else None
    if scratch_dir:
        scratch_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 60}")
    print(f"Trial : {cfg.get('trial_name', Path(args.config).stem)}")
    print(f"Probe : {args.probe}  shank {args.shank}")
    print(f"{'=' * 60}\n")

    run_sort(cfg, Path(args.data_dir), args.probe, args.shank,
             output_dir, scratch_dir=scratch_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
