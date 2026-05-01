#!/usr/bin/env python3
"""Spike sorting pipeline for a single probe/shank using Kilosort4."""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import probeinterface as pi
import spikeinterface.curation as sc
import spikeinterface.extractors as se
import spikeinterface.full as si
import spikeinterface.preprocessing as spre
from kilosort import io, run_kilosort
from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts

from utils.get_artifacts import SaturationArtifactRemover, merge_artifact_logs
from utils.probe_utils import load_probe

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384
GAIN_TO_UV = 3.05176 #from bonsai-onix1 np2e docs
OFFSET_TO_UV = -2048 * GAIN_TO_UV

def get_git_hash() -> str:
    try:
        r = subprocess.run(
            ["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"],
            capture_output=True, text=True,
        )
        return r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def collect_files(data_folder: Path, probe_name: str) -> list[str]:
    """Return all recording files for a given probe within data_folder.

    Args:
        data_folder (Path): Path to the data folder.
        probe_name (str): Probe name prefix to match files against.

    Returns:
        list: File paths matching the probe name.
    """
    recording_files = glob.glob(f"{str(data_folder)}/{probe_name}*")
    print(f"Found {len(recording_files)} files for {probe_name}")
    return recording_files


def get_sample_shifts(n_channels: int) -> np.ndarray:
    """Return inter-sample shifts for Neuropixels ADC correction.

    Args:
        n_channels (int): Total number of channels on the probe.

    Returns:
        np.ndarray: Per-channel sample shift values.
    """
    total_channels = 384
    num_channels_per_adc = 16
    num_channels_in_adc = 16
    return get_neuropixels_sample_shifts(total_channels, num_channels_per_adc, num_channels_in_adc)


def split_recording(
    recording_files: list[str],
    probe_name: str,
    global_probe_data: pi.Probe,
    shank_num: int | str,
    shank_folder: Path,
    scratch_dir: Path | None = None,
    duration_s: float | None = None,
) -> tuple[si.BaseRecording, list[str]]:
    """Process and clean the recording for a specific shank.

    Args:
        recording_files (list): List of file paths for the probe.
        probe_name (str): Probe name (kept for interface compatibility).
        global_probe_data (Probe): Global probe with all shanks; channel_slice on
            this drives per-shank selection and probe propagation.
        shank_num (int or str): Which shank to process.

    Returns:
        tuple: (final_rec, recording_paths) — processed RecordingExtractor
            and list of file paths used.
    """
    recordings = []
    recording_paths = []

    for recording_file in recording_files:
        recording = se.read_binary(
            recording_file,
            dtype='int16',
            sampling_frequency=SAMPLE_RATE,
            num_channels=N_CHANNELS_PROBE,
            gain_to_uV=GAIN_TO_UV,
            offset_to_uV=OFFSET_TO_UV
        )
        print(recording.get_total_duration())

        if recording.get_num_frames() == 0:
            print(f"Warning: {recording_file} is empty.")
            continue

        recordings.append(recording)
        recording_paths.append(recording_file)

    total_recording = si.concatenate_recordings(recordings)
    # Attach the global probe (absolute device_channel_indices straight from JSON)
    # to the full recording. channel_slice below will then auto-slice the probe
    # and remap indices to local positions on the per-shank sub-recording — we
    # never touch device_channel_indices ourselves.
    total_recording = total_recording.set_probe(global_probe_data)

    # Optional truncation for short test runs (live runs pass duration_s=None).
    if duration_s is not None:
        n_frames = min(int(duration_s * SAMPLE_RATE), total_recording.get_num_frames())
        total_recording = total_recording.frame_slice(start_frame=0, end_frame=n_frames)
        print(f"Truncated recording to first {n_frames} samples ({n_frames / SAMPLE_RATE:.1f}s)")

    # 1. Phase shift on full recording (lazy; 384 shifts indexed correctly before split)
    sample_shifts = get_sample_shifts(N_CHANNELS_PROBE)
    total_recording = spre.phase_shift(total_recording, inter_sample_shift=sample_shifts)

    # 2. Slice to this shank's channels, identified by their device_channel_indices
    #    in the global probe. select_channels preserves+remaps the attached probe.
    shank_mask = np.asarray(global_probe_data.shank_ids).astype(str) == str(shank_num)
    shank_channel_ids = np.asarray(global_probe_data.device_channel_indices)[shank_mask].tolist()
    rec = total_recording.select_channels(channel_ids=shank_channel_ids)
    print(f"Shank {shank_num} sub-recording: {rec.get_num_channels()} channels, "
          f"channel_ids={rec.get_channel_ids()}, "
          f"probe device_channel_indices={rec.get_probe().device_channel_indices}")

    # 3. Lazy saturation artifact removal — single pass detect+remove on raw shank data;
    #    each chunk worker logs its detected periods to art_log_dir, merged after intermediate save.
    art_log_dir = Path(shank_folder) / "artifact_log"
    rec = SaturationArtifactRemover(
        rec,
        abs_threshold=3500,
        direction="upper",
        ms_before=100,
        ms_after=100,
        mode="linear",
        margin_ms=500,
        log_dir=art_log_dir,
    )

    # 4. Highpass filter on shank only
    rec = si.highpass_filter(rec, ftype='bessel', dtype='float32')

    # 5. Intermediate save — materialise filter chain once before DREDge.
    #    Use local scratch when available, otherwise fall back to shank_folder.
    if scratch_dir is not None:
        cache_base = scratch_dir
    else:
        cache_base = Path(shank_folder)
        print("WARNING: scratch_dir not provided — intermediate cache will write to shank_folder (network storage, slower).")
    cache_base.mkdir(parents=True, exist_ok=True)
    shank_folder_tmp = cache_base / "intermediate_cache"
    print("Saving intermediate cache...")
    rec = rec.save(folder=str(shank_folder_tmp), overwrite=True,
                   n_jobs=12, chunk_duration="5s")

    # 6. Merge per-chunk artifact logs into artifact_periods.json
    if art_log_dir.exists():
        print("Merging artifact logs...")
        merge_artifact_logs(
            art_log_dir, fs=rec.get_sampling_frequency(),
            output_path=Path(shank_folder) / "artifact_periods.json",
            n_segments=rec.get_num_segments(),
        )
        shutil.rmtree(art_log_dir, ignore_errors=True)

    # 7. DREDge motion correction per shank (before CMR)
    print("Running DREDge motion correction...")
    n_frames_before = rec.get_num_frames()
    rec = spre.correct_motion(
        rec, preset="dredge",
        estimate_motion_kwargs={"win_step_um": 150, "win_scale_um": 150, "win_margin_um": -75},
        n_jobs=12,
    )
    if rec.get_num_frames() != n_frames_before:
        rec = rec.frame_slice(start_frame=0, end_frame=n_frames_before)
    print("Motion correction done.")

    destriped_rec = si.common_reference(rec, operator="median", reference="global")

    print(f"Detecting and interpolating over bad channels in shank {shank_num}...")

    # set these with constants gain to uv and offset to uv when .bin initially extracted
    #destriped_rec.set_channel_gains(1)
    #destriped_rec.set_channel_offsets(0)

    _, all_channels_dead = si.detect_bad_channels(destriped_rec, method='coherence+psd', seed=42)
    dead_mask = (all_channels_dead == 'dead')
    out_mask = (all_channels_dead == 'out')
    print(f"{out_mask.sum()} ({np.mean(out_mask) * 100:.0f}%) out channels in shank {shank_num}")
    dead_channel_ids = destriped_rec.get_channel_ids()[dead_mask]
    prec_dead = np.mean(dead_mask) * 100
    print(f"{dead_mask.sum()} ({prec_dead:.0f}%) dead channels in shank {shank_num}")

    _, all_channels_noise = si.detect_bad_channels(destriped_rec, method='mad', seed=42)
    noise_mask = (all_channels_noise == 'noise')
    noisy_channel_ids = destriped_rec.get_channel_ids()[noise_mask]
    prec_noise = np.mean(noise_mask) * 100
    print(f"{noise_mask.sum()} ({prec_noise:.0f}%) noise channels in shank {shank_num}")

    interp_rec = destriped_rec
    bad_channels_to_interpolate = np.concatenate([dead_channel_ids, noisy_channel_ids])
    if bad_channels_to_interpolate.size > 0:
        interp_rec = si.interpolate_bad_channels(interp_rec, bad_channels_to_interpolate)

    interp_rec = si.common_reference(interp_rec, reference='local')
    final_rec = spre.bandpass_filter(interp_rec, freq_min=300., freq_max=7500., dtype='int16')

    return final_rec, recording_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spike-sort one probe/shank with Kilosort4.")
    parser.add_argument("folder", type=Path, help="Day directory containing data/ and output/.")
    parser.add_argument("probe", choices=["a", "b"])
    parser.add_argument("shank_num", choices=["0", "1", "2", "3"])
    parser.add_argument("--duration_s", type=float, default=None,
                        help="If set, truncate concatenated recording to first N seconds (test runs).")
    parser.add_argument("--output_subdir", type=str, default="output",
                        help="Subdirectory of folder for outputs (default: output).")
    args = parser.parse_args()

    folder = args.folder
    probe = args.probe
    shank_num = args.shank_num
    duration_s = args.duration_s

    data_folder = folder / "data"
    output_folder = folder / args.output_subdir
    probe_name = f"np2-{probe}-ephys"

    probe_file = folder / f"{probe}_probe_conf.json"
    probe_data, _ = load_probe(probe_file)
    _, N_CHANNELS_SHANK = load_probe(probe_file, shank_num)

    if N_CHANNELS_SHANK is None or N_CHANNELS_SHANK == 0:
        print(f"ERROR: Shank {shank_num} has zero channels. Exiting without processing.")
        sys.exit(1)

    print(f"n_channels_shank: {N_CHANNELS_SHANK} (extracted from JSON)")

    output_folder.mkdir(parents=True, exist_ok=True)
    probe_folder = output_folder / probe
    probe_folder.mkdir(parents=True, exist_ok=True)
    shank_folder = probe_folder / f"shank_{shank_num}"
    shank_folder.mkdir(parents=True, exist_ok=True)

    # Auto-detect node-local scratch when running under LSF.
    lsb_jobid = os.environ.get("LSB_JOBID")
    user = os.environ.get("USER") or os.environ.get("USERNAME")
    scratch_dir = Path(f"/scratch/{user}/{lsb_jobid}") if (lsb_jobid and user) else None
    if scratch_dir:
        scratch_dir.mkdir(parents=True, exist_ok=True)

    recording_files = collect_files(data_folder, probe_name)
    shank_recording, recording_paths = split_recording(
        recording_files, probe_name, probe_data, shank_num,
        shank_folder=shank_folder, scratch_dir=scratch_dir,
        duration_s=duration_s,
    )

    print("Saving shank recording...")
    start_time = time.time()

    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        shank_recording, shank_folder, data_name='shank_recording.bin', dtype=np.int16,
        chunksize=30000 * 5, export_probe=True, probe_name='probe.prb',
        max_workers=12,
    )

    cache_base = scratch_dir if scratch_dir else shank_folder
    shutil.rmtree(cache_base / "intermediate_cache", ignore_errors=True)
    print(f"Saved binary recording to {filename}")
    print(f"Duration: {time.time() - start_time:.1f}s")

    assert probe_path is not None, 'No probe information exported by SpikeInterface'
    kilosort_probe = io.load_probe(probe_path)

    print(shank_recording.get_total_duration())
    print("Running Kilosort...")

    settings = {'fs': fs, 'n_chan_bin': c, 'batch_size': 30000, 'nblocks': 0}

    ops, st, clu, tF, Wall, similar_templates, is_ref, \
        est_contam_rate, kept_spikes = run_kilosort(
            settings=settings, probe=kilosort_probe, filename=filename,
        )

    print("Done sorting.")

    # Kilosort hardcodes the absolute path into params.py, which breaks on Windows.
    # Rewrite dat_path to a relative path so Phy works on any OS.
    params_py = filename.parent / 'kilosort4' / 'params.py'
    if params_py.exists():
        text = params_py.read_text()
        text = re.sub(r"^dat_path\s*=.*$", "dat_path = '../shank_recording.bin'",
                      text, flags=re.MULTILINE)
        params_py.write_text(text)
        print(f"Rewrote dat_path in {params_py}")
    else:
        print(f"WARNING: params.py not found at {params_py}, dat_path not patched")

    pipeline_info = {
        "git_hash":       get_git_hash(),
        "run_timestamp":  datetime.now(timezone.utc).isoformat(),
        "day_dir":        str(folder),
        "probe":          probe,
        "shank_num":      shank_num,
        "recording_files": recording_paths,
    }
    info_path = shank_folder / "pipeline_info.json"
    info_path.write_text(json.dumps(pipeline_info, indent=2))
    print(f"Saved pipeline_info.json (git: {pipeline_info['git_hash'][:8]})")

    subprocess.run(["chmod", "-R", "777", str(output_folder)], check=True)
