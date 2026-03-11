#!/usr/bin/env python3
"""Spike sorting pipeline for a single probe/shank using Kilosort4."""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import time
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

from utils.get_artifacts import detect_saturation_periods
from utils.probe_utils import load_probe

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384


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
    shank_probe: pi.Probe,
) -> tuple[si.BaseRecording, list[str]]:
    """Process and clean the recording for a specific shank.

    Args:
        recording_files (list): List of file paths for the probe.
        probe_name (str): Probe name (kept for interface compatibility).
        global_probe_data (Probe): Probe data object with geometry details.
        shank_num (int or str): Which shank to process.
        shank_probe (Probe): Probe object filtered to the target shank.

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
        )
        print(recording.get_total_duration())

        if recording.get_num_frames() == 0:
            print(f"Warning: {recording_file} is empty.")
            continue

        saturation_idx = detect_saturation_periods(
            recording,
            abs_threshold=3900,
            direction="upper",
            chunk_size=30000 * 10,
            n_jobs=12,
        )
        print(saturation_idx)
        recording = si.remove_artifacts(
            recording,
            list_triggers=saturation_idx,
            ms_before=10,
            ms_after=10,
            mode="zeros",
        )
        recordings.append(recording)
        recording_paths.append(recording_file)

    total_recording = si.concatenate_recordings(recordings)
    total_recording.set_probe(global_probe_data)
    total_recording.set_channel_locations(global_probe_data.contact_positions)

    total_recording = si.highpass_filter(total_recording, ftype='bessel', dtype='float32')
    sample_shifts = get_sample_shifts(N_CHANNELS_PROBE)
    total_recording = spre.phase_shift(total_recording, inter_sample_shift=sample_shifts)
    total_recording.set_property("group", global_probe_data.shank_ids)

    rec_split = total_recording.split_by("group")
    shank_key = str(shank_num)
    if shank_key not in rec_split:
        try:
            shank_key = int(shank_num)
        except Exception:
            pass
    if shank_key not in rec_split:
        raise KeyError(
            f"Shank '{shank_num}' (key '{shank_key}') not found in split recording groups: "
            f"{list(rec_split.keys())}"
        )

    rec = rec_split[shank_key]
    destriped_rec = si.highpass_spatial_filter(rec, dtype='int16')
    destriped_rec = destriped_rec.set_probe(shank_probe)

    print(f"Detecting and interpolating over bad channels in shank {shank_num}...")

    destriped_rec.set_channel_gains(1)
    destriped_rec.set_channel_offsets(0)

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
    folder = Path(sys.argv[1])
    probe = sys.argv[2]
    shank_num = sys.argv[3]

    data_folder = folder / "data"
    output_folder = folder / "output"
    probe_name = f"np2-{probe}-ephys"

    probe_file = folder / f"{probe}_probe_conf.json"
    probe_data, _ = load_probe(probe_file)
    shank_probe, N_CHANNELS_SHANK = load_probe(probe_file, shank_num)

    if N_CHANNELS_SHANK is None or N_CHANNELS_SHANK == 0:
        print(f"ERROR: Shank {shank_num} has zero channels. Exiting without processing.")
        sys.exit(1)

    print(f"n_channels_shank: {N_CHANNELS_SHANK} (extracted from JSON)")
    print(shank_probe)

    recording_files = collect_files(data_folder, probe_name)
    shank_recording, recording_paths = split_recording(
        recording_files, probe_name, probe_data, shank_num, shank_probe
    )

    output_folder.mkdir(parents=True, exist_ok=True)
    probe_folder = output_folder / probe
    probe_folder.mkdir(parents=True, exist_ok=True)
    shank_folder = probe_folder / f"shank_{shank_num}"
    shank_folder.mkdir(parents=True, exist_ok=True)

    print("Saving shank recording...")
    start_time = time.time()

    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        shank_recording, shank_folder, data_name='shank_recording.bin', dtype=np.int16,
        chunksize=30000 * 8, export_probe=True, probe_name='probe.prb',
        max_workers=12,
    )
    print(f"Saved binary recording to {filename}")
    print(f"Duration: {time.time() - start_time:.1f}s")

    assert probe_path is not None, 'No probe information exported by SpikeInterface'
    kilosort_probe = io.load_probe(probe_path)

    print(shank_recording.get_total_duration())
    print("Running Kilosort...")

    settings = {'fs': fs, 'n_chan_bin': c, 'batch_size': 30000}

    ops, st, clu, tF, Wall, similar_templates, is_ref, \
        est_contam_rate, kept_spikes = run_kilosort(
            settings=settings, probe=kilosort_probe, filename=filename,
        )

    print("Done sorting.")

    subprocess.run(["chmod", "-R", "777", str(output_folder)], check=True)
