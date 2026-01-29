# load packages
import sys
import os
import glob
import spikeinterface.extractors as se
import spikeinterface.full as si
from pathlib import Path
from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts
import spikeinterface.preprocessing as spre
import numpy as np
import json
import probeinterface as pi
from kilosort import io
from kilosort import run_kilosort
import time
from get_artifacts import detect_saturation_periods

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384
global N_CHANNELS_SHANK
N_CHANNELS_SHANK = 96 #192

def find_active_channels(probe_dict):
    dev_ind = np.array(probe_dict['probes'][0]["device_channel_indices"])
    return dev_ind != -1

def find_shank_channels(probe_dict, shank_num):
    shank_ind = np.array(probe_dict["shank_ids"]) 
    return shank_ind == str(shank_num)

def load_probe(probe_file, shank_num=None):
    # Load probe configuration file
    with open(probe_file, 'r') as f:
        probe_dict = json.load(f)
    
    active_channels_mask = find_active_channels(probe_dict)
    probe = probe_dict['probes'][0] # get the first probe

    # First filter by active channels
    for key in ['contact_positions', 'contact_plane_axes', 'contact_shapes', 'contact_shape_params', 'device_channel_indices', 'contact_ids', 'shank_ids']:
        probe[key] = np.array(probe[key])[active_channels_mask]

    # Then filter by shank if specified
    if shank_num is not None:
        shank_channels_mask = find_shank_channels(probe, shank_num)
        #shank_channels_mask = shank_channels_mask[active_channels_mask]
        for key in ['contact_positions', 'contact_plane_axes', 'contact_shapes', 'contact_shape_params', 'device_channel_indices', 'contact_ids', 'shank_ids']:
            probe[key] = probe[key][shank_channels_mask]
        probe['device_channel_indices'] = np.arange(0, N_CHANNELS_SHANK) #96)

    probe = pi.Probe.from_dict(probe)
    return probe

def collect_files(data_folder, probe_name):
    '''Collect all files for each probe within data_folder
    Args:
    data_folder (Path): path to the data folder
    probe_name (str): probe name
    output_folder (Path, optional): path to output folder for creating probe directories
    Returns:
    recording_files (dict): dictionary with probe names as keys and list of files as values
    '''

    # Collect all files for probe_x within data_folder
    recording_files = glob.glob(f"{str(data_folder)}/{probe_name}*")

    # Print the number of files found for each probe for user feedback
    print(f'Found {len(recording_files)} files for {probe_name}')

    return recording_files

def get_sample_shifts(n_channels):
    total_channels = 384
    num_channels_per_adc = 16
    num_channels_in_adc = 16 

    sample_shifts = get_neuropixels_sample_shifts(total_channels, num_channels_per_adc, num_channels_in_adc)
    return sample_shifts

def split_recording(recording_files, probe_name, global_probe_data, shank_num):
    """
    Process and clean the recording for a specific shank.

    Args:
        recording_files (list): List of file paths for the probe.
        probe_name (str): Probe name (not used, kept for interface compatibility).
        global_probe_data (Probe): Probe data object with geometry details.
        shank_num (int or str): Which shank to process.

    Returns:
        destriped_rec (RecordingExtractor): Processed and cleaned shank recording.
        recording_paths (list): Paths of recording files used.
    """
    recordings = []
    recording_paths = []

    for recording_file in recording_files:
        # Load and quick slice for preview (first 10 seconds, adjust as needed)
        recording = se.read_binary(
            recording_file, 
            dtype='int16', 
            sampling_frequency=SAMPLE_RATE, 
            num_channels=N_CHANNELS_PROBE
        )
        recording = recording.frame_slice(start_frame=0, end_frame=30000 * 10)
        print(recording.get_total_duration())

        if recording.get_num_frames() == 0:
            print(f"Warning: {recording_file} is empty.")
            continue

        # Artifact removal
        saturation_idx = detect_saturation_periods(
            recording,
            abs_threshold=3900,
            direction="upper",
            chunk_size=30000 * 10,
            n_jobs=12,
        )
        recording = si.remove_artifacts(
            recording,
            list_triggers=saturation_idx,
            ms_before=10,
            ms_after=10,
            mode="zeros"
        )
        recordings.append(recording)
        recording_paths.append(recording_file)

    # Concatenate all into a single recording
    total_recording = si.concatenate_recordings(recordings)
    total_recording.set_probe(global_probe_data)
    total_recording.set_channel_locations(global_probe_data.contact_positions)
    total_recording = si.highpass_filter(total_recording, ftype='bessel', dtype='float32')
    sample_shifts = get_sample_shifts(N_CHANNELS_PROBE)
    total_recording = spre.phase_shift(total_recording, inter_sample_shift=sample_shifts)
    total_recording.set_property("group", global_probe_data.shank_ids)

    # Split out just the specified shank
    rec_split = total_recording.split_by("group")
    shank_key = str(shank_num)
    if shank_key not in rec_split:
        try:
            shank_key = int(shank_num)
        except Exception:
            pass
    if shank_key not in rec_split:
        raise KeyError(f"Shank '{shank_num}' (key '{shank_key}') not found in split recording groups: {list(rec_split.keys())}")

    rec = rec_split[shank_key]
    print(f"Detecting and interpolating over bad channels in shank {shank_num}...")

    rec.set_channel_gains(1)
    rec.set_channel_offsets(0)

    # Dead channels
    _, all_channels_dead = si.detect_bad_channels(rec, method='coherence+psd', seed=42)
    dead_mask = (all_channels_dead == 'dead')
    out_mask = (all_channels_dead == 'out')
    dead_channel_ids = rec.get_channel_ids()[dead_mask]
    prec_dead = np.mean(dead_mask) * 100
    print(f"{dead_mask.sum()} ({prec_dead:.0f}%) dead channels in shank {shank_num}")

    # Noisy channels
    _, all_channels_noise = si.detect_bad_channels(rec, method='mad', seed=42)
    noise_mask = (all_channels_noise == 'noise')
    noisy_channel_ids = rec.get_channel_ids()[noise_mask]
    prec_noise = np.mean(noise_mask) * 100
    print(f"{noise_mask.sum()} ({prec_noise:.0f}%) noise channels in shank {shank_num}")

    # Interpolate over bad (dead + noisy) channels
    interp_rec = rec
    bad_channels_to_interpolate = np.concatenate([dead_channel_ids, noisy_channel_ids])
    if bad_channels_to_interpolate.size > 0:
        interp_rec = si.interpolate_bad_channels(interp_rec, bad_channels_to_interpolate)

    interp_rec = si.common_reference(interp_rec, reference='local')

    # Spatial destriping
    destriped_rec = si.highpass_spatial_filter(interp_rec)

    return destriped_rec, recording_paths

if __name__ == "__main__":
    folder = Path(sys.argv[1])
    probe = sys.argv[2]
    shank_num = sys.argv[3]

    # set shank channel count based on probe type
    if probe.lower() == "a":
        N_CHANNELS_SHANK = 96 #*2 depending on probeconfig -> should automate in future
    elif probe.lower() == "b":
        N_CHANNELS_SHANK = 96       
    else:
        N_CHANNELS_SHANK = 96 #192      

    print("n_channels_shank:", N_CHANNELS_SHANK)

    data_folder = folder / "data"
    output_folder = folder / "output"
    probe_name = f"np2-{probe}-ephys" #f"np2-{probe}-amp"

    probe_file = folder / f"{probe}_probe_conf.json"
    probe_data = load_probe(probe_file)

    recording_files = collect_files(data_folder, probe_name)
    shank_recording, recording_paths = split_recording(recording_files, probe_name, probe_data, shank_num)

    output_folder.mkdir(parents=True, exist_ok=True)
    probe_folder = output_folder / probe 
    probe_folder.mkdir(parents=True, exist_ok=True)
    shank_folder = probe_folder / f"shank_{shank_num}"
    shank_folder.mkdir(parents=True, exist_ok=True)

    shank_probe = load_probe(probe_file, shank_num)
    #total_recording[str(shank_num)] = total_recording[str(shank_num)].set_probe(shank_probe)

    print("Saving shank recording...")
    try:
        print(time.time())
        start_time = time.time()
    except:
        pass

    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        shank_recording, shank_folder, data_name=f'shank_recording.bin', dtype=np.int16,
        chunksize=30000 * 8, export_probe=True, probe_name='probe.prb',
        max_workers=12
        )
    print(f"Saved binary recording to {filename}")
    try:
        print(f"duration {time.time() - start_time}")
    except:
        pass
    
    # Specify probe configuration.
    assert probe_path is not None, 'No probe information exported by SpikeInterface'
    kilosort_probe = io.load_probe(probe_path)

    print("Running Kilosort...")
    # This command will both run the spike-sorting analysis and save the results to
    # `DATA_DIRECTORY`.

    settings = {'fs': fs, 'n_chan_bin': c, 'batch_size': 30000 * 8} #, 'Th': [10, 6]}

    ops, st, clu, tF, Wall, similar_templates, is_ref, \
        est_contam_rate, kept_spikes = run_kilosort(
            settings=settings, probe=kilosort_probe, filename=filename
            )
    
    print("done sorting.")
    try:
        print(time.time())
    except:
        pass

    print(f"setting permissions recursively: chmod -R 777 {output_folder}")
    try:
        os.system(f"chmod -R 777 '{output_folder}'")
        print("chmod complete")
    except Exception as e:
        print("chmod failed:", e)
