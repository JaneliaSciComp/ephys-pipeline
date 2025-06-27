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

SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384
N_CHANNELS_SHANK = 96

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
        probe['device_channel_indices'] = np.arange(0, 96)

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

def split_recording(recording_files, probe_names, global_probe_data):
    '''Split the recording into shanks
    Args:
    recording_files (dict): dictionary with probe names as keys and list of files as values
    probe_names (list): list of probe names
    global_probe_data (dict): dictionary with global probe data
    Returns:
    shank_dict (dict): dictionary with probe names as keys and shank recordings as values
    '''

    recordings = []
    recording_paths = []
    # Loop through each recording file for probe_x
    for recording_file in recording_files:
        # Load the recording file
        recording = se.read_binary(recording_file, dtype='int16', sampling_frequency=SAMPLE_RATE, num_channels=N_CHANNELS_PROBE)
        # Sanity check for user
        print(recording.get_total_duration())
        # Append the recording to the list and ignore any empty
        if recording.get_num_frames() == 0:
            print(f"Warning: The recording file {recording_file} is empty.")
        else:
            recording.set_probe(global_probe_data)
            sample_shifts = get_sample_shifts(N_CHANNELS_PROBE)
            recording = spre.phase_shift(recording, inter_sample_shift=sample_shifts)
            recordings.append(recording)
            recording_paths.append(recording_file)
    # Concatenate the recordings
    total_recording = si.concatenate_recordings(recordings)
    # Set the group property to the shank index
    total_recording.set_property("group", global_probe_data.shank_ids)
    # Split the recording by group
    total_recording = total_recording.split_by("group")
    return total_recording, recording_paths

if __name__ == "__main__":
    folder = Path(sys.argv[1])
    probe = sys.argv[2]
    shank_num = sys.argv[3]

    data_folder = folder / "data"
    output_folder = folder / "output"
    probe_name = f"np2-{probe}-amp"

    probe_file = folder / f"{probe}_probe_conf.json"
    probe_data = load_probe(probe_file)

    recording_files = collect_files(data_folder, probe_name)
    total_recording, recording_paths = split_recording(recording_files, probe_name, probe_data)

    output_folder.mkdir(parents=True, exist_ok=True)
    probe_folder = output_folder / probe 
    probe_folder.mkdir(parents=True, exist_ok=True)
    shank_folder = probe_folder / f"shank_{shank_num}"
    shank_folder.mkdir(parents=True, exist_ok=True)

    shank_probe = load_probe(probe_file, shank_num)
    total_recording[str(shank_num)] = total_recording[str(shank_num)].set_probe(shank_probe)

    print("Saving shank recording")

    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        total_recording[str(shank_num)], shank_folder, data_name=f'shank_recording.bin', dtype=np.int16,
        chunksize=30000 * 8, export_probe=True, probe_name='probe.prb',
        max_workers=8
        )
    
    # Specify probe configuration.
    assert probe_path is not None, 'No probe information exported by SpikeInterface'
    kilosort_probe = io.load_probe(probe_path)

    print("Running Kilosort...")
    # This command will both run the spike-sorting analysis and save the results to
    # `DATA_DIRECTORY`.

    settings = {'fs': fs, 'n_chan_bin': c, 'batch_size': 30000 * 8}

    ops, st, clu, tF, Wall, similar_templates, is_ref, \
        est_contam_rate, kept_spikes = run_kilosort(
            settings=settings, probe=kilosort_probe, filename=filename
            )
