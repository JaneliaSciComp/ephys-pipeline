import probeinterface as pi
import json
import numpy as np
from kilosort import io
from kilosort import run_kilosort

# load packages
import sys
import os
import glob
import spikeinterface.extractors as se
import spikeinterface.full as si
from pathlib import Path

import spikeinterface.extractors as se
import spikeinterface.full as si
import spikeinterface.preprocessing as spre

from spikeinterface.extractors.neuropixels_utils import get_neuropixels_sample_shifts

from pathlib import Path

def collect_files(data_folder, probe_name, output_folder=None):
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

def find_active_channels(probe_dict):
    dev_ind = np.array(probe_dict['probes'][0]["device_channel_indices"])
    return dev_ind[dev_ind != -1], dev_ind!= -1

def load_probe(probe_file):
    # Load probe configuration file
    with open(probe_file, 'r') as f:
        probe_dict = json.load(f)
    
    print(probe_dict['probes'][0].keys())
    active_channels, active_channels_mask = find_active_channels(probe_dict)

    # Create probe from dictionary
    probe = pi.Probe.from_dict(probe_dict['probes'][0])
    probe.set_shank_ids(probe_dict['probes'][0]["shank_ids"])
    probe.set_contact_ids(probe_dict['probes'][0]["contact_ids"])
    shank_ind = np.array(probe_dict['probes'][0]['shank_ids'])[active_channels_mask]
    channel_locations = np.array(probe_dict['probes'][0]['contact_positions'])[active_channels_mask]
    probe_dict = {'probe': probe, 'shankInd': shank_ind, 'activeChannels': active_channels, 'file': probe_dict['probes'][0], 'channel_locations': channel_locations}
    return probe_dict

def combine_recording(recording_files, global_probe_data):

    total_channels = 384
    num_channels_per_adc = 12
    num_channels_in_adc = 12 # or 13 if you have ap stream

    sample_shifts = get_neuropixels_sample_shifts(total_channels, num_channels_per_adc, num_channels_in_adc)

    recordings = []
    recording_paths = []
    # Loop through each recording file for probe_x
    for recording_file in recording_files:
        # Load the recording file
        recording = se.read_binary(recording_file, dtype='int16', sampling_frequency=30000, num_channels=384)
        recording.set_probe(global_probe_data['probe'])
        recording.set_property('inter_sample_shift', sample_shifts)
        recording = spre.phase_shift(recording, inter_sample_shift=sample_shifts)
        # Sanity check for user
        
        #print(recording.get_total_duration())
        # Append the recording to the list and ignore any empty
        if recording.get_num_frames() == 0:
            print(f"Warning: The recording file {recording_file} is empty.")
        else:
            recordings.append(recording)
            recording_paths.append(recording_file)
    # Concatenate the recordings
    if len(recordings) > 1:
        total_recording = si.concatenate_recordings(recordings)
        total_recording.set_probe(global_probe_data['probe'])
    else:
        total_recording = recordings[0]
    total_recording = spre.bandpass_filter(total_recording, freq_min=300., freq_max=7500., dtype='int16')
    total_recording.set_channel_locations(global_probe_data['channel_locations'])
    total_recording = spre.common_reference(total_recording, reference='local', operator='median')

    return total_recording


if __name__ == "__main__":
    # take from command line
    folder = Path(sys.argv[1]) # "/groups/voigts/voigtslab/neuropixels_2025/npx06/2025_06_12_npx06_day4/"
    probe_name = sys.argv[2] # a or b
    data_folder = folder / "data"
    output_folder = folder / "output"
    probe_file = folder / f"{probe_name}_probe_conf.json"
    output_folder = folder / "output"
    rec_filename = f"np2-{probe_name}-amp"

    os.makedirs(str(output_folder) , exist_ok=True)

    probe_data = load_probe(probe_file)
    recording_files = collect_files(data_folder, rec_filename, output_folder)
    os.makedirs(f'{str(output_folder)}/{probe_name}' , exist_ok=True)
    total_recording = combine_recording(recording_files, probe_data)

    dtype = np.int16

    output_folder = f'{folder}/output/{probe_name}'

    print("Saving binary file...")
    filename, N, c, s, fs, probe_path = io.spikeinterface_to_binary(
        total_recording, output_folder, data_name=f'{probe_name}.bin', dtype=dtype,
        chunksize=60000, export_probe=True, probe_name='probe.prb',
        max_workers=32
        )

    output_folder = f'{folder}/output/{probe_name}'

    settings = {'fs': fs, 'n_chan_bin': c}

    # Specify probe configuration.
    assert probe_path is not None, 'No probe information exported by SpikeInterface'
    kilosort_probe = io.load_probe(probe_path)

    print("Running Kilosort...")
    # This command will both run the spike-sorting analysis and save the results to
    # `DATA_DIRECTORY`.
    ops, st, clu, tF, Wall, similar_templates, is_ref, \
        est_contam_rate, kept_spikes = run_kilosort(
            settings=settings, probe=kilosort_probe, filename=filename
            )




