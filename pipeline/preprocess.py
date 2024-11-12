# load packages
import utils as ut
import config as cfg
import numpy as np
import os
import spikeinterface.preprocessing as spre
import spikeinterface.extractors as se
from multiprocessing import Pool
from tqdm import tqdm 
import sys
from pathlib import Path

def get_artifacts(args):
    ''' Get artifacts from traces
    Args:
    args (tuple): tuple with recording, channels, start_idx, end_idx
    Returns:
    artifact_idx_filtered (np.array): array with artifact indices
    '''
    # I just wanted to try and use args instead of unpacking it
    # Seemed reasonable as user never interacts with this function
    recording, channels, start_idx, end_idx = args
    # Get traces from the recording
    channel_data = recording.get_traces(start_frame=start_idx, end_frame=end_idx, channel_ids=channels)
    filter_array = channel_data == 0 # Get all the zeros in the traces
    artifact_indices = np.nonzero(filter_array)[0] + start_idx # Get the indices of the zeros
    diffs = np.diff(artifact_indices) # Get the difference between the indices
    non_sequential_indices = np.where(diffs != 1)[0] + 1 # Get the indices where the difference is not 1
    non_sequential_indices = np.insert(non_sequential_indices, 0, 0) # Insert 0 at the beginning
    # Bad code to avoid index error
    try:
        artifact_idx_filtered = artifact_indices[non_sequential_indices]
    except IndexError:
        return artifact_indices
    return artifact_idx_filtered

def phase_shift(recording, n_channels):
    '''Phase shift the recording
    Args:
    recording (se.RecordingExtractor): recording extractor object
    n_channels (int): number of channels
    Returns:
    recording (se.RecordingExtractor): recording extractor object with phase shift
    '''
    banks_phase_shift = [1] # Define banks for phase shift
    num_bank = 48 # Define number of sites per bank
    
    # Create array with phase shift
    phase_shift_arr = np.zeros(int(n_channels)) # Create array with zeros
    for i in banks_phase_shift:
        phase_shift_arr[(i-1)*num_bank:i*num_bank] = np.pi / 2 # Set phase shift to pi/2
    recording = spre.phase_shift(recording, inter_sample_shift=phase_shift_arr) # Phase shift the recording
    return recording

def process_traces(recording_path, p, n_channels, num_cpus=None):
    '''Process traces
    Args:
    recording_path (str): path to the recording file
    p (pi.Probe): probe object
    num_cpus (int): number of cpus to use
    Returns:
    recording (se.RecordingExtractor): processed recording
    artifact_indexes (np.array): array with artifact indices
    '''
    # Load recording
    recording = se.read_binary(recording_path, dtype='int32', sampling_frequency=cfg.SAMPLE_RATE, num_channels=n_channels)
    # User sanity check
    print(recording.get_total_duration())
    # Get number of cpus from path or from system
    num_cpus = num_cpus or os.cpu_count()

    total_frames = recording.get_num_frames() # Get total number of frames
    chunk_size = total_frames // num_cpus # Get size of data to be processed by each cpu core

    chunks = [] # List to store chunks of data

    step = 10 # Number of channels to process at a time
    # Loop through the all channels in steps of 10 and create inputs for the get_artifacts function
    for channel_start in range(0, n_channels, step):
        channel_end = min(channel_start + step, n_channels)
        for i in range(num_cpus):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if (i + 1) * chunk_size < total_frames else total_frames
            chunks.append((recording, list(range(channel_start, channel_end)), start_idx, end_idx))

    # Process traces in parallel
    with Pool(num_cpus) as pool:
        artifact_results = list(tqdm(pool.imap(get_artifacts, chunks), total=len(chunks), desc="Detecting Artifacts"))
    # Get unique artifact indexes
    artifact_indexes = np.unique(np.concatenate(artifact_results))

    # Spike interface functions to process traces
    # Remove artifacts
    recording = spre.remove_artifacts(recording, list_triggers=artifact_indexes,
                                      ms_before=250, ms_after=250, mode='linear')
    # Phase shift the recording as Neuropixels probes have a phase shift
    recording = phase_shift(recording, n_channels)
    # Bandpass filter the recording
    recording = spre.bandpass_filter(recording, freq_min=300., freq_max=7500., dtype='float32')
    # Set the probe geometry to do median removal
    recording = recording.set_probe(p)
    recording = spre.common_reference(recording, reference='local', operator='median')
    
    return recording, artifact_indexes

if __name__ == "__main__":

    # Arguments for input
    user_input = Path(sys.argv[1])
    probe = sys.argv[2]
    shank = sys.argv[3]
    data_path = sys.argv[4]
    chunk = sys.argv[5]

    # Rough way to check if the data has been chunked - needs to be improved
    try:
        chunk = int(chunk)
        chunk = f"chunk_{chunk}"
        output = 'output'
    except ValueError:
        chunk = 'total'
        output = 'output_total'

    # File paths
    data_folder = user_input / data_path # Path to the data folder
    output_folder = data_folder / output # Path to the where output data will be saved
    shank_folder = output_folder / f"probe_{probe}" / f"shank_{shank}.0" # Path to the shank folder
    recording_path = shank_folder / "raw_recording" / chunk / "traces_cached_seg0.raw" # Path to the raw recording

    shank_probe, shank_probe_data = ut.load_probe_from_json(cfg.SHANK_FILE) # Load the shank probe

    # Process traces and artifacts
    recording, artifact_indexes = process_traces(recording_path, shank_probe, cfg.N_CHANNELS_SHANK)

    # Save processed recording
    artifact_path = shank_folder / "recording" / chunk
    recording.save(folder=artifact_path, format='binary', **cfg.JOB_KWARGS, overwrite=True)

    # Save artifacts
    artifact_indexes = np.array(artifact_indexes)
    np.save(artifact_path / "artifact_indexes.npy", artifact_indexes)

