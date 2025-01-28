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
    recording, channels, start_idx, end_idx = args
    channel_data = recording.get_traces(start_frame=start_idx, end_frame=end_idx, channel_ids=channels)
    filter_array = channel_data == 0
    artifact_indices = np.nonzero(filter_array)[0] + start_idx
    diffs = np.diff(artifact_indices)
    non_sequential_indices = np.where(diffs != 1)[0] + 1
    non_sequential_indices = np.insert(non_sequential_indices, 0, 0)
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

def detect_artifacts(recording, num_cpus=None, chunk_size=5):
    """Detect artifacts in recording by finding sequences of zero values
    Args:
        recording (se.RecordingExtractor): recording to detect artifacts in
        num_cpus (int): number of CPUs to use for parallel processing
    Returns:
        artifact_indexes (np.array): array of artifact indices
    """
    # Process 30 minute chunks at a time
    chunk_size = int(chunk_size * 60 * recording.get_sampling_frequency())
    print(chunk_size)
    total_samples = recording.get_num_frames()
    
    channel_ids = recording.get_channel_ids()
    # Sample channels evenly across the probe for artifact detection
    step = len(channel_ids) // 10  # Get ~10 evenly spaced channels
    channel_ids = channel_ids[::step][:10]  # Take first 10 channels after stepping

    # Create list of chunk start/end indices
    chunk_indices = [(start, min(start + chunk_size, total_samples)) 
                    for start in range(0, total_samples, chunk_size)]

    # Process chunks in parallel
    args_list = [(recording, channel_ids, start_idx, end_idx) 
                 for start_idx, end_idx in chunk_indices]
    
    with Pool(processes=num_cpus) as pool:
        chunk_artifacts = list(tqdm(
            pool.imap(get_artifacts, args_list),
            total=len(chunk_indices),
            desc="Processing chunks"
        ))
    
    # Combine results
    artifact_indices = []
    for chunk_result in chunk_artifacts:
        artifact_indices.extend(chunk_result)
    
    artifact_indices = np.array(artifact_indices)
    print(f"Total artifacts found: {len(artifact_indices)}")
    
    # Get non-sequential indices
    diffs = np.diff(artifact_indices)
    non_sequential_indices = np.where(diffs != 1)[0] + 1
    non_sequential_indices = np.insert(non_sequential_indices, 0, 0)
    print("Non-sequential indices processed")
    
    # Get unique artifact indexes
    try:
        artifact_indexes = artifact_indices[non_sequential_indices]
    except IndexError:
        artifact_indexes = artifact_indices
        
    return artifact_indexes

def process_traces(recording_path, p, n_channels, num_cpus=None, chunk_size=5, artifacts=True, art_path=None):
    '''Process traces
    Args:
    recording_path (str): path to the recording file
    p (tuple): tuple containing (probe object, probe data)
    num_cpus (int): number of cpus to use
    Returns:
    recording (se.RecordingExtractor): processed recording
    artifact_indexes (np.array): array with artifact indices
    '''
    # Load recording
    try:
        recording = se.read_binary(recording_path, dtype='int16', sampling_frequency=cfg.SAMPLE_RATE, num_channels=n_channels)
    except:
        recording = recording_path
    # User sanity check
    print(recording.get_total_duration())

    if artifacts:
        print("Processing artifacts...")
        # Detect artifacts
        if art_path == None:
            artifact_indexes = detect_artifacts(recording, num_cpus, chunk_size)
        else:
            print("Loading artifact file")
            art_file = art_path / 'artifact_indexes.npy'
            artifact_indexes = np.load(art_file)

        # Spike interface functions to process traces
        # Remove artifacts
        print("Removing artifacts...")
        recording = spre.remove_artifacts(recording, list_triggers=artifact_indexes,
                                        ms_before=250, ms_after=250, mode='linear')
        
    else:
        artifact_indexes = None
        
    print("Processing phase shift...")
    # Phase shift the recording as Neuropixels probes have a phase shift
    recording = phase_shift(recording, n_channels)
    # Bandpass filter the recording
    print("Bandpass filtering...")
    recording = spre.bandpass_filter(recording, freq_min=300., freq_max=7500., dtype='int16')
    # Set the probe geometry to do median removal
    recording = recording.set_probe(p[0])  # Use only the probe object from the tuple
    print("Median removal...")
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
    recording, artifact_indexes = process_traces(recording_path, (shank_probe, shank_probe_data), cfg.N_CHANNELS_SHANK)

    # Save processed recording
    artifact_path = shank_folder / "recording" / chunk
    recording.save(folder=artifact_path, format='binary', **cfg.JOB_KWARGS, overwrite=True)

    # Save artifacts
    artifact_indexes = np.array(artifact_indexes)
    np.save(artifact_path / "artifact_indexes.npy", artifact_indexes)

