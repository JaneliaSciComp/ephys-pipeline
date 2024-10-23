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
    banks_phase_shift = [1]
    num_bank = 48
    phase_shift_arr = np.zeros(int(n_channels))
    for i in banks_phase_shift:
        phase_shift_arr[(i-1)*num_bank:i*num_bank] = np.pi / 2
    recording = spre.phase_shift(recording, inter_sample_shift=phase_shift_arr)
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
    recording = se.read_binary(recording_path, dtype='int32', sampling_frequency=cfg.SAMPLE_RATE, num_channels=n_channels)
    print(recording.get_total_duration())
    num_cpus = num_cpus or os.cpu_count()
    total_frames = recording.get_num_frames()
    chunk_size = total_frames // num_cpus

    chunks = []
    step = 10
    for channel_start in range(0, n_channels, step):
        channel_end = min(channel_start + step, n_channels)
        for i in range(num_cpus):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if (i + 1) * chunk_size < total_frames else total_frames
            chunks.append((recording, list(range(channel_start, channel_end)), start_idx, end_idx))

    with Pool(num_cpus) as pool:
        artifact_results = list(tqdm(pool.imap(get_artifacts, chunks), total=len(chunks), desc="Detecting Artifacts"))
    artifact_indexes = np.unique(np.concatenate(artifact_results))

    recording = spre.remove_artifacts(recording, list_triggers=artifact_indexes,
                                      ms_before=250, ms_after=250, mode='linear')
    recording = phase_shift(recording, n_channels)
    recording = spre.bandpass_filter(recording, freq_min=300., freq_max=7500., dtype='float32')
    recording = recording.set_probe(p)
    recording = spre.common_reference(recording, reference='local', operator='median')
    
    return recording, artifact_indexes

if __name__ == "__main__":

    # Arguments for input
    user_input = Path(sys.argv[1])
    probe = sys.argv[2]
    shank = sys.argv[3]
    data_path = sys.argv[4]

    data_folder = user_input / data_path
    output_folder = data_folder / "output"
    shank_folder = output_folder / f"probe_{probe}" / f"shank_{shank}.0"
    recording_path = shank_folder / "raw_recording" / "traces_cached_seg0.raw"

    shank_probe, shank_probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    # Process traces and artifacts
    recording, artifact_indexes = process_traces(recording_path, shank_probe, cfg.N_CHANNELS_SHANK)

    # Save processed recording
    artifact_path = shank_folder / "recording"
    recording.save(folder=artifact_path, format='binary', **cfg.JOB_KWARGS, overwrite=True)

    # Save artifacts
    artifact_indexes = np.array(artifact_indexes)
    np.save(artifact_path / "artifact_indexes.npy", artifact_indexes)

