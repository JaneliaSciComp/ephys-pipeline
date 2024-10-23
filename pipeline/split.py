# load packages
import utils as ut
import config as cfg
import sys
import os
import glob
import spikeinterface.extractors as se
import spikeinterface.full as si
from pathlib import Path

def collect_files(data_folder, probe_names):
    '''Collect all files for each probe within data_folder
    Args:
    data_folder (Path): path to the data folder
    probe_names (list): list of probe names
    Returns:
    recording_files (dict): dictionary with probe names as keys and list of files as values
    '''
    recording_files = {}
    for probe_x in probe_names:
        recording_files[probe_x] = glob.glob(f"{str(data_folder)}/{probe_x}_*")
        os.makedirs(f'{str(output_folder)}/{probe_x}' , exist_ok=True)
        print(f'Found {len(recording_files[probe_x])} files for {probe_x}')
    return recording_files

def split_recording(recording_files, probe_names, global_probe_data):
    '''Split the recording into shanks
    Args:
    recording_files (dict): dictionary with probe names as keys and list of files as values
    probe_names (list): list of probe names
    global_probe_data (dict): dictionary with global probe data
    Returns:
    shank_dict (dict): dictionary with probe names as keys and shank recordings as values
    '''
    shank_dict = {}
    for probe_x in probe_names:
        recordings = []
        for recording_file in recording_files[probe_x]:
            recording = se.read_binary(recording_file, dtype='int32', sampling_frequency=cfg.SAMPLE_RATE, num_channels=cfg.N_CHANNELS_PROBE)
            print(recording.get_total_duration())
            if recording.get_num_frames() == 0:
                print(f"Warning: The recording file {recording_file} is empty.")
            else:
                recordings.append(recording)
        total_recording = si.concatenate_recordings(recordings)
        total_recording.set_property("group", global_probe_data['shankInd'])
        shank_dict[probe_x] = total_recording.split_by("group")
        print(f"Split {probe_x} into {len(shank_dict[probe_x])} shanks")
    return shank_dict

def save_shanks(shank_dict, probe_names, output_folder, job_kwargs, chunk=True):
    '''Save the shank recordings
    Args:
    shank_dict (dict): dictionary with probe names as keys and shank recordings as values
    output_folder (Path): path to the output folder
    job_kwargs (dict): dictionary with job arguments
    '''
    for probe_x in probe_names:
        print(probe_names)
        for shank in shank_dict[probe_x]:
            print(shank)
            shank_folder = output_folder / probe_x / f"shank_{shank}"
            os.makedirs(shank_folder, exist_ok=True)
            total_t = int(shank_dict[probe_x][shank].get_total_duration())
            if chunk:
                n_chunk = 0
                for time_chunk in range(0, total_t, cfg.CHUNK_SIZE):
                    print(n_chunk)
                    raw_recording_path = shank_folder / "raw_recording" / f"chunk_{n_chunk}"
                    shank_dict[probe_x][shank].save(folder=raw_recording_path, format='binary', **job_kwargs, time_frame=(time_chunk, time_chunk + cfg.CHUNK_SIZE), overwrite=True)
                    n_chunk += 1
            else:
                raw_recording_path = shank_folder / "raw_recording" / "total"
                shank_dict[probe_x][shank].save(folder=raw_recording_path, format='binary', **job_kwargs, overwrite=True)
    return n_chunk

if __name__ == "__main__":

    # arguments for input
    user_input = Path(sys.argv[1])
    data_path = sys.argv[2]
    chunk = sys.argv[3] == 'true'

    data_folder = user_input / data_path
    output_folder = data_folder / 'output'

    os.makedirs(output_folder, exist_ok=True)

    global_probe, global_probe_data = ut.load_probe_from_json(cfg.PROBE_FILE)

    probe_names = cfg.PROBE_NAMES

    recording_files = collect_files(data_folder, probe_names)
    shank_dict = split_recording(recording_files, probe_names, global_probe_data)
    n_chunks = save_shanks(shank_dict, probe_names, output_folder, cfg.JOB_KWARGS, chunk=chunk)
    print(n_chunks)
