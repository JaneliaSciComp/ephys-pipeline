# load packages
import utils as ut
import config as cfg
import sys
import os
import glob
import spikeinterface.extractors as se
import spikeinterface.full as si
from pathlib import Path

def collect_files(data_folder, probe_names, output_folder=True):
    '''Collect all files for each probe within data_folder
    Args:
    data_folder (Path): path to the data folder
    probe_names (list): list of probe names
    Returns:
    recording_files (dict): dictionary with probe names as keys and list of files as values
    '''
    # Set up dictionary to store files
    recording_files = {}

    # Loop through each probe and collect all files
    for probe_x in probe_names:
        # Collect all files for probe_x within data_folder
        recording_files[probe_x] = glob.glob(f"{str(data_folder)}/{probe_x}_*")
        # Create a folder for each probe (should probl be done in main)
        if output_folder:
            os.makedirs(f'{str(output_folder)}/{probe_x}' , exist_ok=True)
        # Print the number of files found for each probe for user feedback
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
    # Set up dictionary to store shank data
    shank_dict = {}
    # Loop through each probe and split into shanks
    for probe_x in probe_names:
        # Set up list to store recordings
        recordings = []
        # Loop through each recording file for probe_x
        for recording_file in recording_files[probe_x]:
            # Load the recording file
            recording = se.read_binary(recording_file, dtype='int16', sampling_frequency=cfg.SAMPLE_RATE, num_channels=cfg.N_CHANNELS_PROBE)
            # Sanity check for user
            print(recording.get_total_duration())
            # Append the recording to the list and ignore any empty
            if recording.get_num_frames() == 0:
                print(f"Warning: The recording file {recording_file} is empty.")
            else:
                recordings.append(recording)
        # Concatenate the recordings
        total_recording = si.concatenate_recordings(recordings)
        # Set the group property to the shank index
        total_recording.set_property("group", global_probe_data['shankInd'])
        # Split the recording by group
        shank_dict[probe_x] = total_recording.split_by("group")
        # Print the number of shanks for user feedback
        print(f"Split {probe_x} into {len(shank_dict[probe_x])} shanks")
    return shank_dict

def save_shanks(shank_dict, probe_names, output_folder, job_kwargs, chunk=True):
    '''Save the shank recordings
    Args:
    shank_dict (dict): dictionary with probe names as keys and shank recordings as values
    output_folder (Path): path to the output folder
    job_kwargs (dict): dictionary with job arguments
    '''
    # Loop through each probe and shank and save the recordings
    for probe_x in probe_names:
        for shank in shank_dict[probe_x]:
            # Create a folder for each shank
            shank_folder = output_folder / probe_x / f"shank_{shank}"
            os.makedirs(shank_folder, exist_ok=True)

            total_t = int(shank_dict[probe_x][shank].get_total_duration()) # Length of the recording in seconds

            # If chunking is enabled, save the recording in chunks
            if chunk:
                n_chunk = 0 # Counter for the chunk number
                for time_chunk in range(0, total_t, cfg.CHUNK_SIZE):
                    print(f'Saving chunk {n_chunk} for {probe_x} shank {shank}') # User feedback
                    raw_recording_path = shank_folder / "raw_recording" / f"chunk_{n_chunk}" # Path to save the chunk
                    # Save the chunk - exception for the last chunk which may not be the full size
                    try:
                        rec_chunk = shank_dict[probe_x][shank].time_slice(time_chunk, time_chunk + cfg.CHUNK_SIZE) # Slice the recording
                    except AssertionError:
                        rec_chunk = shank_dict[probe_x][shank].time_slice(time_chunk, None)
                    rec_chunk.save(folder=raw_recording_path, format='binary', **job_kwargs, overwrite=True) # Save the chunk
                    n_chunk += 1 # Increment the chunk number
            else:
                raw_recording_path = shank_folder / "raw_recording" / "total" # Path to save the recording
                shank_dict[probe_x][shank].save(folder=raw_recording_path, format='binary', **job_kwargs, overwrite=True) # Save the recording
    return n_chunk

if __name__ == "__main__":

    # Arguments for input
    user_input = Path(sys.argv[1]) # What session of data
    data_path = sys.argv[2] # Directory where data is stored (relative to user_input)
    chunk = sys.argv[3].lower() == 'true' # Whether to chunk the data or not

    data_folder = user_input / data_path # Path to the data folder

    if chunk:
        output_folder = data_folder / 'output' # Path to the output folder if chunked
    else:
        output_folder = data_folder / 'output_total' # Path to the output folder if not chunked

    os.makedirs(output_folder, exist_ok=True) # Create the output folder

    # Load the probe file
    global_probe, global_probe_data = ut.load_probe_from_json(cfg.PROBE_FILE)
    # Get all possible probe names (eg. probe_a, probe_b)
    probe_names = cfg.PROBE_NAMES

    # Collect all files for each probe within data_folder
    recording_files = collect_files(data_folder, probe_names)

    # Split the recording into shanks
    shank_dict = split_recording(recording_files, probe_names, global_probe_data)

    # Save the shank recordings
    n_chunks = save_shanks(shank_dict, probe_names, output_folder, cfg.JOB_KWARGS, chunk=chunk)

    # Print the number of chunks processed for the bash script
    print(f"Chunks Processed: {n_chunks}")  # Ensures the bash script can extract this line

