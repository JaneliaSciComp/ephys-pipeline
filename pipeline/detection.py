import numpy as np
import utils as ut
import config as cfg
import spikeinterface.extractors as se
import spikeinterface.full as si
import spikeinterface.exporters as sexp
import sys
from pathlib import Path
import torch
from shutil import copyfile
import os
import glob
from kilosort import run_kilosort


job_kwargs = cfg.JOB_KWARGS

def process_traces(recording_path, probe):
    '''
    Load traces from binary file and set probe information
    Input:
        recording_path: Path to the binary file
        probe: Probe information
    Output:
        recording: Recording object with probe information
    '''
    recording = se.read_binary(recording_path, sampling_frequency=cfg.SAMPLE_RATE, dtype='float32', num_channels=cfg.N_CHANNELS_SHANK)
    recording = recording.set_probe(probe)
    return recording


def replace_params(src_dir, output_path, n_channels):
    '''
    Replace the params.py file for Phy in the output folder with the correct number of channels and offset
    Input:
        file: Path to the binary file
        output_path: Path to the output folder
        n_channels: Number of channels
        chunk_n: Chunk number
    '''
    params_content = (
        f"n_channels_dat = {int(n_channels)}\n"
        f"offset = 0\n"
        "sample_rate = 30000\n"
        "dtype = 'int16'\n"
        "hp_filtered = False\n"
        f"dat_path = {src_dir}"
    )
    output_file = f"{output_path}/params.py"
    
    with open(output_file, 'w') as f:
        f.write(params_content)


def run_kilosort(recording, ks_path, src_dirs, kilosort_params, chunk_n= None):
    '''
    Run Kilosort on the recording
    Input:
        recording: Recording object
        ks_path: Path to the output folder
        src_dir: Path to the source folder
        kilosort_params: Kilosort parameters
        chunk_n: Chunk number
    Output:
        Spikes detected by Kilosort
        Data exported for Phy
    '''
    # Run Kilosort using spikeinterface
    sorting_KS4 = si.run_sorter(
        sorter_name="kilosort4",
        recording=recording,
        folder=str(ks_path),
        docker_image=None,
        singularity_image=None,
        with_output=True,
        n_jobs=12, 
        remove_existing_folder=True,
        **kilosort_params
    )
    # Read phy outputs
    phy_sorting = si.read_phy(folder_path=str(ks_path / 'sorter_output'))

    # Running on cluster gives an issue where some spikes are detected outside the recording
    # Using spikeinterface function to remove those spikes
    phy_sorting = si.remove_excess_spikes(sorting=sorting_KS4, recording=recording)

    # Create sorting analyzer to recompute some metrics
    print('Sorting Analyzer')
    we = si.create_sorting_analyzer(recording=recording, sorting=phy_sorting, folder=ks_path, overwrite=True, n_jobs=12)

    # Compute metrics
    print('Compute')
    we.compute(['random_spikes', 'waveforms', 'templates', 'noise_levels'], n_jobs=12)
    _ = we.compute('correlograms')
    _ = we.compute('spike_amplitudes', n_jobs=12)

    replace_params(src_dirs, ks_path, n_channels=cfg.N_CHANNELS_PROBE)

    # Phy path
    phy_folder = ks_path / 'phy'

    # Export to Phy
    print('export')
    sexp.export_to_phy(we,
                    output_folder=str(phy_folder),
                    remove_if_exists=True,
                    compute_amplitudes=True,
                    compute_pc_features=True,
                    copy_binary=False,
                    n_jobs=12)
    
    # Replace params.py file in Phy folders - also needed in KS folder
    #replace_params(recording_path, ks_path, n_channels=cfg.N_CHANNELS_SHANK, chunk_n=chunk_n)
    replace_params(src_dirs, phy_folder, n_channels=cfg.N_CHANNELS_PROBE)

    # Copying files created by Kilosort to Phy folder
    #for file_path in glob.glob(os.path.join(src_dir, '*')):
        #filename = os.path.basename(file_path)
        #if os.path.isfile(file_path) and not os.path.isfile(f'{str(phy_folder)}/{filename}'):
            #copyfile(file_path, f'{str(phy_folder)}/{filename}')

    #print(f"Kilosort output: {ks_path}/sorter_output/")
    #print(f"Phy output: {phy_folder}")


if __name__ == "__main__":

    # Read command line arguments
    user_input = Path(sys.argv[1]) # Path to the data folder
    dredge = sys.argv[2].lower() == 'true' # Whether to use DREDge or not
    probe = sys.argv[3] # Probe number
    shank = sys.argv[4] # Shank number
    data_path = sys.argv[5] # Path to the data folder
    chunk_n = sys.argv[6] # Chunk number

    try:
        chunk = int(chunk_n)
        chunk = f"chunk_{chunk_n}"
        output = 'output'
    except ValueError:
        chunk = 'total'
        output = 'output_total'

    # Paths
    data_folder = user_input / data_path / output / f"probe_{probe}" / f"shank_{shank}.0"
    # Loading recording with added path distinction for DREDge
    if dredge:
        data_folder = data_folder / 'dredge'
        recording_path = data_folder / chunk / 'recording' / 'traces_cached_seg0.raw'
    else:
        recording_path = data_folder  / 'recording'  / chunk / 'traces_cached_seg0.raw'
    ks_path = data_folder / 'kilosort4' / chunk
    src_dir = ks_path / 'sorter_output'

    # Use GPU if available
    use_cuda = torch.cuda.is_available()
    device = "cuda" if use_cuda else "cpu"
    print(f"Device: {device}")

    # Set Kilosort parameters
    kilosort_params = cfg.KILOSORT_PARAMS
    kilosort_params['torch_device'] = device

    # Load probe information
    probe, probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    # Process traces
    recording = process_traces(recording_path, probe=probe)
    run_kilosort(recording, ks_path, src_dir, kilosort_params, chunk_n)
