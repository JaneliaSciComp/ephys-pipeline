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
import numpy as np


def process_traces(recording_path, p):
    recording = se.read_binary(recording_path, sampling_frequency=cfg.SAMPLE_RATE, dtype='float32', num_channels=cfg.N_CHANNELS_SHANK)
    recording = recording.set_probe(p)
    return recording

def replace_params(file, output_path, n_channels):
    params_file = f"""n_channels_dat = {int(n_channels)}
offset = 0
sample_rate = 30000
dtype = 'float32'
hp_filtered = False
dat_path = '{file}'"""
    file = str(output_path) + '/params.py'
    with open(file, 'w') as filetowrite:
        filetowrite.write(params_file)


if __name__ == "__main__":

    # Read command line arguments
    user_input = Path(sys.argv[1])
    dredge = sys.argv[2].lower() == 'true'
    probe = sys.argv[3]
    shank = sys.argv[4]
    data_path = sys.argv[5]

    data_folder = user_input / data_path
    data_folder = data_folder / "output" / f"probe_{probe}" / f"shank_{shank}.0"
    if dredge:
        data_folder = data_folder / 'dredge' 
    recording_path = str(data_folder) + '/recording/traces_cached_seg0.raw'
    ks_path = data_folder / 'kilosort4'
    src_dir = ks_path / 'sorter_output'

    # use gpu
    use_cuda = torch.cuda.is_available()
    device = "cuda" if use_cuda else "cpu"
    print("Device: ", device)
    kilosort_params = cfg.KILOSORT_PARAMS
    kilosort_params['torch_device'] = device

    p, probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    recording = process_traces(recording_path, p=p)
        
    sorting_KS4 = si.run_sorter(
        sorter_name="kilosort4",
        recording=recording,
        folder=str(ks_path),
        docker_image=None,
        singularity_image=None,
        with_output=True,
        remove_existing_folder=True,
        **kilosort_params
    )

    phy_sorting = si.read_phy(folder_path= str(ks_path) + '/sorter_output')
    phy_sorting = si.remove_excess_spikes(sorting=sorting_KS4, recording=recording)

    we = si.create_sorting_analyzer(recording=recording, sorting=phy_sorting, folder=ks_path, overwrite=True, **cfg.JOB_KWARGS)

    we.compute(['random_spikes', 'waveforms', 'templates', 'noise_levels'], **cfg.JOB_KWARGS)
    _ = we.compute('correlograms')
    _ = we.compute('spike_amplitudes', **cfg.JOB_KWARGS)

    phy_folder = ks_path / 'phy'

    sexp.export_to_phy(we,
                    output_folder=str(phy_folder),
                    remove_if_exists=True,
                    compute_amplitudes=True,
                    compute_pc_features=True,
                    copy_binary=False,
                    **cfg.JOB_KWARGS)
            
    replace_params(recording_path, src_dir, n_channels=cfg.N_CHANNELS_SHANK)
    replace_params(recording_path, phy_folder, n_channels=cfg.N_CHANNELS_SHANK)

    for file_path in glob.glob(os.path.join(src_dir, '*')):
        filename = os.path.basename(file_path)
        if os.path.isfile(file_path) and not os.path.isfile(f'{str(phy_folder)}/{filename}'):
            copyfile(file_path, f'{str(phy_folder)}/{filename}')

    print("Kilosort output: ", str(ks_path) + '/sorter_output/')
    print("Phy output: ", str(phy_folder))
