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


job_kwargs = cfg.JOB_KWARGS

def process_traces(recording_path, probe):
    recording = se.read_binary(recording_path, sampling_frequency=cfg.SAMPLE_RATE, dtype='float32', num_channels=cfg.N_CHANNELS_SHANK)
    recording = recording.set_probe(probe)
    return recording


def replace_params(file, output_path, n_channels):
    params_content = (
        f"n_channels_dat = {int(n_channels)}\n"
        "offset = 0\n"
        "sample_rate = 30000\n"
        "dtype = 'float32'\n"
        "hp_filtered = False\n"
        f"dat_path = '{file}'"
    )
    output_file = f"{output_path}/params.py"
    
    with open(output_file, 'w') as f:
        f.write(params_content)


def run_kilosort(recording, ks_path, src_dir, kilosort_params):

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

    phy_sorting = si.read_phy(folder_path=str(ks_path / 'sorter_output'))
    phy_sorting = si.remove_excess_spikes(sorting=sorting_KS4, recording=recording)

    print('sorting analyzer')

    we = si.create_sorting_analyzer(recording=recording, sorting=phy_sorting, folder=ks_path, overwrite=True, n_jobs=-1)

    print('compute')

    we.compute(['random_spikes', 'waveforms', 'templates', 'noise_levels'], n_jobs=-1)
    _ = we.compute('correlograms')
    _ = we.compute('spike_amplitudes', n_jobs=-1)

    phy_folder = ks_path / 'phy'

    print('export')

    sexp.export_to_phy(we,
                    output_folder=str(phy_folder),
                    remove_if_exists=True,
                    compute_amplitudes=True,
                    compute_pc_features=True,
                    copy_binary=False,
                    n_jobs=1)
            
    replace_params(recording_path, ks_path, n_channels=cfg.N_CHANNELS_SHANK)
    replace_params(recording_path, phy_folder, n_channels=cfg.N_CHANNELS_SHANK)

    for file_path in glob.glob(os.path.join(src_dir, '*')):
        filename = os.path.basename(file_path)
        if os.path.isfile(file_path) and not os.path.isfile(f'{str(phy_folder)}/{filename}'):
            copyfile(file_path, f'{str(phy_folder)}/{filename}')

    print(f"Kilosort output: {ks_path}/sorter_output/")
    print(f"Phy output: {phy_folder}")


if __name__ == "__main__":

    # Read command line arguments
    user_input = Path(sys.argv[1])
    dredge = sys.argv[2].lower() == 'true'
    probe = sys.argv[3]
    shank = sys.argv[4]
    data_path = sys.argv[5]
    chunk = sys.argv[6]

    data_folder = user_input / data_path / "output" / f"probe_{probe}" / f"shank_{shank}.0"
    if dredge:
        data_folder = data_folder / 'dredge' 

    recording_path = data_folder /  'recording' / 'traces_cached_seg0.raw'
    ks_path = data_folder / 'kilosort4'
    src_dir = ks_path / 'sorter_output'

    # Use GPU if available
    use_cuda = torch.cuda.is_available()
    device = "cuda" if use_cuda else "cpu"
    print(f"Device: {device}")
    kilosort_params = cfg.KILOSORT_PARAMS
    kilosort_params['torch_device'] = device

    # Load probe information
    probe, probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    # Process traces
    recording = process_traces(recording_path, probe=probe)
        
    if chunk == 'false':
        total_folder = ks_path / f'total'
        total_folder.mkdir(parents=True, exist_ok=True)
        run_kilosort(recording, total_folder, src_dir, kilosort_params)
    
    else:
        chunk = int(chunk)
        time_chunk = chunk * 60
        chunk_counter = 1

        t_length = int(recording.get_total_duration())
        print(f"Total duration: {t_length} seconds")
        for i in range(0, t_length, time_chunk):
            end_frame = i + time_chunk
            if end_frame > recording.get_num_frames():
                sub_recording = recording.time_slice(i)
            else:
                sub_recording = recording.time_slice(i, i + time_chunk)
            
            # Create subfolder for chunked output
            chunk_folder = ks_path / f'chunk_{chunk_counter}'
            chunk_folder.mkdir(parents=True, exist_ok=True)

            run_kilosort(sub_recording, chunk_folder, src_dir, kilosort_params)

            print(f"Processed chunk {chunk_counter} in folder {chunk_folder}")
            chunk_counter += 1

            del sub_recording
