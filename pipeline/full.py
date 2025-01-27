import split as sp
import preprocess as pp
import detection as dt
import config as cfg
from pathlib import Path
import utils as ut
import numpy as np

if __name__ == "__main__":

    num = float(sys.argv[1])
    probe_name = str(sys.argv[2])
    data_folder = Path(sys.argv[3])

    output_folder = data_folder / 'output'# Create output folder if it doesn't exist
    output_folder.mkdir(exist_ok=True)


    global_probe, global_probe_data = ut.load_probe_from_json(cfg.PROBE_FILE)
    recording_files = sp.collect_files(data_folder, probe_name, output_folder=False)
    shank_dict, recording_paths = sp.split_recording(recording_files, probe_name, global_probe_data)

    shank = shank_dict[probe_name][num]
    shank_folder = output_folder / probe_name / f"shank_{str(num)}"
    shank_folder.mkdir(parents=True, exist_ok=True)

    shank_probe = ut.load_probe_from_json(cfg.SHANK_FILE)
    recording, artifact_indexes = pp.process_traces(shank, shank_probe, n_channels=cfg.N_CHANNELS_SHANK, num_cpus=16, artifacts=True)

    artifact_path = shank_folder / "artifacts"
    artifact_path.mkdir(parents=True, exist_ok=True)
    np.save(artifact_path / "artifact_indexes.npy", artifact_indexes)

    kilosort_params = cfg.KILOSORT_PARAMS
    kilosort_params['torch_device'] = 'cuda'
    probe, probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    ks_path = shank_folder / "kilosort"
    src_dir = recording_paths
    dt.run_kilosort(recording, ks_path, src_dir, kilosort_params)