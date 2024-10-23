import utils as ut
import config as cfg
import spikeinterface.extractors as se
import spikeinterface.full as si
import sys
from pathlib import Path

def process_dredge(recording_path, shank_probe, dredge_path):
    # Load recording
    recording = se.read_binary(recording_path, dtype='float32', sampling_frequency=cfg.SAMPLE_RATE, num_channels=cfg.N_CHANNELS_SHANK)
    recording = recording.set_probe(shank_probe)

    # Process with DREDge
    recording_corrected, motion, motion_info = si.correct_motion(
        recording, preset="dredge", folder=dredge_path,
        output_motion=True, output_motion_info=True,
        estimate_motion_kwargs=dict(win_step_um=cfg.DREDGE_STEP, win_scale_um=cfg.DREDGE_SCALE),
        **cfg.JOB_KWARGS, overwrite=True
    )

    # Save corrected recording
    recording_corrected.save(folder=str(dredge_path) + '/recording/', format='binary', **cfg.JOB_KWARGS, overwrite=True)


if __name__ == "__main__":

    # arguments for input
    user_input = Path(sys.argv[1])
    probe = sys.argv[2]
    shank = sys.argv[3]
    data_path = sys.argv[4]
    chunk = sys.argv[5]

    try:
        chunk = int(chunk)
        chunk = f"chunk_{chunk}"
    except ValueError:
        chunk = 'total'

    data_folder = user_input / data_path
    output_folder = data_folder / "output"
    shank_folder = output_folder / f"probe_{probe}" / f"shank_{shank}.0"
    recording_path = shank_folder / "recording" / chunk / "traces_cached_seg0.raw"
    dredge_path = shank_folder / "dredge" / chunk

    shank_probe, shank_probe_data = ut.load_probe_from_json(cfg.SHANK_FILE)

    process_dredge(recording_path, shank_probe, dredge_path)