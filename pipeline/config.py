PROBE_FILE = "/groups/voigts/voigtslab/probes/np_96_191.json"
SHANK_FILE = "/groups/voigts/voigtslab/probes/np_shank.json"
JOB_KWARGS = dict(n_jobs=1, chunk_duration="1s", progress_bar=True) # SWITCH BACK TO -1
PROBE_NAMES = ['probe_a', 'probe_b']
N_CHANNELS_PROBE = 384
N_CHANNELS_SHANK = 96
SAMPLE_RATE = 30000
DREDGE_STEP = 15
DREDGE_SCALE = 50
KILOSORT_PARAMS = {
    'do_correction': False,
    'skip_kilosort_preprocessing': False,
    'batch_size': 30000,
}
ANALYSIS_TSTEP = 10