import utils as ut
import config as cfg
import numpy as np
import pandas as pd
from pathlib import Path
import spikeinterface.extractors as se
import os
from tqdm import tqdm
import sys


### NOT PROPERLY DONE IT WILL NOT WORK AS IS :)))))


def find_best_channel(results_dir):
    chan_map = np.load(results_dir / 'channel_map.npy')
    templates = np.load(results_dir / 'templates.npy')
    return chan_map[(templates**2).sum(axis=1).argmax(axis=-1)]

def find_good_units(results_dir):
    contam_pct = pd.read_csv(results_dir / 'cluster_ContamPct.tsv', sep='\t')['ContamPct'].values
    return np.nonzero(contam_pct >= .01)[0]

def get_spike_times(st, clu, unit):
    return st[clu == unit]

def get_spike_subset(spike_times, start_time, end_time, dshift):
    spike_times = spike_times[spike_times > dshift + 1]
    return spike_times[(spike_times > start_time) & (spike_times < end_time)]

def refine_spikes(spikes, max_val):
    max_val_threshold = np.median(max_val) * 2
    spikes_filtered = [spike for spike, val in zip(spikes, max_val) if val <= max_val_threshold]
    amplitudes_filtered = [val for val in max_val if val <= max_val_threshold] 
    spikes_filtered = [spike for spike in spikes_filtered if spike.shape == (60, 1)]
    return spikes_filtered, amplitudes_filtered

def refine_spikes_for_channels(spike_traces_by_channel, max_val_by_channel):
    filtered_traces = {}
    filtered_amplitudes = {}
    
    for channel_id in spike_traces_by_channel:
        max_val = max_val_by_channel[channel_id]
        max_val_threshold = np.median(max_val) * 2
        traces = spike_traces_by_channel[channel_id]
        
        filtered_traces[channel_id] = [trace for trace, val in zip(traces, max_val) if val <= max_val_threshold]
        filtered_amplitudes[channel_id] = [val for val in max_val if val <= max_val_threshold]
    
    return filtered_traces, filtered_amplitudes

def find_coord(probe, idxs, axis):
    return [probe[f'{axis}coords'][idx] for idx in idxs]

def find_avg(value, ratios):
    weight_sum = ratios.sum()
    return (value * ratios / weight_sum).sum()

def get_quantiles(values):
    return np.percentile(values, [25, 50, 75])

def calc_position(rec, spike_sub):
    x_coords = find_coord(probe, list(range(96)), 'x')
    y_coords = find_coord(probe, list(range(96)), 'y')

    traces = [rec.get_traces(start_frame=spike_time, end_frame=spike_time + 1) for spike_time in spike_sub]

    max_trace_values = [np.nanmax(trace) for trace in traces]

    max_trace_median = np.nanmedian(max_trace_values)

    traces = [trace for i, trace in enumerate(traces) if max_trace_values[i] < max_trace_median * 2]

    traces = [(trace / np.nanmax(trace))**16 for trace in traces]

    x_values = [find_avg(x_coords, traces[i].T.flatten()) for i in range(len(traces))]
    y_values = [find_avg(y_coords, traces[i].T.flatten()) for i in range(len(traces))]

    x_25, x_50, x_75 = get_quantiles(x_values)
    y_25, y_50, y_75 = get_quantiles(y_values)

    return x_25, x_50, x_75, y_25, y_50, y_75

def find_surrounding(best_channel):
    if (best_channel % 2) == 0:
        bestchan = np.array(range(best_channel - 2, best_channel + 4))
    else:
        bestchan = np.array(range(best_channel - 3, best_channel + 3))
    return bestchan[(bestchan > -1) & (bestchan < 64)]

def refine_spikes(spikes, max_val):
    # Check they're are not outliers
    max_val_threshold = np.median(max_val) * 5
    spikes_filtered = [spike for spike, val in zip(spikes, max_val) if val <= max_val_threshold]
    amplitudes_filtered = [spike for spike, val in zip(max_val, max_val) if val <= max_val_threshold] # TODO: make more sense

    # Filter spikes with the correct shape (60, 1)
    spikes_filtered = [spike for spike in spikes_filtered if spike.shape == (60, 1)]
    return spikes_filtered, amplitudes_filtered

def get_traces(spike_sub, chan, rec, dshift):
    spikes, max_val, tdiff_min = [], [], []
    for spike_time in spike_sub:
        trace = rec.get_traces(channel_ids=[chan], start_frame=spike_time - dshift, end_frame=spike_time + dshift)
        spikes.append(trace)
        max_val.append(trace.max())
    return refine_spikes(spikes, max_val)

def compute_temporal_stats(results_path, probe, recording_path, freq=30000, t_diff=10, subsample=1000):

    results_directory = Path(results_path)
    stats_directory = results_directory / f'movement_stats_{str(t_diff)}'
    os.makedirs(stats_directory, exist_ok=True)
    chan_best = find_best_channel(results_directory)
    st = np.load(results_directory / 'spike_times.npy')
    clu = np.load(results_directory / 'spike_clusters.npy')
    good_units = find_good_units(results_directory) if os.path.exists(results_directory / 'cluster_ContamPct.tsv') else np.unique(clu)
    dshift = 30
    num_chan = len(probe['chanMap'])
    frames_per_min = freq * 60

    rec = se.read_binary(recording_path, sampling_frequency=freq, dtype='float32', num_channels=num_chan)

    num_samples = rec.get_num_samples()
    time_length = num_samples // frames_per_min

    for unit in tqdm(good_units):
        try:
            spike_times = get_spike_times(st, clu, unit)
            best_channel = chan_best[unit]
            surrounding_channels = find_surrounding(best_channel)
            print(unit)
            print(f'best chan {best_channel}')
    
            # Initialize storage for surrounding channels and overall stats
            channel_data = {f'ch{i+1}': [] for i in range(len(surrounding_channels))}
            median_spikes, mean_spikes, amplitude_25, amplitude_50, amplitude_75, num_spikes = [], [], [], [], [], []
            x25, x50, x75, y25, y50, y75 = [], [], [], [], [], []
    
            for tranges in range(0, frames_per_min * time_length, frames_per_min * t_diff):
                start_time = tranges
                end_time = tranges + frames_per_min * t_diff
                spike_sub = get_spike_subset(spike_times, start_time, end_time, dshift)
    
                num_spikes.append(len(spike_sub))
    
                if len(spike_sub) == 0:
                    # Append NaNs for all stats
                    median_spikes.append(np.nan)
                    mean_spikes.append(np.nan)
                    amplitude_25.append(np.nan)
                    amplitude_50.append(np.nan)
                    amplitude_75.append(np.nan)
                    x25.append(np.nan)
                    x50.append(np.nan)
                    x75.append(np.nan)
                    y25.append(np.nan)
                    y50.append(np.nan)
                    y75.append(np.nan)
                    for key in channel_data.keys():
                        channel_data[key].append(np.nan)  # Append NaNs for surrounding channels
    
                if len(spike_sub) > subsample:
                    spike_sub = np.random.choice(spike_sub, subsample, replace=False)
                # Initialize storage for all channels
                spike_traces_by_channel = {f'ch0': [], **{f'ch{i+1}': [] for i in range(len(surrounding_channels))}}
                max_val_by_channel = {f'ch0': [], **{f'ch{i+1}': [] for i in range(len(surrounding_channels))}}
    
                # Fetch traces for best channel and surrounding channels
                spikes_filtered, amplitudes_filtered = get_traces(spike_sub, best_channel, rec, dshift)
                spike_traces_by_channel['ch0'] = spikes_filtered
                max_val_by_channel['ch0'] = amplitudes_filtered
                
    
                if len(spike_traces_by_channel['ch0']) > 50:
                    median_spikes.append(np.nanmean(spike_traces_by_channel['ch0'], axis=0))
                    mean_spikes.append(np.nanmean(spike_traces_by_channel['ch0'], axis=0))
                    amplitude_25.append(np.percentile(max_val_by_channel['ch0'], 25))
                    amplitude_50.append(np.percentile(max_val_by_channel['ch0'], 50))
                    amplitude_75.append(np.percentile(max_val_by_channel['ch0'], 75))
    
                    x_25, x_50, x_75, y_25, y_50, y_75 = calc_position(rec, spike_sub)
                    x25.append(x_25)
                    x50.append(x_50)
                    x75.append(x_75)
                    y25.append(y_25)
                    y50.append(y_50)
                    y75.append(y_75)
                    
                    for idx, chan in enumerate(surrounding_channels):
                        spikes_filtered, amplitudes_filtered = get_traces(spike_sub, chan, rec, dshift)
                        spike_traces_by_channel[f'ch{idx+1}'] = spikes_filtered
                        max_val_by_channel[f'ch{idx+1}'] = amplitudes_filtered
        
                    # Add statistics for surrounding channels
                    for idx in range(len(surrounding_channels)):
                        channel_data[f'ch{idx+1}'].append(np.mean(spike_traces_by_channel[f'ch{idx+1}'], axis=0))
    
                else:
                    median_spikes.append(np.nan)
                    mean_spikes.append(np.nan)
                    amplitude_25.append(np.nan)
                    amplitude_50.append(np.nan)
                    amplitude_75.append(np.nan)
                    x25.append(np.nan)
                    x50.append(np.nan)
                    x75.append(np.nan)
                    y25.append(np.nan)
                    y50.append(np.nan)
                    y75.append(np.nan)
                    for key in channel_data.keys():
                        channel_data[key].append(np.nan)
    
            # Create DataFrame including surrounding channel data
            time_steps = [f"{i}-{i+t_diff}" for i in range(0, time_length, t_diff)]
            df = pd.DataFrame({
                'time': time_steps,
                'median_spike': median_spikes, 
                'mean_spike': mean_spikes,
                'amplitude_25': amplitude_25,
                'amplitude_50': amplitude_50,
                'amplitude_75': amplitude_75,
                'num_spikes': num_spikes,
                'avg_x_25': x25,
                'avg_x_50': x50,
                'avg_x_75': x75,
                'avg_y_25': y25,
                'avg_y_50': y50,
                'avg_y_75': y75,
                **channel_data  # Include surrounding channel data
            })
    
            df.to_csv(str(stats_directory) + f'/unit_{unit}.csv')
        except ValueError:
            pass

if __name__ == "__main__":
    user_input = Path(sys.argv[1])
    dredge = sys.argv[2].lower() == 'true'
    probe = sys.argv[3]
    shank = sys.argv[4]
    data_path = sys.argv[5]

    try:
        chunk = int(chunk)
        chunk = f"chunk_{chunk}"
        output = 'output'
    except ValueError:
        chunk = 'total'
        output = 'output_total'

    data_folder = user_input / data_path
    data_folder = data_folder / "output" / f"probe_{probe}" / f"shank_{shank}.0"

    if dredge:
        recording_path = data_folder / "dredge" / "recording" / "traces_cached_seg0.raw"
        data_folder = data_folder / "dredge" / "kilosort4"
    else:
        recording_path = data_folder / "recording" / "traces_cached_seg0.raw"
        data_folder = data_folder / "kilosort4"

    p, probe = ut.load_probe_from_json(cfg.SHANK_FILE)

    ks_path = data_folder / 'sorter_output'
    phy_path = data_folder / 'phy'

    compute_temporal_stats(ks_path, probe, recording_path, t_diff=cfg.ANALYSIS_TSTEP)
    compute_temporal_stats(phy_path, probe, recording_path, t_diff=cfg.ANALYSIS_TSTEP)