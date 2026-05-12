# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.7
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
from __future__ import annotations

import cv2
from datetime import datetime
import json
from venv import logger
import h5py
import os, sys
import glob
import numpy as np
import pandas as pd
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from collections import defaultdict

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple
import argparse
from pathlib import PureWindowsPath, PurePosixPath
import platform
import re

import logging
import stat

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.ioff()

try: 
    from .pose_cleaner import PoseCleaner
except ImportError:
    from pose_cleaner import PoseCleaner

# %%
class StreamToLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._buffer = ""

    def write(self, message):
        if not message:
            return

        self._buffer += message
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.rstrip()
            if line: 
                self.logger.log(self.level, line)

    def flush(self):
        line = self._buffer.rstrip()
        if line:
            self.logger.log(self.level, line)
        self._buffer = ""


def setup_logger(log_file: str, verbose: bool = True) -> logging.Logger:
    logger = logging.getLogger("maze")
    logger.setLevel(logging.DEBUG)      
    logger.propagate = False               
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # always log to file
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)             
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # if verbose also log to stdout
    if verbose:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)          
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger

def get_columns(df, search_str, method='contains', exclude=None):
    if method == 'startswith':
        cols = df.columns[df.columns.str.startswith(search_str)]
    elif method == 'contains':
        cols = df.columns[df.columns.str.contains(search_str)]
    else:
        raise ValueError("method must be 'startswith' or 'contains'")
    
    if exclude is not None:
        if isinstance(exclude, str):
            exclude = [exclude]
        for word in exclude:
            cols = [c for c in cols if word not in c]

    df_cols = df[cols]
    return df_cols


# %%
class  DataLoader:
    def __init__(self, config: dict[str,Any]):
        self.config = config
        self.path = config.get('path')
        self.verbose = config.get('verbose', True)
        self.recording = config.get('recording')
        self.output_path = config.get('output_path')
        self.config['tick_res'] = config.get('tick_res', 2.5e8)
 
    def load_all_data(self, max_workers=4):
        if not hasattr(self, "data_paths"):
            _ = self.get_all_data_paths()

        npx_clocks = self.data_paths.get("npx_clocks", [])
        start_times = self.data_paths.get("npx_start_times", [])
        kilosort_files = self.data_paths.get("kilosort_files", [])
        
        centroid_files = self.data_paths.get("centroid_files", [])
        timestamp_files = self.data_paths.get("timestamp_files", [])
        sleap_files = self.data_paths.get("sleap_files", [])    

        bno_files = self.data_paths.get("bno_files", [])
        hs_files = self.data_paths.get("hs_files", [])

        out = {
            "npx_clocks": [None] * len(npx_clocks),
            "npx_start_times": [None] * len(start_times),
            "bno": [None] * len(bno_files),
            "sleap": [None] * len(sleap_files),
            "kilosort": [None] * len(kilosort_files),
            "centroid": [None] * len(centroid_files),
            "timestamp": [None] * len(timestamp_files),
            "headstage": [None] * len(hs_files),
        }

        errors = []

        def _safe_call(tag, idx, fn, *args):
            try:
                result = fn(*args)
                return tag, idx, result, None
            except Exception as e:
                warnings.warn(f"Error loading {tag} index {idx} with args {args}: {e}")
                err = {
                    "tag": tag,
                    "index": idx,
                    "file": args[0] if args else None,
                    "exc_type": type(e).__name__,
                    "exc_msg": str(e),
                    "traceback": traceback.format_exc(),
                }
                return tag, idx, None, err

        tasks = []
        # neural stuff
        for i, f in enumerate(kilosort_files):
            tasks.append(("kilosort", i, self.load_kilosort_data, f))      
        for i, f in enumerate(npx_clocks):
            tasks.append(("npx_clocks", i, self.load_npx_clock, f))
        for i, f in enumerate(start_times):
            tasks.append(("npx_start_times", i, self.load_npx_start_time, f))

        # bno
        for i, f in enumerate(bno_files):
            tasks.append(("bno", i, self.load_bno_data, f))
        for i, f in enumerate(hs_files):
            tasks.append(("headstage", i, self.load_headstage_data, f))

        # sleap, timestamps and centroids
        for i, f in enumerate(sleap_files):
            tasks.append(("sleap", i, self.load_sleap_data, f))
        for i, f in enumerate(centroid_files):
            tasks.append(("centroid", i, self.load_centroid, f))
        for i, f in enumerate(timestamp_files):
            tasks.append(("timestamp", i, self.load_video_timestamps, f))


        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(_safe_call, tag, idx, fn, *args)
                for (tag, idx, fn, *args) in tasks
            ]

            for fut in as_completed(futures):
                tag, idx, result, err = fut.result()
                if err is not None:
                    errors.append(err)
                else:
                    out[tag][idx] = result
        
        self.loaded_data = out
        self.errors = errors

        print('=============== Data loading summary ===============')
        self.data_exists = {}
        for data in list(self.loaded_data.keys()):
            if len(self.loaded_data[data]) == 0:
                print(f'No data loaded for {data}!')
                self.data_exists[data] = False
            else:
                print(f'Loaded {len(self.loaded_data[data])} items for {data}.')
                self.data_exists[data] = True
        print('====================================================')

        return out, errors

###### GET PATHS #######
    def get_all_data_paths(self,):
        data_dir = os.path.join(self.path, "data")
        
        all_data_paths = {
            'npx_start_times': glob.glob(os.path.join(data_dir, '*start-time*')), 
            'npx_clocks': self.get_npx_clock_paths(), 
            'kilosort_files': self.get_kilosort_paths(), 

            'bno_files': glob.glob(os.path.join(data_dir, '*bno055*')), 
            'hs_files': glob.glob(os.path.join(data_dir, '*hs*')),
            
            'video_files': glob.glob(os.path.join(data_dir, '*compressed*')),
            'centroid_files': glob.glob(os.path.join(data_dir, '*centroid*')), 
            'timestamp_files': glob.glob(os.path.join(data_dir, '*timestamps*')),
            'sleap_files': glob.glob(os.path.join(self.path, '*sleap*', '*analysis*')), # since we might have multiple sleap dirs
            }
        
        self.data_paths = all_data_paths
        return all_data_paths    

    def get_npx_clock_paths(self,):
        npx_clocks = glob.glob(os.path.join(self.path, 'data', '*np2-*-clock*'))

        probe_to_clocks = defaultdict(list)

        for fname in npx_clocks:
            core = fname.split('np2-', 1)[1]
            probe = core.split('-clock', 1)[0]
            probe_to_clocks[probe].append(fname)

        npx_probes = np.array(sorted(probe_to_clocks.keys()))
        self.npx_probes = npx_probes
        self.probe_to_npx_clocks = probe_to_clocks
        return npx_clocks
    
    def get_kilosort_paths(self,):
        ks_dir = glob.glob(os.path.join(self.config['path'], 'output', '*'))
        ks_probe_dir_list = [d for d in ks_dir if os.path.isdir(d)]

        shank_dirs = np.array([glob.glob(os.path.join(probe_dir, '*shank*', 'kilosort4')) for probe_dir in ks_probe_dir_list], dtype=object)

        if len(shank_dirs.flatten()) > 0:
            return shank_dirs.flatten().tolist()

        else: 
            raise ValueError('No kilosort directories found in the output folder.')
        

####### LOADERS #######
    def is_datetime_col(self, val):
        try:
            pd.to_datetime(val)
            return True
        except:
            return False
        
    def load_npx_clock(self, clock_file):
        # # below loads quick but becomes slow when dividing by tick_res later
        # mm = np.memmap(clock_file, dtype=np.uint64, mode='r')
        # clock_data = np.asarray(mm)

        clock_data = np.fromfile(clock_file, dtype=np.uint64)

        print(f'loaded npx clock data: {clock_file}') if self.verbose else None
        return clock_data

    def load_npx_start_time(self, start_time_file):
        ts = pd.to_datetime(pd.read_csv(start_time_file)["Timestamp"].iloc[0])
        print(f'loaded npx start time: {start_time_file}') if self.verbose else None
        return ts

    def load_bno_data(self, bno_path):
        bno_df = pd.read_csv(bno_path, header=None)
        bno_df.drop(columns=[8], inplace=True)  #TODO check if this is always the case
        bno_df.columns = ['clock', 
                'euler_angle_x', 'euler_angle_y', 'euler_angle_z', 
                'quaternion_x', 'quaternion_y', 'quaternion_z', 'quaternion_w', 
                'acceleration_x', 'acceleration_y', 'acceleration_z',  
                'gravity_x', 'gravity_y', 'gravity_z', 
                'temperature']

        print(f'loaded bno data: {bno_path}') if self.verbose else None
        return bno_df
    
    def load_headstage_data(self, hs_path):
        hs = pd.read_csv(hs_path, header=None)
        hs.columns = ['temperature', 'time']
        hs.set_index('time', inplace=True)
        print(f'loaded headstage data: {hs_path}') if self.verbose else None
        return hs
        
    def load_kilosort_data(self, kilosort_file): 
        probe_name,shank_name = kilosort_file.split('/')[-3], kilosort_file.split('/')[-2]

        if shank_name is not None:
            spike_dict_name = f'probe_{probe_name}_{shank_name}'
            print(f'will load per shank kilosort data: {spike_dict_name}') if self.verbose else None
        else:
            spike_dict_name = f'probe_{probe_name}'
            print(f'will load per probe kilosort data: {spike_dict_name}') if self.verbose else None
        
        st = np.load( os.path.join(kilosort_file, 'spike_times.npy') )
        clu = np.load( os.path.join(kilosort_file, 'spike_clusters.npy'))

        dfs = []

        if 'cluster_KSLabel.tsv' in os.listdir(kilosort_file):
            ks_labels = pd.read_csv(os.path.join(kilosort_file, 'cluster_KSLabel.tsv'), sep='\t', index_col=0)
            dfs.append(ks_labels)

        if ('unit_labels.tsv' in os.listdir(kilosort_file)):
            ur_labels = pd.read_csv(os.path.join(kilosort_file, 'unit_labels.tsv'), index_col=0)
            dfs.append(ur_labels)

        if ('unitrefine_input_metrics.tsv' in os.listdir(kilosort_file)):
            ur_metrics = pd.read_csv(os.path.join(kilosort_file, 'unitrefine_input_metrics.tsv'), sep='\t', index_col=0)
            dfs.append(ur_metrics)

        neural_labels = pd.concat(dfs, axis=1)

        spike_dict = {
            spike_dict_name: {
            'spike_times': st,
            'spike_clusters': clu,
            'neural_labels': neural_labels,
            'meta_kilosort_file': kilosort_file
            }
        }

        print(f'loaded kilosort data: {kilosort_file}') if self.verbose else None
        return spike_dict

    def load_centroid(self, file_path: str) -> pd.DataFrame:
        print(f'loading centroid timestamps from {file_path}') if self.verbose else None
        centroid_df = pd.read_csv(file_path, header=None)

        if centroid_df.shape[1] == 1:
            centroid_df.columns = ['time']

        elif centroid_df.shape[1] == 2:
            centroid_df.columns = ['bonsai_centroid.x','bonsai_centroid.y']

        elif centroid_df.shape[1] == 3:
            column_num = next((c for c in centroid_df.columns if self.is_datetime_col(centroid_df[c].iloc[0])), None)

            centroid_df = centroid_df.set_index(centroid_df.columns[column_num])   
            centroid_df.index.name = 'time'
            
            centroid_df.columns = ['bonsai_centroid.x','bonsai_centroid.y']

        else:
            raise ValueError("centroid_timestamps needs to be either 1d where it's time, or 3d where its time and centroid")
        
        print(f'loaded centroid timestamps: {file_path}') if self.verbose else None
        return centroid_df
    
    def load_video_timestamps(self, file_path: str) -> pd.DataFrame:
        ts = pd.read_csv(file_path, header=None)

        column_num = next((c for c in ts.columns if self.is_datetime_col(ts[c].iloc[0])), None)

        if column_num is None:
            raise ValueError("File named timestamp whereas there seem to be none in here.")

        ts_df = ts.set_index(ts.columns[column_num])   
        ts_df.index.name = 'time'
        return ts_df

    def read_h5(self, pose_file):
        with h5py.File(pose_file, "r") as f:
            locations = f["tracks"][:].T
            node_names = [n.decode() for n in f["node_names"][:]]
            point_scores = f["point_scores"][:].T

            if locations.shape[-1] != 1: 
                locations = np.nanmean(locations,axis=-1)[..., np.newaxis]
                point_scores = np.nanmean(point_scores,axis=-1)[..., np.newaxis]
                
            return locations, node_names, point_scores

    def load_sleap_data(self, pose_file):
        print(f'loading sleap data from {pose_file}') if self.verbose else None
        locations, node_names, point_scores = self.read_h5(pose_file)
        locations = locations.reshape(locations.shape[0], locations.shape[1] * 2)
        point_scores = point_scores.reshape(point_scores.shape[0], point_scores.shape[1])
        columns = [f"{name}.{coord}" for name in node_names for coord in ['x', 'y']]
        pose_df = pd.DataFrame(locations, columns=columns)
        scores_cols = [f"{name}.score" for name in node_names]
        scores_df = pd.DataFrame(point_scores, columns=scores_cols)
        pose_df = pd.concat([pose_df, scores_df], axis=1)

        print(f'loaded sleap data: {pose_file}') if self.verbose else None
        return pose_df

# %%
class DataProcessor:
    def __init__(self, loader: DataLoader):
        self.loader = loader
        self.save_raw = loader.config.get('save_raw', True)
        self.verbose = loader.verbose
        self.pose_cleaner = PoseCleaner(loader, verbose=self.verbose)

    def create_timeline(self):
        _ = self.get_all_timestamps()
        self.timeline = pd.date_range(start=self.time_global_min, end=self.time_global_max, freq=self.loader.config['freq'])

        print(f'created global timeline from {self.timeline[0]} to {self.timeline[-1]}') if self.verbose else None
        return self.timeline

# -------------------------------------------------------------------------------------
    def process_all_data(self, max_workers=16):
        if self.save_raw:
            raw_data_path = os.path.join(self.loader.output_path, 'raw_data')
            self.raw_data_path = raw_data_path
            os.makedirs(raw_data_path, exist_ok=True)
        
        def _safe_call(tag, idx, fn, *args):
            try:
                result = fn(*args)
                return tag, idx, result, None
            except Exception as e:
                tb = traceback.format_exc()
                warnings.warn(
                    f"Error loading {tag} index {idx}\n"
                    f"Exception: {type(e).__name__}: {e}\n"
                    f"Traceback:\n{tb}"
                )
                err = {
                    "tag": tag,
                    "index": idx,
                    "file": args[0] if args else None,
                    "exc_type": type(e).__name__,
                    "exc_msg": str(e),
                    "traceback": tb,
                }
                return tag, idx, None, err

        if not hasattr(self, 'timeline'):
            _ = self.create_timeline()

        tasks, out = self.build_tasks()
        errors = []

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(_safe_call, tag, idx, fn, *args)
                for (tag, idx, fn, args) in tasks
            ]

            for fut in as_completed(futures):
                tag, idx, result, err = fut.result()
                if err is not None:
                    errors.append(err)
                else:
                    out[tag][idx] = result
        
        self.processed_data = out
        self.errors = errors

        print('=============== Data processing summary ===============') if self.verbose else None
        self.data_exists = {}
        for data in list(self.processed_data.keys()):
            if len(self.processed_data[data]) == 0:
                print(f'No data processed for {data}!') if self.verbose else None
                self.data_exists[data] = False
            else:
                print(f'Processed {len(self.processed_data[data])} items for {data}.') if self.verbose else None
                self.data_exists[data] = True
        print('====================================================') if self.verbose else None

        try:
            self.tests(out)
        except:
            warnings.warn("Error during testing the processed data: " + traceback.format_exc())

        main_dataframe = self.join_all_data(out)

        assert len(main_dataframe) == len(self.timeline), "Final dataframe length does not match timeline length!"

        # postprocess 
        final_dataframe = self.postprocess_dataframe(main_dataframe)
        final_parquet_path = os.path.join(self.loader.output_path, f'final_df_{self.loader.recording}.parquet')
        print(f'saving final dataframe to parquet at \n {final_parquet_path}') if self.verbose else None
        final_dataframe.to_parquet(final_parquet_path)

        return final_dataframe, errors

# -------------------------------------------------------------------------------------

    def build_tasks(self) -> Tuple[List[Tuple], dict]:

        tasks = []
        out = {}

        ts = self.loader.loaded_data.get('timestamp',  [])
        sleap = self.loader.loaded_data.get('sleap', [])
        centroids = self.loader.loaded_data.get('centroid', [])

        bno = self.loader.loaded_data.get('bno', [])
        hs = self.loader.loaded_data.get('headstage', [])   

        self.n_sleaps = len(sleap) // len(ts) if ts and len(sleap) % len(ts) == 0 else 0 # checks for multiple sleap folders, and whether we can assign timestamps to everything

        if self.n_sleaps:
            out['sleap'] = [None] * len(sleap)
            tags = [path.split('/')[-2].split('_')[0] for path in self.loader.data_paths['sleap_files']] # ensure its <tag>_sleap_output or sleap_output

            for i in range(len(sleap)):
                tasks.append(("sleap", i, self.process_sleap_data, (sleap[i], ts[i % len(ts)], tags[i], i)))


        self.n_centroids = len(centroids) // len(ts) if ts and len(centroids) % len(ts) == 0 else 0

        if self.n_centroids:
            out['centroid'] = [None] * len(centroids)
            tags = [path.split('/')[-1].split('_')[1] for path in self.loader.data_paths['centroid_files']] 

            for i in range(len(centroids)):
                tasks.append(("centroid", i, self.process_centroid_data, (centroids[i], ts[i % len(ts)], tags[i], i)))


        n_bnos = len(bno) // len(hs) if hs and len(bno) % len(hs) == 0 else 0

        if n_bnos:
            out["bno"] = [None] * len(bno)
            for i, (bno_df, hs_df) in enumerate(zip(bno, hs)):
                tasks.append(("bno", i, self.process_bno_data, (bno_df, hs_df, i)))
        else:
            out["bno"] = []

        kilosort = self.loader.loaded_data.get("kilosort", [])

        if kilosort:
            valid_kilosort = [ks for ks in kilosort if ks is not None]

            out["neural"] = [None] * len(valid_kilosort)
            for i, ks in enumerate(valid_kilosort):
                tasks.append(("neural", i, self.process_neural_data, (ks, i)))
        else:
            out["neural"] = []

        return tasks, out
    
    def join_all_data(self, loaded_data):
        print("joining neural data") if self.verbose else None
        neural_and_labels = loaded_data.get("neural", [])
        neural_df = pd.concat([row[0] for row in neural_and_labels], axis=1).reindex(self.timeline)

        label_list = [row[1] for row in neural_and_labels]
        labels_df = pd.concat(label_list, axis=0)
        label_output_path = os.path.join(self.loader.output_path, f'neural_labels_{self.loader.recording}.parquet')
        labels_df.to_parquet(label_output_path)
        print(f'saved neural labels to parquet at \n {label_output_path}') if self.verbose else None

        print("joining sleap data") if self.verbose else None
        sleap_df = pd.concat(loaded_data.get("sleap", []), axis=0)
        sleap_df = sleap_df[~sleap_df.index.duplicated(keep='first')].reindex(self.timeline)

        print("joining bno data") if self.verbose else None
        bno_df = pd.concat(loaded_data.get("bno", []), axis=0)
        bno_df = bno_df[~bno_df.index.duplicated(keep='first')].reindex(self.timeline)

        print("joining centroid data") if self.verbose else None
        centroid_df = pd.concat(loaded_data.get("centroid", []), axis=0)
        centroid_df = centroid_df[~centroid_df.index.duplicated(keep='first')].reindex(self.timeline)

        print('all joined; concatenating everything into main_dataframe and filling nans in neural data with 0s') if self.verbose else None

        main_dataframe = pd.concat([neural_df, sleap_df, centroid_df, bno_df], axis=1)
        neural_cols = neural_df.columns
        main_dataframe[neural_cols] = main_dataframe[neural_cols].fillna(0).astype(np.uint16)

        return main_dataframe

    def add_existence_columns(self, df):
        existence_cols = []
        new_cols = {}

        probe_bounds = np.array(self.all_timestamps['npx'][1:3])

        for i, probe in enumerate(self.loader.npx_probes):
            col = f"meta_neural_{probe}_exists"
            existence_cols.append(col)
            arr = np.zeros(len(self.timeline), dtype=np.uint8)
            for r in range(probe_bounds.shape[1]):
                start_idx = np.searchsorted(self.timeline, probe_bounds[0, r, i], side='left')
                end_idx = np.searchsorted(self.timeline, probe_bounds[1, r, i], side='right')
                arr[start_idx:end_idx] = 1
            new_cols[col] = arr

        centroid_bounds = np.array(self.all_timestamps['centroid'][1:3])
        col = 'meta_centroid_exists'
        existence_cols.append(col)
        arr = np.zeros(len(self.timeline), dtype=np.uint8)
        for r in range(centroid_bounds.shape[1]):
            start_idx = np.searchsorted(self.timeline, centroid_bounds[0, r], side='left')
            end_idx = np.searchsorted(self.timeline, centroid_bounds[1, r], side='right')
            arr[start_idx:end_idx] = 1
        new_cols[col] = arr

        bno_bounds = np.array(self.all_timestamps['headstage'][1:3])
        col = 'meta_bno_exists'
        existence_cols.append(col)
        arr = np.zeros(len(self.timeline), dtype=np.uint8)
        for r in range(bno_bounds.shape[1]):
            start_idx = np.searchsorted(self.timeline, bno_bounds[0, r], side='left')
            end_idx = np.searchsorted(self.timeline, bno_bounds[1, r], side='right')
            arr[start_idx:end_idx] = 1
        new_cols[col] = arr

        exist_df = pd.DataFrame(new_cols, index=self.timeline)
        exist_df['meta_all_data_exists'] = (exist_df[existence_cols].sum(axis=1) == len(existence_cols)).astype(np.uint8)
        existence_cols.append('meta_all_data_exists')

        df = pd.concat([df, exist_df], axis=1)

        if self.loader.config.get('plot', True):
            fig = plt.figure(figsize=(12, 4))
            plt.plot(df.index, df[existence_cols].values)
            plt.legend(existence_cols, loc='lower center')
            plt.title('Data Existence Over Time')
            plt.xlabel('Time')
            plt.ylabel('Exists (1) or Not (0)')
            plt.tight_layout()
            fig.savefig(os.path.join(self.loader.output_path, 'data_existence_over_time.png'))
            plt.close(fig)
        return df
    
    def postprocess_dataframe(self, df):
        # meta_all_data_exists, meta_bno_exists, meta_pose_exists, meta_neural_exists; should be columns with binary indicators whether it exists

        # add existence columns
        df = self.add_existence_columns(df)

        # make column wise masks
        data_masks = self.data_masks(df)
        df.attrs['recording_id'] = self.loader.recording
        df.attrs['data_masks'] = data_masks
        df.attrs['loader_config'] = self.loader.config
        df.attrs['data_paths'] = self.loader.data_paths

        with open(os.path.join(self.loader.config['output_path'], 'metadata.json'), 'w') as f:
            json.dump(df.attrs, f, indent=4)
            
        return df

    def data_masks(self, df):
        cols = df.columns
        metadata_mask = (cols.str.contains('meta') | cols.str.contains('score'))
        probe_a_mask = cols.str.contains('a_cluster')
        probe_b_mask = cols.str.contains('b_cluster')
        bno_mask = cols.str.contains('bno') & ~cols.str.contains('meta')
        pose_mask = cols.str.contains('pose') & ~(cols.str.contains('meta') |cols.str.contains('score') )
        good_neurons_mask_kilosort = cols.str.contains('good')
        sua_mask = cols.str.contains('sua')
        neural_unitrefine_mask = cols.str.contains('neural') & ~cols.str.contains('meta')
        other_mask = ~(metadata_mask | probe_a_mask | probe_b_mask | pose_mask | bno_mask | good_neurons_mask_kilosort | sua_mask | neural_unitrefine_mask)
        data_masks = {
            'metadata_mask': metadata_mask.astype(int).tolist(),
            'probe_a_mask': probe_a_mask.astype(int).tolist(),
            'probe_b_mask': probe_b_mask.astype(int).tolist(),
            'good_neurons_mask_kilosort': good_neurons_mask_kilosort.astype(int).tolist(),
            'sua_unitrefine_mask': sua_mask.astype(int).tolist(),
            'neural_unitrefine_mask': neural_unitrefine_mask.astype(int).tolist(),
            'other_mask': other_mask.astype(int).tolist(),
            'bno_mask': bno_mask.astype(int).tolist(),
            'pose_mask': pose_mask.astype(int).tolist(),
        }

        print('data masks summary:') if self.verbose else None
        for key, mask in data_masks.items():
            print(f'  {key}: {np.sum(mask)} columns') if self.verbose else None

        return data_masks

    def get_all_timestamps(self):
        """
        Determine global min/max time bounds.
        """

        bounds_start = []
        bounds_end = []

        out = {}

        npx_times, npx_starts, npx_ends = self.get_npx_time()
        self.npx_timestamps = npx_times
        out["npx"] = (npx_times, npx_starts, npx_ends)

        bounds_start.append(np.asarray(npx_starts).ravel())
        bounds_end.append(np.asarray(npx_ends).ravel())

        centroid_times, centroid_starts, centroid_ends = self.get_df_time(datatype="timestamp")
        self.centroid_timestamps = centroid_times
        out["centroid"] = (centroid_times, centroid_starts, centroid_ends)

        bounds_start.append(np.asarray(centroid_starts).ravel())
        bounds_end.append(np.asarray(centroid_ends).ravel())

        hs_times, hs_starts, hs_ends = self.get_df_time(datatype="headstage")
        self.hs_timestamps = hs_times
        out["headstage"] = (hs_times, hs_starts, hs_ends)

        bounds_start.append(np.asarray(hs_starts).ravel())
        bounds_end.append(np.asarray(hs_ends).ravel())

        if len(bounds_start) == 0 or len(bounds_end) == 0:
            raise ValueError(
                "Cannot build global timeline: no enabled modalities provide timestamps "
                "(neural/pose/bno all disabled or missing)."
            )

        self.time_global_min = np.min(np.concatenate(bounds_start))
        self.time_global_max = np.max(np.concatenate(bounds_end))
        self.all_timestamps = out

        return out

    def skip_initial_seconds(self, df, skip_seconds):
        if skip_seconds > 0:
            skip_time = pd.to_timedelta(skip_seconds, unit='s')
            df = df[df.index >= (df.index[0] + skip_time)]
            print(f'skipped first {skip_seconds} seconds') if self.verbose else None
        return df

    def extra_beh_features(self, pose_df, source):
        # if pose_source is sleap, can add even more and more stable features
        if source == 'centroid_xy':
            # get heading and velocity
            print('adding extra features for centroid-based pose') if self.verbose else None    
        if source == "sleap":
            # perhaps employ 
            print('adding extra features for sleap-based pose') if self.verbose else None    
        return

    def process_sleap_data(self, pose, time, source: str, file_index: int):
        print("processing pose data") if self.verbose else None
        datatype = "pose"

        source = source if self.n_sleaps > 1 else None # we want to append whether its large/box if we have multiple sleaps
        if source == 'sleap':
            source = None
    
        df = self.df_w_timestamps(pose, time, datatype, source)

        if self.save_raw:
            raw_path = os.path.join(self.raw_data_path, f'raw_pose_{file_index}.parquet')
            df.to_parquet(raw_path)
            print(f'saved raw pose data to {raw_path}') if self.verbose else None

        skip_frames = self.loader.config.get('skip_frames', None)
        if skip_frames:
            skip_seconds = skip_frames[file_index]
            df = self.skip_initial_seconds(df, skip_seconds)
        
        if self.loader.config.get('clean_pose', True):
            video_files = self.loader.data_paths.get("video_files", [])
            video_path = video_files[file_index % len(video_files)] if video_files else None
            plot = self.loader.config.get('plot', True) and video_path is not None

            df = self.pose_cleaner.clean_dataframe(
                df,
                video_path,
                plot=plot,
            )

        # interp_df = self.extra_beh_features(interp_df, source)

        original_index = df.index
        df = self.interpolate_df_to_timeline(df, self.timeline)
        df = self.assign_closest_frame(df, original_index, datatype)
    
        df.columns = [f"meta_{col}" if "score" in col else col for col in df.columns]

        df[f'meta_{datatype}_source_file'] = file_index
        print("processed pose data") if self.verbose else None
        return df
    
    def process_centroid_data(self, centroid, time, source: str, file_index: int):
        print("processing centroid data") if self.verbose else None
        datatype = "centroid"

        source = source if self.n_centroids > 1 else None # we want to append whether its large/box if we have multiple centroids
    
        df = self.df_w_timestamps(centroid, time, datatype, source)

        if self.save_raw:
            raw_path = os.path.join(self.raw_data_path, f'raw_centroid_{file_index}.parquet')
            df.to_parquet(raw_path)
            print(f'saved raw centroid data to {raw_path}') if self.verbose else None

        skip_frames = self.loader.config.get('skip_frames', None)
        if skip_frames:
            skip_seconds = skip_frames[file_index]
            df = self.skip_initial_seconds(df, skip_seconds)

        original_index = df.index
        df = self.interpolate_df_to_timeline(df, self.timeline)
        df = self.assign_closest_frame(df, original_index, datatype)

        df[f'meta_{datatype}_source_file'] = file_index

        df.columns = [f"pose_{col}" for col in df.columns]
        print("processed centroid data") if self.verbose else None
        return df

    def process_bno_data(self, bno, time, file_index: int):
        print('processing bno data') if self.verbose else None
        datatype = 'bno'

        df_w_time = self.df_w_timestamps(bno, time, datatype)

        if self.save_raw:
            raw_path = os.path.join(self.raw_data_path, f'raw_bno_{file_index}.parquet')
            df_w_time.to_parquet(raw_path)
            print(f'saved raw bno data to {raw_path}') if self.verbose else None

        interp_df = self.interpolate_df_to_timeline(df_w_time, self.timeline)
        interp_df = self.assign_closest_frame(interp_df, df_w_time.index, datatype)
        interp_df[f'meta_{datatype}_source_file'] = file_index

        print('processed bno data') if self.verbose else None
        return interp_df

    def df_w_timestamps(self, df, time, datatype, source=None):
        df_tmp = df.copy()
        time_tmp = time.copy()

        time_tmp['drop_this'] = np.nan 

        if len(df_tmp) < len(time_tmp):
            warnings.warn(f'{datatype} has less frames than time, assuming they align at start')  if self.verbose else None
            time_tmp = time_tmp.iloc[:len(df_tmp)]
        elif len(df_tmp) > len(time_tmp):
            warnings.warn(f'{datatype} has more frames than time, assuming they align at start') if self.verbose else None
            df_tmp = df_tmp.iloc[:len(time_tmp)]    

        df_tmp.index = time_tmp.index

        # check if there is intersecting columns (e.g. temp is in both hs and bno) and drop it from the time one; we only want to use time_df as a index.
        cols_to_drop = time_tmp.columns.intersection(df_tmp.columns)
        time_tmp = time_tmp.drop(columns=cols_to_drop)

        df_tmp = df_tmp.merge(time_tmp, left_index=True, right_index=True, how='inner')
        df_tmp.drop(columns=['drop_this'], inplace=True)
        df_tmp.index = pd.to_datetime(df_tmp.index)
        df_tmp.columns = [f"{datatype}_{col}" for col in df_tmp.columns]

        if source:
            df_tmp.columns = [f"{col}_{source}" if col.startswith(f"{datatype}_") else col for col in df_tmp.columns]

        return df_tmp

    def interpolate_df_to_timeline(self, df, timeline):

        timeline = self.timeline

        combined = pd.concat([pd.DataFrame([np.nan]*len(timeline), index=timeline, columns=['drop_this']), df])
        combined.index = pd.to_datetime(combined.index)
        combined = combined.sort_index().drop(columns=['drop_this'])

        valid_range = (combined.index >= df.index[0]) & (combined.index <= df.index[-1])
        combined.loc[valid_range] = combined.loc[valid_range].interpolate(method='time')
        final = combined.loc[combined.index.isin(timeline) & valid_range]
        return final

    def assign_closest_frame(self, interp_df: pd.DataFrame, original_index: pd.DatetimeIndex, datatype) -> pd.DataFrame:
        """
        Adds a 'closest_frame' column to interp_df, indicating the index of the closest frame from original_index for each timestamp in interp_df.
        """
        print(f'assigning closest frame for {datatype}') if self.verbose else None
        original_times = original_index.values.astype('int64')
        interp_times = interp_df.index.values.astype('int64')

        idxs = np.searchsorted(original_times, interp_times)

        # Clamp values to valid rangeF
        idxs = np.clip(idxs, 0, len(original_times) - 1)

        prev = np.maximum(idxs - 1, 0)
        next_ = idxs

        # Choose the nearest one between prev and next
        nearest = np.where(
            np.abs(interp_times - original_times[prev]) <= np.abs(interp_times - original_times[next_]),
            prev, next_
        )

        interp_df[f'meta_{datatype}_closest_frame'] = nearest
        return interp_df

    def process_neural_data(self, kilosort_dataset, file_index: int):
        print(f'processing neural data for {list(kilosort_dataset.keys())[0]}') if self.verbose else None
        # get kilosort and npx timestamps
        dataset_name = list(kilosort_dataset.keys())[0]

        split_dataset_name = dataset_name.split('_')
        probe_name = split_dataset_name[1]
        shank_idx = split_dataset_name[-1] if 'shank' in dataset_name else 'all'

        corresponding_npx_timestamps = self.npx_t_s_probes[probe_name]
        start_times = self.loader.loaded_data['npx_start_times']

        st, clu, labels, file = kilosort_dataset[dataset_name].values()

        recording_lengths = [len(t) for t in corresponding_npx_timestamps]
        recording_bounds = np.cumsum([0] + recording_lengths)
        recording_id = np.searchsorted(recording_bounds[1:], st, side='right')

        # assign npx timestamps to spikes
        spike_dfs = []
        for i in range(len(recording_lengths)):
            idx = recording_id == i
            rel_spike_times = st[idx] - recording_bounds[i]

            spike_sec = corresponding_npx_timestamps[i][rel_spike_times]
            start_time = start_times[i]
            spike_real_time = start_time + pd.to_timedelta(spike_sec, unit='s')

            spike_df = pd.DataFrame({'timestamp': spike_real_time, 'cluster': clu[idx]})
            spike_dfs.append(spike_df)
        spike_df = pd.concat(spike_dfs).sort_values('timestamp').reset_index(drop=True)
        
        # saw raw data
        if self.save_raw:
            raw_path = os.path.join(self.raw_data_path, f'raw_neural_{dataset_name}.parquet')
            spike_df.to_parquet(raw_path)
            print(f'saved raw neural data to {raw_path}') if self.verbose else None

        # put a shank into the timeline
        spike_ns = spike_df['timestamp'].astype('int64')
        bin_indices = np.searchsorted(self.timeline.astype('int64'), spike_ns, side='right') - 1

        valid_mask = (bin_indices >= 0) & (bin_indices < len(self.timeline))
        spike_df = spike_df[valid_mask]
        bin_indices = bin_indices[valid_mask]
        spike_df['binned_time'] = self.timeline[bin_indices]

        binned = spike_df.groupby(['binned_time', 'cluster']).size().unstack(fill_value=0)

        labels['shank_idx'] = shank_idx
        labels['probe_name'] = probe_name

        if 'sua_prediction' in labels.columns and 'noise_prediction' in labels.columns:
            binned.columns = [f"{probe_name}_cluster_{c}_{labels.iloc[c]['sua_prediction']}_{labels.iloc[c]['noise_prediction']}_shank_{shank_idx}" for c in binned.columns]
        elif 'KSLabel' in labels.columns:
            binned.columns = [f"{probe_name}_cluster_{c}_{labels.iloc[c]['KSLabel']}_shank_{shank_idx}" for c in binned.columns]

        binned.fillna(0, inplace=True)
        print(f'processed neural data for {dataset_name}') if self.verbose else None
        return binned, labels

    def get_npx_time(self,):
        ''' Get NPX timestamps in seconds, start times, and end times.
        Note: this won't load it into pd.datetime format since it's heavy and we won't need all of them in that format, will be done later.
        
        Also, npx_t_s is a 2D array where each column corresponds to a probe and each row corresponds to a recording segment.'''

        M = len(self.loader.loaded_data["npx_start_times"])          
        P = len(self.loader.loaded_data["npx_clocks"]) // M 

        npx_t = np.array(
            [self.loader.loaded_data["npx_clocks"][p*M:(p+1)*M] for p in range(P)],
            dtype=object).T  

        npx_starts = np.empty(npx_t.shape, dtype=object)
        npx_ends   = np.empty(npx_t.shape, dtype=object)
        npx_t_s   = np.empty(npx_t.shape, dtype=object)

        for i in range(npx_t.shape[0]):
            for j in range(npx_t.shape[1]):
                npx_t_s[i, j] = np.asarray(npx_t[i, j], dtype=np.float64) / self.loader.config["tick_res"]
                npx_starts[i, j] = self.loader.loaded_data["npx_start_times"][i] + pd.Timedelta(seconds=npx_t_s[i, j][0])
                npx_ends[i, j]   = self.loader.loaded_data["npx_start_times"][i] + pd.Timedelta(seconds=npx_t_s[i, j][-1])

        # dict with for i, probe_name in enumerate(self.loader.npx_probes) {probe_name: [:,i]}
        npx_t_s_dict = {}
        for i, probe_name in enumerate(self.loader.npx_probes):
            npx_t_s_dict[probe_name] = npx_t_s[:, i]

        self.npx_t_s_probes = npx_t_s_dict # this is gonna need some rework depending on probe count or something
        return npx_t_s, npx_starts, npx_ends

    def get_df_time(self,datatype):
        time = []
        start = []
        end = []
        for file_n, df in enumerate(self.loader.loaded_data[datatype]):
            ts  = df.index
            if self.loader.config.get('skip_frames', None) and datatype == 'centroid': # skip frames for centroid
                skip_seconds = self.loader.config['skip_frames'][file_n] 
                skip_time = pd.to_timedelta(skip_seconds, unit='s')
                ts = ts[ts >= (ts[0] + skip_time)]              
            ts = pd.to_datetime(ts)    
            time.append(ts)
            start.append(ts[0])
            end.append(ts[-1])
        return time,start,end

    
    def tests(self, out,):
        # check that all the columns are in ascending order just like the cluster indices in the labels
        columns_are_indices = []
        for df in out['neural']:
            for i, col in enumerate(df[0].columns):
                columns_are_indices.append(col.split('_')[2] == str(i))

        try:
            assert np.sum(columns_are_indices) == len(columns_are_indices), 'not all columns have the correct cluster index in their name'
        except AssertionError as e:
            print(str(e))

        # %%
        # check that correct labels are assigned to columns based on index - kinda redundant with the previous
        correct_UR_label = []
        for df in out['neural']:
            for i, col in enumerate(df[0].columns):
                correct_UR_label.append(col.split('_')[3] == df[1].iloc[i]['sua_prediction'])
        
        try:
            assert np.sum(correct_UR_label) == len(correct_UR_label), 'not all columns have the correct UR label in their name'
        except AssertionError as e:
            print(str(e))

        # %%
        # check if num_spikes in the labels thing corresponds with the number of spikes when we sum the neural DF
        for i, df in enumerate(out['neural']):
            try:
                df_sum = df[0].sum()
                num_spikes = out['neural'][i][1]['num_spikes'].values
                assert (df_sum == num_spikes).all(), f'sum of binned df doesnt correspond to num_spikes in labels for neural dataset {i}'
            except AssertionError as e:
                print(str(e))

        # %%
        # check if all of the relevant dataframes have indices which are in processor.timeline
        check_tl = self.timeline.values.astype('int64')
        for i, df in enumerate(out['neural']):
            try: 
                assert np.isin(df[0].index.values.astype('int64'), check_tl).all(), 'not all neural df indices are in the global timeline'
            except AssertionError as e:
                print(str(e))

        for i, df in enumerate(out['sleap']):
            try:
                assert np.isin(df.index.values.astype('int64'), check_tl).all(), 'not all sleap df indices are in the global timeline'
            except AssertionError as e:
                print(str(e))

        for i, df in enumerate(out['centroid']):
            try:
                assert np.isin(df.index.values.astype('int64'), check_tl).all(), 'not all centroid df indices are in the global timeline'
            except AssertionError as e:
                print(str(e))

        for i, df in enumerate(out['bno']):
            try:
                assert np.isin(df.index.values.astype('int64'), check_tl).all(), 'not all bno df indices are in the global timeline'
            except AssertionError as e:
                print(str(e))

        # %%
        # so we notice that the centroids dont always have the same length as the timestamps, which is bonsai closing processing before the last centroid gets written.
        dl = self.loader
        for i in range(len(dl.loaded_data['centroid'])):
            try:
                assert dl.loaded_data['centroid'][i].shape[0] == dl.loaded_data['timestamp'][i].shape[0], f'{i} isnt same shape'
            except AssertionError as e:
                print(str(e))

        # %%
        # whereas I think the sleap and timestamps should be, I hope. Anyway, in either case we're assuming that everything is aligned at the start, but only the last stuff gets cut off, because bonsai closes prematurely.
        
        for i in range(len(dl.loaded_data['sleap'])):
            try: 
                assert dl.loaded_data['sleap'][i].shape[0] == dl.loaded_data['timestamp'][i % len(dl.loaded_data['timestamp'])].shape[0], f'{i} isnt same shape'
            except AssertionError as e:
                print(str(e))   
        return

def main():
    parser = argparse.ArgumentParser(description="Process Neuropixels recording.")
    parser.add_argument("path", help="Path to recording directory")
    parser.add_argument("--output_path", type=str, default=None)
    parser.add_argument("--save-raw", action='store_true', default=True)
    parser.add_argument('--verbose', action='store_true', default=True)
    parser.add_argument('--freq', type=str, default='10ms')
    parser.add_argument('--plot', action='store_true', default=True)
    parser.add_argument('--clean_pose', action='store_true', default=True)
    parser.add_argument('--workers', type=int, default=16)
    parser.add_argument('--skip_frames', type=str, default=None)
    args = parser.parse_args()
    config = vars(args)

    if config['output_path'] is None:
        config['output_path'] = os.path.join(config['path'], "processed_data-" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
        # config['output_path'] = os.path.join(config['path'], "processed_data")
    os.makedirs(config['output_path'], exist_ok=True)

    if config['skip_frames'] is not None:
        import json
        config['skip_frames'] = json.loads(config['skip_frames'])

    config['recording'] = os.path.basename(os.path.normpath(config['path']))
    log_file = os.path.join(config['output_path'], "combiner.log")

    logger = setup_logger(log_file, config['verbose'])
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)

    logger.info(f"Processing recording {config['recording']} at path: {config['path']}")

    loader = DataLoader(config)
    loaded_data, load_errors = loader.load_all_data(max_workers=config['workers'])
    processor = DataProcessor(loader)
    final_dataframe, process_errors = processor.process_all_data(max_workers=config['workers'])

    logger.info(f"Done. Load errors: {len(load_errors)}, Process errors: {len(process_errors)}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Unhandled exception")
        raise
