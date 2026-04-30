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

@dataclass(frozen=True)
class TaskSpec:
    tag: str                      # e.g. "pose", "bno", "neural"
    idx: int
    fn: Callable
    args: Tuple[Any, ...]

@dataclass(frozen=True)
class PosePlan:
    enabled: bool
    source: Optional[str]         # "sleap" | "centroid_xy" | None
    pairs: List[Tuple[Any, Any]]  # list of (pose_df, centroid_df)

@dataclass(frozen=True)
class BnoPlan:
    enabled: bool
    pairs: List[Tuple[Any, Any]]  # list of (bno_df, headstage_df)

@dataclass(frozen=True)
class NeuralPlan:
    enabled: bool
    items: List[Any]              # kilosort dicts

@dataclass(frozen=True)
class ProcessingPlan:
    pose: PosePlan
    bno: BnoPlan
    neural: NeuralPlan

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

def str2bool(v):
    if isinstance(v, bool):
        return v
    v = v.strip().lower()
    if v in {"true", "t", "1", "yes", "y"}:
        return True
    if v in {"false", "f", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {v}")

def _is_windows_drive_path(path: str) -> bool:
    return bool(re.match(r"^[A-Za-z]:[\\/]", path))

def _is_wsl() -> bool:
    if platform.system().lower() != "linux":
        return False
    return (
        "microsoft" in platform.release().lower()
        or "microsoft" in platform.version().lower()
        or bool(os.environ.get("WSL_DISTRO_NAME"))
    )

def _split_path_parts(path: str) -> List[str]:
    return [p for p in re.split(r"[\\/]+", path) if p]

def _recording_id_from_path(path: str) -> str:
    parts = _split_path_parts(path)
    if len(parts) >= 2:
        return "_".join(parts[-2:])
    if len(parts) == 1:
        return parts[0]
    return ""

def _parse_kilosort_dir(path: str) -> Tuple[str, Optional[str]]:
    parts = _split_path_parts(path.replace("\\", "/"))
    if len(parts) < 2:
        raise ValueError(f"Malformed kilosort path (too short): {path}")

    ks_dir = parts[-1].lower()
    if not ks_dir.startswith("kilosort"):
        raise ValueError(f"Malformed kilosort path (expected trailing kilosort directory): {path}")

    parent = parts[-2]
    if "shank" in parent.lower():
        if len(parts) < 3:
            raise ValueError(f"Malformed shank kilosort path (missing probe directory): {path}")
        return parts[-3], parent

    return parent, None

def windows_to_ubuntu_path(win_path: str) -> str:
    win = PureWindowsPath(win_path)
    drive = win.drive.rstrip(":").lower()           
    parts = win.parts[1:]                            
    path = str(PurePosixPath("/mnt", drive, *parts,))
    return path

def normalize_path(raw_path: str) -> str:
    """
    Normalize user-provided paths across Windows and WSL/Linux hosts:
    - On Windows: keep Windows paths as-is.
    - On WSL: convert Windows drive paths (e.g., V:\\...) to /mnt/v/... .
    - Otherwise: leave non-Windows paths as-is.
    """
    if raw_path is None:
        raise ValueError("Path cannot be None")

    raw_path = str(raw_path)
    system = platform.system().lower()

    if system == "windows":
        path = raw_path
    else:
        path = windows_to_ubuntu_path(raw_path) if _is_wsl() and _is_windows_drive_path(raw_path) else raw_path

    return os.path.normpath(path)

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

def setup_logger(log_file: str, verbose: bool) -> logging.Logger:
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


class  DataLoader:
    config: dict[str,Any]
    verbose: bool
    output_path: str

    def __init__(self, config: dict[str,Any]):
        self.path = normalize_path(config.get('path'))
        self.config = config
        self.config['tick_res'] = config.get('tick_res', 2.5e8)
        self.verbose = config.get('verbose', True)
        self.recording_id = _recording_id_from_path(self.path)
        self.output_path = normalize_path(config.get('output_path'))
 
    def load_all_data(self, max_workers=4):
        if not hasattr(self, "data_paths"):
            _ = self.get_all_data_paths()

        clocks = sorted(self.data_paths.get("npx_clocks", []))
        start_times = sorted(self.data_paths.get("npx_start_times", []))
        bno_files = sorted(self.data_paths.get("bno_files", []))
        centroid_files = sorted(self.data_paths.get("centroid_files", []))
        sleap_files = sorted(self.data_paths.get("sleap_files", []))
        kilosort_files = sorted(self.data_paths.get("kilosort_files", []))
        hs_files = sorted(self.data_paths.get("hs_files", []))

        out = {
            "npx_clocks": [None] * len(clocks),
            "npx_start_times": [None] * len(start_times),
            "bno": [None] * len(bno_files),
            "sleap": [None] * len(sleap_files),
            "kilosort": [None] * len(kilosort_files),
            "centroid": [None] * len(centroid_files),
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
        for i, f in enumerate(kilosort_files):
            tasks.append(("kilosort", i, self.load_kilosort_data, f))      
        for i, f in enumerate(clocks):
            tasks.append(("npx_clocks", i, self.load_npx_clock, f))
        for i, f in enumerate(start_times):
            tasks.append(("npx_start_times", i, self.load_npx_start_time, f))
        for i, f in enumerate(sleap_files):
            tasks.append(("sleap", i, self.load_sleap_data, f))
        for i, f in enumerate(bno_files):
            tasks.append(("bno", i, self.load_bno_data, f))
        for i, f in enumerate(centroid_files):
            tasks.append(("centroid", i, self.load_centroid_timestamps, f))
        for i, f in enumerate(hs_files):
            tasks.append(("headstage", i, self.load_headstage_data, f))

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

        self.build_processing_plan()        
        return out, errors

###### GET PATHS #######
    def get_all_data_paths(self,):
        data_dir = os.path.join(self.path, "data")
        sleap_dir = os.path.join(self.path, "sleap_output")

        centroid_files = glob.glob(os.path.join(data_dir, '*centroid_absolutevalue*'))
        if len(centroid_files) == 0:
            centroid_files = glob.glob(os.path.join(data_dir, '*centroid*'))

        all_data_paths = {
            'npx_start_times': glob.glob(os.path.join(data_dir, '*start-time*')), 
            'npx_clocks': self.get_npx_clock_paths(), 
            'kilosort_files': self.get_kilosort_paths(), 

            'bno_files': glob.glob(os.path.join(data_dir, '*bno055*')), 
            'hs_files': glob.glob(os.path.join(data_dir, '*hs*')),
            
            'video_files': glob.glob(os.path.join(data_dir, '*video*')),
            'centroid_files': centroid_files, 
            'sleap_files': glob.glob(os.path.join(sleap_dir, '*analysis*')),
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
        ks_dir = glob.glob(os.path.join(self.path, 'output', '*'))
        ks_probe_dir_list = [d for d in ks_dir if os.path.isdir(d)]

        if len(ks_probe_dir_list) == 0:
            warnings.warn('No probe directories found in the output folder.')
            return []

        shank_dirs = np.array([glob.glob(os.path.join(probe_dir, '*shank*', 'kilosort4')) for probe_dir in ks_probe_dir_list], dtype=object)
        probe_kilosort_dirs = np.array([glob.glob(os.path.join(probe_dir, 'kilosort4')) for probe_dir in ks_probe_dir_list], dtype=object)

        if len(shank_dirs.flatten()) > 0:
            self.sorting = 'shank'
            self.kilosort_probes = np.unique([_parse_kilosort_dir(d)[0] for d in shank_dirs.flatten().tolist()])
            return shank_dirs.flatten().tolist()

        elif len(probe_kilosort_dirs.flatten()) > 0:
            self.sorting = 'probe'
            self.kilosort_probes = np.unique([_parse_kilosort_dir(d)[0] for d in probe_kilosort_dirs.flatten().tolist()])
            return probe_kilosort_dirs.flatten().tolist()
    
        else: 
            raise ValueError('No kilosort directories found in the output folder.')
        

####### LOADERS #######

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
        probe_name, shank_name = _parse_kilosort_dir(kilosort_file)

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

    def load_centroid_timestamps(self, file_path: str) -> pd.DataFrame:
        print(f'loading centroid timestamps from {file_path}') if self.verbose else None
        centroid_df = pd.read_csv(file_path, header=None)
        if centroid_df.shape[1] == 1:
            centroid_df.columns = ['time']
            centroid_df.set_index('time', inplace=True)
        elif centroid_df.shape[1] == 3:
            centroid_df.columns = ['time','bonsai_centroid.x','bonsai_centroid.y']
            centroid_df.set_index('time', inplace=True)
            self.centroid_has_xy = True
        else:
            raise ValueError("centroid_timestamps needs to be either 1d where it's time, or 3d where its time and centroid")
        print(f'loaded centroid timestamps: {file_path}') if self.verbose else None
        return centroid_df

    def read_h5(self, pose_file):
        with h5py.File(pose_file, "r") as f:
            locations = f["tracks"][:].T
            node_names = [n.decode() for n in f["node_names"][:]]
            point_scores = f["point_scores"][:].T

            if locations.shape[-1] != 1: 
                locations = np.nansum(locations,axis=-1)[..., np.newaxis]
                point_scores = np.nansum(point_scores,axis=-1)[..., np.newaxis]
                
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
    

###### BUILD PROCESSING PLAN #######
    def _expected(self, key: str) -> bool:
            """Convenience accessor for config expectations."""
            return bool(self.config.get("config_data_exists", {}).get(key, False))    

    def build_processing_plan(self) -> ProcessingPlan:
        # Raw loaded lists
        sleap = self.loaded_data.get("sleap", [])
        centroid = self.loaded_data.get("centroid", [])
        bno = self.loaded_data.get("bno", [])
        hs = self.loaded_data.get("headstage", [])
        kilosort = self.loaded_data.get("kilosort", [])
        centroid_has_xy = bool(getattr(self, "centroid_has_xy", False))

        pose_enabled = False
        pose_source = None
        pose_pairs: List[Tuple[Any, Any]] = []

        if len(sleap) > 0:
            # hard requirement: sleap and centroid must match count
            if len(centroid) == 0:
                raise ValueError("Pose: SLEAP present but centroid timestamps missing.")
            if len(sleap) != len(centroid):
                raise ValueError(f"Pose: sleap vs centroid mismatch {len(sleap)} vs {len(centroid)}.")
            pose_enabled = True
            pose_source = "sleap"
            pose_pairs = list(zip(sleap, centroid))

        elif len(centroid) > 0 and centroid_has_xy:
            # centroid acts as both pose + timestamps; still require one centroid per recording
            pose_enabled = True
            pose_source = "centroid_xy"
            pose_pairs = [(c, c) for c in centroid]

        else:
            # no pose processing at all
            pose_enabled = False
            pose_source = None
            pose_pairs = []

        # if "expected pose" flag, enforce 
        if self._expected("sleap") and not pose_enabled:
            raise ValueError("Config expects pose, but neither SLEAP nor centroid XY is available.")

        # ---------- BNO PLAN ----------
        # BNO requires both bno and headstage; lengths must match; otherwise stop.
        bno_enabled = False
        bno_pairs: List[Tuple[Any, Any]] = []

        if self._expected("bno"):
            if len(bno) == 0 or len(hs) == 0:
                raise ValueError(f"BNO expected but missing (bno={len(bno)}, headstage={len(hs)}).")
            if len(bno) != len(hs):
                raise ValueError(f"BNO vs headstage mismatch {len(bno)} vs {len(hs)}.")
            bno_enabled = True
            bno_pairs = list(zip(bno, hs))
        else:
            if len(bno) and len(hs):
                if len(bno) != len(hs):
                    raise ValueError(f"BNO vs headstage mismatch {len(bno)} vs {len(hs)}.")
                bno_enabled = True
                bno_pairs = list(zip(bno, hs))

        neural_enabled = False
        neural_items: List[Any] = []
        valid_kilosort = [ks for ks in kilosort if ks is not None]

        if self._expected("neural"):
            if len(valid_kilosort) == 0:
                raise ValueError("Neural expected but no valid kilosort datasets were loaded.")
            neural_enabled = True
            neural_items = valid_kilosort
        else:
            if len(valid_kilosort) > 0:
                neural_enabled = True
                neural_items = valid_kilosort

        self.processing_plan = ProcessingPlan(
            pose=PosePlan(enabled=pose_enabled, source=pose_source, pairs=pose_pairs),
            bno=BnoPlan(enabled=bno_enabled, pairs=bno_pairs),
            neural=NeuralPlan(enabled=neural_enabled, items=neural_items),
        )
        return self.processing_plan   

from dataclasses import dataclass

@dataclass
class DataProcessor:
    loader: DataLoader
    verbose: bool = True
    save_raw: bool = True

    def __post_init__(self):
        self.verbose = self.loader.verbose
        self.save_raw = self.loader.config.get('save_raw', True)

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
                ex.submit(_safe_call, t.tag, t.idx, t.fn, *t.args)
                for t in tasks
            ]

            for fut in as_completed(futures):
                tag, idx, result, err = fut.result()
                if err is not None:
                    errors.append(err)
                else:
                    out[tag][idx] = result
        
        self.processed_data = out
        self.errors = errors

        print('=============== Data loading summary ===============') if self.verbose else None
        self.data_exists = {}
        for data in list(self.processed_data.keys()):
            if len(self.processed_data[data]) == 0:
                print(f'No data loaded for {data}!') if self.verbose else None
                self.data_exists[data] = False
            else:
                print(f'Loaded {len(self.processed_data[data])} items for {data}.') if self.verbose else None
                self.data_exists[data] = True
        print('====================================================') if self.verbose else None
        
        main_dataframe = self.join_all_data(out)

        # postprocess 
        final_dataframe = self.postprocess_dataframe(main_dataframe)
        final_parquet_path = os.path.join(self.loader.output_path, f'final_df_{self.loader.recording_id}.parquet')
        print(f'saving final dataframe to parquet at \n {final_parquet_path}') if self.verbose else None
        final_dataframe.to_parquet(final_parquet_path)

        return final_dataframe, errors

# -------------------------------------------------------------------------------------

    def build_tasks(self) -> Tuple[List[TaskSpec], dict]:
        plan = getattr(self.loader, "processing_plan", None)
        if plan is None:
            raise RuntimeError("Loader has no processing_plan. Call loader.build_processing_plan() after loading.")

        tasks: List[TaskSpec] = []
        out = {}

        if plan.pose.enabled:
            out["pose"] = [None] * len(plan.pose.pairs)
            for i, (pose_df, ts_df) in enumerate(plan.pose.pairs):
                tasks.append(TaskSpec("pose", i, self.process_pose_data, (pose_df, ts_df, plan.pose.source, i)))
        else:
            out["pose"] = []

        if plan.bno.enabled:
            out["bno"] = [None] * len(plan.bno.pairs)
            for i, (bno_df, hs_df) in enumerate(plan.bno.pairs):
                tasks.append(TaskSpec("bno", i, self.process_bno_data, (bno_df, hs_df, i)))
        else:
            out["bno"] = []

        if plan.neural.enabled:
            out["neural"] = [None] * len(plan.neural.items)
            for i, ks in enumerate(plan.neural.items):
                tasks.append(TaskSpec("neural", i, self.process_neural_data, (ks, i)))
        else:
            out["neural"] = []

        return tasks, out
    
    def join_all_data(self, loaded_data):
        plan = getattr(self.loader, "processing_plan", None)
        if plan is None:
            raise RuntimeError("Loader has no processing_plan. Call loader.build_processing_plan() after loading.")

        main_dataframe = pd.DataFrame(index=self.timeline)

        if plan.neural.enabled:
            print("joining neural data") if self.verbose else None
            neural_and_labels = loaded_data.get("neural", [])
            neural_list = [row[0] for row in neural_and_labels]
            neural_df = pd.concat(neural_list, axis=1)
            main_dataframe = main_dataframe.join(neural_df, how="left")
            main_dataframe = main_dataframe.fillna(0).astype(np.uint16)

            # save labels
            label_list = [row[1] for row in neural_and_labels]
            labels_df =pd.concat(label_list,axis=0)
            label_output_path = os.path.join(self.loader.output_path, f'neural_labels_{self.loader.recording_id}.parquet')
            labels_df.to_parquet(label_output_path)
            print(f'saved neural labels to parquet at \n {label_output_path}') if self.verbose else None
            
        if plan.pose.enabled:
            print("joining pose data") if self.verbose else None
            pose_list = loaded_data.get("pose", [])
            pose_df = pd.concat(pose_list, axis=0)
            main_dataframe = main_dataframe.join(pose_df, how="left")

        if plan.bno.enabled:
            print("joining bno data") if self.verbose else None
            bno_list = loaded_data.get("bno", [])
            bno_df = pd.concat(bno_list, axis=0)
            main_dataframe = main_dataframe.join(bno_df, how="left")

        return main_dataframe

    def add_existence_columns(self, df):
        plan = getattr(self.loader, "processing_plan", None)

        existence_cols = []

        if plan.neural.enabled:
            probe_bounds = np.array(self.all_timestamps['npx'][1:3])

            for i, probe in enumerate(self.loader.npx_probes):
                col = f"meta_neural_{probe}_exists"
                existence_cols.append(col)
                df[col] = 0
                for r in range(probe_bounds.shape[1]):
                    start_idx = np.searchsorted(self.timeline, probe_bounds[0, r, i], side='left')
                    end_idx = np.searchsorted(self.timeline, probe_bounds[1, r, i], side='right')
                    df.iloc[start_idx:end_idx, df.columns.get_loc(col)] = 1
                
        if plan.pose.enabled:
            pose_bounds = np.array(self.all_timestamps['centroid'][1:3])
            col = 'meta_pose_exists'
            existence_cols.append(col)
            df[col] = 0
            for r in range(pose_bounds.shape[1]):
                start_idx = np.searchsorted(self.timeline, pose_bounds[0, r], side='left')
                end_idx = np.searchsorted(self.timeline, pose_bounds[1, r], side='right')
                df.iloc[start_idx:end_idx, df.columns.get_loc(col)] = 1

        if plan.bno.enabled:
            bno_bounds = np.array(self.all_timestamps['headstage'][1:3])
            col = 'meta_bno_exists'
            df[col] = 0
            existence_cols.append(col)
            for r in range(bno_bounds.shape[1]):
                start_idx = np.searchsorted(self.timeline, bno_bounds[0, r], side='left')
                end_idx = np.searchsorted(self.timeline, bno_bounds[1, r], side='right')
                df.iloc[start_idx:end_idx, df.columns.get_loc(col)] = 1

        df['meta_all_data_exists'] = df[existence_cols].sum(axis=1) == len(existence_cols)
        existence_cols.append('meta_all_data_exists')

        df[existence_cols] = df[existence_cols].astype(np.uint8)

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
        df.attrs['recording_id'] = self.loader.recording_id
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
        Determine global min/max time bounds using only modalities enabled by the loader plan.
        """
        plan = getattr(self.loader, "processing_plan", None)
        if plan is None:
            raise RuntimeError("Loader has no processing_plan; run loader.build_processing_plan() after loading.")

        bounds_start = []
        bounds_end = []

        out = {}

        if plan.neural.enabled:
            npx_times, npx_starts, npx_ends = self.get_npx_time()
            self.npx_timestamps = npx_times
            out["npx"] = (npx_times, npx_starts, npx_ends)

            bounds_start.append(np.asarray(npx_starts).ravel())
            bounds_end.append(np.asarray(npx_ends).ravel())

        if plan.pose.enabled:
            centroid_times, centroid_starts, centroid_ends = self.get_df_time(datatype="centroid")
            self.centroid_timestamps = centroid_times
            out["centroid"] = (centroid_times, centroid_starts, centroid_ends)

            bounds_start.append(np.asarray(centroid_starts).ravel())
            bounds_end.append(np.asarray(centroid_ends).ravel())

        if plan.bno.enabled:
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

    def process_pose_data(self, pose, time, source: str, file_index: int):
        print("processing pose data") if self.verbose else None
        datatype = "pose"
    
        df = self.df_w_timestamps(pose, time, datatype)

        if self.save_raw:
            raw_path = os.path.join(self.raw_data_path, f'raw_pose_{file_index}.parquet')
            df.to_parquet(raw_path)
            print(f'saved raw pose data to {raw_path}') if self.verbose else None

        skip_frames = self.loader.config.get('skip_frames', None)
        if skip_frames:
            skip_seconds = skip_frames[file_index]
            df = self.skip_initial_seconds(df, skip_seconds)
        
        if self.loader.config.get('clean_pose', True):
            df = self.clean_dataframe(df, self.loader.data_paths['video_files'][file_index], source, plot=self.loader.config.get('plot', True))

        # interp_df = self.extra_beh_features(interp_df, source)

        original_index = df.index
        df = self.interpolate_df_to_timeline(df, self.timeline)
        df = self.assign_closest_frame(df, original_index, datatype)
    
        if source == "sleap":
            df.columns = [f"meta_{col}" if "score" in col else col for col in df.columns]

        df[f'meta_{datatype}_source_file'] = file_index
        print("processed pose data") if self.verbose else None
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

    def df_w_timestamps(self, df, time, datatype):
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
            binned.columns = [f"{probe_name}_cluster_{c}_{labels.iloc[0]['sua_prediction']}_{labels.iloc[0]['noise_prediction']}_shank_{shank_idx}" for c in binned.columns]
        elif 'KSLabel' in labels.columns:
            binned.columns = [f"{probe_name}_cluster_{c}_{labels.iloc[0]['KSLabel']}_shank_{shank_idx}" for c in binned.columns]

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

    def compute_diffs(self, p):
        return np.vstack([[0, 0], np.diff(p, axis=0)])
    
    def group_close_points(self, close_points, max_gap=50):
        """
        Groups consecutive indices if they are within `max_gap` apart.
        Returns a list of index groups.
        """
        if close_points.size == 1:
            return [[close_points, close_points]]
        if len(close_points) == 0:
            return []
        
        groups = [[close_points[0]]]
        for idx in close_points[1:]:
            if idx - groups[-1][-1] <= max_gap:
                groups[-1].append(idx)
            else:
                groups.append([idx])
        return groups
    
    def get_same_diff_groups(self, pos, n_same=20, max_gap = 10, tol = 1e-5):
        diffs = np.diff(pos, axis=0)

        same_diff = np.linalg.norm(np.diff(diffs, axis=0), axis=1) < tol
        same_diff = np.concatenate(([False], same_diff, [False]))

        N = n_same 
        mask = np.zeros_like(same_diff, dtype=bool)

        count = 0
        for i, val in enumerate(same_diff):
            if val:
                count += 1
            else:
                if count >= N:
                    mask[i - count:i] = True
                count = 0
        same_diff = mask

        same_diff_groups = self.group_close_points(np.argwhere(same_diff).squeeze(), max_gap=max_gap)
        return same_diff_groups

    def read_frame(self, cap, frame_idx):
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(frame_idx))
        ret, frame = cap.read()
        if not ret:
            return None
        return cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    
    def plot_groups_w_video_combiner(self, groups, positions, video_path, plotlen=30, max_plots = 10, random=True):
        
        if isinstance(positions, np.ndarray):
            positions = [positions]

        cap = cv2.VideoCapture(video_path)
        if random:
            np.random.shuffle(groups)

        rows, cols = int(np.ceil(max_plots/3)), 3
        fig, axes = plt.subplots(rows, cols, figsize=(20*cols, 20*rows))
        axes = axes.flatten()

        for i, group in enumerate(groups):
            if i == max_plots:
                break
            
            start = max(0, group[0] - plotlen)
            end = min(len(positions[0]), group[-1] + plotlen)

            start_frame = self.read_frame(cap, start)
            end_frame   = self.read_frame(cap, end)

            axes[i].imshow(start_frame)
            axes[i].imshow(end_frame, alpha=0.5)   
            for position in positions:
                axes[i].plot(*position[start:end].T, lw=1)
            axes[i].axis("off")
        
        fig.savefig(os.path.join(self.loader.output_path, 'pose_cleaner_QC.png'))
        plt.close(fig)
        cap.release()

    def corner_row_mask_df(self, df, x_suffix=".x", y_suffix=".y"):
        # collect x/y columns in matching order
        x_cols = [c for c in df.columns if c.endswith(x_suffix)]
        y_cols = [c for c in df.columns if c.endswith(y_suffix)]

        # ensure we only keep pairs that exist in both
        x_base = {c[:-len(x_suffix)] for c in x_cols}
        y_base = {c[:-len(y_suffix)] for c in y_cols}
        bases = sorted(x_base & y_base)

        x_cols = [b + x_suffix for b in bases]
        y_cols = [b + y_suffix for b in bases]

        X = df[x_cols].to_numpy(dtype=float) 
        Y = df[y_cols].to_numpy(dtype=float)

        left   = 200
        right  = 2100
        bottom = 200
        top    = 1800

        in_corner = (
            ((X < left)  & (Y < bottom)) |
            ((X < left)  & (Y > top))    |
            ((X > right) & (Y < bottom)) |
            ((X > right) & (Y > top))
        )  

        return in_corner.any(axis=1)

    def get_all_diffs_df(self, df, keypoints):
        diffs = df.copy()
        for kp in keypoints:
            pos_kp = get_columns(df, kp, 'contains', exclude='score')
            new_df = (pos_kp.bfill()
                        .diff()
                        .mask(pos_kp.shift().isna())
                        .ffill()
                    )
            diffs[f'{kp}.diff'] = np.linalg.norm(new_df.values,axis=1)
        return diffs
    
    def get_jumps_and_unsmooth(self, full_df, keypoints, smooth_window = 3, max_idx_gap = 10, max_idx_gap_smooth = 5, jump_threshold=200, smooth_diff_threshold=25):
        
        jump_groups_dict = {}
        smooth_jump_groups_dict = {}

        for kp_n, kp in enumerate(keypoints):
            # find jumps, write them to dict and set a column for them
            jumps = full_df[f'{kp}.diff'] > jump_threshold 
            jump_groups = self.group_close_points(np.where(jumps.values)[0], max_gap=max_idx_gap)
            jump_groups_dict[kp] = jump_groups
            jumps = [idx for group in jump_groups for idx in group]
            full_df[f'{kp}.jumps'] = 0
            full_df.iloc[jumps, full_df.columns.get_loc(f'{kp}.jumps')] = 1

            # remove the jumps and interpolate them
            full_df[[f'{kp}_clean.x', f'{kp}_clean.y']] = full_df[[f'{kp}.x', f'{kp}.y']].copy().mask(full_df[f'{kp}.jumps'] == 1) 
            full_df[[f'{kp}_clean.x', f'{kp}_clean.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].interpolate(method='time', limit_direction='both')

            # now compare smoothness and normal, find where they differ significantly, write to dict and set column
            full_df[[f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].copy().rolling(window=smooth_window, win_type='gaussian', center=True).mean(std=3)
            diff_between_normal_and_smooth = np.linalg.norm(full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].values - full_df[[f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y']].values, axis=1)
            full_df[f'{kp}_clean_smooth.diff'] = diff_between_normal_and_smooth
            smooth_jumps = full_df[f'{kp}_clean_smooth.diff'] > smooth_diff_threshold
            smooth_jump_groups = self.group_close_points(np.where(smooth_jumps.values)[0], max_gap=max_idx_gap_smooth)
            smooth_jump_groups_dict[kp] = smooth_jump_groups
            smooth_jumps = [idx for group in smooth_jump_groups for idx in group]
            full_df[f'{kp}_clean_smooth_jump'] = 0
            full_df.iloc[smooth_jumps, full_df.columns.get_loc(f'{kp}_clean_smooth_jump')] = 1

            # remove the smooth jumps and interpolate them
            full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].copy().mask(full_df[f'{kp}_clean_smooth_jump'] == 1)
            full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']] = full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']].interpolate(method='time', limit_direction='both')

            # remove all the intermediate stuff, rename the clean columns to the original names + clean
            full_df.drop(columns=[f'{kp}.diff', f'{kp}.jumps', f'{kp}_clean.x', f'{kp}_clean.y', f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y', f'{kp}_clean_smooth.diff', f'{kp}_clean_smooth_jump'], inplace=True)
            full_df.columns = [c.replace(f'{kp}_clean2', f'{kp}_clean') for c in full_df.columns]
            
        return full_df, jump_groups_dict, smooth_jump_groups_dict

    def plot_orig_clean_and_jumps(self, init_kp, clean_kp, jump_groups_dict, smooth_jump_groups_dict, plot_kp):
        fig, ax = plt.subplots(1,3, figsize=(30,10))
        ax[0].plot(*init_kp.T,lw=.3)
        ax[0].set_title("Original")
        ax[1].plot(*clean_kp.T,lw=.3)
        ax[1].set_title("Cleaned")
        # plot the nans_in_plot_kp as separate segments instead of all connected
        for group in smooth_jump_groups_dict[plot_kp]:
            ax[2].plot(*init_kp[group].T, lw = .3)
        for group in jump_groups_dict[plot_kp]:
            ax[2].plot(*init_kp[group].T, lw = .3)
        ax[2].set_title("stuf taken out")
        fig.savefig(os.path.join(self.loader.output_path, 'pose_cleaner_QC_jumps.png'))
        plt.close(fig)

    def clean_dataframe(self, df, video_path, source, plot):
        # check if sleap exists or not, otherwise just 
        
        if source == 'sleap':
            base_keypoint = 'pose_tailstart1'
            sleap_pose = get_columns(df, '','contains', exclude=['score','centroid']).copy() 
            # we exclude cleaning the centroid if we have sleap because the "corners" are different; margin should be 200 for the sleap since its based on the video directly, whereas for the centroid it should be 10 or so. Should find a way to still clean centroid
            if sleap_pose.empty:
                print('no sleap pose columns found, skipping cleaning') if self.verbose else None
                return df, None
            
        elif source == 'centroid_xy':
            base_keypoint = 'pose_bonsai_centroid'
            margin = 10
            sleap_pose = df.copy()

        else: 
            raise ValueError(f'unknown source {source} for cleaning')
        
        xy_cols = sleap_pose.columns
        keypoints = np.unique([c[:-2] for c in xy_cols])
        
        nans0 = self.corner_row_mask_df(sleap_pose)

        df_clean = df.copy()
        df_clean.loc[nans0, xy_cols] = np.nan 
        
        full_df = self.get_all_diffs_df(df_clean, keypoints)
        full_df, jump_groups_dict, smooth_jump_groups_dict = self.get_jumps_and_unsmooth(full_df, keypoints, jump_threshold=200, smooth_diff_threshold=30)

        init_kp = get_columns(sleap_pose, base_keypoint, 'contains', exclude='score').values
        clean_kp = get_columns(full_df, base_keypoint+'_clean', 'contains', exclude='score').values

        big_jumps = jump_groups_dict[base_keypoint]
        smooth_jumps = smooth_jump_groups_dict[base_keypoint]
        
        if plot and sum(len(g) for g in big_jumps + smooth_jumps) > 15:
            self.plot_groups_w_video_combiner(big_jumps + smooth_jumps, [init_kp, clean_kp], video_path, plotlen=15, max_plots=9, random=True) 
            self.plot_orig_clean_and_jumps(init_kp, clean_kp, jump_groups_dict, smooth_jump_groups_dict, base_keypoint)
        else:
            print('data was clean, no bad stuff to plot :) ') if self.verbose else None
        return full_df


    
def main():
    parser = argparse.ArgumentParser(description="Process Neuropixels recording.")
    parser.add_argument("path", help="Path to recording directory")

    parser.add_argument(
        "--save-raw",
        type=str2bool,
        default=True,
        help="Whether to save raw pose/bno/spike parquet files (default: True). "
             "Use --save-raw false to disable.",
    )

    parser.add_argument(
        '--verbose',
        type=str2bool,
        default=True,
        help="Whether to print verbose logs during processing (default: True). "
             "Use --verbose false to disable.",
    )

    parser.add_argument(
        '--freq',
        type=str,
        default='10ms',
        help="Frequency string for main timeline (default: '10ms'). "
             "E.g., '100ms', '1s', '500ms', etc.",
    )

    parser.add_argument(
        '--plot',
        type=str2bool,
        default=True,
        help="Whether to generate plots during processing (default: True). "
             "Use --plot false to disable.",
    )

    parser.add_argument(
        '--clean_pose',
        type=str2bool,
        default=True,
        help="Whether to clean pose data during processing (default: True). Only works for sleap pose."
             "Use --clean_pose false to disable.",
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=16,
        help="Number of parallel workers to use for loading and processing (default: 16).",
    )

    parser.add_argument(
        '--skip_frames',
        type=str,
        default=None,
        help='list of how many seconds to skip at the start of each recording segment, in the format "[10, 5, 0]" (default: None).',
    )

    args = parser.parse_args()
    path = normalize_path(args.path)
        
    config_id = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_path = os.path.join(path, "processed_data") + '-' + config_id
    args.output_path = output_path
    os.makedirs(output_path, exist_ok=True)

    recording = os.path.basename(os.path.normpath(path))
    print(f"Processing recording {recording} at path: {path}")
    log_file = os.path.join(args.output_path, f"process_{recording}.log")
    logger = setup_logger(log_file, args.verbose)
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)

    logger.info("Starting processing")
    logger.info(f"OS: {platform.platform()}")
    logger.info(f"Input path: {args.path}")
    logger.info(f"Normalized path: {path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Save raw: {args.save_raw}")
    logger.info(f"Verbose: {args.verbose}")
    logger.info(f"Frequency: {args.freq}")
    logger.info(f"Plot: {args.plot}")

    config = vars(args)
    loader = DataLoader(config)
    loaded_data, load_errors = loader.load_all_data(max_workers=args.workers)
    processor = DataProcessor(loader)
    final_dataframe, process_errors = processor.process_all_data(max_workers=args.workers)
    logger.info("Processing complete")
    logger.info(f"Total load errors: {len(load_errors)}")
    logger.info(f"Total process errors: {len(process_errors)}")

    logger.info("All tasks completed")

    # Ensure output directory and all contents are read/write on any platform
    # make_writable(args.output_path)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Unhandled exception")
        raise
