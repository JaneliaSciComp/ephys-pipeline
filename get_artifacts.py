from __future__ import annotations

import numpy as np
from typing import List, Optional

try:
    from tqdm.auto import tqdm
    HAVE_TQDM = True
except ImportError:
    HAVE_TQDM = False

from spikeinterface.core.job_tools import (
    fix_job_kwargs,
    ensure_chunk_size,
    ensure_n_jobs,
    ChunkRecordingExecutor,
    divide_recording_into_chunks,
)


def detect_saturation_periods(
    recording,
    abs_threshold: float,
    direction: str = "both",
    min_duration_ms: float = 1.0,
    **job_kwargs,
) -> List[List[int]]:
    """
    Detect saturation artifact frame indices in a recording.
    
    Finds frames where any channel exceeds a saturation threshold. Since different
    channels may saturate at different times, this function identifies all frames
    where at least one channel is saturated.
    
    Parameters
    ----------
    recording : RecordingExtractor
        The recording extractor to detect saturation artifacts in
    abs_threshold : float
        Absolute threshold value for saturation detection. Values above (or below,
        depending on direction) this threshold are considered saturated.
    direction : "upper" | "lower" | "both", default: "both"
        Direction of saturation to detect:
        - "upper": Only detect positive saturation (values above threshold)
        - "lower": Only detect negative saturation (values below -threshold)
        - "both": Detect both positive and negative saturation
    min_duration_ms : float, default: 1.0
        Minimum duration in milliseconds for a saturation period to be considered.
        Shorter periods are ignored.
    **job_kwargs
        Additional keyword arguments for parallel processing (n_jobs, chunk_memory, etc.)
    
    Returns
    -------
    list_periods : list of list of int
        One list per segment. Each list contains start and end frame indices
        interleaved: [start1, end1, start2, end2, ...]. end_frame values are exclusive.
    """
    assert direction in ("upper", "lower", "both"), "'direction' must be 'upper', 'lower', or 'both'"
    
    fs = recording.get_sampling_frequency()
    num_segments = recording.get_num_segments()
    min_duration_samples = int(min_duration_ms * fs / 1000.0)
    threshold = abs_threshold
    
    # Process chunks in parallel for speed
    job_kwargs = fix_job_kwargs(job_kwargs)
    n_jobs = ensure_n_jobs(recording, n_jobs=job_kwargs.get("n_jobs", 1))
    process_chunk_size = ensure_chunk_size(
        recording,
        chunk_size=job_kwargs.get("chunk_size", None),
        chunk_memory=job_kwargs.get("chunk_memory", None),
        chunk_duration=job_kwargs.get("chunk_duration", None),
        n_jobs=n_jobs,
    )
    
    progress_bar = job_kwargs.get("progress_bar", False)
    
    # Initialize worker context
    def _init_worker(recording, threshold, direction):
        worker_ctx = {}
        worker_ctx["recording"] = recording
        worker_ctx["threshold"] = threshold
        worker_ctx["direction"] = direction
        return worker_ctx
    
    # Process each chunk to find saturation frames
    def _process_chunk(segment_index, start_frame, end_frame, worker_ctx):
        recording = worker_ctx["recording"]
        threshold = worker_ctx["threshold"]
        direction = worker_ctx["direction"]
        
        # Get traces for this chunk
        traces = recording.get_traces(
            segment_index=segment_index,
            start_frame=start_frame,
            end_frame=end_frame,
        )
        
        # Directly check if ANY channel exceeds threshold at each timestep (vectorized, no median needed)
        if direction == "upper":
            saturated = np.any(traces >= threshold, axis=1)
        elif direction == "lower":
            saturated = np.any(traces <= -threshold, axis=1)
        else:  # both
            saturated = np.any((traces >= threshold) | (traces <= -threshold), axis=1)
        
        # Convert to absolute frame indices (only where saturated is True)
        saturated_frames = np.where(saturated)[0] + start_frame
        
        return segment_index, saturated_frames
    
    # Run processing with parallelization
    init_func = _init_worker
    init_args = (recording, threshold, direction)
    func = _process_chunk
    
    executor = ChunkRecordingExecutor(
        recording,
        func,
        init_func,
        init_args,
        handle_returns=True,
        progress_bar=progress_bar,
        verbose=job_kwargs.get("verbose", False),
        job_name="Detecting saturation",
        n_jobs=n_jobs,
        chunk_size=process_chunk_size,
        mp_context=job_kwargs.get("mp_context", None),
    )
    
    results = executor.run()
    
    # Combine results from all chunks, grouped by segment (use arrays for efficiency)
    all_saturated_frames_by_segment = [[] for _ in range(num_segments)]
    
    for segment_index, saturated_frames in results:
        if len(saturated_frames) > 0:
            all_saturated_frames_by_segment[segment_index].append(saturated_frames)
    
    # Find contiguous periods for each segment using breaks method
    list_periods = []
    for seg_index in range(num_segments):
        if len(all_saturated_frames_by_segment[seg_index]) == 0:
            list_periods.append([])
            continue
        
        # Concatenate all frames from chunks, then sort and get unique (more efficient)
        if len(all_saturated_frames_by_segment[seg_index]) == 1:
            saturated_frames = np.unique(all_saturated_frames_by_segment[seg_index][0])
        else:
            all_frames = np.concatenate(all_saturated_frames_by_segment[seg_index])
            saturated_frames = np.unique(all_frames)
        
        if len(saturated_frames) == 0:
            list_periods.append([])
            continue
        
        # Find breaks where difference is not 1 (gaps in sequence)
        breaks = np.where(np.diff(saturated_frames) != 1)[0]
        
        # Get start and end frame values directly (avoid multiple concatenations)
        if len(breaks) == 0:
            # Single contiguous period
            start_frame = saturated_frames[0]
            end_frame = saturated_frames[-1] + 1
            if (end_frame - start_frame) >= min_duration_samples:
                list_periods.append([start_frame, end_frame])
            else:
                list_periods.append([])
        else:
            # Multiple periods - use vectorized operations
            start_frames = np.concatenate([[saturated_frames[0]], saturated_frames[breaks + 1]])
            end_frames = np.concatenate([saturated_frames[breaks], [saturated_frames[-1]]]) + 1
            
            # Filter by minimum duration and flatten in one step
            durations = end_frames - start_frames
            valid_mask = durations >= min_duration_samples
            if np.any(valid_mask):
                valid_starts = start_frames[valid_mask]
                valid_ends = end_frames[valid_mask]
                # Interleave starts and ends
                periods = np.empty(len(valid_starts) * 2, dtype=np.int64)
                periods[0::2] = valid_starts
                periods[1::2] = valid_ends
                list_periods.append(periods.tolist())
            else:
                list_periods.append([])
    
    return list_periods
