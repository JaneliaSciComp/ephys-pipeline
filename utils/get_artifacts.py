import numpy as np
from typing import List, Optional

from spikeinterface.preprocessing.basepreprocessor import BasePreprocessor, BasePreprocessorSegment
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


def merge_artifact_logs(log_dir, fs: float, output_path, n_segments: int = 1) -> None:
    """Merge per-chunk artifact logs written by SaturationArtifactRemover into one JSON.

    Periods are deduplicated and sorted within each segment before saving.
    """
    import json
    from pathlib import Path
    log_dir = Path(log_dir)

    seg_events = [[] for _ in range(n_segments)]
    if log_dir.exists():
        for f in log_dir.glob("seg*_*.json"):
            try:
                seg_idx = int(f.name.split("_", 1)[0].lstrip("seg"))
            except ValueError:
                continue
            d = json.loads(f.read_text())
            for s, e in zip(d.get("starts", []), d.get("ends", [])):
                seg_events[seg_idx].append((int(s), int(e)))

    list_periods = []
    for events in seg_events:
        events = sorted(set(events))
        flat = []
        for s, e in events:
            flat.extend([s, e])
        list_periods.append(flat)

    save_artifact_periods(list_periods, fs, output_path)


def save_artifact_periods(list_periods: List[List[int]], fs: float, output_path) -> None:
    """Save detected artifact periods as JSON with frame indices and seconds."""
    import json
    from pathlib import Path

    segments = []
    for periods in list_periods:
        events = []
        for i in range(0, len(periods), 2):
            start_frame = int(periods[i])
            end_frame   = int(periods[i + 1])
            events.append({
                "start_frame":  start_frame,
                "end_frame":    end_frame,
                "start_sec":    round(start_frame / fs, 4),
                "end_sec":      round(end_frame   / fs, 4),
                "duration_sec": round((end_frame - start_frame) / fs, 4),
            })
        segments.append(events)

    n_total   = sum(len(s) for s in segments)
    dur_total = sum(e["duration_sec"] for s in segments for e in s)

    Path(output_path).write_text(json.dumps({
        "n_artifacts":        n_total,
        "total_duration_sec": round(dur_total, 4),
        "segments":           segments,
    }, indent=2))
    print(f"  {n_total} artifact periods ({dur_total:.2f}s total) → {output_path}")


def remove_saturation_artifacts(
    recording,
    list_periods: List[List[int]],
    ms_before: float = 10.0,
    ms_after: float = 10.0,
    mode: str = "zeros",
):
    """
    Zero out saturation periods in a recording, including pre/post padding.

    Each detected period [start, end] is fully blanked by generating trigger
    points at overlapping intervals throughout the period, so the entire
    event is zeroed regardless of duration. Uses si.remove_artifacts internally
    and is compatible with all spikeinterface versions.

    Parameters
    ----------
    recording : RecordingExtractor
        The recording to blank.
    list_periods : list of list of int
        Output of detect_saturation_periods: one list per segment with
        interleaved [start1, end1, start2, end2, ...] frame indices.
    ms_before : float, default 10.0
        Milliseconds of padding to zero before each saturation start (ramp-up).
    ms_after : float, default 10.0
        Milliseconds of padding to zero after each saturation end (recovery).

    Returns
    -------
    RecordingExtractor
        A lazy RecordingExtractor with saturation periods zeroed out.
    """
    import spikeinterface.full as si

    fs = recording.get_sampling_frequency()
    window_samples = int((ms_before + ms_after) * fs / 1000)
    # Step slightly less than the window so adjacent triggers always overlap
    step_samples = max(1, window_samples - 1)

    dense_triggers = []
    for seg in list_periods:
        triggers = []
        for i in range(0, len(seg), 2):
            start, end = seg[i], seg[i + 1]
            # Anchor at start then step through the period until end is covered
            points = list(range(start, end, step_samples))
            if not points or points[-1] < end:
                points.append(end)
            triggers.extend(points)
        dense_triggers.append(triggers)

    return si.remove_artifacts(
        recording,
        list_triggers=dense_triggers,
        ms_before=ms_before,
        ms_after=ms_after,
        mode=mode,
    )


class _SaturationArtifactRemoverSegment(BasePreprocessorSegment):
    def __init__(self, parent_recording_segment,
                 abs_threshold, direction, ms_before_s, ms_after_s,
                 mode, margin_samples, log_dir=None, seg_idx=0):
        super().__init__(parent_recording_segment)
        self.abs_threshold = abs_threshold
        self.direction = direction
        self.ms_before_s = ms_before_s
        self.ms_after_s = ms_after_s
        self.mode = mode
        self.margin_samples = margin_samples
        self.log_dir = log_dir
        self.seg_idx = seg_idx

    def get_traces(self, start_frame, end_frame, channel_indices):
        n_total = self.get_num_samples()
        fetch_start = max(0, start_frame - self.margin_samples)
        fetch_end = min(n_total, end_frame + self.margin_samples)

        # Always fetch all channels so detection covers the full probe
        traces = self.parent_recording_segment.get_traces(
            fetch_start, fetch_end, slice(None)
        )

        t = self.abs_threshold
        if self.direction == "upper":
            sat = np.any(traces >= t, axis=1)
        elif self.direction == "lower":
            sat = np.any(traces <= -t, axis=1)
        else:
            sat = np.any((traces >= t) | (traces <= -t), axis=1)

        if sat.any():
            if self.log_dir is not None:
                self._log_periods(sat, fetch_start, start_frame, end_frame)
            traces = self._remove(traces, sat)

        offset = start_frame - fetch_start
        out = traces[offset: offset + (end_frame - start_frame)]
        if channel_indices is not None:
            out = out[:, channel_indices]
        return out

    def _log_periods(self, sat, fetch_start, start_frame, end_frame):
        """Write periods that BEGIN within the requested chunk to a per-chunk file.

        Periods starting within the margin (before start_frame or after end_frame)
        are dropped — they'll be logged by the chunk that owns them, avoiding
        duplicate detections at chunk boundaries.
        """
        import json
        from pathlib import Path
        changes = np.diff(sat.astype(np.int8), prepend=0, append=0)
        starts = np.where(changes == 1)[0] + fetch_start
        ends   = np.where(changes == -1)[0] + fetch_start
        keep = (starts >= start_frame) & (starts < end_frame)
        if not keep.any():
            return
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        log_path = Path(self.log_dir) / f"seg{self.seg_idx}_{start_frame:012d}.json"
        log_path.write_text(json.dumps({
            "starts": starts[keep].tolist(),
            "ends":   ends[keep].tolist(),
        }))

    def _remove(self, traces, sat):
        traces = traces.copy()
        n = len(sat)
        changes = np.diff(sat.astype(np.int8), prepend=0, append=0)
        starts = np.where(changes == 1)[0]
        ends = np.where(changes == -1)[0]

        for s, e in zip(starts, ends):
            # Expand the blanking window by ms_before / ms_after
            win_start = max(0, s - self.ms_before_s)
            win_end = min(n, e + self.ms_after_s)

            # Anchor indices: last clean sample before window, first after
            left = win_start - 1
            right = win_end

            if self.mode == "linear":
                if 0 <= left and right < n:
                    n_pts = right - left - 1
                    if n_pts > 0:
                        traces[left + 1: right] = np.linspace(
                            traces[left].astype(float),
                            traces[right].astype(float),
                            n_pts + 2,
                        )[1:-1]
                elif left < 0 and right < n:
                    traces[:right] = traces[right]
                elif 0 <= left and right >= n:
                    traces[left + 1:] = traces[left]
                else:
                    traces[:] = 0
            else:
                traces[win_start:win_end] = 0

        return traces


class SaturationArtifactRemover(BasePreprocessor):
    """Detect and remove saturation artifacts in a single lazy pass.

    Drop-in replacement for the detect_saturation_periods +
    remove_saturation_artifacts two-step. Each chunk is processed with a
    margin on both sides (margin_ms) so artifacts that span chunk
    boundaries are handled correctly.

    Artifacts longer than margin_ms are zeroed rather than interpolated
    (no clean anchor available within the fetched window).
    """

    name = "SaturationArtifactRemover"

    def __init__(self, recording, abs_threshold=1500, direction="upper",
                 ms_before=10.0, ms_after=10.0, mode="linear",
                 margin_ms=500.0, log_dir=None):
        super().__init__(recording)

        fs = recording.get_sampling_frequency()
        ms_before_s = int(ms_before * fs / 1000)
        ms_after_s = int(ms_after * fs / 1000)
        margin_samples = int(margin_ms * fs / 1000)

        self._kwargs = dict(
            recording=recording,
            abs_threshold=abs_threshold,
            direction=direction,
            ms_before=ms_before,
            ms_after=ms_after,
            mode=mode,
            margin_ms=margin_ms,
            log_dir=str(log_dir) if log_dir is not None else None,
        )

        for seg_idx in range(recording.get_num_segments()):
            seg = _SaturationArtifactRemoverSegment(
                recording._recording_segments[seg_idx],
                abs_threshold=abs_threshold,
                direction=direction,
                ms_before_s=ms_before_s,
                ms_after_s=ms_after_s,
                mode=mode,
                margin_samples=margin_samples,
                log_dir=str(log_dir) if log_dir is not None else None,
                seg_idx=seg_idx,
            )
            self.add_recording_segment(seg)

        # Used by SI's saving infrastructure to pre-fetch context
        self._margin = margin_samples
