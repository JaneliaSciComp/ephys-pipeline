"""per-rec diagnostic figures for the combiner, so anyone can eyeball that a combined day's timing looks right

ac had claude make this based on her ipynb combiner_figs.ipynb that made them all to start for a given rec

figs are extracted from the combiner_figs.ipynb prototype. the plot bodies are the notebook cells kept verbatim, the
only shared setup (bridge, cam1, analog, drift, xcorr) is pulled once into build_fig_context so each fig fn
stays thin. save_all_figs writes one png per figure plus a combined per-rec pdf into a combiner_figs subdir
next to the final_df parquet. reuses the combiner DataLoader and, when the combiner passes its precomputed
pulses, the already-detected strobe pulses so we do not re-read the many-gb analog file
"""

import os
import traceback
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy.signal import correlate, correlation_lags
from types import SimpleNamespace


# ordered (page number for the filename, human readable name, fig fn name, analog only).
# the number is a sequential 01..14 for the pdf page order
# the plot titles inside each fn keep the notebook's labels.
# analog only is True for figures that need the analog strobe, so they are skipped on no-analog days.
FIGURE_LIST = [
    ("01", "onix_poll_jitter", "fig_onix_poll_jitter", False),
    ("02", "cam_host_jitter", "fig_cam_host_jitter", False),
    ("03", "cam_onix_jitter", "fig_cam_onix_jitter", False),
    ("04", "analog_strobe_and_edges", "fig_analog_strobe", True),
    ("05", "first_frame_spacings", "fig_first_frame_spacings", False),
    ("06", "analog_pulse_rate", "fig_analog_pulse_rate", True),
    ("07", "timing_source_lineup", "fig_timing_source_lineup", False),
    ("08", "host_drift_bridge_correction", "fig_host_drift", False), # still runs just fewer subplots when no analog
    ("09", "xcorr_camera_vs_bno_accel", "fig_xcorr", False),
    ("10", "timing_summary_table", "fig_summary_table", False),
    ("11", "keypoint_position_over_time", "fig_position_over_time", False),
    ("12", "keypoint_position_zoom", "fig_position_zoom", False),
    ("13", "keypoint_position_2d_scatter", "fig_position_2d_scatter", False),
    ("14", "keypoint_position_2d_line", "fig_position_2d_line", False),
]


def _rec_mask(final_df, rec_idx):
    # boolean mask of the rows belonging to one recording, since the combiner puts all recs of a day in one
    # final_df on one timeline. prefer the pose source col, fall back to centroid/bno, and to the _{source}
    # suffixed name on multi model days
    for base in ("meta_pose_source_file", "meta_centroid_source_file", "meta_bno_source_file"):
        if base in final_df.columns:
            return final_df[base] == rec_idx
        suffixed = [c for c in final_df.columns if c.startswith(base + "_")]
        if suffixed:
            return final_df[suffixed[0]] == rec_idx
    # no meta source col at all (single rec day) use the whole frame
    return pd.Series(True, index=final_df.index)


def build_fig_context(loader, rec_idx, final_df, precomputed_pulses=None, combiner_log_path=None):
    """pull every raw source and shared derived quantity a rec's figures need into one namespace

    loader is the combiner DataLoader, final_df the combined dataframe (all recs of the day), precomputed_pulses
    the optional {rec: {...}} the combiner stashes so the strobe pulses are not re-detected, combiner_log_path
    the optional combiner.log to read the applied offset and warnings from for the summary table
    """
    # careful with circular imports here, try/except matches the flat/package import idiom the combiner uses
    try:
        from .combiner_pipeline import (
            HEARTBEAT_CLOCK_COL, STROBE_CHANNEL, N_ANALOG_CHANNELS, STROBE_THRESHOLD)
    except ImportError:
        from combiner_pipeline import (  # type: ignore  # flat path for script-run, pylance only sees the package path
            HEARTBEAT_CLOCK_COL, STROBE_CHANNEL, N_ANALOG_CHANNELS, STROBE_THRESHOLD)

    ctx = SimpleNamespace()
    ctx.rec_idx = rec_idx
    ctx.STROBE_CHANNEL = STROBE_CHANNEL
    ctx.N_ANALOG_CHANNELS = N_ANALOG_CHANNELS
    ctx.combiner_log_path = combiner_log_path

    paths = loader.get_all_data_paths()
    ctx.day = getattr(loader, "recording", None) or os.path.basename(str(loader.config["path"]).rstrip("/\\"))

    def check_have_files(key):
        return rec_idx < len(paths.get(key, []))
    ctx.has_analog = check_have_files("analog_voltage_files") and check_have_files("analog_clock_files")

    start_time = loader.load_npx_start_time(paths["npx_start_times"][rec_idx])
    acq_hz = loader.load_acquisition_clock_hz(paths["npx_start_times"][rec_idx])
    ctx.start_time = start_time
    ctx.acq_hz = acq_hz

    # bridge, same precedence as the combiner (heartbeat, else bno+hs, else start-time anchor)
    if check_have_files("heartbeat_files"):
        bridge_name = "heartbeat"
        heartbeat = loader.load_heartbeat(paths["heartbeat_files"][rec_idx])
        bridge_host_ns = heartbeat["host_ts"].astype("int64").to_numpy()
        bridge_onix_ticks = heartbeat[HEARTBEAT_CLOCK_COL].to_numpy().astype(np.float64)
    elif check_have_files("bno_files") and check_have_files("hs_files"):
        bridge_name = "bno_hs"
        bridge_onix_ticks = loader.load_bno_data(paths["bno_files"][rec_idx])["clock"].to_numpy().astype(np.float64)
        bridge_host_ns = pd.to_datetime(loader.load_headstage_data(paths["hs_files"][rec_idx]).index).astype("int64").to_numpy()
    else:
        bridge_name = "none (start-time anchor)"
        bridge_host_ns = None
        bridge_onix_ticks = None
    ctx.bridge_name = bridge_name
    ctx.bridge_host_ns = bridge_host_ns
    ctx.bridge_onix_ticks = bridge_onix_ticks

    # cam1 frame host timestamps, then mapped to onix via the combiner bridge
    cam1 = loader.load_video_timestamps(paths["timestamp_files"][rec_idx])
    cam1_host_ns = pd.to_datetime(cam1.index).astype("int64").to_numpy()
    cam1_onix_ticks = loader.bridge_host_to_onix_ticks(cam1_host_ns.astype(np.float64), rec_idx)
    ctx.cam1_host_ns = cam1_host_ns
    ctx.cam1_onix_ticks = cam1_onix_ticks
    ctx.cam1_onix_diff_ms = (np.diff(cam1_onix_ticks) / acq_hz) * 1000

    # analog first ~7 s chunk for the strobe plot, and the full pulse ticks (reused from precomputed_pulses
    # when the combiner passes it, else detected here which is the slow ~89 gb read)
    cached = precomputed_pulses.get(rec_idx) if precomputed_pulses else None
    # the applied frame->pulse offset straight from the combiner when it passed precomputed_pulses, so the
    # summary table does not have to parse it back out of the log. None when running standalone
    ctx.applied_offset = cached.get("pulse_offset") if cached else None
    if ctx.has_analog:
        seconds_to_show = 7
        analog_clock = np.memmap(paths["analog_clock_files"][rec_idx], dtype=np.uint64, mode="r")
        analog_voltage = np.memmap(paths["analog_voltage_files"][rec_idx], dtype=np.float32, mode="r")
        sample_dt_ticks = float(analog_clock[1] - analog_clock[0])
        sample_rate_hz = acq_hz / sample_dt_ticks
        n_samples = int(seconds_to_show * sample_rate_hz)
        onix_ticks_chunk = np.asarray(analog_clock[:n_samples], dtype=np.float64)
        analog_v_chunk = np.asarray(analog_voltage[:n_samples * N_ANALOG_CHANNELS]).reshape(-1, N_ANALOG_CHANNELS)[:, STROBE_CHANNEL]
        analog_onix_s = (onix_ticks_chunk - onix_ticks_chunk[0]) / acq_hz
        high = analog_v_chunk > STROBE_THRESHOLD
        rising = np.flatnonzero(~high[:-1] & high[1:]) + 1
        pulse_onix_s = analog_onix_s[rising]
        if cached is not None and cached.get("pulse_ticks") is not None:
            pulse_ticks = np.asarray(cached["pulse_ticks"], dtype=np.float64)
        else:
            pulse_ticks = loader.detect_strobe_edges(
                paths["analog_voltage_files"][rec_idx], paths["analog_clock_files"][rec_idx]).astype(np.float64)
        ctx.seconds_to_show = seconds_to_show
        ctx.analog_clock = analog_clock
        ctx.analog_v_chunk = analog_v_chunk
        ctx.analog_onix_s = analog_onix_s
        ctx.rising = rising
        ctx.pulse_onix_s = pulse_onix_s
        ctx.pulse_ticks = pulse_ticks
    else:
        ctx.seconds_to_show = None
        ctx.analog_clock = None
        ctx.analog_v_chunk = None
        ctx.analog_onix_s = None
        ctx.rising = None
        ctx.pulse_onix_s = None
        ctx.pulse_ticks = None

    # start offsets of each source relative to start_time onix tick 0
    start_ns = pd.Timestamp(start_time).value
    probe0 = loader.npx_probes[0]
    neural_start_tick = float(np.memmap(loader.probe_to_npx_clocks[probe0][rec_idx], dtype=np.uint64, mode="r")[0])
    neural_start_s = neural_start_tick / acq_hz
    cam1_first_s = (cam1_host_ns[0] - start_ns) / 1e9
    bridge_first_s = (bridge_host_ns[0] - start_ns) / 1e9 if bridge_host_ns is not None else np.nan
    start_offsets = {
        f"start_time (anchor) is our 0 from datetime start_time {start_time}": 0.0,
        "neural clock[0]": neural_start_s,
        "bridge first": bridge_first_s,
        "cam1 first frame": cam1_first_s,
    }
    if ctx.has_analog:
        analog_start_s = float(ctx.analog_clock[0]) / acq_hz
        first_pulse_s = ctx.pulse_ticks[0] / acq_hz
        start_offsets["analog_clock[0]"] = analog_start_s
        start_offsets["first strobe pulse"] = first_pulse_s
    ctx.start_ns = start_ns
    ctx.neural_start_tick = neural_start_tick
    ctx.neural_start_s = neural_start_s
    ctx.cam1_first_s = cam1_first_s
    ctx.bridge_first_s = bridge_first_s
    ctx.start_offsets = start_offsets

    # cam frame host vs onix drift, and (if analog) the cam frame vs its true strobe pulse residuals matched
    # by the combiner style frame->pulse offset so it works on camera-leads/lags days not just offset 0
    cam_host_elapsed_s = (cam1_host_ns - cam1_host_ns[0]) / 1e9
    cam_onix_elapsed_s = (cam1_onix_ticks - cam1_onix_ticks[0]) / acq_hz
    bridge_drift_s = cam_host_elapsed_s - cam_onix_elapsed_s
    total_drift_s = bridge_drift_s[-1]
    duration_hr = cam_onix_elapsed_s[-1] / 3600
    ctx.cam_host_elapsed_s = cam_host_elapsed_s
    ctx.cam_onix_elapsed_s = cam_onix_elapsed_s
    ctx.bridge_drift_s = bridge_drift_s
    ctx.total_drift_s = total_drift_s
    ctx.duration_hr = duration_hr
    if ctx.has_analog:
        pulse_ticks = ctx.pulse_ticks
        period = (pulse_ticks[-1] - pulse_ticks[0]) / (len(pulse_ticks) - 1)
        assigned_pulse = np.floor((cam1_onix_ticks - pulse_ticks[0]) / period)
        offset_per_frame = np.arange(len(cam1_onix_ticks)) - assigned_pulse
        pulse_offset = int(np.round(np.median(offset_per_frame[:min(50, len(cam1_onix_ticks) // 2)])))
        matched_pulse_idx = np.arange(len(cam1_onix_ticks)) - pulse_offset
        valid = (matched_pulse_idx >= 0) & (matched_pulse_idx < len(pulse_ticks))
        pulse_s = pulse_ticks[matched_pulse_idx[valid]] / acq_hz
        cam_host_s = (cam1_host_ns[valid] - start_ns) / 1e9
        cam_bridged_s = cam1_onix_ticks[valid] / acq_hz
        before_resid_s = cam_host_s - pulse_s
        after_resid_s = cam_bridged_s - pulse_s
        ctx.period = period
        ctx.pulse_offset = pulse_offset
        ctx.pulse_s = pulse_s
        ctx.before_resid_s = before_resid_s
        ctx.after_resid_s = after_resid_s
    else:
        ctx.period = None
        ctx.pulse_offset = None
        ctx.pulse_s = None
        ctx.before_resid_s = None
        ctx.after_resid_s = None

    # xcorr of camera position accel magnitude vs bno accel magnitude, per rec. guarded so a day missing bno
    # or pose columns does not sink the whole context, the summary table reads peak_lag_s (nan when absent)
    final_df_dt = 0.01
    ctx.final_df_dt = final_df_dt
    fdf = final_df[_rec_mask(final_df, rec_idx)]
    ctx.fdf = fdf
    ctx.have_xcorr = False
    ctx.cam_source = "n/a"
    ctx.peak_lag_s = np.nan
    ctx.max_lag_s = 2.0
    try:
        ear_cols = ["pose_earL.x", "pose_earL.y", "pose_earR.x", "pose_earR.y"]
        if all(c in fdf.columns for c in ear_cols) and fdf["pose_earL.x"].notna().any():
            cam_x = (fdf["pose_earL.x"] + fdf["pose_earR.x"]) / 2
            cam_y = (fdf["pose_earL.y"] + fdf["pose_earR.y"]) / 2
            cam_source = "sleap ear midpoint"
        else:
            cx = [c for c in fdf.columns if "centroid" in c and c.endswith(".x") and "meta" not in c][0]
            cy = [c for c in fdf.columns if "centroid" in c and c.endswith(".y") and "meta" not in c][0]
            cam_x = fdf[cx]
            cam_y = fdf[cy]
            cam_source = "bonsai centroid"
        bno_accel_mag = np.sqrt(fdf["bno_acceleration_x"]**2 + fdf["bno_acceleration_y"]**2 + fdf["bno_acceleration_z"]**2)
        motion = pd.DataFrame({"cam_x": cam_x, "cam_y": cam_y, "bno_accel_mag": bno_accel_mag}).dropna()

        cam_vx = np.gradient(motion["cam_x"].to_numpy(), final_df_dt)
        cam_vy = np.gradient(motion["cam_y"].to_numpy(), final_df_dt)
        cam_accel_mag = np.sqrt(np.gradient(cam_vx, final_df_dt)**2 + np.gradient(cam_vy, final_df_dt)**2)
        bno_mag = motion["bno_accel_mag"].to_numpy()
        max_lag_s = 2.0
        cross_corr = correlate(cam_accel_mag, bno_mag, mode="full")
        lags_s = correlation_lags(len(cam_accel_mag), len(bno_mag), mode="full") * final_df_dt
        in_range = np.abs(lags_s) <= max_lag_s
        peak_lag_s = lags_s[in_range][np.argmax(cross_corr[in_range])]

        ctx.have_xcorr = True
        ctx.cam_source = cam_source
        ctx.motion = motion
        ctx.cam_accel_mag = cam_accel_mag
        ctx.bno_mag = bno_mag
        ctx.cross_corr = cross_corr
        ctx.lags_s = lags_s
        ctx.in_range = in_range
        ctx.peak_lag_s = peak_lag_s
    except Exception:
        warnings.warn(f"rec {rec_idx}: could not compute cam vs bno xcorr: {traceback.format_exc()}")

    return ctx


# one fig fn per plot, each pastes its notebook plot cell and returns the figure instead of plt.show()

def fig_onix_poll_jitter(ctx):
    bridge_onix_ticks = ctx.bridge_onix_ticks
    acq_hz = ctx.acq_hz
    bridge_name = ctx.bridge_name
    # plot 1: onix-tick spacing between consecutive bridge samples for onix poll jitter
    onix_diff_ms = (np.diff(bridge_onix_ticks) / acq_hz) * 1000

    fig = plt.figure(figsize=(8, 4))
    plt.hist(onix_diff_ms, bins=200)
    plt.yscale("log") # so could see outliers potentially
    plt.xlabel("onix tick spacing between consecutive bridge samples (ms)")
    plt.ylabel("count (log)")
    plt.title(f"plot1: onix poll jitter, bridge = {bridge_name}  (median {np.median(onix_diff_ms)} ms, min {onix_diff_ms.min()} ms, max {onix_diff_ms.max()} ms)")
    plt.ticklabel_format(style="plain", axis="x", useOffset=False)
    plt.tight_layout()
    return fig


def fig_cam_host_jitter(ctx):
    cam1_host_ns = ctx.cam1_host_ns
    # plot 2: spacing between consecutive camera-frame host timestamps for camera host jitter + rate
    cam1_diff_ms = np.diff(cam1_host_ns) / 1e6 # ns to ms

    fig = plt.figure(figsize=(8, 4))
    plt.hist(cam1_diff_ms, bins=200)
    plt.yscale("log")
    plt.xlabel("host-time spacing between consecutive frames (ms)")
    plt.ylabel("count (log)")
    plt.axvline(np.median(cam1_diff_ms), color="red", linestyle="--", label="median")
    plt.title(f"plot2: cam1 host jitter (median {np.median(cam1_diff_ms)} ms, min {cam1_diff_ms.min()} ms, max {cam1_diff_ms.max()} ms)")
    plt.ticklabel_format(style="plain", axis="x", useOffset=False)
    plt.tight_layout()
    plt.legend()
    return fig


def fig_cam_onix_jitter(ctx):
    cam1_onix_ticks = ctx.cam1_onix_ticks
    acq_hz = ctx.acq_hz
    # plot 3: cam1 frame spacing after mapping to onix, host drift removed but host-stamp jitter still there
    cam1_onix_diff_ms = (np.diff(cam1_onix_ticks) / acq_hz) * 1000

    fig = plt.figure(figsize=(8, 4))
    plt.hist(cam1_onix_diff_ms, bins=200)
    plt.yscale("log")
    plt.xlabel("onix-time spacing between consecutive cam1 frames using heartbeat/bno+hs bridge (ms)")
    plt.ylabel("count (log)")
    plt.axvline(np.median(cam1_onix_diff_ms), color="red", linestyle="--", label="median")
    plt.title(f"plot3: cam1 mapped to onix t (median {np.median(cam1_onix_diff_ms)} ms, min {cam1_onix_diff_ms.min()} ms, max {cam1_onix_diff_ms.max()} ms)")
    plt.ticklabel_format(style="plain", axis="x", useOffset=False)
    plt.tight_layout()
    plt.legend()
    return fig


def fig_analog_strobe(ctx):
    analog_onix_s = ctx.analog_onix_s
    analog_v_chunk = ctx.analog_v_chunk
    pulse_onix_s = ctx.pulse_onix_s
    rising = ctx.rising
    seconds_to_show = ctx.seconds_to_show
    STROBE_CHANNEL = ctx.STROBE_CHANNEL
    rec_idx = ctx.rec_idx
    # plot 4: first ~7s of strobe chan, then a zoom on the first few pulses with detected edges
    fig = plt.figure(figsize=(12, 6))
    ax1 = plt.subplot(2, 1, 1)
    ax1.plot(analog_onix_s, analog_v_chunk, lw=0.4)
    ax1.set_ylabel(f"ch{STROBE_CHANNEL} voltage (v)")
    ax1.set_title(f"plot4: first {seconds_to_show}s analog strobe, {rising.size} pulses detected")
    ax2 = plt.subplot(2, 1, 2)

    n_zoom_pulses = 3 # 3 to show 4
    zoom_end_s = pulse_onix_s[n_zoom_pulses] + 0.01 if len(pulse_onix_s) > n_zoom_pulses else analog_onix_s[-1]
    zoom = np.logical_and(analog_onix_s <= zoom_end_s, analog_onix_s >= pulse_onix_s[0]-0.05) # show 0.05 s before first pulse and n pulses after to zoom in
    ax2.plot(analog_onix_s[zoom], analog_v_chunk[zoom])
    for pulse in pulse_onix_s[pulse_onix_s <= zoom_end_s]:
        ax2.axvline(pulse, color="r", ls="--", label="detected rising edge" if pulse == pulse_onix_s[0] else None)
    ax2.set_xlabel("onix time from first analog data sample (s)")
    ax2.set_ylabel(f"ch{STROBE_CHANNEL} voltage (v)")
    ax2.set_title(f"zoom to first {n_zoom_pulses+1} pulses")
    plt.tight_layout()
    plt.legend()
    return fig


def fig_first_frame_spacings(ctx):
    cam1_host_ns = ctx.cam1_host_ns
    cam1_onix_diff_ms = ctx.cam1_onix_diff_ms
    # plot 5: spacing between the first ~50 camera frames, to see if they start clumped or on the regular ~33 ms rate
    n_first = 50
    cam1_first_diff_ms = np.diff(cam1_host_ns[:n_first + 1]) /1e6 # ns to ms

    fig = plt.figure(figsize=(10, 4))
    plt.plot(cam1_first_diff_ms, marker="o", ms=4)
    plt.axhline(np.median(cam1_onix_diff_ms), color="red", ls="--", label=f"whole-rec median onix t diff {np.median(cam1_onix_diff_ms)}")
    plt.xlabel("cam1 frame idx")
    plt.ylabel("host t spacing from prev frame (ms)")
    plt.title(f"plot5: first {n_first} frame spacings, check for clumps (min {cam1_first_diff_ms.min()} ms, median {np.median(cam1_first_diff_ms)} ms, max {cam1_first_diff_ms.max()} ms)")
    plt.legend()
    plt.tight_layout()
    return fig


def fig_analog_pulse_rate(ctx):
    pulse_ticks = ctx.pulse_ticks
    acq_hz = ctx.acq_hz
    # plot 6: spacing between consecutive analog pulses for the true trigger rate (+ any generator-gap outliers, none expected from prev analysis)
    pulse_diff_ms = np.diff(pulse_ticks) / acq_hz * 1000

    fig = plt.figure(figsize=(8, 4))
    plt.hist(pulse_diff_ms, bins=200)
    plt.yscale("log")
    plt.axvline(np.median(pulse_diff_ms), color="red", ls="--", label="median")
    plt.xlabel("onix-time spacing between detected analog pulses (ms)")
    plt.ylabel("count (log)")
    plt.title(f"plot6: analog pulse rate (median {np.median(pulse_diff_ms)} ms ~{1000 / np.median(pulse_diff_ms)} hz, min {pulse_diff_ms.min()} ms, max {pulse_diff_ms.max()} ms)")
    plt.ticklabel_format(style="plain", axis="x", useOffset=False)
    plt.tight_layout()
    plt.legend()
    return fig


def fig_timing_source_lineup(ctx):
    start_offsets = ctx.start_offsets
    day = ctx.day
    rec_idx = ctx.rec_idx
    # plot 7: start points as vlines, full range on left, zoomed near start_time on right so the ms-scale
    # sources (analog, neural, bridge) that overlap at full scale become readable
    colors = plt.cm.tab10(np.arange(len(start_offsets)))
    small = [v for v in start_offsets.values() if abs(v) < 1.0]   # the near-start cluster (excludes cam/pulse ~2s)

    fig, ax = plt.subplots(2, 1, figsize=(8, 5))
    for a in ax:
        for (name, val), color in zip(start_offsets.items(), colors):
            a.axvline(val, color=color, label=f"{name} ({val:.4f}s)")
        a.set_yticks([])
        a.set_xlabel("approx s from start_time")
    ax[0].set_title("full range")
    ax[0].legend(loc="upper right", fontsize=7)
    ax[1].set_xlim(min(small) - 0.01, max(small) + 0.01)
    ax[1].set_title("zoom near start_time")
    plt.suptitle(f"plot7: how all timing sources line up on onix time, {day} rec{rec_idx}")
    plt.tight_layout()
    return fig


def fig_host_drift(ctx):
    has_analog = ctx.has_analog
    cam_onix_elapsed_s = ctx.cam_onix_elapsed_s
    bridge_drift_s = ctx.bridge_drift_s
    pulse_s = ctx.pulse_s
    before_resid_s = ctx.before_resid_s
    after_resid_s = ctx.after_resid_s
    total_drift_s = ctx.total_drift_s
    duration_hr = ctx.duration_hr
    bridge_name = ctx.bridge_name
    day = ctx.day
    rec_idx = ctx.rec_idx
    # plot 9: (1) bridge drift always. (2,3) cam-vs-pulse only if analog. own y-scale each
    if has_analog:
        fig, ax = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
        ax[0].plot(cam_onix_elapsed_s / 3600, bridge_drift_s)
        ax[0].set_ylabel("cam 1 frame host t - cam1 bridged onix (s)")
        ax[0].set_title("bridge drift: cam host elapsed t minus cam bridged-onix elapsed t")
        ax[1].plot(pulse_s / 3600, before_resid_s)
        ax[1].set_ylabel("host - pulse (s)")
        ax[1].set_title("before host t drift correction: cam host t minus strobe-pulse t")
        ax[2].plot(pulse_s / 3600, after_resid_s)
        ax[2].axhline(0, color="k")
        ax[2].axhline(np.median(after_resid_s), ls="--", label=f"median {np.median(after_resid_s)*1000} ms ~cam1 frame latency from pulse detection")
        ax[2].legend()
        ax[2].set_ylabel("bridged - pulse (s)")
        ax[2].set_title("after host t drift correction: cam bridged-onix minus true strobe-pulse time (flat ~latency)")
        ax[2].set_xlabel("onix time (hours)")
    else:
        fig, ax = plt.subplots(1, 1, figsize=(10, 3))
        ax.plot(cam_onix_elapsed_s / 3600, bridge_drift_s)
        ax.set_ylabel("cam1 host t - cam1 bridged onix (s)")
        ax.set_title("bridge drift only (no analog, no strobe-pulse comparison)")
        ax.set_xlabel("onix time (hours)")
    plt.suptitle(f"plot9: host drift {total_drift_s} s = {total_drift_s / duration_hr} s/hr, removed by {bridge_name} bridge,\n{day} rec{rec_idx}")
    plt.tight_layout()
    return fig


def fig_xcorr(ctx):
    if not ctx.have_xcorr:
        fig = plt.figure(figsize=(10, 3))
        plt.axis("off")
        plt.text(0.5, 0.5, "plot10: no cam vs bno xcorr (missing pose/centroid or bno columns)", ha="center", va="center")
        return fig
    cam_source = ctx.cam_source
    lags_s = ctx.lags_s
    cross_corr = ctx.cross_corr
    in_range = ctx.in_range
    peak_lag_s = ctx.peak_lag_s
    # xcorr with + lag means camera later than bno (cam lags), - lag = camera earlier (cam leads)
    fig, ax = plt.subplots(2,1,figsize=(10, 6), sharey=True)
    ax[0].scatter(lags_s[in_range], cross_corr[in_range], s=0.2)
    ax[0].axvline(peak_lag_s, color="r", ls="--", label=f"peak {peak_lag_s*1000} ms")
    ax[0].axvline(0, color="k", alpha=0.5)
    ax[0].set_xlabel("lag (s), + = camera later than bno, - = cam earlier")
    ax[0].set_ylabel("xcorr(cam accel, bno accel)")
    ax[0].set_title(f"plot10: xcorr {cam_source} accel vs bno accel mag, peak {peak_lag_s*1000} ms")
    ax[0].legend()
    ax[1].scatter(lags_s[in_range], cross_corr[in_range], s=0.2)
    ax[1].axvline(peak_lag_s, color="r", ls="--", label=f"peak {peak_lag_s*1000} ms")
    ax[1].axvline(0, color="k", alpha=0.5)
    ax[1].set_xlim(peak_lag_s-0.5, peak_lag_s+ 0.5) #zoom around peak
    plt.tight_layout()
    return fig


def fig_position_over_time(ctx):
    final_df = ctx.fdf
    # plot 12: x and y over time for each sleap keypoint and the centroid, to spot dropouts (gaps) and jumps (spikes)
    pos_cols = [c for c in final_df.columns if c.startswith("pose_") and (c.endswith(".x") or c.endswith(".y")) and "meta" not in c and "score" not in c]
    t_hours = (final_df.index - final_df.index[0]).total_seconds() / 3600

    fig, ax = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
    for c in pos_cols:
        (ax[0] if c.endswith(".x") else ax[1]).plot(t_hours, final_df[c], label=c.replace("pose_", "").rsplit(".", 1)[0])
    ax[0].set_ylabel("x (px)")
    ax[0].set_title("plot12: keypoint + centroid position over t")
    ax[1].set_ylabel("y (px)")
    ax[1].set_xlabel("onix t combined (hours)")
    handles, labels = ax[0].get_legend_handles_labels()
    ax[0].legend(dict(zip(labels, handles)).values(), dict(zip(labels, handles)).keys(), fontsize=7, ncol=5, loc="upper right")
    plt.tight_layout()
    return fig


def fig_position_zoom(ctx):
    final_df = ctx.fdf
    final_df_dt = ctx.final_df_dt
    # plot 12 zoom: keypoint + centroid position over a 5 s chunk, 10 min into the rec, to inspect tracking closely
    pos_cols = [c for c in final_df.columns if c.startswith("pose_") and (c.endswith(".x") or c.endswith(".y")) and "meta" not in c and "score" not in c]
    win_start_min = 10
    win_dur_s = 30
    i0 = int(win_start_min * 60 / final_df_dt)
    i1 = i0 + int(win_dur_s / final_df_dt)
    final_df_win = final_df.iloc[i0:i1]
    t_win_s = np.arange(len(final_df_win)) * final_df_dt   # seconds within the window

    fig, ax = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
    for c in pos_cols:
        (ax[0] if c.endswith(".x") else ax[1]).plot(t_win_s, final_df_win[c], lw=0.8, marker=".", ms=2, label=c.replace("pose_", "").rsplit(".", 1)[0])
    ax[0].set_ylabel("x (px)")
    ax[0].set_title(f"plot12 zoom: keypoint + centroid x, {win_dur_s}s at {win_start_min} min")
    ax[1].set_ylabel("y (px)")
    ax[1].set_xlabel(f"seconds from {win_start_min} min")
    handles, labels = ax[0].get_legend_handles_labels()
    ax[0].legend(dict(zip(labels, handles)).values(), dict(zip(labels, handles)).keys(), fontsize=7, ncol=5, loc="upper right")
    plt.tight_layout()
    return fig


def fig_position_2d_scatter(ctx):
    final_df = ctx.fdf
    day = ctx.day
    rec_idx = ctx.rec_idx
    # plot 13: 2d x-y scatter per keypoint and centroid
    pos_cols = [c for c in final_df.columns if c.startswith("pose_") and (c.endswith(".x") or c.endswith(".y")) and "meta" not in c and "score" not in c]
    bases = sorted(set(c.rsplit(".", 1)[0] for c in pos_cols))
    step = 1  # downsample if u want its a lot of pts

    n = len(bases)
    ncols = int(np.ceil(np.sqrt(n)))
    nrows = int(np.ceil(n / ncols))

    fig, ax = plt.subplots(nrows, ncols, figsize=(5 * ncols, 5 * nrows))
    ax = np.atleast_1d(ax).ravel()
    for i, base in enumerate(bases):
        ax[i].scatter(final_df[base + ".x"][::step], final_df[base + ".y"][::step], s=0.1, alpha=0.3)
        ax[i].set_title(base.replace("pose_", ""), fontsize=9)
        ax[i].set_aspect("equal")
        ax[i].invert_yaxis()   # image coords have y increasing downward, match the video
    for j in range(n, len(ax)):
        ax[j].axis("off")
    plt.suptitle(f"plot13: 2d position per keypoint + centroid, {day} rec{rec_idx}")
    plt.tight_layout()
    return fig


def fig_position_2d_line(ctx):
    final_df = ctx.fdf
    day = ctx.day
    rec_idx = ctx.rec_idx
    # recompute the same layout as plot 13 (the notebook cell relied on it running just after the scatter)
    pos_cols = [c for c in final_df.columns if c.startswith("pose_") and (c.endswith(".x") or c.endswith(".y")) and "meta" not in c and "score" not in c]
    bases = sorted(set(c.rsplit(".", 1)[0] for c in pos_cols))
    step = 1
    n = len(bases)
    ncols = int(np.ceil(np.sqrt(n)))
    nrows = int(np.ceil(n / ncols))

    # also make 13b
    #line not scatter?
    fig, ax = plt.subplots(nrows, ncols, figsize=(5 * ncols, 5 * nrows))
    ax = np.atleast_1d(ax).ravel()
    for i, base in enumerate(bases):
        ax[i].plot(final_df[base + ".x"][::step], final_df[base + ".y"][::step], lw=0.2)
        ax[i].set_title(base.replace("pose_", ""), fontsize=9)
        ax[i].set_aspect("equal")
        ax[i].invert_yaxis()   # image coords have y increasing downward, match the video
    for j in range(n, len(ax)):
        ax[j].axis("off")
    plt.suptitle(f"plot13: 2d position per keypoint + centroid, {day} rec{rec_idx}")
    plt.tight_layout()
    return fig


def fig_summary_table(ctx):
    day = ctx.day
    rec_idx = ctx.rec_idx
    bridge_name = ctx.bridge_name
    has_analog = ctx.has_analog
    cam1_host_ns = ctx.cam1_host_ns
    cam1_first_s = ctx.cam1_first_s
    total_drift_s = ctx.total_drift_s
    duration_hr = ctx.duration_hr
    peak_lag_s = ctx.peak_lag_s
    pulse_ticks = ctx.pulse_ticks
    after_resid_s = ctx.after_resid_s
    combiner_log_path = ctx.combiner_log_path
    # plot 11: per-rec timing summary of what the diagnostics found straight from combiner
    # the applied frame->pulse offset comes from the combiner's precomputed pulses (ctx.applied_offset), only
    # falling back to parsing it from the log when running standalone. warnings always come from the log
    applied_offset = str(ctx.applied_offset) if ctx.applied_offset is not None else "n/a"
    warnings_seen = set()
    if combiner_log_path and os.path.exists(combiner_log_path):
        with open(combiner_log_path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                if applied_offset == "n/a" and f"recording {rec_idx}: offset-aligned camera" in line:
                    applied_offset = line.split("offset=")[1].split(",")[0]
                if "UserWarning:" in line:
                    warnings_seen.add(line.split("UserWarning:")[1].strip())

    if applied_offset == "0":
        align_note = "frame k matched to pulse k, no shift"
    elif applied_offset != "n/a":
        o = int(applied_offset)
        align_note = f"frame k matched to pulse k-{o} (camera {'leads' if o > 0 else 'starts late'})"
    else:
        align_note = "n/a"

    summary = {
        "day": day,
        "rec": rec_idx,
        "bridge": bridge_name,
        "has analog:": has_analog,
        "n cam frames": len(cam1_host_ns),
        "camera starts after acq (s)": cam1_first_s,
        "host drift (s/hr)": total_drift_s / duration_hr,
        "xcorr camera vs bno from combiner (ms)": peak_lag_s * 1000,
        "n warnings in combiner log": len(warnings_seen),
    }
    if has_analog:
        summary["n strobe pulses"] = len(pulse_ticks)
        summary["strobe - cam1"] = len(pulse_ticks) - len(cam1_host_ns)
        summary["frame->pulse offset applied"] = f"{applied_offset}  ({align_note})"
        summary["camera latency from analog rising edge detection (ms)"] = np.median(after_resid_s) * 1000
    else:
        summary["strobe"] = "no analog, camera bridged to onix not strobe-aligned"

    preview_len = 70 # chars of each warning to preview
    rows = [[k, str(v)] for k, v in summary.items()]
    for i, w in enumerate(sorted(warnings_seen)):
        rows.append([f"warning {i + 1}", w[:preview_len] + ("..." if len(w) > preview_len else "")])

    fig, tax = plt.subplots(figsize=(11, 0.4 * len(rows) + 1)) # height grows with row count
    tax.axis("off")
    tbl = tax.table(cellText=rows, loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    tax.set_title(f"timing summary info, {day} rec{rec_idx}")
    plt.tight_layout()
    return fig


def save_all_figs(loader, rec_idx, final_df, output_path, precomputed_pulses=None, combiner_log_path=None):
    """build the context once and write every figure for one rec as a png plus a combined pdf

    figures land in output_path/combiner_figs/. each png is named {day}_rec{N}_{NN}_{plot_name}.png and the
    per-rec pdf is combiner_figs_rec{N}.pdf. analog-only figures are skipped when the rec has no analog. each
    figure is wrapped so one bad plot cannot abort the rest of the rec
    """
    ctx = build_fig_context(loader, rec_idx, final_df, precomputed_pulses=precomputed_pulses, combiner_log_path=combiner_log_path)
    figs_dir = os.path.join(output_path, "combiner_figs")
    os.makedirs(figs_dir, exist_ok=True)
    pdf_path = os.path.join(figs_dir, f"combiner_figs_rec{rec_idx}.pdf")

    with PdfPages(pdf_path) as pdf:
        for number, plot_name, fn_name, analog_only in FIGURE_LIST:
            if analog_only and not ctx.has_analog:
                continue
            try:
                fig = globals()[fn_name](ctx)
                png_path = os.path.join(figs_dir, f"{ctx.day}_rec{rec_idx}_{number}_{plot_name}.png")
                fig.savefig(png_path, dpi=120)
                # rasterize the data artists (points/lines) so the pdf page stays small on the big scatter
                # plots, text and axes stay vector/crisp
                for ax in fig.axes:
                    for artist in list(ax.collections) + list(ax.lines):
                        artist.set_rasterized(True)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)
            except Exception:
                warnings.warn(f"rec {rec_idx}: figure {number} {plot_name} failed: {traceback.format_exc()}")
    return pdf_path
