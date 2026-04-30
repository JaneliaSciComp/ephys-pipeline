#!/usr/bin/env python3
"""Check whether detected artifacts were actually removed from shank_recording.bin.

For each artifact period in artifact_periods.json, samples a window of the final
recording and reports max/min/std to distinguish:
  - Raw saturation present (max ≈ 3974) → removal failed
  - Large transients only at edges → filter ringing (removal worked but bandpass rang)
  - Quiet throughout → removal worked cleanly

Usage:
    python trials/check_artifact_removal.py <shank_folder>

Example:
    python trials/check_artifact_removal.py \\
        /Volumes/voigtslab/neuropixels_2025/npx11/2026_03_05_npx11_large_maze/output/trials/17_artifacts/probe_a/shank_0
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np

SAMPLE_RATE = 30000


def _resolve_n_channels(shank_folder: Path) -> int | None:
    """Try to determine n_channels for the shank from various sources."""
    # 1) probe.prb in shank folder (kilosort export)
    probe_prb = shank_folder / "probe.prb"
    if probe_prb.exists():
        ns = {}
        try:
            exec(probe_prb.read_text(), ns)
            for cg in ns.get("channel_groups", {}).values():
                if "channels" in cg:
                    return len(cg["channels"])
        except Exception:
            pass

    # 2) Walk up to day_dir and use {probe}_probe_conf.json + shank num from path
    # Path layout: <day>/output/trials/<trial>/probe_<probe>/shank_<n>
    parts = shank_folder.parts
    try:
        probe_dir = parts[-2]    # probe_a
        shank_dir = parts[-1]    # shank_0
        probe = probe_dir.split("_", 1)[1]
        shank_num = int(shank_dir.split("_", 1)[1])
        # day_dir is 4 levels up (output/trials/<trial>/probe_<x>/shank_<n>)
        day_dir = shank_folder.parent.parent.parent.parent.parent
        conf = day_dir / f"{probe}_probe_conf.json"
        if conf.exists():
            d = json.loads(conf.read_text())
            probe = d["probes"][0]
            shank_ids = probe["shank_ids"]
            dci = probe.get("device_channel_indices", [None] * len(shank_ids))
            # Count contacts that are both on this shank AND mapped to a recording channel
            return sum(1 for s, d_ in zip(shank_ids, dci)
                       if int(s) == shank_num and d_ is not None and d_ != -1)
    except Exception:
        pass

    return None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("shank_folder", type=Path)
    p.add_argument("--n_check", type=int, default=10,
                   help="Number of artifact periods to inspect (default 10)")
    p.add_argument("--pad_ms", type=float, default=200.0,
                   help="Extra ms before/after period to include in window")
    p.add_argument("--n_channels", type=int, default=None,
                   help="Override n_channels in shank_recording.bin (default: auto-detect)")
    p.add_argument("--raw_file", type=Path, default=None,
                   help="Optional: raw file to also inspect at the same indices (full 384 channels)")
    args = p.parse_args()

    bin_path = args.shank_folder / "shank_recording.bin"
    json_path = args.shank_folder / "artifact_periods.json"

    if not bin_path.exists():
        print(f"ERROR: not found: {bin_path}")
        return 1
    if not json_path.exists():
        print(f"ERROR: not found: {json_path}")
        return 1

    n_ch = args.n_channels or _resolve_n_channels(args.shank_folder)
    if n_ch is None:
        print(f"ERROR: could not determine n_channels; pass --n_channels")
        return 1

    size = bin_path.stat().st_size
    n_frames = size // (2 * n_ch)
    print(f"shank_recording.bin: {size/1e9:.2f} GB, {n_ch} channels, {n_frames:,} frames "
          f"({n_frames/SAMPLE_RATE/60:.1f} min)")

    data = json.loads(json_path.read_text())
    print(f"artifact_periods.json: {data['n_artifacts']} artifacts, "
          f"{data['total_duration_sec']:.2f}s total\n")

    # Flatten events from all segments
    events = []
    for seg in data["segments"]:
        events.extend(seg)
    if not events:
        print("No artifacts to check.")
        return 0

    # Pick events to check: first few, last, and one in middle
    idxs = list(range(min(args.n_check - 2, len(events))))
    if len(events) > args.n_check - 1:
        idxs.append(len(events) // 2)
        idxs.append(len(events) - 1)
    idxs = sorted(set(idxs))

    # Optional: open raw file (full 384 channels)
    raw_handle = None
    raw_n_ch = 384
    raw_n_frames = 0
    if args.raw_file is not None:
        if not args.raw_file.exists():
            print(f"ERROR: raw file not found: {args.raw_file}")
            return 1
        raw_size = args.raw_file.stat().st_size
        raw_n_frames = raw_size // (2 * raw_n_ch)
        print(f"raw file: {raw_size/1e9:.2f} GB, {raw_n_ch} channels, {raw_n_frames:,} frames "
              f"({raw_n_frames/SAMPLE_RATE/60:.1f} min)\n")
        raw_handle = open(args.raw_file, "rb")

    pad_samples = int(args.pad_ms * SAMPLE_RATE / 1000)

    header = f"{'period#':>8} {'start_sec':>10} {'dur_ms':>8} "
    header += f"{'pre_max':>9} {'in_max':>9} {'in_p99':>9} {'post_max':>9}"
    if raw_handle:
        header += f"   {'raw_pre':>9} {'raw_in':>9} {'raw_post':>9}"
    header += "  verdict"
    print(header)

    f_handle = open(bin_path, "rb")
    try:
        for i in idxs:
            ev = events[i]
            s = ev["start_frame"]
            e = ev["end_frame"]
            dur_ms = (e - s) / SAMPLE_RATE * 1000

            # Read [s - pad, e + pad] from preprocessed
            read_start = max(0, s - pad_samples)
            read_end = min(n_frames, e + pad_samples)
            if read_start >= n_frames or read_end <= read_start:
                print(f"{i:>8d} {s/SAMPLE_RATE:>10.2f} {dur_ms:>8.1f}  "
                      f"SKIPPED — artifact at frame {s} is beyond recording length {n_frames}")
                continue
            f_handle.seek(read_start * n_ch * 2)
            buf = f_handle.read((read_end - read_start) * n_ch * 2)
            chunk = np.frombuffer(buf, dtype=np.int16).reshape(-1, n_ch)

            pre = chunk[: s - read_start]
            inside = chunk[s - read_start : e - read_start]
            post = chunk[e - read_start :]

            pre_max = int(np.abs(pre).max()) if pre.size else 0
            in_max = int(np.abs(inside).max()) if inside.size else 0
            in_p99 = int(np.percentile(np.abs(inside), 99)) if inside.size else 0
            post_max = int(np.abs(post).max()) if post.size else 0

            # Read same window from raw if requested
            raw_pre = raw_in = raw_post = -1
            if raw_handle is not None and read_start < raw_n_frames:
                r_end = min(raw_n_frames, read_end)
                raw_handle.seek(read_start * raw_n_ch * 2)
                rbuf = raw_handle.read((r_end - read_start) * raw_n_ch * 2)
                rchunk = np.frombuffer(rbuf, dtype=np.int16).reshape(-1, raw_n_ch)
                rpre = rchunk[: s - read_start]
                rin = rchunk[s - read_start : e - read_start]
                rpost = rchunk[e - read_start :]
                raw_pre = int(rpre.max()) if rpre.size else 0
                raw_in = int(rin.max()) if rin.size else 0
                raw_post = int(rpost.max()) if rpost.size else 0

            # Verdict heuristic (based on preprocessed)
            if in_max > 3500:
                verdict = "RAW SATURATION REMAINS"
            elif in_max > 5 * max(pre_max, post_max, 1):
                verdict = "filter ringing / partial removal"
            else:
                verdict = "clean"

            row = f"{i:>8d} {s/SAMPLE_RATE:>10.2f} {dur_ms:>8.1f} "
            row += f"{pre_max:>9d} {in_max:>9d} {in_p99:>9d} {post_max:>9d}"
            if raw_handle:
                row += f"   {raw_pre:>9d} {raw_in:>9d} {raw_post:>9d}"
            row += f"  {verdict}"
            print(row)
    finally:
        f_handle.close()
        if raw_handle:
            raw_handle.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
