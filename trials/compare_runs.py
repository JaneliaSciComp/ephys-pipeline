#!/usr/bin/env python3
"""Compare preproc speed benchmark results across trials.

Usage:
    python trials/compare_runs.py <day_dir>

    # or point at specific result files:
    python trials/compare_runs.py --files \
        /data/day/preproc_speed/1_baseline/preproc_timing.json \
        /data/day/preproc_speed/2_lazy_artifact/preproc_timing.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

STEPS = ["artifact_detect", "intermediate_save", "dredge", "bad_channels", "save", "total"]


def load_results(files: list[Path]) -> list[dict]:
    results = []
    for f in files:
        with open(f) as fh:
            results.append(json.load(fh))
    return sorted(results, key=lambda r: r["trial_name"])


def find_result_files(day_dir: Path) -> list[Path]:
    return sorted(day_dir.glob("preproc_speed/*/preproc_timing.json"))


def print_table(results: list[dict]) -> None:
    if not results:
        print("No results found.")
        return

    col_w = 22
    step_w = 20

    header = f"{'step':<{step_w}}" + "".join(
        f"{r['trial_name'][:col_w]:>{col_w}}" for r in results
    )
    print(header)
    print("-" * len(header))

    for step in STEPS:
        row = f"{step:<{step_w}}"
        for r in results:
            val = r["timings"].get(step)
            if val is None:
                row += f"{'—':>{col_w}}"
            elif step == "total":
                row += f"{val/60:>{col_w-1}.1f}m"
            else:
                row += f"{val:>{col_w-1}.1f}s"
        print(row)

    print()
    if len(results) > 1:
        baseline = results[0]["timings"].get("total", 0)
        print("speedup vs first trial:")
        for r in results[1:]:
            t = r["timings"].get("total", 0)
            if baseline and t:
                print(f"  {r['trial_name']}: {baseline/t:.2f}x")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare preproc benchmark runs")
    parser.add_argument("day_dir", nargs="?", help="Day directory containing preproc_speed/")
    parser.add_argument("--files", nargs="+", help="Explicit preproc_timing.json paths")
    args = parser.parse_args()

    if args.files:
        files = [Path(f) for f in args.files]
    elif args.day_dir:
        files = find_result_files(Path(args.day_dir))
        if not files:
            print(f"No preproc_timing.json files found under {args.day_dir}/preproc_speed/")
            return 1
    else:
        parser.print_help()
        return 1

    results = load_results(files)
    print_table(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
