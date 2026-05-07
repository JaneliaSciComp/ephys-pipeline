#!/usr/bin/env python3
"""Run the full spike-sorting pipeline for one probe/shank."""

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PYTHON = sys.executable


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=SCRIPT_DIR)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("probe", choices=["a", "b"])
    parser.add_argument("shank", choices=["0", "1", "2", "3"])
    parser.add_argument("--duration_s", type=float, default=None,
                        help="If set, truncate concatenated recording for short test runs.")
    parser.add_argument("--output_subdir", type=str, default="output",
                        help="Subdirectory under data_dir for outputs (default: output).")
    args = parser.parse_args()

    extra = ["--output_subdir", args.output_subdir]
    if args.duration_s is not None:
        extra += ["--duration_s", str(args.duration_s)]

    run([PYTHON, "-s", "-u", str(SCRIPT_DIR / "pipeline" / "run_shank.py"),
         args.data_dir, args.probe, args.shank, *extra])
    run([PYTHON, "-s", str(SCRIPT_DIR / "pipeline" / "postproc.py"),
         args.data_dir, args.probe, args.shank,
         "--output_subdir", args.output_subdir])
    run([PYTHON, "-s", str(SCRIPT_DIR / "pipeline" / "extract_unitmatch_data.py"),
         "--data_dir", args.data_dir, "--probe", args.probe, "--shank", args.shank,
         "--output_subdir", args.output_subdir])


if __name__ == "__main__":
    main()
