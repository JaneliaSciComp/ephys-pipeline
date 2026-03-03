#!/usr/bin/env python3
"""Run the full spike-sorting pipeline for one probe/shank."""

import argparse
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
# sys.executable may be a symlink back to base conda Python; use sys.prefix to
# get the active env root and construct the real interpreter path.
PYTHON = os.path.join(sys.prefix, "bin", "python")


def run(cmd):
    subprocess.run(cmd, check=True, cwd=SCRIPT_DIR)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("probe", choices=["a", "b"])
    parser.add_argument("shank", choices=["0", "1", "2", "3"])
    args = parser.parse_args()

    run([PYTHON, "-u", "run_shank.py", args.data_dir, args.probe, args.shank])
    run([PYTHON, "postproc.py", args.data_dir, args.probe, args.shank])
    run([PYTHON, "extract_unitmatch_data.py",
         "--data_dir", args.data_dir, "--probe", args.probe, "--shank", args.shank])


if __name__ == "__main__":
    main()
