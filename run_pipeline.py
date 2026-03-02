#!/usr/bin/env python3
"""Run the full spike-sorting pipeline for one probe/shank."""

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def run(cmd):
    subprocess.run(cmd, check=True, cwd=SCRIPT_DIR)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("probe", choices=["a", "b"])
    parser.add_argument("shank", choices=["0", "1", "2", "3"])
    args = parser.parse_args()

    run([sys.executable, "-u", "run_shank.py", args.data_dir, args.probe, args.shank])
    run([sys.executable, "postproc.py", args.data_dir, args.probe, args.shank])
    run([sys.executable, "extract_unitmatch_data.py",
         "--data_dir", args.data_dir, "--probe", args.probe, "--shank", args.shank])


if __name__ == "__main__":
    main()
