#!/usr/bin/env python3
"""
Check whether unit_labels.tsv is present in all kilosort4 folders for a day.

Usage:
    python check_unit_labels.py <day_dir>
"""

import sys
from pathlib import Path


def check_unit_labels(day_dir: Path):
    output_dir = day_dir / "output"
    if not output_dir.exists():
        print(f"ERROR: output directory not found: {output_dir}")
        sys.exit(1)

    kilosort_dirs = sorted(output_dir.glob("*/shank_*/kilosort4"))

    if not kilosort_dirs:
        print(f"No kilosort4 folders found under {output_dir}")
        sys.exit(1)

    present, missing, skipped = [], [], []

    for ks_dir in kilosort_dirs:
        rel = ks_dir.relative_to(day_dir)
        shank_dir = ks_dir.parent
        no_spikes_sentinel = shank_dir / "NO_GOOD_SPIKES"

        if no_spikes_sentinel.exists():
            skipped.append(rel)
        elif (ks_dir / "unit_labels.tsv").exists():
            present.append(rel)
        else:
            missing.append(rel)

    total = len(present) + len(missing)  # skipped don't count as expected
    print(f"\nDay: {day_dir}")
    print(f"Kilosort4 folders found: {len(kilosort_dirs)}  "
          f"(expected: {total}, skipped/no-spikes: {len(skipped)})\n")

    if present:
        print(f"[OK]  unit_labels.tsv present ({len(present)}/{total}):")
        for p in present:
            print(f"      {p}")

    if skipped:
        print(f"\n[--]  Skipped (NO_GOOD_SPIKES sentinel) ({len(skipped)}):")
        for p in skipped:
            print(f"      {p.parent.relative_to(day_dir)}")

    if missing:
        print(f"\n[MISSING]  unit_labels.tsv absent ({len(missing)}/{total}):")
        for p in missing:
            print(f"      {p}")

    print()
    if not missing:
        print("All expected kilosort4 folders have unit_labels.tsv.")
        return True
    else:
        print(f"WARNING: {len(missing)} folder(s) are missing unit_labels.tsv.")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    day_dir = Path(sys.argv[1])
    if not day_dir.exists():
        print(f"ERROR: directory not found: {day_dir}")
        sys.exit(1)

    ok = check_unit_labels(day_dir)
    sys.exit(0 if ok else 1)
