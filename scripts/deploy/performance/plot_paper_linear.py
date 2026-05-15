#!/usr/bin/env python3
"""
plot_paper_linear.py — Regenerate every curated paper figure with linear axes
(no log scale) into das_results/paper_plots/curated_linear/.

Drives the three curated plotters (plot_paper.py, plot_paper_faulty_curated.py,
plot_paper_batch_curated.py) with PLOT_LINEAR=1, which they read on import to
redirect OUT_DIR and skip set_{x,y}scale("log") calls. PDFs only.
"""

import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS = [
    "plot_paper.py",
    "plot_paper_faulty_curated.py",
    "plot_paper_batch_curated.py",
]


def main():
    env = os.environ.copy()
    env["PLOT_LINEAR"] = "1"
    for name in SCRIPTS:
        path = SCRIPT_DIR / name
        print(f"\n>>> {name} (linear)")
        rc = subprocess.call([sys.executable, str(path)], env=env)
        if rc != 0:
            print(f"  {name} failed with exit code {rc}", file=sys.stderr)
            sys.exit(rc)
    print("\nDone.")


if __name__ == "__main__":
    main()
