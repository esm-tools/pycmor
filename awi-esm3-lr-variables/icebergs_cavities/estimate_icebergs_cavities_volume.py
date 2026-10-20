#!/usr/bin/env python
"""Regenerate cmip7_icebergs_cavities_variables.csv and _todo.md (engine: ../obgc/estimate_lr_volume.py)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "obgc"))
import estimate_lr_volume  # noqa: E402

if __name__ == "__main__":
    estimate_lr_volume.main(["ice"])
