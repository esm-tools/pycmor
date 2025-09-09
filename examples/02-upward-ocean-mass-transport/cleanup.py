#!/usr/bin/env python3
"""
Cleans up from example runs
"""
import shutil
from pathlib import Path


def rm_file(fname):
    try:
        fname.unlink()
        print(f"Removed file: {fname}")
    except Exception as e:
        print(f"Error removing file {fname}: {e}")


def rm_dir(dirname):
    try:
        shutil.rmtree(dirname)
        print(f"Removed directory: {dirname}")
    except Exception as e:
        print(f"Error removing directory {dirname}: {e}")


def cleanup():
    current_dir = Path.cwd()

    for item in current_dir.rglob("*"):
        if (
            item.is_file()
            and item.name.startswith("slurm")
            and item.name.endswith("out")
        ):
            rm_file(item)
        if (
            item.is_file()
            and item.name.startswith("pycmor")
            and item.name.endswith("json")
        ):
            rm_file(item)
        if item.is_file() and item.name.endswith("nc"):
            rm_file(item)
        if item.name == "pymor_report.log":
            rm_file(item)
        elif item.is_dir() and item.name == "logs":
            rm_dir(item)
    print("Cleanup completed.")


if __name__ == "__main__":
    cleanup()
