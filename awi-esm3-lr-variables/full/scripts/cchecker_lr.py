#!/usr/bin/env python3
"""Execute the installed cchecker unchanged, with the CF label adapter loaded."""
import runpy
import shutil

from checker_compat import apply_compatibility

apply_compatibility()
binary=shutil.which('cchecker.py')
if binary is None:
    raise RuntimeError('Installed cchecker.py not found on PATH')
runpy.run_path(binary,run_name='__main__')
