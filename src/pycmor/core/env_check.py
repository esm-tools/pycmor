"""Runtime check of the netCDF4/HDF5 stack pycmor is linked against.

pycmor's write path benefits a lot from two properties of the loaded
HDF5 / libnetcdf build:

* **Thread safety**: the ``netcdf_write_scheduler: threads`` knob only
  parallelises if ``H5is_library_threadsafe`` returns 1. With a
  non-thread-safe build (e.g. the PyPI ``netCDF4`` wheel bundles one),
  threaded writes serialise on a module-wide lock and are strictly
  slower than the synchronous scheduler.
* **Modern codecs**: ``zstd`` and ``blosc_*`` only work if libnetcdf was
  compiled against them AND the corresponding HDF5 filter plugins are
  discoverable via ``HDF5_PLUGIN_PATH``. The PyPI wheel's bundled
  libnetcdf has neither.

The checks here run once at startup, log the stack's capabilities, and
warn loudly if pycmor is asked to do something the stack can't honour.
They never raise — pycmor on a degraded stack is still usable, just
slower.
"""

from __future__ import annotations

import ctypes
import os
import tempfile

from .logging import logger


def _probe_threadsafe() -> bool | None:
    """Return True/False if detected, or None if we couldn't probe."""
    for candidate in ("libhdf5.so.310", "libhdf5.so.200", "libhdf5.so"):
        try:
            lib = ctypes.CDLL(candidate)
            break
        except OSError:
            continue
    else:
        return None
    try:
        flag = ctypes.c_int(0)
        lib.H5is_library_threadsafe(ctypes.byref(flag))
        return bool(flag.value)
    except Exception:
        return None


def _probe_codecs() -> set[str]:
    """Return the set of compression codecs that work in this build.

    Tries a tiny write for each codec; silently skips codecs that error
    (typical reason: filter plugin not compiled into libnetcdf, or
    missing from HDF5_PLUGIN_PATH).
    """
    try:
        import netCDF4
        import numpy as np
    except Exception:
        return set()
    working: set[str] = set()
    arr = np.zeros((16, 16), dtype="f4")
    for codec in ("zlib", "zstd", "blosc_lz4", "blosc_zstd"):
        p = tempfile.mktemp(suffix=".nc")
        try:
            with netCDF4.Dataset(p, "w") as ds:
                ds.createDimension("x", 16)
                ds.createDimension("y", 16)
                v = ds.createVariable(
                    "v", "f4", ("x", "y"), compression=codec, complevel=1
                )
                v[:] = arr
            working.add(codec)
        except Exception:
            pass
        finally:
            try:
                os.unlink(p)
            except OSError:
                pass
    return working


def run_env_check(verbose: bool = True) -> dict:
    """Log the detected HDF5/netCDF4 stack and return a summary dict.

    Never raises. Callers pass ``verbose=True`` for the one-shot
    startup log line; bench / test code can pass ``verbose=False`` and
    inspect the returned dict.
    """
    summary: dict = {"ok": True}
    try:
        import netCDF4
        import h5py
    except ImportError as e:
        logger.error(f"env_check: netCDF4/h5py import failed: {e}")
        summary.update(ok=False, error=str(e))
        return summary

    threadsafe = _probe_threadsafe()
    codecs = _probe_codecs()

    summary.update(
        netcdf4=netCDF4.__version__,
        libnetcdf=netCDF4.__netcdf4libversion__,
        hdf5=h5py.version.hdf5_version,
        threadsafe=threadsafe,
        codecs=codecs,
    )

    if verbose:
        logger.info(
            "env_check: "
            f"netCDF4 {summary['netcdf4']}, "
            f"libnetcdf {summary['libnetcdf']}, "
            f"HDF5 {summary['hdf5']}, "
            f"threadsafe={threadsafe}, "
            f"codecs={sorted(codecs)}"
        )
        if threadsafe is False:
            logger.warning(
                "env_check: HDF5 is NOT thread-safe. "
                "'netcdf_write_scheduler: threads' will serialise on a "
                "module lock and run slower than synchronous. Consider "
                "activating pycmor_py312_ts or rebuilding netCDF4/h5py "
                "against a thread-safe HDF5."
            )
        missing_modern = {"zstd", "blosc_lz4", "blosc_zstd"} - codecs
        if missing_modern:
            logger.info(
                f"env_check: codecs unavailable in this build: "
                f"{sorted(missing_modern)}. "
                "'netcdf_compression_codec=zstd/blosc_*' requires "
                "libnetcdf >= 4.9.0 with those filters compiled in."
            )

    return summary
