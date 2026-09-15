"""Bench-only steps: explicit dask rechunk + chunked-load.

The pycmor save_dataset path mirrors current dask chunks into the output
NetCDF chunk encoding. Source files often have very small native chunks
(e.g. (1, 2, 421120) for OIFS XIOS output) which results in a 5840-task
dask graph that xarray builds-and-evaluates into ~30 GB of RAM despite
the data being only 17 GB raw. Rechunking earlier in the pipeline lets
us decouple the in-memory working set from the on-disk chunk grid.
"""

from pycmor.core.logging import logger


def dask_rechunk(data, rule):
    spec = rule.get("dask_rechunk")
    if not spec:
        return data
    if not hasattr(data, "chunk"):
        return data
    logger.info(f"dask_rechunk: applying chunk spec {spec}")
    new = data.chunk(spec)
    if hasattr(new, "chunks"):
        try:
            sizes = {dim: max(c) for dim, c in zip(new.dims, new.chunks)}
            logger.info(f"dask_rechunk: new max chunk sizes {sizes}")
        except Exception:
            pass
    return new


def load_mfdataset_chunked(data, rule):
    """Replacement for pycmor.core.gather_inputs.load_mfdataset that opens
    inputs with explicit ``chunks=`` so the dask graph is built at the
    desired granularity from the start, instead of inheriting native
    NetCDF chunks (often (1, 2, N_cells) for OIFS XIOS output → 5840 tiny
    chunks per variable for a 1-year file).

    Reads ``rule.load_chunks`` (a dict of dim_name → chunk_size).
    """
    import xarray as xr
    from pycmor.core.logging import logger

    engine = rule._pymor_cfg("xarray_open_mfdataset_engine")
    parallel = rule._pymor_cfg("xarray_open_mfdataset_parallel")
    chunks = rule.get("load_chunks") or {}
    files = []
    for col in rule.inputs:
        for f in col.files:
            files.append(str(f))
    logger.info(f"load_mfdataset_chunked: chunks={chunks}, {len(files)} files")
    ds = xr.open_mfdataset(
        files,
        parallel=parallel,
        use_cftime=True,
        engine=engine,
        chunks=chunks if chunks else None,
    )
    time_dimname = rule.get("time_dimname")
    if time_dimname and time_dimname in ds.dims and "time" not in ds.dims:
        ds = ds.rename({time_dimname: "time"})
    return ds


def save_dataset_per_slab(data, rule):
    """Replacement for pycmor.std_lib.files.save_dataset — splits the
    incoming (lazy) Dataset along ``time`` into N slabs of ``slab_size``
    timesteps each, computes + writes each slab to its own file, then
    explicitly drops the reference and runs gc + posix_fadvise(DONTNEED)
    to drive page-cache reclaim before the next slab runs.

    Rule attributes:
      - slab_size (int): number of timesteps per slab. Default 30.
      - output_directory: parent directory (from inherit).
      - all the standard cmor naming/encoding attrs.

    NOTE: this bypasses pycmor's filename derivation and time-encoding
    fix-ups; for a bench it produces files named
    ``<rule.name>_slabNNN_<start>-<end>.nc`` next to the cmorized output dir.
    Goal here is *only* to measure peak; switching to the proper CMIP
    filename / encoding path is left for the production version.
    """
    import gc
    import os
    import xarray as xr
    from pathlib import Path
    from pycmor.core.logging import logger

    slab_size = int(rule.get("slab_size") or 30)
    out_dir = Path(rule.get("output_directory") or "./cmorized_output/per_slab")
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(data, xr.DataArray):
        if data.name is None:
            data = data.rename("data")
        data = data.to_dataset()

    from pycmor.std_lib.dataset_helpers import get_time_label
    time_dim = get_time_label(data)
    if not time_dim:
        # fall back: first dim with datetime-like values
        for d in data.dims:
            c = data.coords.get(d)
            if c is not None and (c.dtype.kind == "M" or "datetime" in str(c.dtype) or "cftime" in str(c.dtype)):
                time_dim = d
                break
    if not time_dim:
        raise KeyError(f"save_dataset_per_slab: cannot find time dim in {list(data.dims)}")
    logger.info(f"save_dataset_per_slab: detected time dim '{time_dim}'")
    n = data.sizes[time_dim]
    n_slabs = (n + slab_size - 1) // slab_size
    logger.info(
        f"save_dataset_per_slab: {n} timesteps along '{time_dim}', "
        f"slab_size={slab_size} → {n_slabs} slabs"
    )

    enable_comp = bool(rule.get("netcdf_enable_compression", True))
    codec = rule.get("netcdf_compression_codec") or "blosc_zstd"
    level = int(rule.get("netcdf_compression_level") or 3)

    for var in data.data_vars:
        enc = {}
        if enable_comp:
            if codec == "zlib":
                enc["zlib"] = True
                enc["complevel"] = level
                enc["shuffle"] = True
            else:
                enc["compression"] = codec
                enc["complevel"] = level
                if codec.startswith("blosc"):
                    enc["blosc_shuffle"] = 1
        # Match dask chunks if the dataset is chunked, else let HDF5 default.
        chs = data[var].chunks
        if chs is not None:
            enc["chunksizes"] = tuple(max(c) for c in chs)
        if str(var).endswith(("_bnds", "_bounds")) or str(var).startswith("bounds_"):
            enc["_FillValue"] = None
        data[var].encoding.update({k: v for k, v in enc.items() if k not in data[var].encoding})

    rule_name = getattr(rule, "name", "var")
    for i in range(n_slabs):
        s, e = i * slab_size, min((i + 1) * slab_size, n)
        slab = data.isel({time_dim: slice(s, e)})
        path = out_dir / f"{rule_name}_slab{i:03d}_{s:06d}-{e-1:06d}.nc"
        logger.info(f"save_dataset_per_slab: writing slab {i+1}/{n_slabs} -> {path}")
        slab.to_netcdf(path, mode="w", format="NETCDF4")
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        except Exception as exc:
            logger.debug(f"  → fadvise(DONTNEED) failed for {path}: {exc}")
        del slab
        gc.collect()

    # Return None — no follow-up steps depend on the data after save.
    return None


def save_dataset_per_slab_single_file(data, rule):
    """Like save_dataset_per_slab, but writes to ONE CMIP-style output
    file by appending each slab along the unlimited time dim. First slab
    creates the file (mode='w'), subsequent slabs append (mode='a').

    Eliminates the post-merge / ncrcat step while preserving slab-bounded
    memory peak.
    """
    import gc
    import os
    import xarray as xr
    from pathlib import Path
    from pycmor.core.logging import logger

    slab_size = int(rule.get("slab_size") or 30)
    out_dir = Path(rule.get("output_directory") or "./cmorized_output/per_slab_single")
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(data, xr.DataArray):
        if data.name is None:
            data = data.rename("data")
        data = data.to_dataset()

    from pycmor.std_lib.dataset_helpers import get_time_label
    time_dim = get_time_label(data)
    if not time_dim:
        for d in data.dims:
            c = data.coords.get(d)
            if c is not None and (c.dtype.kind == "M" or "datetime" in str(c.dtype) or "cftime" in str(c.dtype)):
                time_dim = d
                break
    if not time_dim:
        raise KeyError(f"save_dataset_per_slab_single_file: cannot find time dim in {list(data.dims)}")

    n = data.sizes[time_dim]
    n_slabs = (n + slab_size - 1) // slab_size
    logger.info(
        f"save_dataset_per_slab_single_file: {n} timesteps along '{time_dim}', "
        f"slab_size={slab_size} → {n_slabs} slabs"
    )

    enable_comp = bool(rule.get("netcdf_enable_compression", True))
    codec = rule.get("netcdf_compression_codec") or "blosc_zstd"
    level = int(rule.get("netcdf_compression_level") or 3)
    for var in data.data_vars:
        enc = {}
        if enable_comp:
            if codec == "zlib":
                enc["zlib"] = True
                enc["complevel"] = level
                enc["shuffle"] = True
            else:
                enc["compression"] = codec
                enc["complevel"] = level
                if codec.startswith("blosc"):
                    enc["blosc_shuffle"] = 1
        chs = data[var].chunks
        if chs is not None:
            enc["chunksizes"] = tuple(max(c) for c in chs)
        if str(var).endswith(("_bnds", "_bounds")) or str(var).startswith("bounds_"):
            enc["_FillValue"] = None
        data[var].encoding.update({k: v for k, v in enc.items() if k not in data[var].encoding})

    rule_name = getattr(rule, "name", "var")
    path = out_dir / f"{rule_name}_combined.nc"
    if path.exists():
        path.unlink()

    for i in range(n_slabs):
        s, e = i * slab_size, min((i + 1) * slab_size, n)
        slab = data.isel({time_dim: slice(s, e)})
        if i == 0:
            logger.info(f"save_dataset_per_slab_single_file: creating {path} (slab 1/{n_slabs}, unlimited={time_dim})")
            slab.to_netcdf(path, mode="w", format="NETCDF4", unlimited_dims=[time_dim])
        else:
            logger.info(f"save_dataset_per_slab_single_file: appending slab {i+1}/{n_slabs}")
            slab.to_netcdf(path, mode="a")
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        except Exception as exc:
            logger.debug(f"  → fadvise(DONTNEED) failed for {path}: {exc}")
        del slab
        gc.collect()
    return None
