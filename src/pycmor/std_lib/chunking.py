"""
NetCDF chunking strategies for optimizing I/O performance.

This module provides utilities to determine optimal chunk sizes for NetCDF files
based on target chunk size, dimension aspect ratios, and I/O performance considerations.

The implementation is inspired by the dynamic_chunks library:
https://github.com/jbusecke/dynamic_chunks
"""

import itertools
import logging
from typing import Dict, List, Union

import numpy as np
import xarray as xr
from dask.utils import parse_bytes

logger = logging.getLogger(__name__)


class NoMatchingChunks(Exception):
    """Raised when no chunk combination satisfies the constraints."""

    pass


def _maybe_parse_bytes(target_chunk_size: Union[str, int]) -> int:
    """
    Parse byte size from string or return int.

    Parameters
    ----------
    target_chunk_size : Union[str, int]
        Size as integer (bytes) or string like '100MB'

    Returns
    -------
    int
        Size in bytes
    """
    if isinstance(target_chunk_size, str):
        return parse_bytes(target_chunk_size)
    else:
        return target_chunk_size


def get_memory_size(ds: xr.Dataset, chunks: Dict[str, int]) -> int:
    """
    Estimate memory size for a chunk configuration.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    chunks : Dict[str, int]
        Chunk sizes per dimension

    Returns
    -------
    int
        Estimated memory size in bytes (maximum across all variables)
    """
    ds_single_chunk = ds.isel({dim: slice(0, chunk) for dim, chunk in chunks.items()})
    mem_size = max([ds_single_chunk[var].nbytes for var in ds_single_chunk.data_vars])
    return mem_size


def even_divisor_chunks(n: int) -> List[int]:
    """
    Get all values that evenly divide n.

    Parameters
    ----------
    n : int
        Dimension size

    Returns
    -------
    List[int]
        List of chunk sizes that evenly divide n
    """
    divisors = []
    for i in range(1, n + 1):
        if n % i == 0:
            divisors.append(n // i)
    return divisors


def normalize(a: np.ndarray) -> np.ndarray:
    """Convert to a unit vector."""
    return a / np.sqrt(np.sum(a**2))


def similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Calculate Euclidean distance between vectors."""
    return np.sqrt(np.sum((a - b) ** 2))


def calculate_chunks_even_divisor(
    ds: xr.Dataset,
    target_chunk_size: Union[int, str] = "100MB",
    target_chunks_aspect_ratio: Dict[str, int] = None,
    size_tolerance: float = 0.5,
) -> Dict[str, int]:
    """
    Calculate optimal chunks using even divisor algorithm.

    This algorithm finds all possible chunk combinations with even divisors
    and chooses the best fit based on desired chunk aspect ratio and size.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    target_chunk_size : Union[int, str], optional
        Desired chunk size. Can be integer (bytes) or string like '100MB'.
        Default is '100MB'.
    target_chunks_aspect_ratio : Dict[str, int], optional
        Dictionary mapping dimension names to desired aspect ratio of total
        number of chunks along each dimension. A value of -1 prevents chunking
        along that dimension. If None, defaults to preferring time chunking.
    size_tolerance : float, optional
        Chunk size tolerance. Resulting chunk size will be within
        [target_chunk_size*(1-size_tolerance), target_chunk_size*(1+size_tolerance)].
        Default is 0.5 (50%).

    Returns
    -------
    Dict[str, int]
        Target chunk dictionary. Can be passed to ds.chunk() or encoding.

    Raises
    ------
    NoMatchingChunks
        If no chunk combination satisfies the size constraint.

    Examples
    --------
    >>> ds = xr.Dataset({'temp': (['time', 'lat', 'lon'], np.random.rand(100, 180, 360))})
    >>> chunks = calculate_chunks_even_divisor(ds, target_chunk_size='50MB')
    >>> ds_chunked = ds.chunk(chunks)
    """
    target_chunk_size = _maybe_parse_bytes(target_chunk_size)

    # Default aspect ratio: prefer chunking along time dimension
    if target_chunks_aspect_ratio is None:
        target_chunks_aspect_ratio = {}
        for dim in ds.dims:
            if dim in ["time", "t"]:
                target_chunks_aspect_ratio[dim] = 10  # Prefer more chunks in time
            else:
                target_chunks_aspect_ratio[dim] = 1  # Keep spatial dims less chunked

    # Fill in missing dimensions with default (no chunking)
    for dim in ds.dims:
        if dim not in target_chunks_aspect_ratio:
            target_chunks_aspect_ratio[dim] = -1

    logger.info(f"Running dynamic chunking with target size: {target_chunk_size} bytes")
    logger.info(f"Aspect ratio: {target_chunks_aspect_ratio}")

    # Separate chunked and unchunked dimensions
    target_chunks_aspect_ratio_chunked_only = {
        dim: ratio for dim, ratio in target_chunks_aspect_ratio.items() if ratio != -1
    }
    unchunked_dims = [
        dim for dim in target_chunks_aspect_ratio.keys() if dim not in target_chunks_aspect_ratio_chunked_only.keys()
    ]

    # Generate all possible chunk combinations
    possible_chunks = []
    for dim, s in ds.sizes.items():
        if dim in unchunked_dims:
            possible_chunks.append([s])  # Keep dimension unchunked
        else:
            possible_chunks.append(even_divisor_chunks(s))

    combinations = [{dim: chunk for dim, chunk in zip(ds.dims.keys(), c)} for c in itertools.product(*possible_chunks)]

    # Filter by size tolerance
    combination_sizes = [get_memory_size(ds, c) for c in combinations]
    tolerance = size_tolerance * target_chunk_size
    combinations_filtered = [
        c for c, s in zip(combinations, combination_sizes) if abs(s - target_chunk_size) < tolerance
    ]

    if len(combinations_filtered) == 0:
        raise NoMatchingChunks(
            f"Could not find any chunk combinations satisfying the size constraint "
            f"(target: {target_chunk_size} bytes, tolerance: {size_tolerance}). "
            f"Consider increasing tolerance or adjusting target_chunk_size."
        )

    # Find combination closest to desired aspect ratio
    if len(target_chunks_aspect_ratio_chunked_only) > 0:
        combinations_filtered_chunked_only = [
            {dim: chunk for dim, chunk in c.items() if dim not in unchunked_dims} for c in combinations_filtered
        ]

        dims_chunked_only = list(target_chunks_aspect_ratio_chunked_only.keys())
        shape_chunked_only = np.array([ds.sizes[dim] for dim in dims_chunked_only])

        ratio = [
            shape_chunked_only / np.array([c[dim] for dim in dims_chunked_only])
            for c in combinations_filtered_chunked_only
        ]
        ratio_normalized = [normalize(r) for r in ratio]

        target_ratio_normalized = normalize(
            np.array([target_chunks_aspect_ratio_chunked_only[dim] for dim in dims_chunked_only])
        )
        ratio_similarity = [similarity(target_ratio_normalized, r) for r in ratio_normalized]

        combinations_sorted = [c for _, c in sorted(zip(ratio_similarity, combinations_filtered), key=lambda a: a[0])]

        best_chunks = combinations_sorted[0]
    else:
        # All dimensions unchunked, just return first combination
        best_chunks = combinations_filtered[0]

    logger.info(f"Selected chunks: {best_chunks}")
    logger.info(f"Estimated chunk size: {get_memory_size(ds, best_chunks)} bytes")

    return best_chunks


def calculate_chunks_iterative(
    ds: xr.Dataset,
    target_chunk_size: Union[int, str] = "100MB",
    target_chunks_aspect_ratio: Dict[str, int] = None,
    size_tolerance: float = 0.5,
) -> Dict[str, int]:
    """
    Calculate optimal chunks using iterative ratio increase algorithm.

    This algorithm starts with a normalized chunk aspect ratio and iteratively
    scales it until the desired chunk size is reached.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    target_chunk_size : Union[int, str], optional
        Desired chunk size. Can be integer (bytes) or string like '100MB'.
        Default is '100MB'.
    target_chunks_aspect_ratio : Dict[str, int], optional
        Dictionary mapping dimension names to desired aspect ratio. A value of -1
        prevents chunking along that dimension. If None, defaults to preferring
        time chunking.
    size_tolerance : float, optional
        Chunk size tolerance. Default is 0.5 (50%).

    Returns
    -------
    Dict[str, int]
        Target chunk dictionary.

    Raises
    ------
    NoMatchingChunks
        If no chunk combination satisfies the size constraint.
    """
    target_chunk_size = _maybe_parse_bytes(target_chunk_size)

    # Default aspect ratio: prefer chunking along time dimension
    if target_chunks_aspect_ratio is None:
        target_chunks_aspect_ratio = {}
        for dim in ds.dims:
            if dim in ["time", "t"]:
                target_chunks_aspect_ratio[dim] = 10
            else:
                target_chunks_aspect_ratio[dim] = 1

    # Fill in missing dimensions
    for dim in ds.dims:
        if dim not in target_chunks_aspect_ratio:
            target_chunks_aspect_ratio[dim] = -1

    logger.info(f"Running iterative chunking with target size: {target_chunk_size} bytes")

    def maybe_scale_chunk(ratio, scale_factor, dim_length):
        """Scale a single dimension by a given scaling factor."""
        if ratio == -1:
            return dim_length
        else:
            max_chunk = dim_length / ratio
            scaled_chunk = max(1, round(max_chunk / scale_factor))
            return scaled_chunk

    def scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, scale_factor):
        """Scale all chunks by a factor."""
        scaled_normalized_chunks = {
            dim: maybe_scale_chunk(ratio, scale_factor, ds.sizes[dim])
            for dim, ratio in target_chunks_aspect_ratio.items()
        }
        return scaled_normalized_chunks

    max_chunks = scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, 1)
    max_scale_factor = max(max_chunks.values())

    scale_factors = np.arange(1, max_scale_factor + 1)
    sizes = np.array(
        [get_memory_size(ds, scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, sf)) for sf in scale_factors]
    )

    size_mismatch = abs(sizes - target_chunk_size)
    optimal_scale_factor = [sf for _, sf in sorted(zip(size_mismatch, scale_factors))][0]

    optimal_target_chunks = scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, optimal_scale_factor)
    optimal_size = get_memory_size(ds, optimal_target_chunks)

    lower_bound = target_chunk_size * (1 - size_tolerance)
    upper_bound = target_chunk_size * (1 + size_tolerance)

    if not (optimal_size >= lower_bound and optimal_size <= upper_bound):
        raise NoMatchingChunks(
            f"Could not find any chunk combinations satisfying the size constraint "
            f"(target: {target_chunk_size} bytes, tolerance: {size_tolerance}). "
            f"Consider increasing tolerance or adjusting target_chunk_size."
        )

    logger.info(f"Selected chunks: {optimal_target_chunks}")
    logger.info(f"Estimated chunk size: {optimal_size} bytes")

    return optimal_target_chunks


def calculate_chunks_simple(
    ds: xr.Dataset,
    target_chunk_size: Union[int, str] = "100MB",
    prefer_time_chunking: bool = True,
) -> Dict[str, int]:
    """
    Calculate chunks using a simple heuristic approach.

    This is a simpler, faster algorithm that chunks primarily along the time
    dimension (if present) to optimize for typical climate data access patterns.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    target_chunk_size : Union[int, str], optional
        Desired chunk size. Default is '100MB'.
    prefer_time_chunking : bool, optional
        If True, preferentially chunk along time dimension. Default is True.

    Returns
    -------
    Dict[str, int]
        Target chunk dictionary.

    Examples
    --------
    >>> ds = xr.Dataset({'temp': (['time', 'lat', 'lon'], np.random.rand(100, 180, 360))})
    >>> chunks = calculate_chunks_simple(ds, target_chunk_size='50MB')
    """
    target_chunk_size = _maybe_parse_bytes(target_chunk_size)

    # Estimate bytes per element (assume float64 as default)
    bytes_per_element = 8
    for var in ds.data_vars:
        if hasattr(ds[var], "dtype"):
            bytes_per_element = max(bytes_per_element, ds[var].dtype.itemsize)

    # Calculate total elements per chunk
    target_elements = target_chunk_size // bytes_per_element

    chunks = {}

    # Find time dimension
    time_dim = None
    for dim in ds.dims:
        if dim in ["time", "t", "Time"]:
            time_dim = dim
            break

    if prefer_time_chunking and time_dim is not None:
        # Chunk along time, keep other dimensions full
        time_size = ds.sizes[time_dim]

        # Calculate spatial size
        spatial_elements = 1
        for dim in ds.dims:
            if dim != time_dim:
                spatial_elements *= ds.sizes[dim]

        # How many time steps fit in target chunk?
        time_chunk = max(1, min(time_size, target_elements // spatial_elements))

        chunks[time_dim] = time_chunk
        for dim in ds.dims:
            if dim != time_dim:
                chunks[dim] = ds.sizes[dim]  # Keep full
    else:
        # Distribute chunking across all dimensions proportionally
        total_elements = np.prod([ds.sizes[dim] for dim in ds.dims])
        scale_factor = (target_elements / total_elements) ** (1.0 / len(ds.dims))

        for dim in ds.dims:
            chunks[dim] = max(1, int(ds.sizes[dim] * scale_factor))

    logger.info(f"Simple chunking selected: {chunks}")
    logger.info(f"Estimated chunk size: {get_memory_size(ds, chunks)} bytes")

    return chunks


def get_encoding_with_chunks(
    ds: xr.Dataset,
    chunks: Dict[str, int] = None,
    compression_level: int = 4,
    enable_compression: bool = True,
) -> Dict[str, Dict]:
    """
    Generate encoding dictionary with chunking and compression settings.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    chunks : Dict[str, int], optional
        Chunk sizes per dimension. If None, no chunking is applied.
    compression_level : int, optional
        Compression level (1-9). Default is 4.
    enable_compression : bool, optional
        Whether to enable zlib compression. Default is True.

    Returns
    -------
    Dict[str, Dict]
        Encoding dictionary suitable for xr.Dataset.to_netcdf()

    Examples
    --------
    >>> ds = xr.Dataset({'temp': (['time', 'lat', 'lon'], np.random.rand(100, 180, 360))})
    >>> chunks = calculate_chunks_simple(ds)
    >>> encoding = get_encoding_with_chunks(ds, chunks)
    >>> ds.to_netcdf('output.nc', encoding=encoding)
    """
    encoding = {}

    for var in ds.data_vars:
        var_encoding = {}

        if chunks is not None:
            # Get chunk sizes for this variable's dimensions
            var_dims = ds[var].dims
            var_chunks = tuple(chunks.get(dim, ds.sizes[dim]) for dim in var_dims)
            var_encoding["chunksizes"] = var_chunks

        if enable_compression:
            var_encoding["zlib"] = True
            var_encoding["complevel"] = compression_level

        encoding[var] = var_encoding

    return encoding
