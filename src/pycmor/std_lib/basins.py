"""Decode basin flags by their declared meanings, not by source numeric IDs.

``BASIN_IDS`` are internal codes for calculations only. A source mask may use
different IDs, including either Arctic/Southern ordering for flags 10 and 11.
Its ``flag_values`` and ``flag_meanings`` are therefore always authoritative.
"""

import numpy as np


BASIN_NAMES = ("atlantic", "pacific", "indian", "arctic", "southern")
BASIN_IDS = (1, 2, 3, 10, 11)
_CANONICAL_IDS = dict(zip(BASIN_NAMES, BASIN_IDS))


def _numeric_codes(values, label):
    """Validate numeric flags without truncating, casting, or changing them."""
    if values is None:
        raise ValueError(f"Missing {label}")
    if np.ma.isMaskedArray(values) and np.any(np.ma.getmaskarray(values)):
        raise ValueError(f"{label} must not contain masked values")
    array = np.asarray(values)
    if array.ndim != 1:
        raise ValueError(f"{label} must be one-dimensional")
    if array.dtype.kind not in "iuf":
        raise ValueError(f"{label} must contain numeric integer values")
    if not np.all(np.isfinite(array)):
        raise ValueError(f"{label} must contain finite integer values")
    if array.dtype.kind == "f" and not np.all(array == np.floor(array)):
        raise ValueError(f"{label} must contain integer values")
    return array


def basin_flag_mapping(flag_values, flag_meanings):
    """Return source ID to canonical basin name from complete CF metadata.

    Exactly the five supported basins must be declared, each once, alongside
    unique finite integer flags. Meanings are case-insensitive and may include
    the ``_ocean`` suffix. Missing metadata is an error, never an invitation to
    infer a source encoding from the numeric flags.
    """
    flags = _numeric_codes(flag_values, "basin flag_values")
    if not isinstance(flag_meanings, str) or not flag_meanings.strip():
        raise ValueError("Missing or invalid basin flag_meanings")
    names = [name.lower().removesuffix("_ocean") for name in flag_meanings.split()]
    if len(flags) != len(names):
        raise ValueError("basin flag_values and flag_meanings must have matching lengths")
    source_ids = [int(value) for value in flags]
    if len(set(source_ids)) != len(source_ids):
        raise ValueError("basin flag_values must be unique")
    if len(set(names)) != len(names):
        raise ValueError("basin flag_meanings must be unique")
    if set(names) != set(BASIN_NAMES):
        raise ValueError(
            "basin flag_meanings must declare exactly the five known basins: "
            + ", ".join(BASIN_NAMES)
        )
    return dict(zip(source_ids, names))


def canonical_basin_ids(values, flag_values, flag_meanings):
    """Return independent one-dimensional integer codes in internal order.

    Source values and metadata are never modified. Every source value must be
    finite, integral, unmasked, and declared by the validated flag metadata.
    The result uses ``BASIN_IDS`` paired with ``BASIN_NAMES``; it is not a
    replacement for the source file's flag encoding or its metadata.
    """
    mapping = basin_flag_mapping(flag_values, flag_meanings)
    source = _numeric_codes(values, "basin values")
    unique, inverse = np.unique(source, return_inverse=True)
    # Python integer keys avoid NumPy promoting mixed integer/float comparisons
    # to floating point, which could equate distinct sufficiently large IDs.
    source_ids = [int(value) for value in unique]
    unknown = [value for value in source_ids if value not in mapping]
    if unknown:
        raise ValueError(f"basin values contain undeclared IDs: {unknown}")
    canonical = np.array([_CANONICAL_IDS[mapping[value]] for value in source_ids], dtype=np.int64)
    return canonical[inverse]
