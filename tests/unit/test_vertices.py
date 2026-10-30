"""CMOR naming and encoding of unstructured cell bounds."""

import numpy as np
import xarray as xr

from pycmor.std_lib.chunking import get_encoding_with_chunks
from pycmor.std_lib.files import _normalise_vertices_naming


def _cells(lat_bnds):
    lat = np.asarray(lat_bnds).mean(axis=1)
    return xr.Dataset(
        {"tas": ("cell", np.full(len(lat), 280.0, dtype="float32"))},
        coords={
            "lat": ("cell", lat, {"bounds": "lat_bnds"}),
            "lat_bnds": (("cell", "nvertex"), np.asarray(lat_bnds), {"units": "degrees_north"}),
        },
    )


def test_bounds_are_renamed_to_vertices_latitude():
    ds = _normalise_vertices_naming(_cells([[10.0, 11.0, 12.0], [20.0, 21.0, 22.0]]))
    assert ds["vertices_latitude"].dims == ("cell", "vertices")
    assert ds["lat"].attrs["bounds"] == "vertices_latitude"


def test_vertices_are_not_quantized():
    """BitGroom turned the polar corners of g122 from -90 into -90.00024."""
    ds = _normalise_vertices_naming(_cells([[-90.0, -89.6, -89.6], [89.6, 90.0, 89.6]]))
    ds = ds.reset_coords("vertices_latitude")
    enc = get_encoding_with_chunks(ds, chunks={"cell": 2, "vertices": 3})
    assert "quantize_mode" not in enc["vertices_latitude"]
    assert enc["vertices_latitude"]["_FillValue"] is None
    assert enc["tas"]["quantize_mode"] == "BitGroom"
