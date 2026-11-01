"""extract_bottom: the deepest wet level on both FESOM meshes in use.

DARS2 (HR) stores depth_lev as the 0-based index of the deepest wet level,
CORE3 (LR) as the number of wet levels; rules on CORE3 say so with
depth_lev_counts_levels. Either way the step must return the last finite
value of each column.
"""

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402


class _Rule:
    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


def _column_data(last_wet, nz=6):
    """(time, nod2, nz1) with value 100*level + node, NaN below last_wet."""
    nod = last_wet.size
    lev = np.arange(nz)[None, :]
    vals = 100.0 * lev + np.arange(nod)[:, None]
    vals[lev > last_wet[:, None]] = np.nan
    return xr.DataArray(np.stack([vals, vals + 0.5]), dims=("time", "nod2", "nz1"), name="temp")


@pytest.mark.parametrize("counts_levels", [False, True], ids=["dars2", "core3"])
def test_picks_last_wet_level(tmp_path, counts_levels):
    last_wet = np.array([0, 2, 5, 3])
    depth_lev = last_wet + 1 if counts_levels else last_wet
    mesh = tmp_path / "mesh.nc"
    xr.Dataset({"depth_lev": (("ncells",), depth_lev)}).to_netcdf(mesh)
    data = _column_data(last_wet)

    kw = {"depth_lev_counts_levels": True} if counts_levels else {}
    out = cs.extract_bottom(data, _Rule(grid_file=str(mesh), **kw))

    assert out.dims == ("time", "nod2")
    want = 100.0 * last_wet + np.arange(last_wet.size)
    np.testing.assert_array_equal(out.isel(time=0).values, want)
    assert np.isfinite(out.values).all()
