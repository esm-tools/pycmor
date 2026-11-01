"""remap_flags_to_data_request: basin codes follow the data request, not the mask file."""

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from pycmor.data_request.variable import CMIP7DataRequestVariable

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402

TABLE_VALUES = "0 1 2 3 4 5 6 7 8 9 10"
TABLE_MEANINGS = (
    "global_land southern_ocean atlantic_ocean pacific_ocean arctic_ocean indian_ocean "
    "mediterranean_sea black_sea hudson_bay baltic_sea red_sea"
)


def _rule(flag_values=TABLE_VALUES, flag_meanings=TABLE_MEANINGS):
    drv = SimpleNamespace(flag_values=flag_values, flag_meanings=flag_meanings)
    return SimpleNamespace(model_variable="basin", cmor_variable="basin", data_request_variable=drv)


def _mask(values):
    return xr.DataArray(
        np.array(values, dtype=np.int32),
        dims="nod2",
        name="basin",
        attrs={
            "flag_values": np.array([1, 2, 3, 10, 11], dtype=np.int32),
            "flag_meanings": "atlantic_ocean pacific_ocean indian_ocean southern_ocean arctic_ocean",
            "standard_name": "region",
        },
    )


def test_codes_follow_the_meaning():
    out = cs.remap_flags_to_data_request(_mask([1, 2, 3, 10, 11, 1]), _rule())
    # atlantic 2, pacific 3, indian 5, southern 1, arctic 4 in the table
    assert out.values.tolist() == [2, 3, 5, 1, 4, 2]
    assert out.dtype == np.int32
    assert out.attrs["flag_values"].tolist() == list(range(11))
    assert out.attrs["flag_meanings"] == TABLE_MEANINGS
    assert out.attrs["standard_name"] == "region"


def test_dataset_in_dataset_out():
    ds = xr.Dataset({"basin": _mask([10, 11])})
    out = cs.remap_flags_to_data_request(ds, _rule())
    assert isinstance(out, xr.Dataset)
    assert out["basin"].values.tolist() == [1, 4]


def test_meaning_unknown_to_the_data_request_is_an_error():
    with pytest.raises(ValueError, match="arctic_ocean"):
        cs.remap_flags_to_data_request(
            _mask([1]), _rule("0 1 2 3", "global_land southern_ocean atlantic_ocean pacific_ocean indian_ocean")
        )


def test_value_without_meaning_is_an_error():
    with pytest.raises(ValueError, match="7"):
        cs.remap_flags_to_data_request(_mask([1, 7]), _rule())


def test_data_request_variable_keeps_the_flags():
    data = {
        "frequency": "fx",
        "modeling_realm": "ocean",
        "standard_name": "region",
        "units": "1",
        "cell_methods": "area: mean",
        "cell_measures": "area: areacello",
        "long_name": "Region Selection Index",
        "comment": "",
        "dimensions": "longitude latitude",
        "out_name": "basin",
        "type": "integer",
        "positive": "",
        "spatial_shape": "XY-na",
        "temporal_shape": "None",
        "flag_values": TABLE_VALUES,
        "flag_meanings": TABLE_MEANINGS,
    }
    drv = CMIP7DataRequestVariable.from_dict(data, compound_name="ocean.basin.ti-u-hxy-u.fx.glb")
    assert drv.flag_values == TABLE_VALUES
    assert drv.flag_meanings == TABLE_MEANINGS
    assert "flag_values" not in drv.attrs
