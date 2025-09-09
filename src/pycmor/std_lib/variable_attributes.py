"""
Pipeline steps to attach metadata attributes to the xarrays
"""

from typing import Union

import xarray as xr

from ..core.logging import logger
from ..core.rule import Rule


def set_variable_attrs(
    ds: Union[xr.Dataset, xr.DataArray], rule: Rule
) -> Union[xr.Dataset, xr.DataArray]:
    if isinstance(ds, xr.Dataset):
        given_dtype = xr.Dataset
        da = ds[rule.model_variable]
    elif isinstance(ds, xr.DataArray):
        given_dtype = xr.DataArray
        da = ds
    else:
        raise TypeError("Input must be an xarray Dataset or DataArray")

    # Use the associated data_request_variable to set the variable attributes
    missing_value = rule._pymor_cfg("xarray_default_missing_value")
    attrs = rule.data_request_variable.attrs.copy()  # avoid modifying original

    # Set missing value in attrs if not present
    for attr in ["missing_value", "_FillValue"]:
        if attrs.get(attr) is None:
            attrs[attr] = missing_value

    skip_setting_unit_attr = rule._pymor_cfg("xarray_skip_unit_attr_from_drv")
    if skip_setting_unit_attr:
        attrs.pop("units", None)

    # Remove _FillValue and missing_value from attrs before setting .attrs
    attrs_for_encoding = {}
    for enc_attr in ["_FillValue", "missing_value"]:
        if enc_attr in attrs:
            attrs_for_encoding[enc_attr] = attrs.pop(enc_attr)

    logger.info("Setting the following attributes:")
    for k, v in attrs.items():
        logger.info(f"{k}: {v}")
    da.attrs.update(attrs)

    # Set encoding for missing values:
    for k, v in attrs_for_encoding.items():
        if k == "_FillValue":
            da.encoding["_FillValue"] = v
        if k == "missing_value":
            # Optionally, also set in encoding, but not needed by default
            da.encoding["missing_value"] = v

    if given_dtype == xr.Dataset:
        return ds
    elif given_dtype == xr.DataArray:
        return da
    else:
        raise TypeError(
            "Given data type is not an xarray Dataset or DataArray, refusing to continue!"
        )


# Alias name for the function
set_variable_attributes = set_variable_attrs
