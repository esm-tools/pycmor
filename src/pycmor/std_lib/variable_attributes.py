"""
Pipeline steps to attach metadata attributes to the xarrays
"""

from typing import Union

import xarray as xr

from ..core.logging import logger
from ..core.rule import Rule


def set_variable_attrs(ds: Union[xr.Dataset, xr.DataArray], rule: Rule) -> Union[xr.Dataset, xr.DataArray]:
    if isinstance(ds, xr.Dataset):
        given_dtype = xr.Dataset
        da = ds[rule.model_variable]
        if rule.model_variable != rule.cmor_variable:
            ds = ds.rename({rule.model_variable: rule.cmor_variable})
            da = ds[rule.cmor_variable]
    elif isinstance(ds, xr.DataArray):
        given_dtype = xr.DataArray
        da = ds
        if da.name != rule.cmor_variable:
            da = da.rename(rule.cmor_variable)
    else:
        raise TypeError("Input must be an xarray Dataset or DataArray")

    # Use the associated data_request_variable to set the variable attributes
    missing_value = rule._pycmor_cfg("xarray_default_dataarray_attrs_missing_value")
    attrs = rule.data_request_variable.attrs.copy()  # avoid modifying original

    # Flag variables (CF flag-type) are integer-valued, so the float
    # default (1e20) does not round-trip through int32. wcrp ATTR001 still
    # demands both missing_value and _FillValue on every variable, so pick
    # the netCDF integer default (NC_FILL_INT = -2147483647) instead of
    # skipping. Basin/siline flag values sit well inside that range.
    is_flag = (
        ("flag_values" in attrs)
        or ("flag_meanings" in attrs)
        or ("flag_values" in da.attrs)
        or ("flag_meanings" in da.attrs)
    )

    _fill = -2147483647 if is_flag else missing_value
    for attr in ["missing_value", "_FillValue"]:
        if attrs.get(attr) is None:
            attrs[attr] = _fill

    skip_setting_unit_attr = rule._pycmor_cfg("xarray_default_dataarray_processing_skip_unit_attr_from_drv")
    if skip_setting_unit_attr:
        attrs.pop("units", None)

    # Remove _FillValue and missing_value from attrs before setting .attrs
    attrs_for_encoding = {}
    for enc_attr in ["_FillValue", "missing_value"]:
        if enc_attr in attrs:
            attrs_for_encoding[enc_attr] = attrs.pop(enc_attr)

    # Drop CMIP7 data-request placeholders. As of v1.2.2.3 some variables
    # carry literal ``--MODEL`` (and similar ``--<ALLCAPS>``) strings in
    # fields that the modelling group is expected to fill in. Writing
    # them verbatim fails the cf §7.2 cell_measures format check. Drop
    # them and let the rule override (if any) populate the real value.
    _placeholders = {k: v for k, v in attrs.items() if isinstance(v, str) and v.startswith("--") and v[2:].isupper()}
    for k in _placeholders:
        logger.warning(
            f"variable_attrs: dropping data-request placeholder "
            f"{k!r}={_placeholders[k]!r} for {rule.cmor_variable!r}"
        )
        attrs.pop(k, None)

    # When cell_measures was specifically the ``--MODEL`` placeholder,
    # substitute the realm default. ``--MODEL`` means "model-specific
    # area; fill in your model's value", and the on-disk convention is:
    #   ocean / seaIce / landIce      -> "area: areacello"
    #   atmos / aerosol / land / atmosChem -> "area: areacella"
    # Without this substitution wcrp ATTR001 reports cell_measures
    # missing on variables whose data request used the placeholder
    # (cli72: siu, siv, sidmasstranx, sistrxdtop and similar). Rules
    # may still override via rule-level cell_measures attr if a
    # different area variable applies (e.g. siitdconc → areacello).
    if "cell_measures" in _placeholders and "cell_measures" not in attrs:
        drv = rule.data_request_variable
        realm = (getattr(drv, "modeling_realm", "") or "").lower().split()
        first_realm = realm[0] if realm else ""
        ocean_like = {"ocean", "seaice", "landice", "ocnbgchem"}
        atmos_like = {"atmos", "aerosol", "land", "atmoschem"}
        if first_realm in ocean_like:
            attrs["cell_measures"] = "area: areacello"
        elif first_realm in atmos_like:
            attrs["cell_measures"] = "area: areacella"
        # If realm is unrecognised, leave cell_measures absent so the
        # downstream check surfaces it rather than mislabel.

    # cf-checker §7.2 uses a single-pair regex
    # ``^(?:area|volume):\s+\w+$`` against cell_measures, which accepts
    # neither the empty string (global integrals / scalars, where the DReq
    # ships ``''``) nor the combined ``area: areacello volume: volcello``
    # form that 3D ocean variables carry.
    #
    #   - empty cell_measures -> drop the attribute entirely. CF allows
    #     absence, and a global integral has no cell area to point at.
    #
    # The combined ``area: X volume: Y`` form used to be trimmed to ``area: X``
    # for the same reason, on the grounds that volcello was not shipped as a
    # sibling. Both halves of that have since stopped holding: volcello is now
    # written (fx, mon and dec), and the trim cost 39 wcrp ATTR004 findings in
    # cli111 because the data request asks for both pairs.
    #
    # The data request value is now written verbatim. This is the form that
    # passes both checkers once the outstanding fix lands: wcrp ATTR004 wants
    # it today, and ioos/compliance-checker#1323 makes cf §7.2 accept multiple
    # pairs as CF 1.11 always allowed. Until that merges the combined form
    # trades 39 ATTR004 findings for 39 cf §7.2 ones; trimming instead would
    # fail ATTR004 permanently, since the DReq asks for both.
    _cm = attrs.get("cell_measures")
    _cm_drop = False
    if isinstance(_cm, str) and _cm.strip() == "":
        attrs.pop("cell_measures", None)
        _cm_drop = True

    # CF §3.1 / UDUNITS: practical salinity unit "psu" is not UDUNITS-
    # recognised. The CMIP convention since CMIP6 is to spell it as a
    # dimensionless scaling factor; CMIP7 registry standardises on
    # ``1E-03`` (uppercase E, two-digit exponent), which UDUNITS accepts.
    # wcrp ATTR004 does a literal string compare against the registry's
    # cf_units, so writing ``1e-3`` (lowercase, one-digit exponent) trips
    # the check even though UDUNITS would accept either. The CMIP7 data
    # request still ships ``psu`` in some compound-unit strings (e.g.
    # ``vsfcorr`` -> ``m s-1 psu``); we substitute it just before
    # applying attrs.
    _u = attrs.get("units")
    if isinstance(_u, str) and "psu" in _u:
        import re as _re

        _new_u = _re.sub(r"\bpsu\b", "1E-03", _u)
        if _new_u != _u:
            logger.info(f"variable_attrs: rewriting non-UDUNITS units {_u!r} -> {_new_u!r}")
            attrs["units"] = _new_u

    logger.info("Setting the following attributes:")
    for k, v in attrs.items():
        logger.info(f"{k}: {v}")
    da.attrs.update(attrs)

    # When the DReq supplied cell_measures='' (canonical for scalar
    # / globally-integrated quantities), we dropped it from ``attrs``
    # above. The source DataArray may still carry an empty-string
    # cell_measures inherited from FESOM/XIOS; strip it here so the
    # saved file has no cell_measures attribute at all, which both
    # cf §7.2 and wcrp ATTR004 accept when the registry has no value.
    if _cm_drop:
        da.attrs.pop("cell_measures", None)

    # Source-inherited units may also be non-canonical even when the DReq
    # supplies no units (skip_setting_unit_attr=True path) or when the
    # substitution above was bypassed. Normalise the on-disk units string
    # in place so the registry literal-compare passes. FESOM salt fields
    # ship ``units = "1e-3"`` (XIOS default); CMIP7 registry expects the
    # ``1E-03`` form. Same applies to ``psu`` strings that survived the
    # source path.
    _on_disk_units = da.attrs.get("units")
    if isinstance(_on_disk_units, str):
        import re as _re

        _normalized = _on_disk_units
        _normalized = _re.sub(r"\bpsu\b", "1E-03", _normalized)
        _normalized = _re.sub(r"\b1[eE]-0?(\d)\b", lambda m: f"1E-0{m.group(1)}", _normalized)
        if _normalized != _on_disk_units:
            logger.info(f"variable_attrs: canonicalising units {_on_disk_units!r} -> {_normalized!r}")
            da.attrs["units"] = _normalized

    # CF §3.1.2 (CF 1.11): variables on an absolute-temperature scale
    # should declare ``units_metadata`` so consumers know the value is
    # measured-temperature, not a temperature difference. The CF
    # standard_names with "_temperature" cover the absolute scale; the
    # earlier ``sn.endswith("temperature")`` test missed compound forms
    # like ``sea_water_potential_temperature_at_sea_floor`` (tob),
    # ``sea_surface_temperature``, ``sea_water_conservative_temperature``.
    sn = (da.attrs.get("standard_name") or "").lower()
    units = (da.attrs.get("units") or "").strip()
    is_temperature_sn = "_temperature" in sn or sn.endswith("temperature") or sn.startswith("temperature_")
    is_abs_temp = (
        (units in {"K", "degK", "kelvin", "Kelvin", "degC", "Celsius"} or is_temperature_sn)
        and "difference" not in sn
        and "anomaly" not in sn
    )
    if is_abs_temp and "units_metadata" not in da.attrs:
        da.attrs["units_metadata"] = "temperature: on_scale"

    # CMIP/CF requires `_FillValue` via encoding and `missing_value` as a CF attribute
    # with matching dtype. xarray casts encoded _FillValue to the variable dtype; we must
    # match that manually for the attribute to avoid dtype-mismatch warnings.
    import numpy as np

    for k, v in attrs_for_encoding.items():
        if k == "_FillValue":
            da.encoding["_FillValue"] = v
        if k == "missing_value":
            try:
                if da.dtype.kind == "i":
                    info = np.iinfo(da.dtype)
                    if not (info.min <= v <= info.max):
                        continue  # value doesn't fit integer dtype; skip attr
                    cast = da.dtype.type(v)
                elif da.dtype.kind == "f":
                    cast = da.dtype.type(v)
                else:
                    cast = np.float32(v)
            except Exception:
                cast = np.float32(v)
            da.attrs["missing_value"] = cast

    if given_dtype == xr.Dataset:
        return ds
    elif given_dtype == xr.DataArray:
        return da
    else:
        raise TypeError("Given data type is not an xarray Dataset or DataArray, refusing to continue!")


# Alias name for the function
set_variable_attributes = set_variable_attrs
