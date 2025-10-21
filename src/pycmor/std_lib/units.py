"""
This module deals with the auto-unit conversion in the cmorization process.
In case the units in model files differ from CMIP Tables, this module attempts to
convert them automatically.

Conversion to-or-from a dimensionless quantity is ambiguous. In this case,
provide a mapping of what this dimensionless quantity represents and that
is used for the conversion. ``data/dimensionless_mappings.yaml`` contains some
examples on how the mapping is written.

:func:`.handle_unit_conversion` is the only function users care about as it handles
the unit conversion of an :class:`xr.DataArray` according to a :class:`.Rule`. The rest
of the functions in this module are support functions.
"""

import re
from typing import Pattern, Union

import cf_xarray.units  # noqa: F401 # pylint: disable=unused-import
import pint
import pint_xarray
import xarray as xr
from chemicals import periodic_table

from ..core.logging import logger
from ..core.rule import Rule

ureg = pint_xarray.unit_registry


def _get_units(
    da: xr.DataArray,
    rule: Rule,
) -> tuple[str, str, str]:
    """
    Get the units from a DataArray and a Rule.

    This function extracts the units from a DataArray and a Rule. If the Rule
    contains a model_units entry, this takes precedence over the units defined
    in the dataset. The function also handles dimensionless units by looking up
    a unit alias in the dimensionless_unit_mappings dictionary of the Rule.

    Parameters
    ----------
    da : xarray.DataArray
        The DataArray to extract the units from.
    rule : dict
        The Rule to extract the units from.

    Returns
    -------
    from_unit : str
        The unit of the DataArray.
    to_unit : str
        The unit to convert the DataArray to.
    to_unit_dimensionless_mapping : str
        The unit alias used for representing the to_unit.
    """
    model_unit = rule.get("model_unit", None)
    from_unit = da.attrs.get("units", None)
    if model_unit is not None:
        logger.info(
            f"user defined units {model_unit!r} takes precedence"
            f" over units defined in dataset {from_unit!r}"
        )
        from_unit = model_unit
    to_unit = rule.data_request_variable.units
    to_unit_dimensionless_mapping = None
    cmor_variable = rule.data_request_variable.variable_id
    dimless_mapping = rule.get("dimensionless_unit_mappings", {})
    if cmor_variable in dimless_mapping:
        try:
            to_unit_dimensionless_mapping = dimless_mapping.get(cmor_variable)[to_unit]
            # Check if the mapping is empty
            if (
                to_unit_dimensionless_mapping is None
                or to_unit_dimensionless_mapping == ""
            ):
                raise ValueError(
                    f"Empty dimensionless mapping found for variable '{cmor_variable}' with unit '{to_unit}'. "
                    f"Please update the {dimless_mapping} file with an appropriate value. "
                    f"See the Pycmor documentation at "
                    f"https://pycmor.readthedocs.io/en/latest/cookbook.html#working-with-dimensionless-units "
                    f"for more information on how to contribute dimensionless mappings."
                )
            logger.info(
                f"unit alias {to_unit_dimensionless_mapping!r} used for representing {to_unit!r}."
                f" see dimensionless variable map for variable {cmor_variable!r}"
            )
        except KeyError:
            raise KeyError(
                f"Dimensionless unit '{to_unit}' not found in mappings for variable '{cmor_variable}'. "
                f"Please add an appropriate mapping to {dimless_mapping}. "
                f"See the Pycmor documentation at "
                f"https://pycmor.readthedocs.io/en/latest/cookbook.html#working-with-dimensionless-units "
                f"for more information on how to contribute dimensionless mappings."
            )
    if from_unit is None:
        raise ValueError(f"Unit not defined: {from_unit=}")
    if not (to_unit or to_unit_dimensionless_mapping):
        raise ValueError(
            f"Unit not defined: {to_unit=}, {to_unit_dimensionless_mapping=}"
        )
    return from_unit, to_unit, to_unit_dimensionless_mapping


def handle_chemicals(
    s: Union[str, None] = None,
    pattern: Pattern = re.compile(
        r"mol(?P<symbol>\w+)",
    ),
) -> None:
    """
    Handle units containing chemical symbols.

    If the unit string contains a chemical symbol (e.g. molNaCl), Pint will
    raise an error because it does not know the definition of the chemical
    symbol. This function attempts to detect chemical symbols in the unit
    string and register a unit definition for it with the aid of chemicals
    package.

    Parameters
    ----------
    s : str
        The unit string to parse.
    pattern : re.Pattern, optional
        The regular expression pattern to use for searching for chemical
        symbols in the unit string. Defaults to a pattern that matches
        "mol" followed by any number of word characters.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the chemical symbol is not recognized.

    See Also
    --------
    ~chemicals.elements.periodic_table: Periodic table of elements
    ~re.compile: `Python's regex syntax <https://docs.python.org/3/library/re.html#regular-expression-syntax>`_.
    """
    if s is None:
        return
    try:
        ureg(s)
    except pint.errors.UndefinedUnitError:
        if match := pattern.search(s):
            d = match.groupdict()
            try:
                element = getattr(periodic_table, d["symbol"])
            except AttributeError:
                raise ValueError(
                    f"Unknown chemical element {d['symbol']} in {match.group()}"
                )
            else:
                logger.debug(f"Chemical element {element.name} detected in units {s}.")
                logger.debug(
                    f"Registering definition: {match.group()} = {element.MW} * g"
                )
                ureg.define(f"{match.group()} = {element.MW} * g")


def handle_scalar_units(
    da: xr.DataArray,
    from_unit: str,
    to: str,
) -> xr.DataArray:
    """
    Convert a DataArray with scalar units from one unit to another.

    This function handles the conversion of a `xarray.DataArray` containing
    scalar units to another unit. The function uses the `pint` library for
    unit conversion. If the initial quantification fails due to an undefined
    unit, it attempts to assign and quantify the unit manually.

    Parameters
    ----------
    da : xarray.DataArray
        The DataArray to be converted.
    from_unit : str
        The unit of the input DataArray.
    to : str
        The unit to convert the DataArray to.

    Returns
    -------
    xarray.DataArray
        The converted DataArray with the new unit.

    Raises
    ------
    ValueError
        If the conversion between the specified units is not possible.
    """
    try:
        new_da = da.pint.quantify(from_unit)
    except ValueError as e:
        assert "scaling factor" in e.args[0]
        _from = ureg(from_unit)
        new_da = da.assign_attrs({"units": _from.units})
        new_da = new_da.pint.quantify() * _from.magnitude
    try:
        return new_da.pint.to(to).pint.dequantify()
    except ValueError as e:
        assert "scaling factor" in e.args[0]
        _to = ureg(to)
        new_da = new_da.pint.to(_to.units)
        new_da = new_da / _to.magnitude
        new_da = new_da.assign_attrs({"units": _to.units})
        return new_da.pint.dequantify()


def convert(
    da: xr.DataArray,
    from_unit: str,
    to_unit: str,
    to_unit_dimensionless_mapping: Union[str, None] = None,
) -> xr.DataArray:
    """
    Convert a DataArray from one unit to another.

    This function handles the conversion of a `xarray.DataArray` from one unit
    to another, taking into account chemical symbols and scaling factor in units.
    It uses the `pint` library for unit conversion and supports aliasing of target units.

    Parameters
    ----------
    da : xarray.DataArray
        The DataArray to be converted.
    from_unit : str
        The unit of the input DataArray.
    to_unit : str
        The unit to convert the DataArray to.
    to_unit_dimensionless_mapping : str, optional
        An alias for the target unit, if any. Defaults to None.

    Returns
    -------
    xarray.DataArray
        The converted DataArray with the new unit.

    Raises
    ------
    ValueError
        If the conversion between the specified units is not possible.
    """

    handle_chemicals(from_unit)
    to = to_unit_dimensionless_mapping or to_unit
    handle_chemicals(to)

    try:
        new_da = da.pint.quantify(from_unit).pint.to(to).pint.dequantify()
    except ValueError as e:
        if "scaling factor" in e.args[0]:
            if str(ureg.Quantity(to).units) != "dimensionless":
                new_da = handle_scalar_units(da, from_unit, to)
            else:
                raise e
        else:
            raise e
    if new_da.units != to_unit:
        new_da = new_da.assign_attrs({"units": to_unit})
    return new_da


def handle_unit_conversion(
    da: xr.DataArray,
    rule: Rule,
) -> xr.DataArray:
    """
    Handle unit conversion of a DataArray according to a Rule.

    This function applies the necessary unit conversion to a DataArray based on
    the units defined in the Rule. It takes into account user-defined units,
    chemical symbols and dimensionless units.

    Parameters
    ----------
    da : xarray.DataArray
        The DataArray to be converted.
    rule : dict
        The Rule containing the units to convert to.

    Returns
    -------
    xarray.DataArray
        The converted DataArray with the new unit.
    """
    if isinstance(da, xr.Dataset):
        model_variable = rule.model_variable
        new_da = da[model_variable]
        from_unit, to_unit, to_unit_dimensionless_mapping = _get_units(new_da, rule)

        if from_unit != to_unit:
            logger.info(f"Converting units: '{from_unit}' → '{to_unit}'")
            if to_unit_dimensionless_mapping:
                logger.debug(
                    f"  Using dimensionless mapping: {to_unit_dimensionless_mapping}"
                )
        else:
            logger.debug(f"Units already correct: '{from_unit}'")

        converted_da = convert(
            new_da, from_unit, to_unit, to_unit_dimensionless_mapping
        )
        da[model_variable] = converted_da
        return da
    else:
        from_unit, to_unit, to_unit_dimensionless_mapping = _get_units(da, rule)

        if from_unit != to_unit:
            logger.info(f"Converting units: '{from_unit}' → '{to_unit}'")
            if to_unit_dimensionless_mapping:
                logger.debug(
                    f"  Using dimensionless mapping: {to_unit_dimensionless_mapping}"
                )
        else:
            logger.debug(f"Units already correct: '{from_unit}'")

        return convert(da, from_unit, to_unit, to_unit_dimensionless_mapping)
