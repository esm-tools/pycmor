import numpy as np
import pint
import pytest
import xarray as xr
from chemicals import periodic_table

from pycmor.core.cmorizer import CMORizer
from pycmor.std_lib.units import handle_chemicals, handle_unit_conversion, ureg

#  input samples that are found in CMIP6 tables and in fesom1 (recom)
allunits = [
    "%",
    "0.001",
    "1",
    "1.e6 J m-1 s-1",
    "1e-06",
    "1e-3 kg m-2",
    "1e3 km3",
    "J m-2",
    "K",
    "K Pa s-1",
    "K s-1",
    "K2",
    "Pa2 s-2",
    "W m^-2",
    "W/m2",
    "W/m^2",
    "day",
    "degC",
    "degC kg m-2",
    "degC2",
    "degree",
    "degrees_east",
    "degrees_north",
    "kg kg-1",
    "kg m-2 s-1",
    "kg m-3",
    "kg s-1",
    "km-2 s-1",
    "m-1 sr-1",
    "m-2",
    "m^-3",
    "m^2",
    "mol/kg",
    "mol/m2",
    "mol m-2",
    "mol m^-2",
    "(mol/kg) / atm",
    "mmol/m2/d",
    "uatm",
    "year",
    "yr",
]


@pytest.mark.parametrize("test_input", allunits)
def test_can_read_units(test_input):
    ureg(test_input)


units_with_chemical_element = [
    "mmolC/(m2*d)",
    "mmolC/d",
    "mmolC/m2/d",
    "mmolN/(m2*d)",
    "mmolN/d",
    "umolFe/m2/s",
]


@pytest.mark.parametrize("test_input", units_with_chemical_element)
def test_handle_chemicals(test_input):
    """Ensures the unit registry can add new units when parsed by ``handle_chemicals``."""
    handle_chemicals(test_input)
    ureg(test_input)


def test_can_handle_simple_chemical_elements(rule_with_mass_units, mocker):
    from_unit = "molC"
    to_unit = "g"
    rule_spec = rule_with_mass_units
    # Mock the getter of the property
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = to_unit
    da = xr.DataArray(10, attrs={"units": from_unit})
    new_da = handle_unit_conversion(da, rule_spec)
    assert new_da.data == np.array(periodic_table.Carbon.MW * 10)
    assert new_da.attrs["units"] == to_unit


def test_can_handle_chemical_elements(rule_with_data_request, mocker):
    rule_spec = rule_with_data_request
    from_unit = "mmolC/m2/d"
    to_unit = "kg m-2 s-1"
    # Mock the getter of the property
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = to_unit
    da = xr.DataArray(10, attrs={"units": from_unit})
    new_da = handle_unit_conversion(da, rule_spec)
    assert np.allclose(new_da.data, np.array(1.39012731e-09))
    assert new_da.attrs["units"] == to_unit


def test_user_defined_units_takes_precedence_over_units_in_dataarray(
    rule_with_data_request,
    mocker,
):
    rule_spec = rule_with_data_request
    to_unit = "g"
    rule_spec.model_unit = "molC"
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = to_unit
    da = xr.DataArray(10, attrs={"units": "kg"})
    # here, "molC" will be used instead of "kg"
    new_da = handle_unit_conversion(da, rule_spec)
    assert new_da.data == np.array(periodic_table.Carbon.MW * 10)
    assert new_da.attrs["units"] == to_unit


def test_without_defining_uraninum_to_weight_conversion_raises_error():
    """Checks that only elements we added are defined"""
    with pytest.raises(pint.errors.UndefinedUnitError):
        ureg("mmolU/m**2/d")


def test_recognizes_previous_defined_chemical_elements():
    assert "mmolC/m^2/d" in ureg


@pytest.mark.skip(reason="No use case for this test (??)")
@pytest.mark.parametrize("from_unit", ["m/s", None, ""])
def test_when_target_units_is_None_overrides_existing_units(rule_with_data_request, from_unit):
    rule_spec = rule_with_data_request
    drv = rule_spec.data_request_variable
    if hasattr(drv, "unit"):
        drv.unit = from_unit
    rule_spec.model_unit = None
    da = xr.DataArray(10, attrs={"units": from_unit})
    new_da = handle_unit_conversion(da, rule_spec)
    assert new_da.attrs["units"] == drv.unit


@pytest.mark.parametrize("from_unit", ["m/s", None])
def test_when_tartget_unit_is_empty_string_raises_error(rule_with_data_request, from_unit):
    rule_spec = rule_with_data_request
    rule_spec.model_unit = ""
    da = xr.DataArray(10, attrs={"units": from_unit})
    with pytest.raises(ValueError):
        handle_unit_conversion(da, rule_spec)


def test_not_defined_unit_checker(rule_with_data_request):
    """Test the checker for unit not defined from the output"""
    rule_spec = rule_with_data_request
    da = xr.DataArray(10, name="var1", attrs={"units": None})

    with pytest.raises(ValueError, match="Unit not defined"):
        new_da = handle_unit_conversion(da, rule_spec)  # noqa: F841


@pytest.mark.skip(reason="The new API does not allow for a DataRequestVariable to not have units")
def test_data_request_missing_unit(rule_with_data_request):
    """Test for missing unit attribute in the data request"""
    rule_spec = rule_with_data_request
    del rule_spec.data_request_variable.units
    da = xr.DataArray(10, name="var1", attrs={"units": "kg m-2 s-1"})

    with pytest.raises(AttributeError, match="DataRequestVariable' object has no attribute 'unit'"):
        new_da = handle_unit_conversion(da, rule_spec)  # noqa: F841


def test_data_request_not_defined_unit(rule_with_data_request, mocker):
    """Test the checker for unit not defined in the data request"""
    rule_spec = rule_with_data_request
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = None

    da = xr.DataArray(10, name="var1", attrs={"units": "kg m-2 s-1"})

    with pytest.raises(ValueError, match="Unit not defined"):
        new_da = handle_unit_conversion(da, rule_spec)  # noqa: F841


def test_dimensionless_unit_missing_in_unit_mapping(rule_with_data_request, mocker):
    """Test the checker for missing dimensionless unit in the unit mappings"""
    rule_spec = rule_with_data_request
    rule_spec.dimensionless_unit_mappings = {"var1": {"0.001": "g/kg"}}
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = "0.1"
    da = xr.DataArray(10, name="var1", attrs={"units": "g/kg"})
    with pytest.raises(KeyError, match="not found in mappings"):
        handle_unit_conversion(da, rule_spec)


def test_units_with_g_kg_to_0001_g_kg(rule_sos, CMIP_Tables_Dir, CV_dir):
    """Test the conversion of dimensionless units"""
    cmorizer = CMORizer(
        pycmor_cfg={
            "parallel": False,
            "enable_dask": False,
        },
        general_cfg={
            "CMIP_Tables_Dir": CMIP_Tables_Dir,
            "cmor_version": "CMIP6",
            "CV_Dir": CV_dir,
        },
        rules_cfg=[rule_sos],
    )
    da = xr.DataArray(10, name="sos", attrs={"units": "g/kg"})
    new_da = handle_unit_conversion(da, cmorizer.rules[0])
    assert new_da.attrs.get("units") == "0.001"
    # Check the magnitude of the data after conversion:
    assert np.equal(new_da.values, 10)


def test_units_with_g_g_to_0001_g_kg(rule_sos, CMIP_Tables_Dir, CV_dir):
    """Test the conversion of dimensionless units"""
    cmorizer = CMORizer(
        pycmor_cfg={
            "parallel": False,
            "enable_dask": False,
        },
        general_cfg={
            "CMIP_Tables_Dir": CMIP_Tables_Dir,
            "cmor_version": "CMIP6",
            "CV_Dir": CV_dir,
        },
        rules_cfg=[rule_sos],
    )
    da = xr.DataArray(10, name="sos", attrs={"units": "g/g"})

    new_da = handle_unit_conversion(da, cmorizer.rules[0])
    assert new_da.attrs.get("units") == "0.001"
    # Check the magnitude of the data after conversion:
    assert np.equal(new_da.values, 10000)


def test_catch_unit_conversion_problem(rule_with_data_request, mocker):
    """Test the checker for unit conversion problem"""
    rule_spec = rule_with_data_request
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)

    # Set the return value for the property
    mock_getter.return_value = "broken_kg m-2 s-1"
    da = xr.DataArray(10, name="var1", attrs={"units": "broken_kg m-2 s-1"})

    # In older versions of pint-xarray, it raises a ValueError with a specific message
    with pytest.raises(ValueError) as excinfo:
        handle_unit_conversion(da, rule_spec)

    # Check that the error message contains the expected text
    assert "Cannot parse units" in str(excinfo.value)
    assert "broken_kg" in str(excinfo.value)


def test_scalar_units_with_g_g_to_0001_g_kg(rule_sos, CMIP_Tables_Dir, CV_dir):
    """Test the conversion of dimensionless units"""
    cmorizer = CMORizer(
        pycmor_cfg={
            "parallel": False,
            "enable_dask": False,
            "warn_on_no_rule": False,
        },
        general_cfg={
            "CMIP_Tables_Dir": CMIP_Tables_Dir,
            "cmor_version": "CMIP6",
            "CV_Dir": CV_dir,
        },
        rules_cfg=[rule_sos],
    )
    da = xr.DataArray(10, name="sos", attrs={"units": "1e3 g/g"})
    new_da = handle_unit_conversion(da, cmorizer.rules[0])
    assert new_da.attrs.get("units") == "0.001"
    # Check the magnitude of the data after conversion:
    assert np.equal(new_da.values, 10_000_000)


def test_scalar_units_1000_kg_to_1000_kg(rule_with_data_request, mocker):
    rule_spec = rule_with_data_request
    mock_getter = mocker.patch.object(type(rule_spec.data_request_variable), "units", new_callable=mocker.PropertyMock)
    # Set the return value for the property
    mock_getter.return_value = "1e3 kg"
    da = xr.DataArray(10, name="var1", attrs={"units": "1e3 kg"})
    new_da = handle_unit_conversion(da, rule_spec)
    assert np.equal(new_da.values, 10)
    assert new_da.units == "1e3 kg"
