import pytest

from pycmor.data_request.variable import DataRequestVariable


@pytest.fixture
def dr_sos():
    return DataRequestVariable(
        variable_id="sos",
        unit="0.001",
        description="Salinity of the ocean",
        time_method="MEAN",
        table="Omon",
        frequency="Monthly",
        realms="Ocean",
        standard_name="salinity_ocean",
        cell_methods="area: mean where sea",
        cell_measures="area: areacello",
    )
