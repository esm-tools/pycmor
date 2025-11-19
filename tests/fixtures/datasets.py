import pytest
import xarray as xr

from tests.utils.constants import TEST_ROOT


@pytest.fixture
def fesom_pi_sst_ds():
    return xr.open_dataset(TEST_ROOT / "data/test_experiments/piControl_on_PI/output_pi/sst.fesom.1948.nc")
