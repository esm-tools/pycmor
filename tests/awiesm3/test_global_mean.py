"""global_mean: tos/sos at hm are area-weighted surface means, not ldiag_cmor's volume means."""

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402
import generate_gr_yaml as gg  # noqa: E402

AREA = np.array([1.0, 2.0, 3.0, 4.0])


class Rule(dict):
    pass


@pytest.fixture
def mesh(tmp_path):
    path = tmp_path / "mesh.nc"
    xr.Dataset({"cell_area": ("nod2", AREA)}).to_netcdf(path)
    return path


def _field(values):
    return xr.DataArray(np.array(values, dtype=np.float32), dims=("time", "nod2"), name="sst", attrs={"units": "degC"})


def test_mean_is_weighted_by_cell_area(mesh):
    got = cs.global_mean(_field([[10.0, 20.0, 30.0, 40.0]]), Rule(grid_file=str(mesh)))
    assert float(got[0]) == pytest.approx((10 + 40 + 90 + 160) / 10)
    assert got.attrs["units"] == "degC"
    assert got.dims == ("time",)


def test_missing_cells_drop_out_of_both_sums(mesh):
    # NaN is land or an ice-shelf cavity: neither its value nor its area counts.
    got = cs.global_mean(_field([[10.0, np.nan, 30.0, np.nan]]), Rule(grid_file=str(mesh)))
    assert float(got[0]) == pytest.approx((10 + 90) / 4)


def test_surface_area_file_replaces_cell_area(mesh, tmp_path):
    diag = tmp_path / "fesom.mesh.diag.nc"
    nod_area = np.array([[1.0, 1.0, 1.0, 1.0], [9.0, 9.0, 9.0, 9.0]])
    xr.Dataset({"nod_area": (("nz", "nod2"), nod_area)}).to_netcdf(diag)
    got = cs.global_mean(_field([[10.0, 20.0, 30.0, 40.0]]), Rule(grid_file=str(mesh), surface_area_file=str(diag)))
    assert float(got[0]) == pytest.approx(25.0)


def test_cell_count_must_match_the_mesh(mesh):
    with pytest.raises(ValueError, match="cells"):
        cs.global_mean(_field([[1.0, 2.0, 3.0]]), Rule(grid_file=str(mesh)))


@pytest.mark.parametrize(
    "path",
    [
        "awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml",
        "awi-esm3-lr-variables/lrcs_ocean/cmip7xml_awiesm3-lr_lrcs_ocean.yaml",
    ],
)
def test_tos_and_sos_global_means_read_the_surface_fields(path):
    d = yaml.safe_load((REPO / path).read_text())
    rules = {r["name"]: r for r in d["rules"]}
    pipelines = {p["name"]: p for p in d["pipelines"]}
    for name, source in (("tos_ga", "sst"), ("sos_ga", "sss")):
        rule = rules[name]
        assert rule["model_variable"] == source
        assert rule["inputs"][0]["pattern"].startswith(source)
        assert rule["pipelines"] == ["global_mean_pipeline"]
        # The gr twin would get lat/lon data the step cannot weight.
        assert gg.pipeline_needs_fesom_mesh(pipelines["global_mean_pipeline"])
