"""QC must only touch the files of its own rule: it repacks them in place, and a
native rule and its regridded twin run in different shards at the same time."""

from types import SimpleNamespace

from pycmor.std_lib.qc import _rule_files

TAIL = "AWI-ESM3-4-2-veg-HR_piControl_r1i1p1f1_185201-185212.nc"


def _rule(out_dir, grid_label):
    return SimpleNamespace(
        output_directory=str(out_dir),
        cmor_variable="mlotst",
        compound_name="ocean.mlotst.tavg-u-hxy-sea.mon.glb",
        grid_label=grid_label,
    )


def test_native_and_regridded_rules_do_not_share_files(tmp_path):
    """cli126 y1852: the gr shard repacked mlotst g130 while the native shard
    did, and the file came out with 0 time steps."""
    for grid in ("g130", "g131"):
        d = tmp_path / grid
        d.mkdir()
        (d / f"mlotst_tavg-u-hxy-sea_mon_glb_{grid}_{TAIL}").touch()
    assert [p.parent.name for p in _rule_files(_rule(tmp_path, "g130"))] == ["g130"]
    assert [p.parent.name for p in _rule_files(_rule(tmp_path, "g131"))] == ["g131"]


def test_rule_without_grid_label_keeps_the_old_prefix(tmp_path):
    (tmp_path / f"mlotst_tavg-u-hxy-sea_mon_glb_g130_{TAIL}").touch()
    assert len(_rule_files(_rule(tmp_path, None))) == 1
