"""generate_gr_yaml: mesh-derived fx rules with gr_variant get a g131 twin."""

import importlib.util
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]


def _generator():
    spec = importlib.util.spec_from_file_location("generate_gr_yaml", REPO / "examples" / "generate_gr_yaml.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TIER = """
general:
  name: core_ocean
inherit:
  grid_label: g130
  grid_file: /mesh/mesh.nc
pipelines:
  - name: fx_sftof_pipeline
    steps:
      - script://$PYCMOR_HOME/examples/custom_steps.py:load_gridfile
      - script://$PYCMOR_HOME/examples/custom_steps.py:compute_sftof
rules:
  - name: sftof
    inputs:
      - path: /mesh
        pattern: mesh.nc
    compound_name: ocean.sftof.ti-u-hxy-u.fx.glb
    model_variable: sftof
    pipelines: [fx_sftof_pipeline]
    gr_variant:
      grid_file: /mesh/gr_fx.nc
      model_variable: sftof
  - name: orog_like
    inputs:
      - path: /mesh
        pattern: mesh.nc
    compound_name: ocean.thkcello.ti-ol-hxy-sea.fx.glb
    model_variable: thkcello
    pipelines: [fx_sftof_pipeline]
"""


def test_fx_rule_with_gr_variant_gets_a_twin(tmp_path, monkeypatch):
    gen = _generator()
    src, dst = tmp_path / "tier.yaml", tmp_path / "tier_gr.yaml"
    src.write_text(TIER)
    monkeypatch.setattr("sys.argv", ["generate_gr_yaml.py", str(src), str(dst)])
    gen.main()
    out = yaml.safe_load(dst.read_text())
    assert [r["name"] for r in out["rules"]] == ["sftof"]
    twin = out["rules"][0]
    assert twin["grid_file"] == "/mesh/gr_fx.nc"
    assert twin["inputs"] == [{"path": "/mesh", "pattern": r"gr_fx\.nc"}]
    assert twin["pipelines"] == ["gr_fx_pipeline"]
    assert "gr_variant" not in twin
    assert twin["ocean_mask_file"] == ""
    assert out["inherit"]["grid_label"] == "g131"
    assert out["inherit"]["ocean_mask_file"] == gen.GR_FX_FILE
    steps = next(p for p in out["pipelines"] if p["name"] == "gr_fx_pipeline")["steps"]
    assert steps[:2] == [
        "script://$PYCMOR_HOME/examples/custom_steps.py:load_gridfile",
        "pycmor.std_lib.generic.get_variable",
    ]


def test_hr_fx_rules_carry_gr_variants():
    names, files = set(), set()
    for tier in ("core_ocean/cmip7_awiesm3-veg-hr_ocean.yaml", "lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml"):
        d = yaml.safe_load((REPO / "awi-esm3-veg-hr-variables" / tier).read_text())
        variants = [r for r in d["rules"] if "gr_variant" in r]
        names |= {r["compound_name"].split(".")[1] for r in variants}
        files |= {r["gr_variant"]["grid_file"] for r in variants}
    assert names == {"areacello", "sftof", "deptho", "volcello"}
    # the fx twins and the land mask come from one file
    assert files == {_generator().GR_FX_FILE}
