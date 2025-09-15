#!/usr/bin/env python

"""
Upward Ocean Mass Transport (wo)
================================

wo -> wmo cmorization

Step.1 convert nodes to levels
  - wo (time, nodes_3d=3668773)
  - griddes.cell_area(ncells=126859)

  Transform `wo` to (time, level, nodes_2d)
  As nodes_2d and ncells have same dimensional values, cell_area can be applied

Step.2 Apply cell_area calculations

  `wo` * cell_area *  (reference density 𝜌0=1035 kg m−3)
"""

import xarray as xr

import pycmor.fesom_1p4
from pycmor.std_lib.units import ureg


def nodes_to_levels(data, rule):
    mesh_path = rule.get("mesh_path")
    if mesh_path is None:
        raise ValueError(
            "Set `mesh_path` path in yaml config."
            "Required for converting nodes to levels"
        )
    return pycmor.fesom_1p4.nodes_to_levels(data, rule)


def weight_by_cellarea_and_density(data, rule):
    gridfile = rule.get("grid_file")
    if gridfile is None:
        raise ValueError(
            "Set `grid_file` in yaml config."
            "Required for getting cell_area information from the grid file"
        )
    grid = xr.open_dataset(gridfile)
    cellarea = grid["cell_area"]
    density = ureg("1035 kg m-3")
    data = data.pint.quantify() * density
    return (data * cellarea.pint.quantify()).pint.dequantify()
