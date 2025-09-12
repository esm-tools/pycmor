====================================
Upward ocean mass transport
====================================

The vertical velocity component 𝑤 (saved as ``wo`` in fesom) is scaled by the
cell area as well as a reference density 𝜌0 = 1035 kg m−3.

``Pycmor`` tool does not have in-built function that can handle such a
computation but it is simple and straightforward to define a custom function and
include it in the pipeline.

.. code-block:: python
   :linenos:

      def weight_by_cellarea_and_density(data, rule):
          gridfile = rule.get("grid_file")
          grid = xr.open_dataset(gridfile)
          cellarea = grid["cell_area"]
          density = ureg("1035 kg m-3")
          data = data.pint.quantify() * density
          return (data * cellarea.pint.quantify()).pint.dequantify()

Notice in the above function, ``density`` is defined as a unit aware
quantity so that the correct dimensionality reduction happens
automatically in the calculations. Also notice the ``ureg``
function. This function is provided by ``pycmor`` tool.  The advantage
of using this function as opposed to ``UnitRegistry`` from ``pint``
library is that it can handle various kinds of unit notations. For
instance, ``pint`` does not recognize the unit notation ``kg m-3``. It
requires units to be defined as ``kg / m**3``.

Another useful function related to units is ``handle_chemicals``. It can detect
chemical elements defined in units and registers them with ``ureg`` so ``pint``
library can recognize the units. For instance, say the units of some variable is
``mmolC/m2/d``. In user defined functions, this can be handled as follows

.. code-block:: python
   :linenos:
      from pycmor.ze.units import ureg, handle_chemicals
      handle_chemicals("mmolC/m2/d")    # this call registers moles of Carbon in grams.
      somevariable = ureg("mmolC/m2/d")

Before using the function ``weight_by_cellarea_and_density``, some additional
steps are required to represent the 3D variable ``wo`` in a 2D space (i.e.,
(nodes_3d) -> (levels, nodes_2d)) since ``cell_area`` from griddes file is in 2D
space. ``Pymorize`` tool has a built-in function (for fesom 1.4 grids) to do
this transformation.

.. code-block:: python
   :linenos:
      def nodes_to_levels(data, rule):
          mesh_path = rule.get("mesh_path")
          if mesh_path is None:
              raise ValueError(
                  "Set `mesh_path` path in yaml config."
                  "Required for converting nodes to levels"
              )
          return pycmor.fesom_1p4.nodes_to_levels(data, rule)

Note: It is possible to use ``pycmor.fesom_1p4.nodes_to_levels`` directly in
the pipeline instead of this wrapped function.

Check out ``wo_cellarea.yaml`` for details on how these functions are inserted
in the pipeline, ``pycmor.wo_cellarea.slurm`` for the job submission
script. The ``wo_cellarea.py`` has these custom functions defined.
