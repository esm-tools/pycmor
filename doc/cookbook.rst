=====================
The PyCMOR Cookbook
=====================

A showcase of some more complicated use cases.

If you'd like to contribute with your own recipe, or ask for a recipe, please open a
documentation issue on `our GitHub repository <https://github.com/esm-tools/pycmor/issues/new>`_.

.. include:: ../examples/01-default-unit-conversion/README.rst
.. include:: ../examples/03-incorrect-units-in-source-files/README.rst
.. include:: ../examples/04-multivariable-input-with-vertical-integration/README.rst

Working with Dimensionless Units
----------------------------------

Problem
~~~~~~~

You need to work with variables that have ambiguous dimensionless units in CMIP6, such as:

* Pure dimensionless quantities (unit: "1")
* Percentage values (unit: "%")
* Ratios (e.g., "kg kg-1")
* Salinity values (unit: "0.001")
* Parts-per-million concentrations (unit: "1e-06")

Solution
~~~~~~~~

1. First, check if the variable exists in the dimensionless mappings file. The file is typically located at:

   ``<your_pycmor_installation>/src/pycmor/data/dimensionless_mappings.yaml``

   Open this file and search for your variable name (e.g., "sisali") to see if it already exists.

2. If your variable exists but has an empty mapping (or if it's missing entirely), you need to add the appropriate mapping:

   .. code-block:: yaml

      # For salinity variables (with unit "0.001")
      sisali:  # sea_ice_salinity
        "0.001": g/kg

3. If you have added a new mapping, you can now use it in your regular PyCMOR workflow.

4. To contribute your dimensionless mappings back to the PyCMOR repository:

   a. Fork the PyCMOR repository on GitHub: https://github.com/esm-tools/pycmor
   b. Clone your fork and create a branch for your changes
   c. Update the dimensionless_mappings.yaml file
   d. Commit your changes with a descriptive message
   e. Push your changes and create a pull request

   Your contributions help improve PyCMOR for the entire climate science community!
