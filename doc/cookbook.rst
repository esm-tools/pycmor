=====================
The Pycmor Cookbook
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

And you need these units to be properly converted or recognized by Pycmor. In essence, we need to tell
Pycmor more about the physical meaning of the units so that it can convert from one unit to another arbitrarily.
An example is a mass ratio of 0.001. If mass is not specified that is ambiguous because it could also be, for example,
a volume ratio. The ratios of mass and volume are different depending on the density. To help Pycmor to
convert to 0.001 (mass ratio), we need to tell Pycmor that it is a mass ratio and we do this by indicating that
0.001 means g/kg or 10-3 kg kg-1.

Solution
~~~~~~~~

1. First, check if the variable exists in the dimensionless mappings file. The file is typically located at:

   ``<your_pycmor_installation>/src/pycmor/data/dimensionless_mappings.yaml``

   For example: ``/Users/username/Codes/pycmor/src/pycmor/data/dimensionless_mappings.yaml``

   Open this file and search for your variable name (e.g., "sisali") to see if it already exists.

2. If your variable exists but has an empty mapping (or if it's missing entirely), you need to add the appropriate mapping:

   .. code-block:: yaml

      # For salinity variables (with unit "0.001")
      sisali:  # sea_ice_salinity
        "0.001": g/kg

      # For pure dimensionless variables (with unit "1")
      abs550aer:  # atmosphere_absorption_optical_thickness_due_to_aerosol_particles
        "1":

      # For variables with mole fraction units
      co2:  # mole_fraction_of_carbon_dioxide_in_air
        "1e-06":

3. If you have added a new mapping, you can now use it in your regular Pycmor workflow. The cmorize function will automatically
   use the dimensionless mapping to interpret and convert the units correctly.

4. To contribute your dimensionless mappings back to the Pycmor repository:

   a. Fork the Pycmor repository on GitHub: https://github.com/esm-tools/pycmor
   b. Clone your fork and create a branch for your changes
   c. Update the dimensionless_mappings.yaml file with your additions/corrections
   d. Commit your changes with a descriptive message explaining the mappings you've added
   e. Push your changes and create a pull request to the main repository

   Your contributions help improve Pycmor for the entire climate science community!
