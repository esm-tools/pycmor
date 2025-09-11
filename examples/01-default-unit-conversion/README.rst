========================
Default Unit conversions
========================

In this example, we show how to do "default unit conversions". ``pycmor`` can handle some standard cases, assuming
your NetCDF files are sensible. We show two examples:

1. ``mmolC/m2/d`` --> ``kg m-2 s-2``, as in ``fgco2`` for ``CMIP6_Omon.json``:

.. code:: json

        "fgco2": {
            "frequency": "mon",
            "modeling_realm": "ocnBgchem",
            "standard_name": "surface_downward_mass_flux_of_carbon_dioxide_expressed_as_carbon",
            "units": "kg m-2 s-1",
            "cell_methods": "area: mean where sea time: mean",
            "cell_measures": "area: areacello",
            "long_name": "Surface Downward Mass Flux of Carbon as CO2 [kgC m-2 s-1]",
            "comment": "Gas exchange flux of CO2 (positive into ocean)",
            "dimensions": "longitude latitude time depth0m",
            "out_name": "fgco2",
            "type": "real",
            "positive": "down",
            "valid_min": "",
            "valid_max": "",
            "ok_min_mean_abs": "",
            "ok_max_mean_abs": ""
        },

2. ``µatm`` --> ``Pa``, as in ``spco2`` for ``CMIP6_Omon.json``:

.. code:: json

        "spco2": {
            "frequency": "mon",
            "modeling_realm": "ocnBgchem",
            "standard_name": "surface_partial_pressure_of_carbon_dioxide_in_sea_water",
            "units": "Pa",
            "cell_methods": "area: mean where sea time: mean",
            "cell_measures": "area: areacello",
            "long_name": "Surface Aqueous Partial Pressure of CO2",
            "comment": "The surface called 'surface' means the lower boundary of the atmosphere. The partial pressure of a dissolved gas in sea water is the partial pressure in air with which it would be in equilibrium. The partial pressure of a gaseous constituent of air is the pressure which it alone would exert with unchanged temperature and number of moles per unit volume. The chemical formula for carbon dioxide is CO2.",
            "dimensions": "longitude latitude time depth0m",
            "out_name": "spco2",
            "type": "real",
            "positive": "",
            "valid_min": "",
            "valid_max": "",
            "ok_min_mean_abs": "",
            "ok_max_mean_abs": ""
        },


You can download test data with the provided script::

  $ ./download-example-ata.sh

This extracts ten years of example data for the two variables in questions.

In our configuration file, we don't need to specify anything extra, since the default pipeline can handle these cases.
