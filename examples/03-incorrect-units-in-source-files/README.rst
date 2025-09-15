Dealing with wrong input units
------------------------------

In this example we show how to deal with incorrect units set on the input data. The motivating example comes from the variables
``dissic`` and ``talk`` for oceanic biogeochemistry:

.. code-block:: json
    :caption: cmip6-cmor-tables/Tables/CMIP6_Omon.json

        "dissic": {
            "standard_name": "mole_concentration_of_dissolved_inorganic_carbon_in_sea_water",
            "units": "mol m-3",
        },
        "talk": {
            "standard_name": "sea_water_alkalinity_expressed_as_mole_equivalent",
            "units": "mol m-3",
        },

These two variables need DIC and Talk, which are saved as ``bgc02`` and ``bgc03`` by REcoM. Unfortunately, the ``units`` attribute in the NetCDF
files is wrong, e.g.:

.. code-block:: bash
   :caption: Excerpt of ``ncdump -h bgc02_3d.nc``

    float bgc02(time, nodes_3d) ;
            bgc02:description = "bgc tracer 02" ;
            bgc02:units = "" ;
            bgc02:grid_type = "unstructured" ;
            bgc02:_FillValue = 1.e+30f ;

The actual input unit in both cases is :math:`\frac{mmol}{m^{3}}`, i.e. use ``mult_factor = 1/1e3`` to convert from mmol to mol.

Happily enough, ``pycmor`` provides an easy way to rectify this without needing to edit the source files with something like ``ncatted``. You
can specify the correct units in the ``model_unit`` setting of the ``rule``. Here is an example of how to do that:

.. literalinclude:: ../examples/03-incorrect-units-in-source-files/incorrect_units.yaml
   :language: yaml
   :linenos:
   :emphasize-lines: 31, 49
