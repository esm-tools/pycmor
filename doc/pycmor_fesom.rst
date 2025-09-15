===============================================
Usage: ``pycmor`` functionality for ``fesom``
===============================================

In addition to the generic pipeline steps, we also include a few that are specific for ``FESOM``.

Regridding to a regular grid
----------------------------

If you would like to regrid your output to a regular 1x1 degree grid, you can use a pipeline step that
will do this. It is automatically checked with data on the ``pi`` mesh, so it should also handle bigger
regridding tasks, but you may still run into memory issues for very large datasets. Open an issue with a
reproducible mimimal example if you run into this!

In your ``Rule`` specification, you need to point to the ``mesh_file`` that you would like to use:

.. code-block:: yaml

  rules:
    - name: regrid
      mesh_file: /path/to/mesh_folder/with/nod2d  # Note, just the folder, not the actual file!
    pipelines:
      - my_pipeline

Then, in your pipeline, you can use the step ``pycmor.fesom.regrid_to_regular``:

.. code-block:: yaml

  pipelines:
    - name: my_pipeline
      steps:
        - pycmor.fesom.regrid_to_regular

