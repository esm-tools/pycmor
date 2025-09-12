===========
Quick start
===========

Installation
------------

Installation from source repository::

  git clone https://esm-tools/pycmor.git
  cd pycmor
  pip install pycmor[<extras>]

For more details in installation options, please refer section installation_

Setting up a task to pycmor
-----------------------------

At the heart of ``pycmor`` is the yaml configuration file. ``pycmor`` gathers all
the information it needs to perform CMORization of your data from this file.

The yaml file has 4 sections:
- ``general`` global settings that are applicable to all the rules
- ``pycmor`` settings for controlling the behavior of the tool
- ``rules`` each rule defines parameters per variable.
- ``pipelines`` processing steps to carry out cmorization procress.

For detailed description on this sections, please refer to pycmor_building_blocks_

As an example task to cmorize ``FESOM 1.4``'s ``CO2f`` variable, create a file called ``basic.yaml`` and populate with the following content

  .. code:: yaml

    general:
      cmor_version: "CMIP6"
      CMIP_Tables_Dir: "/Users/pasili001/repos/pycmor/cmip6-cmor-tables/Tables"
      CV_Dir: /Users/pasili001/repos/tmp_rename/cmip6-cmor-tables/CMIP6_CVs
    pycmor:
      warn_on_no_rule: False
      dask_cluster: "local"
      enable_output_subdirs: False
    rules:
      - name: process_CO2f
        inputs:
          - path: /Users/pasili001/sampledata
            pattern: CO2f_fesom_.*nc
        cmor_variable: fgco2
        model_variable: CO2f
        output_directory: .
        variant_label: r1i1p1f1
        experiment_id: piControl
        source_id: AWI-CM-1-1-HR
        model_component: seaIce
        grid_label: gn
        pipelines:
          - default
    pipelines:
      - name: default
        steps:
          - "pycmor.gather_inputs.load_mfdataset"
          - "pycmor.generic.get_variable"
          - "pycmor.timeaverage.compute_average"
          - "pycmor.units.handle_unit_conversion"
          - "pycmor.global_attributes.set_global_attributes"
          - "pycmor.generic.trigger_compute"
          - "pycmor.files.save_dataset"
      - name: partial
        steps:
          - "pycmor.gather_inputs.load_mfdataset"
          - "pycmor.generic.get_variable"
          - "pycmor.units.handle_unit_conversion"

Here is a brief description of each field in each section.

  .. code:: plaintext

    general:
      cmor_version: <- specify CMIP version. i.e., CMIP6 or CMIP7
      CMIP_Tables_Dir: <- path to CMIP tables
      CV_Dir: <- path to CMIP controlled vocabularies
    pycmor:
      warn_on_no_rule: <- Turn on or off warnings (not mandatory)
      dask_cluster: <- Specify the dask cluster to use. i.e., "local" or "slurm"
      enable_output_subdirs: <- if True, creates sub-dirs according to DRS described in CVs
    rules:
      - name: <- any descriptive name like process_CO2f or test_run_CO2f or anything
        inputs:
          - path: <- directory where the source data files are residing
            pattern: <- pattern to match the desired files. example: CO2f_fesom_.*nc
        cmor_variable: <- variable name to map in the CMIP Table. example: fgco2
        model_variable: <- variable name in the source data files. example: CO2f
        output_directory: <- directory where the output is to be written.
        variant_label:   |
        experiment_id:   |
        source_id:       | <- required for populating Global Attributes.
        model_component: |
        grid_label:      |
        pipelines:
          - default <- which pipeline to use. (choose default or partial)
    pipelines:
      - name: default  <- any descriptive name
        steps:
          - "pycmor.gather_inputs.load_mfdataset"
          - "pycmor.generic.get_variable"
          - "pycmor.timeaverage.compute_average"
          - "pycmor.units.handle_unit_conversion"
          - "pycmor.global_attributes.set_global_attributes"ß
          - "pycmor.generic.trigger_compute"
          - "pycmor.files.save_dataset"
      - name: partial
        steps:
          - "pycmor.gather_inputs.load_mfdataset"
          - "pycmor.generic.get_variable"
          - "pycmor.units.handle_unit_conversion"


There is more that can be specified in the configuration file but for
now this is good enough to get started.

Before running the task, it should be possible to validate the config
for a sanity check as follows

.. code:: shell

  ➜ pycmor validate config basic.yaml

To run the task just run the following command

.. code:: shell

  ➜ pycmor process basic.yaml

As the tool is working on the task, a lot of logging information is
printed out to the terminal screen. The same information is also written
to a log file in ``./logs`` directory. There are some useful information
to watch out for in the logs.

- Dask diagnostics dashboard:
  It is quite interesting to look at the
  resource usage by the task in the dashboard. This is available only
  while the task is running. To get to the dashboard search for it in
  the logs

  .. code:: shell

    ➜ grep Dashboard $(ls -rdt logs/pycmor-process* | tail -n 1)
    2025-03-14 06:45:52.825 | INFO     | pycmor.cmorizer:_post_init_create_dask_cluster:192 - Dashboard http://127.0.0.1:8787/status

  The dashboard link ``http://127.0.0.1:8787/status`` almost remains
  the same unless some other dask dashboard is already running on the
  same machine. In this cases, the port number may change. The correct
  port number is recorded in the log file.

  When running the task on a compute node, additional steps may be
  required (like setting up a tunnel) to open the dashboard. Pymor
  provides a convenient function to do that and it is also records in
  the logs. Search for ``ssh`` in the logs

  .. code:: shell

    ➜ grep ssh $(ls -rdt logs/pycmor/process* | tail -n 1)
    pycmor ssh-tunnel --username a270243 --compute-node l10395.lvt.dkrz.de

- checking unit conversion:
  In this example, model variable ``CO2f`` has
  units ``mmolC/m2/d``. The cmor variable ``fgco2`` has units
  ``kg m-2 s-1``. This means there needs to be a conversion factor to
  express moles of Carbon in grams. Pymor detects such units and
  applies the appropriate unit conversion factor. Search for ``molC``
  in the logs

  .. code:: shell

    ➜ grep -i "molC" $(ls -rthd logs/pycmor/process* | tail -n 1 )
    2025-03-13 09:06:37.158 | INFO     | pycmor.units:handle_unit_conversion:148 - Converting units: (CO2f -> fgco2) mmolC/m2/d -> kg m-2 s-1 (kg m-2 s-1)
    2025-03-13 09:06:37.158 | DEBUG    | pycmor.units:handle_chemicals:67 - Chemical element Carbon detected in units mmolC/m2/d.
    2025-03-13 09:06:37.158 | DEBUG    | pycmor.units:handle_chemicals:68 - Registering definition: molC = 12.0107 * g
    2025-03-13 09:06:37.470 | INFO     | pycmor.units:handle_unit_conversion:148 - Converting units: (CO2f -> fgco2) mmolC/m2/d -> kg m-2 s-1 (kg m-2 s-1)

Hopefully, this is good enough as a starting point for using this tool.

As next steps checkout ``examples`` directory for ``sample.yaml`` file which
contains more configuration options and also ``pycmor.slurm`` file which is
used for submitting the job to slurm