=====================================
Usage: ``pycmor``'s Building Blocks
=====================================

The ``pycmor`` command line interface (CLI) and its main configuration file has a few basic concepts you should be familiar with before you start using it. This guide
will give you a brief overview of these concepts.

Configuration
-------------

The configuration is the central piece of the ``pycmor`` CLI. It is a YAML file that specifies the behavior of
the CLI. The configuration file is divided into several sections:

1. ``pycmor``: This section contains the configuration for the CLI itself. It specifies the program version, log verbosity, the location of the user configuration file, and the location of the log file.
2. ``general``: This section contains information that will be passed to all pipelines. You will specify the location of the data directory (your model output files),
   the output directory (where re-written data should be stored), the location of the CMOR tables, the location of your model's geometry description file (or files), and
   any other information that may be needed by all pipelines.
3. ``pipelines``: This section contains the configuration for the pipelines. Each pipeline is a sequence of operations that will be applied to the data. You can specify the name of the pipeline, the class
   that implements the pipeline, and any parameters that the pipeline needs.
4. ``rules``: This section contains the configuration for the rules. Each rule describes a set of files needed to produce a CMOR output variable. You must specify the CMOR variable of interest, the input
   patterns to use to find the files, and the pipeline(s) to apply to the files.
5. ``inherit``: This section contains key-value pairs that will be added to all rules, unless the rules already have an attribute
   of this name attached to them.

The handbook section on :ref:`schemas` provides more information about the data
types and structures that are expected in the configuration file.

Steps
-----
The true heart of ``pycmor`` are so-called ``steps``, which are nothing more than Python functions
that take a data as input and return a possible modified dataset as output:

  .. code-block:: python

      def dummy_step(data, rule):
          # Do something with data
          data = ...
          return data

Steps will **always** have the call signature ``def step(data: Any, rule: Rule) -> Any``. The ``data``
parameter is the data that this step will operate on, and could be *anything*, but is typically either
a :py:class:`~xarray.Dataset` or a :py:class:`~numpy.ndarray`. The ``rule`` parameter is a special ``pycmor``
object that contains information about the current data being processed, including it's CMOR name,
the files it comes from, and other valuable information described in the CMIP data requests.

Pipelines
---------

:py:class:`~pycmor.pipeline.Pipeline`'s come in two flavors. The first is a predefined pipeline, which is
attached to the configuration via a ``uses`` directive. In the user configuration file, you would specify it
like this:

  .. code-block:: yaml

      # ... other configuration
      pipelines:
        - name: my_pipeline
          uses: pycmor.pipeline.DefaultPipeline
      # ... other configuration

Alternatively you can define your own pipeline by specifying the steps it should take. Here is an example of a
custom pipeline:

  .. code-block:: yaml

      # ... other configuration
      pipelines:
        - name: my_pipeline
          steps:
            - pycmor.generic.dummy_load_data
            - pycmor.generic.dummy_process_data
            - pycmor.generic.dummy_save_data
      # ... other configuration

Rules
-----

Rules describe which files the program is aware of and how it should process them. Each rule
specifies the files needed to produce **one single** particular CMOR output variable, and minimally has a name,
a CMOR variable, and a list of input patterns. The Rule is also connected to one or more pipelines.
Here's an example of a rule:

  .. code-block:: yaml

      # ... other configuration
      rules:
        - name: my_rule
          cmor_variable: tas
          inputs:
            - path: /some/path/to/files/
              pattern: 'tas_*.nc'
          pipelines:
            - my_pipeline
      # ... other configuration

  .. note::

       If you do not specify a pipeline, the default pipeline will be run!

Inheritance
-----------

Time Units and Calendar Configuration
------------------------------------

When working with time-dependent data, you can specify custom time units and calendar types in your rule configuration. This is particularly useful for historical experiments or when working with models that use non-standard calendars.

Example configuration:

.. code-block:: yaml

    rules:
      - name: historical_experiment
        cmor_variable: "tas"
        # ... other required fields ...

        # Custom time configuration
        time_units: "days since 1850-01-01"  # CF-compliant time units
        time_calendar: "proleptic_gregorian"  # Calendar type

Time Units:
- Must follow the CF convention: "<units> since <reference date>"
- Supported units: days, hours, minutes, seconds, milliseconds, microseconds, nanoseconds
- Example: "days since 1850-01-01", "hours since 2000-01-01 00:00:00"

Calendar Types:
- ``standard`` or ``gregorian``: Standard Gregorian calendar (default)
- ``proleptic_gregorian``: Gregorian calendar extended to dates before 1582-10-15
- ``noleap`` or ``365_day``: No leap years, all years are 365 days
- ``all_leap`` or ``366_day``: All years are leap years (366 days)
- ``360_day``: All years are 360 days divided into 30-day months
- ``julian``: Julian calendar
- ``none``: No calendar

If not specified, the default calendar is "standard" and the time units will be determined from the input data.

Rules can inherit global values. To do so, you should include them in the ``inherit`` section of the configuration file. Here is an example:

  .. code-block:: yaml

      # ... other configuration
      inherit:
        frequency: mon
      # ... other configuration

      rules:
        - name: my_rule
          cmor_variable: tas
          inputs:
            - path: /some/path/to/files/
              pattern: 'tas_*.nc'
          pipelines:
            - my_pipeline

The rule ``my_rule`` will inherit the frequency ``mon`` from the global configuration, and can be accessed in
Python code as ``rule_spec.frequency``. This would be useful when you want global attributes applied to all the rules,
such as institution ID, realization of the simulation, and so on.
