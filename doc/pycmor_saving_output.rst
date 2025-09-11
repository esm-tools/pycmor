====================
Usage: Saving Output
====================

This section of the documentation shows what control you have over output file generation.

Output Location
---------------

Use the key ``output_directory`` to determine where output will be stored:

.. code-block:: yaml

    rules:
      - ... other rule configuration ..
        output_directory: /some/path/on/the/system
        ... other rule configuration ...
      - ... other rule configuration ..
        output_directory: .  # Relative to the current working path
        ... other rule configuration ...
      - ...another rule...

Frequency Grouping
------------------

In the rule section for a particular output, you can control how many timesteps (expressed in days, months, years, etc)
should be contained in each file. You can use the key ``"file_timespan"``:

.. code-block::  yaml

    rules:
      - ... other rule configuration ...
        file_timespan: 50YE
        ... other rule configuration ...
      - ...another rule...

The full list of possibilities for the frequency strings can be found here: https://pandas.pydata.org/docs/user_guide/timeseries.html#offset-aliases

This can also be changed globally and overridden on a per-rule basis. You can either do this in the inherit section, or, in the ``pycmor`` configuration as
the key ``file_timespan``. Note that the ``pycmor`` configuration can also be shared across runs, see the detailed information in :ref:`pycmor.configuration`
