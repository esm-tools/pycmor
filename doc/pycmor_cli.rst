===========================
Usage: The ``pycmor`` CLI
===========================

``pycmor`` is the command line interface to the ``pycmor`` package. It provides
a simple way to interface with the underlying Python, without needing to know too
many details about what is going on behind the scenes. The CLI is hopefully simple
and is the recommended way to get going.

You can get help with::

  pycmor -h

The CLI is divided into a few subcommands. The main one you will want to use is::

  pycmor process <configuration_yaml>

This will process the configuration file and run the CMORization process.

* ``pycmor develop``: Tools for developers

  - Subcommand ``ls``: Lists a directory and stores the output as a ``yaml``. Possibly
    useful for development work and creating in-memory representations of certain folders.

* ``pycmor externals``: List external program status

  You might want to use ``NCO`` or ``CDO`` in your workflows. The ``pycmor externals`` command
  lists information about the currently found versions for these two programs.

* ``pycmor plugins``: Extending the command line interface

  The user can extend the pycmor CLI by adding their own plugins to the main command. This
  lists the docstrings of those plugins.

  .. note:: Paul will probably throw this out when we clean up the project for release.

* ``pycmor process``: The main command. Takes a yaml file and runs through the CMORization process.

* ``pycmor ssh-tunnel``: Creates port forwarding for Dask and Prefect dashboards. You should provide
  your username and the remote **compute** node, **not the login node**. The tunnels will default to ``8787`` for
  Dask and ``4200`` for Prefect.

  .. important:: You need to run this from your laptop!

* ``pycmor table-explorer``: Opens up the web-based table explorer. This is a simple way to explore the
  tables that are available in the CMIP6 data request.

* ``pycmor validate``: Runs checks on a configuration file.

Command Line Reference
======================

.. click:: pycmor.cli:cli
   :prog: pycmor
   :nested: full
