.. |logo| image:: assets/Pycmor1280x640.png

===============================================
``pycmor``: A Python package to simplify CMOR
===============================================

``pycmor`` is a Python package to simplify the standardization of output into the Climate Model Output Rewriter (CMOR) standard.

.. image:: https://github.com/esm-tools/pycmor/actions/workflows/CI-test.yaml/badge.svg
    :target: https://github.com/esm-tools/pycmor/actions/workflows/CI-test.yaml
.. image:: https://img.shields.io/pypi/v/pycmor.svg
    :target: https://pypi.python.org/pypi/pycmor
    :alt: Latest PyPI version
.. image:: https://readthedocs.org/projects/pycmor/badge/?version=latest
    :target: https://pycmor.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://img.shields.io/github/license/esm-tools/pycmor
    :target: https://pycmor.readthedocs.io/en/latest/?badge=latest
.. image:: https://zenodo.org/badge/DOI/10.5281/zenodo.15530131.svg
    :target: https://doi.org/10.5281/zenodo.15530131

------

|logo|

  "Makes CMOR Simple" :-)

``pycmor`` is designed as a wrapper around various CMORization tools and NetCDF
command line tools to make reformatting data into CMIP6 and CMIP7 compliant format as simple
and flexible as possible.

The package is designed to be modular and extensible, with a plugin system that allows
users to add their own subcommands to the main ``pycmor`` command line interface, as
well as including their own functionality to the standardization pipelines. The package is
also designed to be used as a library, with a simple API that allows users to use the
package in their own scripts.

To get started, you can install it via ``pip``::

    pip install pycmor

For CMIP7 support, install with the optional CMIP7 dependency::

    pip install pycmor[cmip7]

Then you can run the main command line interface. Start out by getting some help::

    pycmor --help


The most basic command you will run is::

    pycmor process <CONFIG_FILE>

More detailed install instructions can be found in the :ref:`installation` section, and usage
is summarized in the usage sections.


Licence
-------

``pycmor`` is licensed under the MIT license. See the LICENSE file for more details.

Contributors
------------

Thank you to all of our contributors!

.. image:: https://contrib.rocks/image?repo=esm-tools/pycmor
   :target: https://github.com/esm-tools/pycmor/graphs/contributors
   :alt: Contributors

Authors
-------

``pycmor`` was developed by the High Performance Computing and Data Processing group at
the Alfred Wegener Institute for Polar and Marine Research, Bremerhaven, Germany. It was
designed by `Paul Gierz <pgierz@awi.de>`_, and written by `Paul Gierz <pgierz@awi.de>`_ and
`Pavan Siligam <pavankumar.siligam@awi.de>`_.
