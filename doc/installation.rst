============
Installation
============

The package can be installed in several ways. The most straightforward way is to install it from PyPI with::

  pip install pycmor

More details are given below

RECOMMENDED: Inside a Conda Environment
---------------------------------------

This is the recommended way to install the software while we are still in the development phase, since all the requirements will be isolated and you can
easily activate this in your scripts. Start off by creating a new conda environment::

    conda create -n pycmor python=3.10
    conda activate pycmor

Then, you can install the package by running::

    pip install pycmor[<extras>]

Or with a clone::

    git clone https://github.com/esm-tools/pycmor.git
    cd pycmor
    pip install -e .[<extras>]

Note that the ``-e`` switch allows you to edit the source code.

You might also want to add the ``--isolated`` option to the pip command to
ensure that the installation is isolated from other packages in your environment. This can be done as follows::

    pip install --isolated pycmor.[<extras>]

or::

    pip install --isolated -e .[<extras>]

.. note::

  ``[<extras>]`` allows you to install additional packages that might be use for some specific tasks in pycmor. The extras available are:

  * ``dev`` packages useful for development
  * ``fesom`` installs ``pyfesom2`` that is used by some of the steps in the rule that deal with FESOM meshes. If you are working with FESOM data
    you'll probably need this extra

An example of using both the ``dev`` and the ``fesom`` extras is::

    pip install -e .[dev,fesom]

In some systems the extra syntax will confuse the shell, so you might need to add quotes. For example::

    pip install -e ".[dev,fesom]"

On an HPC System
----------------

If you are on an HPC system, you probably don't have root access. In this case, you can install the package in your home directory by running::

    pip install --user pycmor[<extras>]

Or directly from GitHub::

    pip install --user git+https://github.com/esm-tools/pycmor.git[<extras>]

From PyPI
---------

This is the most straightforward way to install the package if you don't need to modify the source code. Just run::

    pip install pycmor[<extras>]

You can also install the latest version from the repository by running::

  pip install git+https://github.com/esm-tools/pycmor.git[<extras>]

If you want to ensure an isolated install and make sure nothing conflicts with other packages you have, and you **do not want to change source code**, you can have a look at
`pipx <https://pipx.pypa.io/stable/>`_.

From conda-forge
----------------

The package is not yet available on conda-forge. We are working on it.

From source
-----------

If you want to modify the source code, you can install the package by cloning the repository and running::

    git clone https://github.com/esm-tools/pycmor.git
    cd pycmor
    python -m pip install -e .[<extras>]

If you need the developer requirements as well (i.e. for running tests), you can install them by running::

    python -m pip install -e .[dev,<extras>]
