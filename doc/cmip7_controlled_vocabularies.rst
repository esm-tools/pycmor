=======================================
CMIP7 Controlled Vocabularies Reference
=======================================

Summary
=======

The CMIP7-CVs repository has been added as a git submodule to provide local access to CMIP7 controlled vocabularies.

Submodule Details
=================

- **Repository**: https://github.com/WCRP-CMIP/CMIP7-CVs.git
- **Branch**: ``src-data`` (contains the actual CV JSON files)
- **Local Path**: ``CMIP7-CVs/`` (at repository root)

Installation
============

For new clones of this repository, initialize the submodule with::

    git submodule update --init CMIP7-CVs

To update the submodule to the latest version::

    git submodule update --remote CMIP7-CVs

Usage
=====

Loading from Vendored Submodule (Recommended)
----------------------------------------------

The :py:class:`~pycmor.core.controlled_vocabularies.CMIP7ControlledVocabularies` class automatically uses the vendored submodule when no path is specified:

.. code-block:: python

    from pycmor.core.controlled_vocabularies import CMIP7ControlledVocabularies

    # Loads from the CMIP7-CVs submodule automatically
    cvs = CMIP7ControlledVocabularies.load()

    # Access experiments
    picontrol = cvs["experiment"]["picontrol"]
    print(picontrol["description"])

    # Access frequencies
    frequencies = cvs["frequency"]
    print(frequencies)  # ['1hr', '3hr', '6hr', 'day', 'mon', ...]

    # Print all experiments
    cvs.print_experiment_ids()

Loading from Custom Path
-------------------------

You can also specify a custom path:

.. code-block:: python

    cvs = CMIP7ControlledVocabularies.load("/path/to/CMIP7-CVs")

Loading from GitHub
-------------------

To load directly from GitHub (without using the local submodule):

.. code-block:: python

    cvs = CMIP7ControlledVocabularies.load_from_git(branch="src-data")

Directory Structure
===================

The CMIP7-CVs submodule contains::

    CMIP7-CVs/
    ├── experiment/          # Individual experiment JSON files
    │   ├── picontrol.json
    │   ├── historical.json
    │   ├── 1pctco2.json
    │   └── ...
    ├── project/            # Project-level CV lists
    │   ├── frequency-list.json
    │   ├── license-list.json
    │   ├── activity-list.json
    │   └── ...
    └── @context           # JSON-LD context file

Key Differences from CMIP6
===========================

CMIP6 Structure
---------------

- Single JSON files per CV type (e.g., ``CMIP6_experiment_id.json``)
- All experiments in one nested dictionary
- Flat directory structure

CMIP7 Structure
---------------

- **One file per entry**: Each experiment is a separate JSON file
- **Directory-based**: Organized in ``experiment/``, ``project/`` subdirectories
- **JSON-LD format**: Uses semantic web standards (``@context``, ``@type``, ``id``)
- **List-based project CVs**: Files like ``frequency-list.json`` contain arrays

Implementation Details
======================

The :py:class:`~pycmor.core.controlled_vocabularies.CMIP7ControlledVocabularies` class provides:

1. **load(table_dir=None)** - Main entry point

   - If ``table_dir`` is None, uses vendored submodule
   - Otherwise loads from specified path

2. **from_directory(directory)** - Loads from local directory

   - Scans ``experiment/`` for individual experiment files
   - Scans ``project/`` for list-based CVs
   - Skips special files (``@context``, ``graph.jsonld``)

3. **load_from_git(tag, branch)** - Loads from GitHub

   - Defaults to ``src-data`` branch
   - Downloads key experiments and project CVs

4. **print_experiment_ids()** - Display helper

   - Shows experiment IDs with start/end years and parents
   - Handles CMIP7 field naming conventions

Testing
=======

Run the test script to verify the setup::

    conda activate pycmor-dev
    python3 test_cmip7_cv_local.py

Expected output:

- ✓ Successfully loaded 72+ experiments
- ✓ Available frequencies, licenses, and other project CVs
- ✓ Experiment details displayed correctly

Data Access Examples
====================

.. code-block:: python

    # Get all experiment IDs
    experiment_ids = list(cvs["experiment"].keys())

    # Get experiment details
    historical = cvs["experiment"]["historical"]
    print(f"Start: {historical['start']}")      # 1850
    print(f"End: {historical['end']}")          # 2021
    print(f"Parent: {historical['parent-experiment']}")  # ['picontrol']

    # Get available frequencies
    frequencies = cvs["frequency"]
    # ['1hr', '1hrcm', '1hrpt', '3hr', '3hrpt', '6hr', '6hrpt',
    #  'day', 'dec', 'fx', 'mon', 'monc', 'monpt', 'subhrpt', 'yr', 'yrpt']

    # Get license information
    licenses = cvs["license"]

Maintenance
===========

To update the CMIP7-CVs to the latest version::

    cd CMIP7-CVs
    git pull origin src-data
    cd ..
    git add CMIP7-CVs
    git commit -m "Update CMIP7-CVs submodule"
