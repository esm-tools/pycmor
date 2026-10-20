AWI-ESM3 LR CMORization
======================

Directory structure
-------------------

The 17 tier directories mirror ``awi-esm3-veg-hr-variables``:
``core_*``, ``lrcs_*``, ``veg_*``, ``extra_*`` and ``cap7_*``.
Their YAML files are the reusable LR variable definitions. ``rules`` lists
enabled mappings; ``not_enabled`` explicitly records HR products not yet
enabled for this LR diagnostic profile. An empty tier is intentional, not
permission to use HR conversions or invent unavailable diagnostics.

Each tier directory also holds a second file, ``cmip7xml_awiesm3-lr_*.yaml``.
These are the full HR rule set adapted to TCO95/CORE3, plus the rules that
were switched off for HR and can come back for LR
(``notes/LR_reactivation_plan.md``). They target runs written with the
CMIP7 XIOS output of v3.5.1 (``oifs.cmip7_cmor_output`` and
``fesom.cmip7_cmor_output``), not the raw-monthly archive this workflow
reads. They run through the HR machinery with ``MODEL=lr``
(``examples/repoint_hr_year.py``, ``examples/submit_hr_year_shards.sh``).
``profile.yaml`` does not list them, so this workflow never loads them,
and the ``MODEL=lr`` glob only matches the ``cmip7xml_`` prefix.

``obgc/``, ``co2/`` and ``icebergs_cavities/`` hold the Data Request
v1.2.2.5 lists for ocean biogeochemistry, the carbon cycle and icebergs
and ice-shelf cavities, with size estimates per model year. No rules yet.

``profile.yaml`` contains shared pipelines, defaults and the tier-file list.
``campaigns/*.yaml`` contains experiment settings: paths, requested years,
metadata, grids and Slurm resources. No leg length is configured.

Only reusable workflow files belong here. Generated recipes and run state
are written outside this source directory, under the campaign's ``state_dir``.
The development recipes and one-off preparation/retry scripts have been
removed. There is no implicit practice-run mode: every ``campaign.py``
command requires a campaign YAML.

The remaining support files have these roles:

* ``start_campaign.sh`` and ``campaign.py`` are the user entry points.
* ``experiment_campaign.py`` manages preparation, submission, restarts,
  validation records and final output.
* ``discovery.py`` identifies complete source years across archive legs.
* ``run_experiment_driver.sh`` and ``run_experiment_task.sh`` are the
  Slurm controller and array-worker entry points.
* ``run_recipe.sh`` sets up the worker environment; ``run_recipe.py`` runs
  pycmor and records the code, recipe and input hashes.
* ``validate_outputs.py`` independently checks generated data against inputs.

The environment, CMIP metadata and three audited grid files are external
prerequisites, referenced by the campaign YAML. They remain under
``cmorization_LR/venv``, ``cmorization_LR/metadata`` and
``cmorization_LR/cache``. The old experiment-specific grid-generation
script is not needed to run campaigns using these grids. A different grid
requires a separately reviewed grid file and scientific profile.

Start a full experiment
-----------------------

For the prepared v3.5.1-cc example::

    cd /work/ab0995/a270270/cmorization_LR/pycmor/awi-esm3-lr-variables
    bash start_campaign.sh campaigns/from_albedo_yearly.yaml

This submits a controller, not the climate model. Before launching, inspect
the archive coverage with::

    /work/ab0995/a270270/cmorization_LR/venv/bin/python campaign.py inspect campaigns/from_albedo_yearly.yaml

The call chain is::

    start_campaign.sh
      campaign.py start <campaign.yaml>
        Slurm controller: campaign.py drive <pinned campaign.yaml>
          discover complete years; prepare recipes; submit task array
            run_experiment_task.sh
              campaign.py task
                run_recipe.sh -> run_recipe.py -> pycmor process
                                             -> validate_outputs.py
                promote validated files; record completion

Generated recipes, immutable code copies, attempts, coverage and validation
reports live in the configured ``state_dir``, not in the source tier folders.
Successful files are moved into ``output_root/<realm>_<year>/``; the three
fixed fields have separate directories and run only once per campaign.
State and output must be on the same filesystem so promotion is an atomic
rename. Publication here means moving validated files into the configured
output directory, not uploading anything or certifying CMIP compliance.

To use another experiment, copy the example campaign YAML and change
``name``, ``run_root``, ``state_dir``, ``output_root``, ``first_year``,
``last_year`` and experiment-specific ``inherit`` metadata. Keep the
scientific profile and grids only if they actually match that experiment.
Campaign settings and code are pinned on first preparation/submission.
Subsequent source-code edits do not change a running campaign. A different
configuration or scientific fix requires a new state/output location.

Leg discovery and restarts
-------------------------

One-, five-, ten-, twenty-year and mixed-length legs use the same workflow.
It searches the archived ``outdata/oifs``, ``outdata/fesom`` and
``outdata/lpj_guess`` trees recursively and reads the actual time/Year
columns. NetCDF and LPJ files may contain multiple years; each task selects
one calendar year. Filename selectors are defined in the tiers. Alternate
archive roots can be set with ``input_roots``; alternate selectors can be
set per rule with ``input_patterns`` or in a reviewed tier mapping.

A year is released only when all requested realms have complete records,
matching grids and expected units. Overlapping records, missing months,
duplicate cells, incompatible calendars and recently modified files block
that year. Source files are hashed before/after processing and never edited.
Only the archived outdata tree is used, never active model work directories.

``follow: true`` keeps checking for future archived years until the requested
last year is validated; the controller renews its allocation before its
time limit. ``follow: false`` finishes available work and reports missing
years instead of claiming the whole experiment succeeded. The same start
command resumes a campaign: validated tasks are skipped, active jobs are
not submitted twice, and failures get a new isolated attempt directory up
to ``max_attempts``. No attempt overwrites previous output. An uncertain
submission is stopped for reconciliation rather than blindly submitted again.

Check status with::

    /work/ab0995/a270270/cmorization_LR/venv/bin/python campaign.py report campaigns/from_albedo_yearly.yaml

For a bounded first-year validation before starting the controller::

    /work/ab0995/a270270/cmorization_LR/venv/bin/python campaign.py submit campaigns/from_albedo_yearly.yaml --only atmos_2190 ocean_2190 seaice_2190 land_2190 fx_areacella_atmos fx_areacella_lpj fx_areacello

Scope
-----

This is the opt-in TCO95L91 / CORE3 raw-monthly workflow.
It reads experiments without modifying them. REcoM and atmospheric CO2
products are excluded. The current catalog contains 195 supported products
per year and three fixed-grid area products (978 files for five years;
5853 for thirty years).
This does not claim that every HR data-request product can be made
from the archived monthly LR diagnostics.

Input formats
-------------

OpenIFS and FESOM diagnostics are read from NetCDF (``.nc``) files.
LPJ-GUESS diagnostics are read directly from whitespace-delimited text
tables (``.out``); these are model data, not Slurm job logs. No preliminary
NetCDF conversion is required. REcoM diagnostics are also NetCDF, but are
not enabled in this campaign. All CMORized products are NetCDF files.

Output is written under::

    /work/ab0995/a270270/cmorization_LR/output/

The directory name is not a publication-readiness status.
``LR-PRACTICE`` is deliberately not a registered source identifier.
Source, experiment, member and grid metadata need scientific/team
approval before publication. Formal CF/WCRP checker results are retained,
including failures; the workflow does not hide them or treat numerical
validation as certification for publication.

Implementation
--------------

``src/pycmor/awiesm3_lr.py`` supplies strict monthly-time checks, linear
diagnostic combinations, native CORE3 layer interfaces and bottom-cell
selection, LPJ monthly/annual/land-use readers and fixed-area loading.
The shared NetCDF writer keeps decoded dates until encoding at write
time, including when splitting output by year. A one-record annual
dataset is saved without reopening its text source to find file split
points. Calendar-based file spans are compared as complete periods,
independent of today's date; actual time bounds retain leap days.

Conversions follow the archived data and model implementation:

* OIFS monthly accumulated flux fields represent 3600-second model-step
  accumulations, so flux conversion divides by 3600, not the XIOS sampling
  period. Linear combinations use float64 before arithmetic.
* FESOM freshwater flux is positive upward, in metres per second after
  division by the model's water density. ``wfo = -1025 * fw``.
* ``hfds = -fh``. Velocity output already has geographic vector rotation.
* Vertical velocity uses both adjacent archived interfaces; no artificial
  bottom zero is inserted. Bottom properties use ``depth_lev - 2``.
* ``sieqthick`` is the whole-cell equivalent ice thickness, not thickness
  divided by ice concentration. Zero-ice filling excludes shelf cavities.
* CORE3 areas use actual model element areas distributed to adjacent nodes.
  LPJ areas are native full-cell areas on the 10157-cell land subset.

Do not derive daily extremes, nonlinear monthly quantities or missing
subdaily products from monthly means. Ocean volume measures are not
invented from constant horizontal areas: cavity and partial-cell geometry
require further treatment. The polar polygon checker warning is retained
rather than altering spherical grid geometry to satisfy a planar check.

Each job snapshots its implementation and recipe, hashes its
inputs before and after processing, and records actual output and exit
status under the configured ``state_dir/provenance``. A separate NumPy/netCDF4
validator checks every value and missing mask against source data,
coordinates, exact dates and bounds, and expected product counts. Its
numeric tolerance accounts for five-significant-digit NetCDF quantization.
Slurm success requires numerical validation and unchanged inputs.
