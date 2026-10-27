====================
Production campaigns
====================

A campaign cmorizes one experiment of one model run, year by year, from a
single configuration file. It is the way production runs are done for
AWI-ESM3 (HR, and LR with ``model: lr``). For a first test on your own output
see ``GETTING_STARTED.md`` at the repository root instead.

A campaign pins everything that decides what it writes: the pycmor code, the
Python environment, the CMIP7 tables, the controlled vocabularies, the Data
Request and the static input files. Work going on elsewhere (LR development, a
``pip install``, a ``git pull``, a CV release) cannot change a campaign that is
running. Each yearly job checks the pins before it starts and stops if
anything has moved.

The tool lives in ``examples/``: ``start_campaign.sh`` and ``campaign.py``.


Starting a campaign
===================

.. code-block:: bash

   mkdir -p /work/bb1469/$USER/cmor_campaigns/1pctCO2
   cp examples/campaign_1pctCO2.example.yaml /work/bb1469/$USER/cmor_campaigns/1pctCO2/campaign.yaml
   # edit campaign.yaml: output_root, mail, drs_version, code_ref
   examples/start_campaign.sh /work/bb1469/$USER/cmor_campaigns/1pctCO2/campaign.yaml

The first start clones pycmor into the campaign and pins the commit. It also
builds the production environment, which takes about 25 minutes the first
time and is reused by every later campaign with the same profile. Then it
submits the first year. Later starts skip finished years in seconds, so
restarting from the beginning is always safe. A second argument starts from a
given year.


Where things live
=================

``campaign_root`` is the directory campaigns run in. It is never the output.
It comes from ``campaign_root`` in ``campaign.yaml``, else
``$PYCMOR_CAMPAIGN_ROOT``, else ``/work/bb1469/$USER/cmor_campaigns``.

.. code-block:: text

   <campaign_root>/
     envs/<profile>/          production environment, read-only, shared by your campaigns
     <name>/
       campaign.yaml          the only file you write
       code/                  pycmor at the pinned commit
       pinned.json            the pin
       pin_history.log        who pinned what, when
       cmip7-cmor-tables/     CMIP7 tables at the pinned commit
       esgvoc/                controlled vocabularies at the pinned versions
       dreq/                  Data Request metadata at the pinned version
       provenance/            pip freeze, tables commit, input sizes and mtimes
       logs/                  SLURM logs of the chain and the shards
       work/yYYYY/            tier yamls, shards, files in progress, manifests, QC reports
       climatologies/         multi-year accumulators, e.g. pfull climatologies

   <output_root>/             complete years only, in the ESGF directory structure

Do not edit anything except ``campaign.yaml``.


campaign.yaml
=============

.. list-table::
   :header-rows: 1
   :widths: 22 58 20

   * - Key
     - Meaning
     - Pinned
   * - ``name``
     - letters, digits and ``-``; goes into job names
     -
   * - ``run_root``
     - the model run directory
     - yes
   * - ``first_year``, ``last_year``
     - years to cmorize
     - no
   * - ``output_root``
     - where complete years go (the ESGF project)
     - yes
   * - ``drs_version``
     - ``vYYYYMMDD``, the version directory in the output
     - yes
   * - ``experiment``
     - a profile in ``EXPERIMENTS`` of ``examples/repoint_hr_year.py``
     - yes
   * - ``code_ref``
     - branch, tag or commit; resolved to a commit at the first start
     - yes
   * - ``mail``
     - address for failure mails (SLURM ``--mail-user``)
     - no
   * - ``model``
     - ``hr`` (default) or ``lr``
     - yes
   * - ``production``
     - a profile in ``examples/production/``; default ``awiesm3-veg-hr-cmip7-1`` for HR,
       none for LR. ``null`` uses the shared ``conda_env`` and the shared tables and CVs
     - yes
   * - ``inherit``
     - extra tier ``inherit`` values. Keys the experiment profile sets are refused
     - yes
   * - ``with_gr``
     - ``true`` also cmorizes the regridded FESOM output (XIOS 0.5°, grid g131)
       next to the native one; default ``false``
     - yes
   * - ``campaign_root``
     - see above
     -
   * - ``workroot``
     - default ``<campaign>/work``
     -
   * - ``account``
     - SLURM account, default ``ab0246``
     - no
   * - ``driver_attempts``, ``first_year_attempts``, ``max_consecutive_fails``
     - retry policy, defaults 3, 1 and 2 (see below)
     - no

Experiment metadata (``experiment_id``, parent experiment, branch times,
forcing year) is never written in ``campaign.yaml``. It comes from the named
profile in ``repoint_hr_year.py``, so it is reviewed in git and pinned with the
code.

Changing a pinned key needs ``campaign.py setup --repin <campaign.yaml>``; the
next yearly job refuses to run otherwise. Years already done keep what they
were made with, and every manifest records the commit that produced it.


Production profiles
===================

A profile is a directory ``examples/production/<profile>/`` with three files:

``conda-explicit.txt``
   The conda packages (Python and a few base packages) with exact builds and md5.

``requirements.lock``
   Every pip package, as ``name==version`` or ``name @ git+<url>@<commit>``. This
   includes tripyview, compliance-checker and the QC plugins.

``pins.yaml``
   The CMIP7 tables commit, the esgvoc versions, the Data Request version with
   the sha256 of its metadata, and the md5 of every static input the tier yamls
   read (mesh, basin mask, tripyview mesh files, GHG forcing).

``setup`` builds ``<campaign_root>/envs/<profile>`` from the lock files and
accepts it only if ``pip freeze`` then equals the lock. It then clones the
tables, installs the CVs into the campaign's own ``ESGVOC_HOME``, exports the
Data Request metadata and checks its hash, and checks the input files. The
tier yamls are pointed at the campaign's Data Request copy, and the shards do
not refresh the CVs.

A profile's lock files must not change once an environment exists; ``setup``
refuses an environment built from other files. For new versions, copy the
profile under a new name, change it, validate it on one year, and set
``production:`` to the new name.

``awiesm3-veg-hr-cmip7-1`` is the environment that cmorized the validated HR
year (piControl 1852, campaign cli124), with Data Request v1.2.2.5.


What happens during a campaign
==============================

One chain job per year (``pycmor-<name>-y<YEAR>``) checks the pins, submits
the tiers as SLURM arrays and waits for them. It then reads the per-shard
manifests to see whether every rule wrote its output.

* Complete year: the files are moved into ``output_root``, ``YYYY.done`` is
  written and the next year is submitted. ``output_root`` only ever holds
  complete years.
* Some tiers short: only those are resubmitted, up to ``driver_attempts``.
* Every tier short on the first attempt: a shared cause (code, input, disk).
  The year gives up at once.
* The first year of every start gets ``first_year_attempts`` (1), and the
  chain stops if it fails: a broken setup costs one year of node hours.
* ``max_consecutive_fails`` failed years in a row halt the chain.

Each shard runs under a watchdog that cancels it after 90 minutes without
progress. A rare hang of this kind is cured by the retry.

Watching:

.. code-block:: bash

   squeue -u $USER -o '%.12i %.45j %.8T %.10M' | grep --color=never <name>
   ls <campaign>/work/*/*.done | wc -l      # years done
   ls <campaign>/work/*/*.FAILED            # years given up

You get a mail per year that could not be completed. ``work/yYYYY/YYYY.FAILED``
lists every shard and rule without output; the shard logs are in ``logs/``.
Fix the cause and run ``start_campaign.sh`` again. To redo a year on purpose,
delete its ``YYYY.done`` and ``work/yYYYY/cmorized/_manifests/``.


Making changes
==============

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - To
     - Edit
     - Then
   * - extend the years, change mail or attempts
     - ``campaign.yaml``
     - ``start_campaign.sh``
   * - fix a rule or a processing step
     - the tier yaml in ``awi-esm3-veg-hr-variables/`` or ``examples/custom_steps.py``; commit
     - set ``code_ref``, ``setup --repin``
   * - change an experiment's metadata
     - ``EXPERIMENTS`` in ``examples/repoint_hr_year.py``; commit
     - ``setup --repin``
   * - update a package, the tables, the CVs or the Data Request
     - a new profile in ``examples/production/``; commit
     - ``production:``, ``setup --repin``
   * - run somewhere else
     - ``campaign_root`` or ``$PYCMOR_CAMPAIGN_ROOT``
     - a new campaign directory
   * - rerun a few rules by hand
     - ``RULES=a,b`` for ``examples/submit_hr_year_shards.sh``
     -

The defaults that apply to every user (account, conda installation, default
``campaign_root``, default profile per model) are at the top of
``examples/campaign.py``.


Files of the tool
=================

``examples/start_campaign.sh``
   Start or restart a campaign.

``examples/campaign.py``
   ``setup [--repin]``, ``env``, ``verify``, ``apply``; the defaults.

``examples/run_year_chain.sbatch``
   The yearly chain job.

``examples/submit_hr_year_shards.sh``
   Repoint, shard and submit the tiers of one year. Knobs: ``TIER``, ``RULES``,
   ``SHARD_SIZE``, ``WALLTIME``, ``N_WORKERS``, ``MEM_PER_WORKER``, ``WITH_GR``.

``examples/run_hr_shard.sh``
   One shard: resources, Dask, Prefect, watchdog.

``examples/repoint_hr_year.py``
   Experiment profiles (``EXPERIMENTS``) and model identities (``MODELS``).

``awi-esm3-veg-hr-variables/``, ``awi-esm3-lr-variables/``
   The tier yamls of each model.

``examples/custom_steps.py``
   The processing steps the tier yamls use.

``examples/production/<profile>/``
   The pinned software and reference data.

The LR rule set also comes with its own workflow in ``awi-esm3-lr-variables/``
(its README describes it), with its own ``campaign.py``. It is a separate tool;
what this page describes applies to the campaigns started with
``examples/start_campaign.sh``.
