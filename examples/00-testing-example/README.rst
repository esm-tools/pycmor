====================================
Usage: Running ``pycmor`` on Slurm
====================================

See the ``examples`` directory for a sample configuration file, ``sample.yaml``. This contains
three rules, each of which runs the ``default`` pipeline on a different dataset. To run this, you
can use the provided ``pycmor.slurm`` script, which looks like this:

.. literalinclude:: ../examples/00-testing-example/pycmor.slurm
   :linenos:
   :language: bash

After analysing the configuration file, the script will submit several ``Dask`` worker jobs to
Slurm, and then feed the pipeline jobs to those workers. The script waits for all the jobs to finish
before completing, so if one of the pipelines fails, the rest will still keep going. You can monitor the
progress of the jobs by running ``squeue -u <username>``, and follow in more detail on the Prefect dashboard.

You can run the example via::

  sbatch -A <YOUR ACCOUNT> pycmor.slurm

The ``sample.yaml`` file shows a configuration for an ``AWI-CM 1``
simulation, and processes one set of files, ``fgco2``, which was
called ``CO2f`` in ``FESOM 1``. The default pipeline is used, and
nothing special is done.

If you want to cleanup after your run::

  python cleanup.py

or::

  ./cleanup.py

Monitoring the Dask Progress
============================

``pycmor`` makes heavy use of ``dask``, and ``dask`` provides a dashboard to view the progress, however, you
need to set up SSH tunnels to properly see it from your local computer. As a convenient shortcut, ``pycmor``
has tunneling built into it's command line interface::

  pycmor ssh-tunnel --gateway=<LOGIN_NODE> --username=<USER> --compute-node=<JOB_NODE>

**Or even more convenient!** Search for ``ssh-tunnel`` in your ``slurm-<JOB_ID>.out`` (or in the stdout if you
are running ``pycmor process`` directly from the login node). You should be able to find the precise
command you need to use in your local computer, matching the syntax above.

Note that ``JOB_NODE`` is where your main ``pycmor`` job starts, and **not** one of the dask worker
jobs.

You can also generate the required SSH tunnels by hand. On your local workstation::

  ssh -L 8080:localhost:8080 -L 8080:<COMPUTE_NODE>:8787 <USER>@<SPECIFIC_LOGIN_NODE>

On the login node::

  ssh -L 8080:localhost:8787 <COMPUTE_NODE>
