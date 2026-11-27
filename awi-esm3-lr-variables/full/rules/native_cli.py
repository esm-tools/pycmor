"""Use pycmor's native sequential pipelines without the outer Prefect task.

89c49bd still decorates CMORizer._process_rule with @task even when both
parallel=False and workflow_backend=native. Calling its .fn keeps the
upstream processing/retry/manifest implementation but needs no API server.
This adapter changes no scientific steps and does not edit the installation.
"""
import faulthandler
import dask
from pycmor.core.cmorizer import CMORizer
from pycmor.cli import cli

# Also tolerate upstream replacing the task with a plain function in future.
CMORizer._process_rule = staticmethod(getattr(CMORizer._process_rule, 'fn', CMORizer._process_rule))

if __name__ == '__main__':
    faulthandler.enable()
    # parallel=False only serializes rules, not xarray/Dask array evaluation.
    # Keep NetCDF/HDF5 open/read/close on one thread in this shared workflow.
    # Retry recipes additionally set enable_dask=False to avoid an unused
    # distributed cluster and its background threads. Values/rules are unchanged.
    with dask.config.set(scheduler='synchronous', num_workers=1):
        print('LR CMOR launcher: synchronous single-threaded array evaluation', flush=True)
        cli()
