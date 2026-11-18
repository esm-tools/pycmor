"""Imported at startup by every Python process of a shard (run_hr_shard.sh
puts this directory on PYTHONPATH, and Dask workers and the Prefect server
inherit it).

SIGUSR1 makes the process print the stack of every thread to stderr, which is
the shard log. The watchdog sends it before it cancels a hung shard, so the
log shows where the shard was stuck.
"""

import faulthandler
import signal

faulthandler.register(signal.SIGUSR1, all_threads=True)
