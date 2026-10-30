"""The shard watchdog's stack dump: every Python process below the shard shell
prints its threads' stacks, and nothing else gets the signal."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEBUG = REPO / "examples" / "shard_debug"


def test_stacks_of_parent_thread_and_spawned_child(tmp_path):
    hang = tmp_path / "hang.py"
    hang.write_text(
        textwrap.dedent(
            """
        import multiprocessing as mp, threading, time
        def stuck_in_child():
            time.sleep(60)
        def stuck_in_thread():
            time.sleep(60)
        if __name__ == "__main__":
            mp.set_start_method("spawn")
            p = mp.Process(target=stuck_in_child, daemon=True)
            p.start()
            threading.Thread(target=stuck_in_thread, daemon=True).start()
            print("ready", flush=True)
            p.join()
        """
        )
    )
    shard = tmp_path / "shard.sh"
    # /usr/bin/time stands in for the wrapper in run_hr_shard.sh: it must
    # survive the dump, since SIGUSR1 kills anything without a handler.
    shard.write_text(
        textwrap.dedent(
            f"""
        export PYTHONPATH={DEBUG}
        ( until grep -q ready {tmp_path}/log 2>/dev/null; do sleep 0.2; done; sleep 2
          bash {DEBUG}/dump_stacks.sh $$; sleep 2
          kill -0 $(pgrep -f -x '/usr/bin/time .*') 2>/dev/null && echo "time alive" >&2
          pkill -f {hang} ) &
        /usr/bin/time {sys.executable} {hang}
        """
        )
    )
    with open(tmp_path / "log", "w") as log:
        subprocess.run(["bash", str(shard)], stdout=log, stderr=log, timeout=60, env={**os.environ})
    out = (tmp_path / "log").read_text()
    assert "in stuck_in_child" in out
    assert "in stuck_in_thread" in out
    assert "time alive" in out
    assert "User defined signal" not in out
