#!/usr/bin/env python3
"""Fork native pycmor conversions from one interpreter that has already imported pycmor.

Starting ``python native_cli.py process recipe.yaml`` costs about five seconds
before the recipe is even read, nearly all of it importing pycmor, prefect,
xarray and pint. An array task converting several products pays that once:

* :class:`ForkLauncher` (used by the workers) starts this file as a separate
  interpreter, with the first product's environment and working directory and
  the same ``sys.path`` a direct launch of ``native_cli.py`` gets.
* That interpreter executes ``native_cli.py`` once without its ``__main__``
  block, which imports pycmor and applies the launcher's adapter.
* For each product it forks. The child takes over the product's environment,
  working directory, argv and log (stdout and stderr), then executes the same
  ``native_cli.py`` as ``__main__``, exits through normal interpreter shutdown
  and reports its exit status, exactly like the subprocess it replaces.

Each product therefore still runs in its own process: a crash, an OOM kill or
a timeout ends that product alone, and nothing a conversion does stays behind
for the next one. Only process state created at import time is shared, and the
few pieces of it that depend on per-attempt settings are re-established in the
child (see :func:`_reset_after_fork`). If the preloaded interpreter cannot be
started, conversions fall back to an ordinary subprocess.
"""

import json
import os
import runpy
import selectors
import signal
import subprocess
import sys
import threading
import time

# Environment entries that name the attempt's own scratch space. Every other
# entry must be the same for all products served by one preloaded interpreter.
PER_ATTEMPT = ("PREFECT_HOME", "XDG_CACHE_HOME", "TMPDIR", "DASK_TEMPORARY_DIRECTORY")
READY_TIMEOUT = 900


def _launcher_key(python, launcher, env):
    return python, launcher, tuple(sorted((k, v) for k, v in env.items() if k not in PER_ATTEMPT))


class ForkLauncher:
    """Drop-in for ``subprocess.run`` of ``[python, native_cli.py, ...]`` with a log file."""

    def __init__(self, script=None, ready_timeout=READY_TIMEOUT, out=None):
        self.script = str(script or os.path.abspath(__file__))
        self.ready_timeout = ready_timeout
        self.out = out or sys.stdout
        self._process = None
        self._key = None
        self._buffer = b""
        self.disabled = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        process, self._process = self._process, None
        self._key, self._buffer = None, b""
        if process is None:
            return
        try:
            process.stdin.close()
        except OSError:
            pass
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()

    def _say(self, message):
        print(f"native launcher: {message}", file=self.out, flush=True)

    def _line(self, deadline):
        """Next protocol line, or None on end of file; TimeoutError at the deadline."""
        while b"\n" not in self._buffer:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError
            with selectors.DefaultSelector() as selector:
                selector.register(self._process.stdout, selectors.EVENT_READ)
                if not selector.select(remaining):
                    continue
            chunk = os.read(self._process.stdout.fileno(), 65536)
            if not chunk:
                return None
            self._buffer += chunk
        line, self._buffer = self._buffer.split(b"\n", 1)
        return line.decode()

    def _start(self, python, launcher, env, cwd):
        self.close()
        started = time.monotonic()
        self._process = subprocess.Popen(
            [python, self.script, launcher], env=env, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE
        )
        try:
            line = self._line(started + self.ready_timeout)
        except TimeoutError:
            line = "timeout"
        if line != "ready":
            self._say(f"preloading failed ({line!r}); using one interpreter per product")
            self.close()
            self.disabled = True
            return False
        self._key = _launcher_key(python, launcher, env)
        self._say(f"preloaded {launcher} in {time.monotonic() - started:.1f}s (pid {self._process.pid})")
        return True

    def run(self, argv, env, cwd, log, timeout):
        """Run one conversion; return CompletedProcess or raise TimeoutExpired like subprocess.run."""
        python, launcher = argv[0], argv[1]
        key = _launcher_key(python, launcher, env)
        if not self.disabled and (self._process is None or self._process.poll() is not None or key != self._key):
            self._start(python, launcher, env, cwd)
        if self.disabled:
            with open(log, "a") as stream:
                return subprocess.run(argv, env=env, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
        deadline = time.monotonic() + timeout
        request = dict(argv=list(argv), env=dict(env), cwd=str(cwd), log=str(log))
        try:
            self._process.stdin.write(json.dumps(request).encode() + b"\n")
            self._process.stdin.flush()
            line = self._line(None)
        except OSError:
            line = None
        if line is None or not line.startswith("started "):
            self.close()
            raise RuntimeError("Preloaded native launcher stopped before starting the conversion")
        pid = int(line.split()[1])
        timed_out = False
        while True:
            try:
                line = self._line(None if timed_out else deadline)
            except TimeoutError:
                # subprocess.run kills its child with SIGKILL and reports the timeout.
                timed_out = True
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                continue
            if line is None:
                self.close()
                raise RuntimeError("Preloaded native launcher died during the conversion")
            if line.startswith(f"exit {pid} "):
                break
        if timed_out:
            raise subprocess.TimeoutExpired(argv, timeout)
        return subprocess.CompletedProcess(argv, int(line.split()[2]))


def _reset_after_fork(env):
    """Give the child what a fresh interpreter started with ``env`` would have."""
    import tempfile

    os.environ.clear()
    os.environ.update(env)
    # tempfile caches TMPDIR on first use; dask reads DASK_* when it is imported.
    tempfile.tempdir = None
    if "dask" in sys.modules:
        sys.modules["dask"].config.refresh()
    # prefect creates PREFECT_HOME when it is imported.
    if env.get("PREFECT_HOME"):
        os.makedirs(env["PREFECT_HOME"], exist_ok=True)
    # pycmor starts its netCDF lock guard thread on import; threads do not survive fork.
    locks = sys.modules.get("pycmor.core.xarray_locks")
    if locks is not None and getattr(locks, "_installed", None):
        threading.Thread(
            target=locks._guard, args=(locks._installed, locks.CHECK_EVERY_SEC), name="xarray-lock-guard", daemon=True
        ).start()


def _die_with_parent():
    try:
        import ctypes

        ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL)  # PR_SET_PDEATHSIG
    except (OSError, AttributeError):
        pass


def _child(launcher, request, reply):
    _die_with_parent()
    reply.close()
    sys.stdout.flush()
    sys.stderr.flush()
    stdin = os.open(os.devnull, os.O_RDONLY)
    os.dup2(stdin, 0)
    os.close(stdin)
    log = os.open(request["log"], os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    os.dup2(log, 1)
    os.dup2(log, 2)
    os.close(log)
    os.chdir(request["cwd"])
    _reset_after_fork(request["env"])
    sys.argv = list(request["argv"][1:])
    # Exceptions and SystemExit propagate to the interpreter, as in a direct launch.
    runpy.run_path(launcher, run_name="__main__")
    return 0


def serve(launcher):
    """Preload the launcher, then fork one child per request read from stdin."""
    # A direct launch puts the launcher's resolved directory first on sys.path.
    sys.path[0] = os.path.dirname(os.path.realpath(launcher))
    reply = os.fdopen(os.dup(1), "w", buffering=1)
    os.dup2(2, 1)
    runpy.run_path(launcher, run_name="__lr_native_preload__")
    sys.stdout.flush()
    sys.stderr.flush()
    reply.write("ready\n")
    for line in sys.stdin:
        request = json.loads(line)
        if request["argv"][1] != launcher:
            raise ValueError(f'Preloaded {launcher}, asked to run {request["argv"][1]}')
        pid = os.fork()
        if pid == 0:
            return _child(launcher, request, reply)
        reply.write(f"started {pid}\n")
        _, status = os.waitpid(pid, 0)
        reply.write(f"exit {pid} {os.waitstatus_to_exitcode(status)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(serve(sys.argv[1]))
