# PLAN: pycmor save_dataset reliability under Lustre contention (round 2)

Status: design, no code written. Audience: implementer.

Round 2 addresses review feedback in
[REVIEW_save_dataset_reliability_round1.md](REVIEW_save_dataset_reliability_round1.md):
fixes A's atomicity story (§1), rewrites E with realistic syscall-stuck
semantics (§2), changes tmpfs default to auto-detection (§3), drops
`mark_progress()` (§4), picks a concrete cap7_ocean policy (§5), and
adds context (§6, §7).

## Problem

`pycmor` runs hang indefinitely in `save_dataset` heartbeat
(`save_dataset[X] still running (t=…s, heartbeat #N)`) under Levante
midday load. Fingerprint:

- One or more dask workers parked on `to_netcdf(...)` for tens of minutes
- Zero growth on the output `.nc` file size for the entire stuck period
- The heartbeat keeps firing every 60 s — the dask future never returns
- No useful error logged; SLURM walltime eventually kills the run

Observed across **cli11 (TPW=4)** and **cli12 (TPW=1)** on lrcs_seaice and
lrcs_ocean. Rules that hang differ per run (`obvfsq`, `wfo`, `pbo`,
`siarea_day_nh`, …) — not a per-rule bug. Pre-CLI runs (cli2–cli9, weekend
mornings) did not hit this.

Root cause (high confidence): **netCDF4/HDF5 POSIX write-lock contention
on Lustre `/scratch` under load**, exacerbated by
`netcdf_write_scheduler: synchronous` (commit `65945ef` — switched from
`threads` to avoid the fork-bomb risk where the threads scheduler spawned
`os.cpu_count() = 256` threads per worker; that fix made GIL contention
worse under Lustre stalls because `synchronous` holds the GIL through the
entire blocking syscall, so dask can't re-dispatch the task). Once a
worker blocks on Lustre, dask can't re-dispatch and the heartbeat keeps
reporting "still running" forever.

`cli13b` = the most recent reference config (8 workers × 1 thread ×
32 GB/W, 512 GB cgroup, 3 h walltime, submitted in the `cli13b` dispatch
suffix in `pycmor_hr_cli_*` logs). It is the baseline against which we
measure A's impact.

We need pycmor to keep running on busy days. Two complementary fixes:

## Option A — Three-stage write: tmpfs → Lustre `.tmp` → atomic rename

The actual atomic-write pattern that works across filesystems:

1. Write the netCDF on node-local tmpfs (`/tmp`). Fast, no Lustre lock
   contention during the slow incremental HDF5 write.
2. Copy from tmpfs to the target Lustre directory as `<final_path>.tmp`.
   Single linear write; no POSIX write-lock-per-chunk like HDF5's
   incremental writes.
3. `os.rename(<final_path>.tmp, <final_path>)`. Atomic on POSIX *because
   both paths are on the same filesystem* (Lustre). Metadata-only — no
   data copy.

Round 1's `shutil.move` claim was wrong: across-filesystem `shutil.move`
falls back to `copy2 + unlink`, with the destination growing visibly
during the copy. Three-stage gives true atomicity at the cost of one
extra Lustre write.

**Files touched:**
- `src/pycmor/std_lib/files.py:_safe_to_netcdf` (line ~390) — wrap.

**Implementation sketch:**

```python
import os
import shutil
import tempfile

def _atomic_to_netcdf(ds, final_path, *args, scheduler="synchronous", **kwargs):
    """Write to tmpfs, copy to target FS as .tmp, atomic-rename to final.

    Stage 1: fast tmpfs write avoids Lustre's POSIX write-lock contention
             during HDF5's incremental write loop.
    Stage 2: bounded linear copy to target FS as `<final_path>.tmp`. Held
             write lock is brief and predictable.
    Stage 3: same-FS rename; metadata-only and atomic. The final path
             never has partial content.
    """
    if not _tmpfs_staging_available(final_path):
        return _safe_to_netcdf(ds, final_path, *args, scheduler=scheduler, **kwargs)

    tmpdir = os.environ.get("PYCMOR_TMPFS_DIR", "/tmp")
    fd, tmp_path = tempfile.mkstemp(
        dir=tmpdir, prefix=os.path.basename(final_path) + ".", suffix=".tmp"
    )
    os.close(fd)
    stage_path = final_path + ".tmp"
    try:
        # Stage 1: tmpfs write
        _safe_to_netcdf(ds, tmp_path, *args, scheduler=scheduler, **kwargs)
        # Stage 2: copy to target FS (no atomicity yet, but bounded write)
        shutil.copy2(tmp_path, stage_path)
        os.unlink(tmp_path)
        # Stage 3: atomic same-FS rename
        os.rename(stage_path, final_path)
    except Exception:
        for p in (tmp_path, stage_path):
            try: os.unlink(p)
            except FileNotFoundError: pass
        raise
```

`_tmpfs_staging_available(final_path)` does runtime detection (see §3
below): if `/tmp` is not tmpfs, or has insufficient free space for the
typical file size, fall back to direct write with a one-time WARNING log.

**Knob:**
- `PYCMOR_TMPFS_STAGING={auto,on,off}` (default `auto` — detection-based).
- Per-tier inherit: `netcdf_tmpfs_staging: false` overrides to off.
- Per-rule yaml: `netcdf_tmpfs_staging: false` overrides per-rule (used
  for cap7_ocean's `hfx_3D`/`hfy_3D`/`thetao_3D`/`so_3D` — see §5).

**Budget impact:**
- tmpfs uses node RAM. Add to existing budget check in
  `examples/run_hr_yaml_cli.sh`:
  ```
  total_gb = N_WORKERS × MEM_PER_WORKER + estimated_peak_tmpfs_gb
  total_gb ≤ 75% × CGROUP_GB
  ```
- For lrcs_*: ~8 GB peak tmpfs is plenty of slack on 256-512 GB cgroup.

**Acceptance criterion:**
- Run cli13b config (8×1×32GB, 8 task slots) for lrcs_seaice and
  lrcs_ocean during peak Levante hours. Both finish without a single
  heartbeat going past 5 minutes on the same rule. Output files have
  correct sizes (matches cli9r-era successful runs).
- Mid-write inspection of the output directory shows only `<file>` and
  `<file>.tmp` entries — never a partial `<file>`.

## Option A.5 — Tmpfs auto-detection

Default `PYCMOR_TMPFS_STAGING=auto` checks at first call:

```python
def _tmpfs_staging_available(final_path, _cache={}):
    mode = os.environ.get("PYCMOR_TMPFS_STAGING", "auto").lower()
    if mode == "off":
        return False
    if mode == "on":
        return True
    # mode == "auto" (default)
    if "result" in _cache:
        return _cache["result"]
    tmpdir = os.environ.get("PYCMOR_TMPFS_DIR", "/tmp")
    try:
        st = os.statvfs(tmpdir)
        free_gb = st.f_bavail * st.f_frsize / 1e9
    except OSError:
        _cache["result"] = False
        return False
    is_tmpfs = _is_tmpfs(tmpdir)
    min_free_gb = float(os.environ.get("PYCMOR_TMPFS_MIN_FREE_GB", "4"))
    ok = is_tmpfs and free_gb >= min_free_gb
    if not ok:
        logger.warning(
            f"tmpfs staging disabled: {tmpdir} tmpfs={is_tmpfs} "
            f"free={free_gb:.1f}GB (need ≥{min_free_gb}GB tmpfs). "
            f"Falling back to direct Lustre write."
        )
    _cache["result"] = ok
    return ok

def _is_tmpfs(path):
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[1] == path and parts[2] == "tmpfs":
                    return True
    except OSError:
        pass
    return False
```

Caches the result in module state — checked once per process.

Net effect: On Levante compute nodes (63 GB tmpfs at `/tmp`), staging is
on automatically. On dev machines (often non-tmpfs `/tmp` or tight quotas),
it's off automatically with a clear warning. No silent regressions on
non-Levante environments.

## Option E — save_dataset timeout + retry (revised semantics)

Add a watchdog: poll `os.path.getsize(tmp_path or final_path)` every K
seconds. If no growth in `PYCMOR_SAVE_TIMEOUT_MIN` minutes, raise
`SaveTimeout` in the parent dask task; this propagates up and lets the
caller retry.

**Realistic semantics — round 1 was misleading.** `future.cancel()`
sets a flag asking the worker to stop at the next Python checkpoint.
Under our failure mode (worker blocked in a POSIX write syscall on
Lustre), the syscall doesn't return so `cancel()` does nothing until the
worker eventually unblocks. Three honest options:

1. **(picked) Soft cancel via parent-side exception.** The watchdog
   raises `SaveTimeout` in the *parent* task — the one orchestrating
   the dask future. The parent drops its reference to the stuck
   future and re-dispatches the task. **The stuck worker is leaked
   until SLURM kills the job**, eating one worker slot for the rest
   of the run. Other workers continue.
2. Hard kill via SIGTERM on the worker PID. Frees the slot
   immediately, leaves a partial file on tmpfs (cleanup needed),
   adds complexity around PID lookup and signal handling.
3. Set `signal.alarm()` inside the task before the write. Interrupts
   the syscall via signal. Works for synchronous writes; only one
   alarm per process; competes with other signal users.

**This round picks (1)** because it's the simplest and matches dask's
existing recovery semantics. The leaked worker is degraded service
rather than failure: if 1 of 8 workers gets stuck per run, you still
process 7/8 the throughput. If multiple workers stick on the same node
(suggesting a hung Lustre client), the watchdog raises after retries are
exhausted and the run fails loudly within `MAX_RETRIES × timeout` instead
of hanging silently at walltime.

**Files touched:**
- `src/pycmor/std_lib/files.py:_Heartbeat` class — add file-size watcher
  + timeout (no `mark_progress` API).
- `src/pycmor/std_lib/files.py:save_dataset` — wrap in retry loop.

**Implementation sketch:**

```python
class SaveTimeout(Exception):
    pass

class _Heartbeat:
    def __init__(self, name, watch_path=None,
                 timeout_minutes=None, poll_interval_s=30):
        self.name = name
        self.watch_path = watch_path
        self.timeout = ((timeout_minutes
                         or int(os.environ.get("PYCMOR_SAVE_TIMEOUT_MIN", "15")))
                        * 60)
        self.poll_interval_s = poll_interval_s
        self._stop = threading.Event()
        self._last_size = -1
        self._last_progress_ts = time.time()
        self._timed_out = False
        self._watcher = None

    def __enter__(self):
        if self.watch_path:
            self._watcher = threading.Thread(target=self._watch, daemon=True)
            self._watcher.start()
        return self

    def _watch(self):
        while not self._stop.wait(self.poll_interval_s):
            # heartbeat log line every poll
            logger.info(f"⟳ {self.name} still running …")
            try:
                size = os.path.getsize(self.watch_path)
            except OSError:
                size = 0
            if size > self._last_size:
                self._last_size = size
                self._last_progress_ts = time.time()
            elif time.time() - self._last_progress_ts > self.timeout:
                logger.error(
                    f"{self.name}: no I/O progress for "
                    f"{self.timeout/60:.0f} min; flagging SaveTimeout"
                )
                self._timed_out = True
                self._stop.set()
                return

    def __exit__(self, *a):
        self._stop.set()
        if self._watcher:
            self._watcher.join(timeout=2)
        if self._timed_out:
            raise SaveTimeout(self.name)

def save_dataset(da, rule):
    cmor_var = getattr(rule, "cmor_variable", None) or getattr(rule, "name", "?")
    max_retries = int(os.environ.get("PYCMOR_SAVE_MAX_RETRIES", "2"))
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            # _save_dataset_impl reports its watch_path back to the heartbeat;
            # if A is active, that's the tmpfs path; otherwise the final path.
            return _save_dataset_impl_with_heartbeat(da, rule, attempt)
        except SaveTimeout as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    f"save_dataset[{cmor_var}] timed out "
                    f"(attempt {attempt+1}/{max_retries+1}); retrying"
                )
            else:
                logger.error(
                    f"save_dataset[{cmor_var}] timed out after "
                    f"{max_retries+1} attempts; giving up"
                )
                raise
```

**Watch path coupling with A.** When A is active, the watcher polls the
tmpfs path (where the actual write is happening). When A is off, it
polls the Lustre final path. The watch_path is the same one
`_atomic_to_netcdf` or `_safe_to_netcdf` is writing to — implementation
must thread it through.

**Knobs:**
- `PYCMOR_SAVE_TIMEOUT_MIN=15` (default 15 min)
- `PYCMOR_SAVE_MAX_RETRIES=2` (default 2 retries = 3 total attempts)
- `PYCMOR_SAVE_POLL_INTERVAL_S=30` (between size checks)
- Per-rule yaml: `save_dataset_timeout_min: 30` for known-slow rules.

**Acceptance criterion:**
- Synthetic: inject a sleep-forever into `_safe_to_netcdf` for a known
  rule; observe `SaveTimeout` after `timeout` minutes, retry attempt
  logged, eventual `TimeoutError` after `max_retries + 1` attempts.
- Real: a cli run that previously hung at hour 1 now either succeeds
  via retry-onto-fresh-worker, or fails loudly within
  `(max_retries + 1) × timeout = 45 min`. Either way, operator can act
  promptly rather than waiting out walltime.
- Worker-slot leak is documented in the log on each timeout
  (`save_dataset[…] timed out; worker may be stuck — slot leaked`).

## Option E.5 — Optional: hard kill (not in round 2 scope)

If real-world experience shows the leaked-worker pattern is unacceptable
(e.g. half a run's workers leak by hour 3), revisit with:

- Look up the worker PID via `dask.distributed` client API
- `os.kill(worker_pid, signal.SIGTERM)` from the watchdog
- Clean up any half-written tmpfs / Lustre `.tmp` files
- Wait for dask scheduler to detect dead worker (~10 s) and re-dispatch

~4-6 h additional work. Out-of-scope for this round.

## §5 cap7_ocean concrete policy

The hfx_3D / hfy_3D / thetao_3D / so_3D rules in `cap7_ocean` produce
~8 GB compressed files; with 8 workers in flight, peak tmpfs would be
~64 GB which is the entire `/tmp` capacity.

**Decision: per-rule yaml flag, default OFF for those four rules.**

In `awi-esm3-veg-hr-variables/cap7_ocean/cmip7_awiesm3-veg-hr_cap7_ocean.yaml`,
add to each large-3D rule:

```yaml
- name: hfx
  inputs: ...
  netcdf_tmpfs_staging: false  # 8 GB files; staging would risk tmpfs OOM
  pipelines:
    - ...
```

Same for `hfy`, `thetao`, `so` 3D rules. They keep current direct-Lustre
behaviour. Smaller cap7_ocean rules get staging.

Net trade: those four rules retain the current hang-risk; everything
else benefits from A. Acceptable because (a) those four ran in earlier
cycles cli2-cli9 without obvious hangs, and (b) E provides timeout
recovery for them as a backstop.

If staging is later confirmed safe at higher tmpfs budgets (e.g. a node
type with bigger tmpfs), flip the flag.

## Ordering

1. **Implement A + A.5** first (atomic 3-stage staging + auto-detection).
   Cheaper, lower-risk, and may eliminate enough hangs that E becomes
   optional.
2. **Test A on cli14** for lrcs_seaice and lrcs_ocean. If hangs
   disappear, ship.
3. **If A is insufficient, implement E.** Layers on top of A; the
   watcher polls A's tmpfs path during the slow phase.

## Out-of-scope

- Replacing the netcdf4 engine with `h5netcdf` (different lock semantics).
  Worth trying if A+E doesn't suffice; could be a `netcdf_engine:
  h5netcdf` inherit knob.
- Revisiting `netcdf_write_scheduler: threads` *given* A's tmpfs staging
  (the original 256-thread fork-bomb risk is reduced because each
  tmpfs write completes in seconds, so threads don't pile up). Worth a
  separate experiment.
- Hard-kill stuck workers (Option E.5 above).
- Better dask scheduler-side hang detection (vs file-size watcher).

## Risks and rollback

- **A introduces tmpfs RAM use.** Auto-detection avoids the worst cases
  (small `/tmp` on dev machines). On Levante compute it's well within
  budget. Cap7_ocean opt-out handles the per-rule overflow risk.
- **A adds one extra Lustre write** for the staging copy. For 1 GB
  files this is 5-10 s of extra wall time. Acceptable; offset by hangs
  avoided.
- **E's leaked-worker pattern** means a stuck worker eats one slot for
  the rest of the run. Currently the entire run hangs; "one slot lost"
  is strictly better. Documented in log.
- **E's timeout default (15 min)** may abort a legitimately slow write.
  Per-rule override available. Default suits the tiers we've benchmarked.
- **Rollback for both:** `PYCMOR_TMPFS_STAGING=off`,
  `PYCMOR_SAVE_TIMEOUT_MIN=0` (disable timeout). No yaml migration.

## Test plan

1. Unit tests in `tests/unit/test_files.py`:
   - `_atomic_to_netcdf` writes a file readable by `xr.open_dataset`,
     byte-identical to direct `_safe_to_netcdf` output (or
     `xr.testing.assert_identical` after reload).
   - With `PYCMOR_TMPFS_STAGING=off`, behaviour matches old
     `_safe_to_netcdf`.
   - `_tmpfs_staging_available` returns `False` on non-tmpfs `/tmp`
     (mock `/proc/mounts`).
   - `_tmpfs_staging_available` returns `False` when free space below
     threshold.
   - Three-stage write leaves no partial `final_path` on disk if
     stage 2 fails (verify via mock failures).
   - `_Heartbeat` timeout fires when watch_path doesn't grow.
   - Retry succeeds when first attempt times out and second succeeds.
   - Retry exhausted raises `SaveTimeout`.
2. Integration test on a single small lrcs_seaice rule via
   `pycmor process` with `PYCMOR_TMPFS_STAGING=on` forced.
3. Full cli14 lrcs_seaice + lrcs_ocean at peak Levante hours. Compare
   wall-time and hang rate to cli13b baseline.

## Effort estimate (revised)

| Step | LoC | Time |
|---|---|---|
| Option A `_atomic_to_netcdf` + wiring | ~100 | 2 h |
| Option A.5 `_tmpfs_staging_available` + detection | ~60 | 1 h |
| Option A/A.5 unit tests | ~180 | 3 h |
| Option E `_Heartbeat` file-size watchdog + `SaveTimeout` | ~120 | 3 h |
| Option E retry loop in `save_dataset` | ~40 | 1 h |
| Option E unit tests | ~150 | 3 h |
| §5 yaml flags on cap7_ocean 3D rules | ~10 | 30 min |
| Budget-check update in run_hr_yaml_cli.sh | ~20 | 30 min |
| Integration smoke test + tuning | — | 2 h interactive |
| **Total** | **~680** | **~16 h** |

Realistic land time: 2 working days for A + A.5 + E + tests. A + A.5
alone: ~6 h, with optional follow-up land of E another working day.
