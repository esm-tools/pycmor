# Review: PLAN_save_dataset_reliability.md (round 1)

## Verdict: structurally sound, two real bugs

The two-option layering (A = tmpfs staging, E = timeout+retry) is the
right shape — independent, ordered, with knobs for opt-out. Line
references check out (`_Heartbeat` at files.py:64, `_safe_to_netcdf`
at 390, `save_dataset` at 1011). Effort estimate breakdown is honest.

But Option A's atomicity story is wrong on the cross-filesystem path,
and Option E's cancellation mechanism doesn't work against the exact
failure mode it's trying to fix.

---

## 1. Major — Option A's `shutil.move` is NOT atomic across filesystems

Lines 64-66:

```python
result = _safe_to_netcdf(ds, tmp_path, ...)
shutil.move(tmp_path, final_path)
```

with the comment at line 88-90: "shutil.move() copies + unlinks if
tmpfs and Lustre are on different mounts (which they are). This is
slower than true os.rename() but still has the property that the final
file appears atomically."

**That last claim is false.** `shutil.move` is implemented as
`copy2(src, dst)` + `unlink(src)`. `copy2` writes to `dst` directly —
readers can see a partial file at `final_path` during the copy phase.
The destination filesystem (Lustre) sees a growing file at the
final path; it does NOT appear atomically.

The whole point of staging-and-rename is atomicity. If A loses
atomicity in the cross-fs case, it's just adding a tmpfs hop with the
same race window as a direct write.

**Correct pattern** — two-stage:

```python
# stage on tmpfs (fast write, no Lustre lock contention)
_safe_to_netcdf(ds, tmp_path, ...)
# copy to TARGET filesystem at a .tmp suffix (slow but bounded)
shutil.copy2(tmp_path, final_path + ".tmp")
os.unlink(tmp_path)
# atomic rename WITHIN target filesystem (metadata-only, instant)
os.rename(final_path + ".tmp", final_path)
```

Now the final file appears atomically because `os.rename` within the
same filesystem is atomic. The Lustre-side `.tmp` file is visible
during the copy, but `final_path` itself never has partial content.

Cost: one extra Lustre write of the file (the staging copy). Still
worth it because the slow `_safe_to_netcdf` runs on tmpfs (no
lock contention), and the Lustre copy is a single linear write that
doesn't hold POSIX write-locks the way HDF5's incremental write does.

The plan's note at line 92-96 (alternative: stage on `$PYCMOR_SCRATCH`
under Lustre and use real `os.rename`) actually has correct
atomicity but loses the tmpfs benefit. The two-stage pattern above
combines both — and that's what the plan should describe.

---

## 2. Major — Option E's `future.cancel()` doesn't unblock syscall-stuck workers

Lines 157-162:

> - `os.path.getsize(final_path)` or `tmp_path` every K seconds in the
>   watcher thread
> - Reset progress timestamp when size grows
> - If progress timestamp hasn't moved for `timeout_minutes`, kill the
>   dask future (`future.cancel()`) and raise `SaveTimeout`

`Future.cancel()` sets a flag and asks the worker to stop *at the
next checkpoint*. But the worker is blocked in a POSIX write syscall
waiting on a Lustre lock — that's a kernel-level blocking call. The
Python interpreter doesn't get control back until the syscall
returns, and `cancel()` is a Python-level signal.

In other words: under the exact scenario the plan targets
(worker blocked on Lustre write), `future.cancel()` does nothing
until the syscall finally completes (whenever that is).

**What actually works**:

- `dask-nanny` can kill a worker process (SIGTERM/SIGKILL). That
  unblocks the syscall by tearing down the process. Dask scheduler
  then re-dispatches the task to a new worker. But nanny only does
  this on memory limits today, not on time-based hangs.
- Manually `os.kill(worker_pid, signal.SIGTERM)` from the watcher
  thread. Works but requires knowing the worker PID, and the
  worker's `to_netcdf` will leave a partial file on tmpfs.
- Set a `signal.alarm()` inside the task itself before the write.
  Works for synchronous writes (Python raises a signal exception
  when the alarm fires). But only one alarm per process; competes
  with other signal users.

The plan's retry sketch is conceptually right ("attempt the write
again on a new worker"), but the cancellation mechanism that's
supposed to free the stuck worker doesn't work. Either:

- Document explicitly that "cancellation" means raising `SaveTimeout`
  in the *parent* (which then drops the stuck future's reference
  and lets the next retry start on whichever worker becomes free —
  the originally-stuck worker may stay stuck forever, eating one
  worker slot). The retry succeeds on the second slot, the first
  worker is effectively leaked until SLURM kills the job.
- Or implement a real worker-kill mechanism (worker PID lookup +
  SIGTERM), accepting the partial-file cleanup complexity.

Realistic effort for E with correct semantics: probably 6-8 hours,
not 3+3. And it requires deciding which mechanism above is acceptable.

---

## 3. Moderate — `PYCMOR_TMPFS_STAGING=1` default risks for non-tmpfs `/tmp`

Line 50: `use_tmpfs = bool(int(os.environ.get("PYCMOR_TMPFS_STAGING", "1")))`

Default ON for everyone. But:

- On Levante compute nodes, `/tmp` is 63 GB tmpfs ✓
- On Levante login nodes, `/tmp` is a real disk (verified on this
  node: ext2/ext3, 57 TB)
- On developer machines / CI runners / other clusters, `/tmp` could
  be small, slow, or even read-only

Risk: someone runs unit tests or a small dev job on a login node;
staging writes a 8 GB rule's tmp file to disk-backed `/tmp`; either
fills `/tmp` or just runs slow. Bigger risk: a different cluster's
`/tmp` quota is 1 GB.

**Mitigation**: detect tmpfs at startup:

```python
import os
fs = os.statvfs("/tmp")
free_gb = fs.f_bavail * fs.f_bsize / 1e9
is_tmpfs = ... # check /proc/mounts or os.statvfs(f_flag)
if not is_tmpfs or free_gb < REQUIRED_GB:
    logger.warning(f"/tmp not tmpfs or too small ({free_gb}GB), staging disabled")
    use_tmpfs = False
```

Or, safer: default OFF, opt-in per-tier. The "ship A first to fix the
hangs" goal only needs it on Levante compute partitions where it
matters. Forcing it everywhere is broader scope than the problem
warrants.

---

## 4. Moderate — `mark_progress()` API doesn't fit the design

Lines 134-135:

```python
def mark_progress(self):
    self._last_progress_ts = time.time()
```

The text at line 153-155 says:

> `mark_progress()` should be called from inside the netCDF write loop
> whenever the output file size grows.

But `to_netcdf` is an xarray-internal call. There's no place outside
xarray to call `mark_progress()` from "inside the netCDF write loop."
You'd have to monkey-patch xarray or fork the netCDF4 backend.

The plan's later mechanism (file-size polling in the watcher thread,
lines 157-160) is the correct one and doesn't need `mark_progress`.
Either:

- Drop the `mark_progress()` API entirely; the watcher thread polls
  file size and resets `_last_progress_ts` itself.
- Keep `mark_progress` only for non-`to_netcdf` write paths where
  the caller controls the loop (e.g., chunked custom writers).

Mixing both produces a confused API.

---

## 5. Moderate — cap7_ocean tmpfs budget is unactioned

Lines 81-82:

> For cap7_ocean (8 GB hfx_3D × 8 workers = 64 GB peak): may need to
> disable staging or stage one at a time.

"May need to disable" or "stage one at a time" — neither is concretely
chosen. Disabling defeats A's purpose for the rules that need it most.
"Stage one at a time" requires per-rule serialization that pycmor
doesn't currently have.

**Concrete options**:

- Per-rule yaml flag: `netcdf_tmpfs_staging: false` for hfx_3D,
  thetao_3D, so_3D — those rules write directly to Lustre. Accept
  that they're slow-and-occasionally-hang, since the small-rule
  fixes free up enough capacity overall.
- Per-tier override at the inherit level (already mentioned in the
  plan) — disable for cap7_ocean entirely.

Pick one and put it in the plan. The current "may need" handwave
means the cap7_ocean operator discovers the OOM at runtime.

---

## 6. Minor — cli13b reference at line 99 isn't defined here

> Run cli13b config (8×1×32GB, 8 task slots) for lrcs_seaice and
> lrcs_ocean during peak Levante hours.

The reader doesn't know what cli13b is from this plan. Worth a one-line
gloss: "cli13b = 8 workers × 1 thread × 32 GB/worker, the
configuration used in the most recent gate-A attempt" or a pointer
to the dispatch log naming convention.

---

## 7. Minor — synchronous scheduler rationale isn't stated

Line 22 mentions `netcdf_write_scheduler: synchronous` (commit
`65945ef`) "doesn't yield the GIL during a slow write." Out-of-scope
later (line 192) hints that the original threads scheduler had a
fork-bomb risk. Worth a one-line in Problem: "switched to synchronous
in commit 65945ef to avoid the threads-scheduler fork-bomb risk; that
fix made GIL contention worse under Lustre stalls."

Sets context for why the current state is what it is and motivates
why "go back to threads after A lands" is a legitimate follow-up.

---

## Strong points

- **Two-option layering** is honest about A being cheaper / lower-risk
  and E being the safety net.
- **Effort estimate is broken out** rather than a single number; the
  ~12h total is at least the right order of magnitude (with my §2
  correction probably 16h).
- **Rollback is concrete**: env-var knobs to disable both options.
  No yaml migration needed for rollback.
- **Risks section** flags tmpfs RAM use and cross-fs copy overhead —
  surface-honest, even if §1 shows the cross-fs analysis is wrong.
- **Test plan** distinguishes unit / integration / production smoke,
  with a concrete acceptance criterion for each.
- **Out-of-scope section** preserves future-work items (h5netcdf,
  threads-scheduler revisit) without scope-creep.

---

## Bottom line

Three concrete fixes before implementing:

1. **§1 (A's atomicity)**: replace `shutil.move(tmp_path, final_path)`
   with two-stage `copy2(tmp_path, final_path + ".tmp")` →
   `os.unlink(tmp_path)` → `os.rename(final_path + ".tmp",
   final_path)`. Re-state the atomicity property correctly.
2. **§2 (E's cancellation)**: either document the realistic
   "stuck-worker-leaks" semantics, or commit to a real
   worker-kill mechanism (and raise the effort estimate
   accordingly). Don't claim `future.cancel()` will unblock a
   syscall-stuck worker.
3. **§3 (tmpfs default)**: either add a startup tmpfs-detection
   check or change the default to opt-in. Forcing tmpfs staging on
   non-tmpfs `/tmp` makes pycmor worse on dev machines.

Plus smaller cleanups: drop the `mark_progress` API in favor of
file-size polling (§4), pick a concrete cap7_ocean policy (§5),
add the cli13b gloss (§6) and synchronous-scheduler rationale (§7).

Architecture is right. Two real bugs to fix in the implementation
sketch, plus one defaults question. After those, ship A first as
planned.
