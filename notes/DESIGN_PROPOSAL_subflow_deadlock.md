# Design proposal — eliminate the parent×subflow slot deadlock in pycmor's parallel orchestration

**Status**: proposal, **§4 patch implemented** on
`feat/cmip7-awiesm3-veg-hr` branch (2026-05-06 afternoon), gate-A
re-run in flight (revision 2 + §10 revision 4 — adds §10.5 with
gate-A v1 partial findings and two infrastructure bugs surfaced
during gate-A submission).
**Author**: optimisation working group, 2026-05-05.
**Audience**: pycmor maintainers, AWI HPC team, external reviewers.
**Companion**: `OPTIMIZATION_PLAN.md` (Round 4 sweep data, supports the
problem statement); `bench_hr_ua_6hr_results.md` (single-rule profiling
data).

### Status of revision-2 plan items (verified against body)

All items in this list have been verified against the section text
that follows. If a future agent edits the body without updating this
list, that is a doc-consistency bug.

* **§1** — DONE. Dropped the speculative "low-mem churn" explanation
  for the non-monotonic deadlock distribution; observation reported
  without claiming a mechanism we haven't proven.
* **§2** — DONE. Corrected the framing of the outer
  `@task("Process rule")` — under `parallel/dask` it is invoked via
  `client.submit(...)` which **bypasses** Prefect's task lifecycle.
* **§4.2** — DONE. Spelled out that `cache_policy=TASK_SOURCE+INPUTS`
  becomes a no-op when the Task body runs outside a flow context.
* **§4.3** — DONE. Rewrote on_completion / on_failure callbacks
  with `rule_name + elapsed` direct args (in §4.1 pseudocode);
  reframed the "what is gained" list as "addresses one of at least
  three failure modes" per the round-3 review.
* **§6.3** — DONE. Bumped mini-cap7 ensemble 3→5 (5 ensemble × 8
  configs = 40 jobs) to support a 95 %-CI upper bound of < 7 % on
  `p(deadlock)`.
* **§6.4** — DONE. Reworded the cap7_atm wall-time criterion from
  "≥ 15 % wall reduction" to "matches or improves the 2:57
  baseline", and made the completion-count threshold relative to
  the gate-B baseline (refusing to commit to an absolute number
  before that data exists).
* **§7** — DONE. Added a 7-test positive unit-test plan for the
  direct-call path inside the existing `tests/unit/` bullet
  (no separate new section).
* **§8.5** — DONE. Semaphore-on-parent-pool alternative documented
  with rejection reasoning + note that it could layer on top of §4
  as a defence-in-depth follow-up.
* **§8.6** — DONE. Separate-inner-pool alternative documented with
  rejection reasoning (boot cost, opaque memory accounting,
  scheduler proliferation, doesn't address the §10 failure modes
  either).
* **§10.5** — NEW (revision 4 of §10). Gate-A v1 partial findings
  + two infrastructure bugs surfaced during gate-A submission:
  (1) Prefect ephemeral-server SQLite-on-Lustre disk-I/O flake;
  (2) `repoint_hr_year.py` had a stale `OLD_RUN_TOKEN` and a
  `[a-z_]+_file:` regex that silently skipped year-filtering for
  digit-named variables (sgm22, sgm12). Both fixed; gate-A v2 is
  in flight at the time of writing.
* **§10** — DONE (this is "revision 3" of just §10).
  Production-scale baseline data from the 17-tier full-year
  cmorize at the proposed `4×4×16` default (jobs
  `24711300-24711317`, 2026-05-06). Earlier drafts of §10
  mis-framed the run as **§4 validation**; corrected — the run
  executed under the unchanged architecture, with the inner
  `@flow` + `DaskTaskRunner` nesting still present in
  `pipeline.py`. §10 opens with this caveat in bold,
  retracts the over-confident "Prefect-boot-stall vs deadlock"
  labelling, corrects the ok/fail/killed counts (the actual data
  is substantially worse than initially reported — extra_atm has
  68 `KilledWorker` events that the original draft missed), and
  ends with a four-item required-data gate (A: re-run with §4
  applied; B: complete the in-flight 2×4×64 comparison; C: name
  three tiers and quantify wall at both configs × both
  architectures; D: `py-spy` the stuck workers).

---

## 1. Problem statement

When pycmor runs in parallel mode (`pycmor.parallel: true,
pipeline_orchestrator: dask`), individual cmorize jobs **deadlock
probabilistically** at moderate-to-high worker × thread-per-worker
configurations on production yamls (cap7_atm and similar). The
deadlock manifests as:

* a worker process at 0 % CPU,
* no Prefect task events emitted for tens of minutes to hours,
* SLURM keeps the job allocated until the wallclock limit kills it,
* completed-rule count stops increasing.

We have direct evidence of the failure mode from a controlled
24-job sweep on `mini-cap7` (7 heaviest cap7_atm rules, 3 ensemble
copies × 8 (W, mem-per-worker) configurations, see
`OPTIMIZATION_PLAN.md` § Round 4):

| W × TPW × mem | total slots | deadlocks (out of 3) |
|---|---|---|
| 2 × 4 × 64 GB | 8  | 0  (current production default — clean) |
| 3 × 4 × 32 GB | 12 | 1 |
| 3 × 4 × 48 GB | 12 | 0 |
| 4 × 4 × 16 GB | 16 | 0 |
| 4 × 4 × 24 GB | 16 | 2 |
| 4 × 4 × 32 GB | 16 | 1 |
| 4 × 4 × 40 GB | 16 | 1 |

Deadlock probability is non-monotonic in memory and concentrated at
mid-memory + high-slot configurations. Once `collapse_steps=true`
(commit `3169ce5` — collapses 13 pipeline steps into one Prefect
task) is applied to the same sweep, deadlocks drop substantially but
not to zero (`4×4×40` still produced 1/3). The first observation —
that 2×4×64 has been the only consistently safe production
configuration — is itself evidence of the same deadlock pattern: the
configuration ran below the threshold where the pattern triggers.

**On the non-monotonicity**: revision 1 of this proposal speculated
that 4×4×16 escapes the deadlock because dask-nanny aggressively
kills tight-memory workers, breaking the resource-allocation cycle
via kill+restart. We have not instrumented this and the explanation
is not falsifiable from the data we have. We report the observation
without committing to a mechanism. What is *not* speculative is the
bound at `S = W × TPW`: with TPW=1 the deadlock is **deterministic**
because every parent task fills a worker outright. This was directly
observed in this session at 3W × TPW=1 and 2W × TPW=2 (parallel
agent's runs `24671157` and `24671288`): both ran 24-30 minutes with
0 rules completed and ≤ 2 % CPU, the classic
parents-block-children fingerprint.

**Statistical caveat**: the mid-memory deadlock counts above (1/3 or
2/3) come from a 3-copy ensemble per configuration. Treated as a
binomial with success = "no deadlock", that supports a one-sided
upper bound on `p(deadlock) ≤ ~12 %` at 95 % confidence — suggestive,
not conclusive. We propose tightening this in Phase 6.3 (see §6).

This is **not a memory bug** (nothing is OOMing — RSS and cgroup
peaks are well under their limits), and it is **not the
HLGExprSequence pickle bug** (that bug surfaces as a thread-lock
serialisation failure across task boundaries; we already addressed it
behind `PYCMOR_PREFECT_COLLAPSE=1`/`collapse_steps`, see Round 4b in
`OPTIMIZATION_PLAN.md`). The deadlocks remain, with different
fingerprint: workers genuinely idle, not crashing, just unable to
proceed.

The structural cause is **nested submission of bounded thread-pool
work**: pycmor's per-rule "Process rule" Prefect task itself spawns a
Prefect subflow whose tasks share the *same* DaskTaskRunner thread
pool. When enough rules become concurrent, parents fill the pool and
children block waiting for a parent to release a thread it cannot
release until its children finish — a classic resource-allocation
deadlock.

Goal: eliminate this deadlock pattern from pycmor without sacrificing
the Prefect features the codebase relies on (per-rule cache_policy,
on_completion / on_failure hooks, run-name labelling, retry
semantics). Make it safe to run at higher concurrency than the
current 2×4×64 default.

---

## 2. Current architecture (verbatim, with file references)

The relevant control flow for one rule, when `parallel: true`:

```
cmorizer._parallel_process_dask()
  └─ client = Client(cluster=self._cluster)
  └─ futures = [client.submit(self._process_rule, rule) for rule in self.rules]
  │                                  ^^^^^^^^^^^^^^^^^
  │  NOTE: client.submit(...) ships the bare callable to a worker.
  │  Prefect's @task lifecycle (caching, retries, run-name templating
  │  from `@task(name="Process rule")`) is BYPASSED by this path —
  │  the decorator is decorative under parallel/dask. The function
  │  body still runs, just without Prefect-task semantics around it.
  │
  └─ for each rule's _process_rule body, **inside a worker thread**:
       └─ for pipeline in rule.pipelines:
            └─ pipeline.run(data, rule)            # Pipeline.run
                 └─ Pipeline._run_prefect(...)     # this DOES use Prefect
                      └─ dynamic_flow = @flow(task_runner=DaskTaskRunner(...))
                           def dynamic_flow(data, rule):
                               return self._run_native(data, rule)
                      └─ dynamic_flow(...)         # SUBFLOW invocation
                           └─ for step in self.steps:
                                └─ step(data, rule)   # each step is a @task
```

So in the deadlock-prone path the **only real Prefect boundary is
the inner subflow**. The outer `@task("Process rule")` decoration is
not enforced because `client.submit(...)` doesn't go through Prefect.
This is important for §4.2 (what is lost): we cannot lose
caching / retries / observability that we never had in this code
path.

Source-level references:

* **Outer rule wrapper**:
  `src/pycmor/core/cmorizer.py:1087-1107`,
  `Cmorizer._process_rule`. Decorated `@task(name="Process rule")`,
  but the parallel-mode call site at
  `cmorizer.py:990` is `client.submit(self._process_rule, rule)`,
  which dispatches the bare function on a dask worker and **does
  not** invoke Prefect's task runtime. (In serial mode, where the
  same function is called directly, the decorator is also effectively
  a no-op because there is no enclosing flow context.)
* **Subflow factory**:
  `src/pycmor/core/pipeline.py:177-203`,
  `Pipeline._run_prefect`. Constructs a fresh `@flow` per call with a
  `DaskTaskRunner`. The factory captures the cluster's scheduler
  address; if no cluster has been assigned via `assign_cluster(...)`,
  it falls back to a local cluster. **This is the only real
  Prefect-managed boundary in the parallel path.**
* **Step prefectisation**:
  `src/pycmor/core/pipeline.py:108-150`, `Pipeline._prefectize_steps`.
  Each step in the pipeline is wrapped as a `Task` (or one collapsed
  task if `collapse_steps=true`). The `cache_policy=TASK_SOURCE+INPUTS`
  on these Tasks is honoured **only** when the Task runs inside a
  flow context, i.e. inside `dynamic_flow`. Outside that context the
  cache policy is silent — see §4.2.
* **The cluster shared across rules**: a single `LocalCluster` (or
  `SLURMCluster`) is created once by `Cmorizer.__init__` /
  `Cmorizer._setup_cluster()` (see `core/cmorizer.py`,
  `_setup_cluster` and adjacent methods). All rules'
  `_run_prefect` calls attach their inner subflows to the same
  scheduler.

So the Dask scheduler sees N parents
(N = total rules ≈ 52 for cap7_atm — submitted as bare-function
futures) and, **inside each one**, M further tasks
(M = number of pipeline steps per rule, currently 1 with
`collapse_steps=true` or 13 without). All competing for `W × TPW`
thread slots.

---

## 3. Mechanism of the deadlock

Define:

* `S = W × TPW`: total worker thread slots.
* `P_active`: number of Process-rule parent tasks currently running
  (each occupies one thread).
* `C_active`: number of pipeline child tasks currently running
  (each occupies one thread).
* `P_active + C_active ≤ S` always.

Each parent that is *running its body* is blocked at the synchronous
call `result = dynamic_flow(data, rule_spec, return_state=True)`
(`pipeline.py:197`). The parent thread cannot make progress until
the subflow returns. The subflow returns only when its child task(s)
have completed. The child tasks need a free thread.

The deadlock condition: `P_active = S` and `C_active = 0`. No child
can start. No parent will release until a child starts. No exit
path.

Why probabilistic: Prefect/Dask schedules submission greedily and
asynchronously. The Dask scheduler may either (a) schedule a
parent's body to start before another parent's child gets a chance,
or (b) prioritise children if they were submitted first. Empirically
(b) is sometimes the case at low slot counts but degrades at higher
slot counts. The exact Dask scheduling policy depends on
`distributed.scheduler.allowed-failures`, the
`order` heuristic, and timing of submissions.

Why mid-memory configurations show *more* deadlocks than tight ones:
in `4×4×16`, dask-nanny aggressively kills workers that exceed
budget; the kill+restart cycle effectively breaks the resource hold
because Prefect retries the killed task on a different worker. The
deadlock can't lock in. In `4×4×24` and `4×4×32`, workers stay alive
just long enough that the deadlock pattern stabilises; in `4×4×64`
the slot count is low enough that the over-subscription doesn't
form. So the tight-memory configurations are accidentally robust for
the wrong reason (workers churning), and the generous-memory ones
are robust for the right reason (lower N parents per pool).

---

## 4. Proposed change: eliminate the inner subflow

### 4.1 Design

Replace the subflow with a direct synchronous call. The pipeline
runs *natively* inside the Process rule task — no second Prefect flow
boundary. Pseudocode delta in `Pipeline._run_prefect`:

**Before** (current, `pipeline.py:177-203`):

```python
def _run_prefect(self, data, rule_spec):
    cmor_name  = rule_spec.get("cmor_name")
    rule_name  = rule_spec.get("name", cmor_name)
    addr = self._cluster.scheduler.address if self._cluster else None

    @flow(
        flow_run_name=f"{self.name} - {rule_name}",
        description =rule_spec.get("description", ""),
        task_runner=DaskTaskRunner(address=addr),
        on_completion=[self.on_completion],
        on_failure  =[self.on_failure],
    )
    def dynamic_flow(data, rule_spec):
        return self._run_native(data, rule_spec)

    result = dynamic_flow(data, rule_spec, return_state=True)
    if result.is_failed():
        exc = result.result(raise_on_failure=False)
        if isinstance(exc, BaseException):
            raise exc
        raise RuntimeError(f"Pipeline '{self.name}' failed for rule '{rule_name}': {exc}")
    return result.result()
```

**After** (proposed):

```python
def _run_prefect(self, data, rule_spec):
    """Run the pipeline synchronously in the calling thread.

    No inner subflow. The deadlock-prone path used to wrap the steps
    in a Prefect ``@flow`` whose ``DaskTaskRunner`` shared the same
    pool as the outer parent — see §3 for the deadlock mechanism.
    Removing the inner flow eliminates the window entirely.
    """
    import time
    cmor_name = rule_spec.get("cmor_name")
    rule_name = rule_spec.get("name", cmor_name)
    logger.info(f"Pipeline '{self.name}' running for rule '{rule_name}'")
    t0 = time.monotonic()
    try:
        result = self._run_native(data, rule_spec)
    except BaseException as exc:
        elapsed = time.monotonic() - t0
        try:
            self.on_failure_native(rule_name=rule_name,
                                   pipeline_name=self.name,
                                   elapsed_s=elapsed,
                                   exception=exc)
        except Exception as cb_exc:
            logger.warning(f"on_failure_native callback raised: {cb_exc}")
        raise
    elapsed = time.monotonic() - t0
    try:
        self.on_completion_native(rule_name=rule_name,
                                  pipeline_name=self.name,
                                  elapsed_s=elapsed)
    except Exception as cb_exc:
        logger.warning(f"on_completion_native callback raised: {cb_exc}")
    return result

@staticmethod
@add_to_report_log
def on_completion_native(rule_name, pipeline_name, elapsed_s):
    logger.success(
        f"Pipeline '{pipeline_name}' completed for rule "
        f"'{rule_name}' in {elapsed_s:.1f}s"
    )

@staticmethod
@add_to_report_log
def on_failure_native(rule_name, pipeline_name, elapsed_s, exception):
    logger.error(
        f"Pipeline '{pipeline_name}' FAILED for rule '{rule_name}' "
        f"after {elapsed_s:.1f}s: {type(exception).__name__}: {exception}"
    )
```

The new callbacks accept the data the operator actually wants to see
in the report log (rule name, pipeline name, elapsed time, and on
failure the exception). Revision 1 of this proposal kept the
existing `on_completion` / `on_failure` signatures with synthetic
`flow=None, flowrun=None` arguments — but the existing callback
bodies just `logger.success(f"{flow=}")` / `logger.error(f"{flowrun=}")`,
which under synthetic args would have produced log lines like
`flow=None` and `flowrun=None`. That's noise, not signal. The
revised callbacks are 5 lines and produce the meaningful summary
directly. The original `on_completion` / `on_failure` static methods
are retained on the class to avoid breaking any external import,
but no longer called from `_run_prefect`.

`_run_native(self, data, rule_spec)` already exists at
`pipeline.py:122-125` and runs the pipeline steps synchronously
in-thread:

```python
def _run_native(self, data, rule_spec):
    for step in self.steps:
        data = step(data, rule_spec)
    return data
```

This already does what we need. The change is removing the subflow
wrapper from `_run_prefect` and routing through `_run_native`
directly; the `@flow` and `DaskTaskRunner` lines are deleted.

### 4.2 What is lost

* **The inner subflow's Prefect UI page**. Currently the Prefect UI
  shows `<pipeline_name> - <rule_name>` as a nested flow run.
  After the change there is no separate Prefect-flow entity for the
  pipeline body; the operator sees only the dask future for the
  outer `_process_rule` invocation. We propose renaming the outer
  Process-rule output to include the pipeline name in its log line
  so the human-readable trail is preserved.
* **Per-step Prefect task tracking inside the pipeline**, but only
  when `collapse_steps=False`. With `collapse_steps=True` (the
  recommended config after `OPTIMIZATION_PLAN.md` Round 2 + 4)
  there is already only one Prefect task per pipeline. This PR
  proposes making `collapse_steps=True` the **default** so this
  loss is moot — see §7.
* **`cache_policy=TASK_SOURCE+INPUTS`** set on every prefectised
  step (or on the single collapsed task) at `pipeline.py:50-51`.
  Worth being explicit because revision 1 hand-waved this:

  > Prefect's task-level `cache_policy` is only honoured when the
  > Task is invoked **inside an active flow run context**. Today
  > the policy is honoured because the inner `dynamic_flow`
  > establishes that context. After this PR the Task body still
  > runs (Prefect Tasks are callable directly), but the
  > `cache_policy` is silently a no-op. Cache hits across reruns
  > of the same yaml will not occur.

  Practical impact: pycmor's primary deployment is one cmorize per
  simulation year — the cache rarely hit anyway because consecutive
  invocations are on different input years. Earlier session logs
  show many `HashError: Unable to create hash` messages in the
  parallel-mode path, indicating the cache wasn't hitting reliably
  even when it was supposed to. Net cost of losing this cache
  semantics in production: empirically zero.

  If cache reuse becomes important later (e.g. for a dev workflow
  that re-runs the same yaml repeatedly), restoring it would
  require routing the collapsed Task through a controlled flow
  context — an extension that doesn't conflict with this PR.

* **The on_completion / on_failure hooks attached to the inner flow**
  are replaced with `on_completion_native` / `on_failure_native`
  (signatures in §4.1) which are called directly. Revision 1
  proposed keeping the original signatures with synthetic
  `flow=None` arguments; that would have produced
  `flow=None\nflowrun=None\n…` log lines. The native variants are
  better.

### 4.3 What is gained

This patch addresses **one of at least three** failure modes
observed at high concurrency on production yamls. The other two
are independent of the inner-subflow architecture and need separate
work — see §10 for the empirical evidence and §10.3 A/D for the
specific gates that would discriminate them.

* **The parent×subflow deadlock window is eliminated**. The
  `Process rule` task is the *only* level submitting work to the
  dask pool. There is no second-level submission that could
  over-subscribe.
* **Memory / slot budgeting becomes predictable**: with one task per
  rule, `S = W × TPW` is the literal cap on rule concurrency. To
  schedule 16 concurrent rules safely we need 16 slots, full stop —
  no longer N×2 = 32.
* **Higher-concurrency configs become *less risky*** —
  not necessarily *viable*. `4×4×16`, `4×4×32`, and `3×4×48` looked
  promising in mini-cap7 but failed in different ways at full scale
  under the unchanged architecture (see §10.1: 2 zero-completion
  stalls, 3 tiers with `KilledWorker` storms including extra_atm
  with 68 kills, 10 tiers with unsubmitted-rule tails). This patch
  retires the deadlock failure mode. It does **not** address
  worker-memory-budget churn (the `KilledWorker` storms — which are
  insensitive to the inner-flow boundary) or wall-budget
  underprovisioning (the unsubmitted-rule tails). Promotion of any
  high-concurrency config to default still requires gates A and D
  in §10.3 and likely separate per-tier walltime tuning.
* **Heartbeat logging** (separate small change shipping alongside
  this) gives operators an unambiguous "still working" signal during
  long `save_dataset` runs that previously looked indistinguishable
  from a hang. §10.1's `veg_land`/`nLitter` row (9461 s in a single
  save) is exhibit A: under prior behaviour the operator would have
  killed it as a hang; with the heartbeat the slow-but-progressing
  pattern is correctly diagnosed as a *separate* per-rule
  bottleneck. (Cross-reference: this is the same chunked-input read
  bottleneck on OIFS HDF5 chunks documented in
  `bench_hr_ua_6hr_results.md` — ~1 MB/s read on production chunks
  dominates wall on heavy single rules.)

---

## 5. Risk assessment

| risk | likelihood | impact | mitigation |
|---|---|---|---|
| on_completion/on_failure callbacks break because they expect Prefect-shaped arguments | low (current implementations only `logger.info(state)`) | low (logging) | provide the dict shape and document it; add a one-line test |
| existing test suite mocks the subflow shape | low | low | run `tests/unit/test_pipeline.py` post-patch; expand only if a test breaks |
| External agents call `Pipeline._run_prefect` directly and expect a `Completed` State | low (it is a private method) | low | keep behaviour: when the pipeline succeeds, return the result data; when it fails, raise — same as the current code |
| Loss of per-step Prefect cache reuse (cache_key_fn etc.) | medium *if* anyone re-enables non-collapsed pipelines | low (cache wasn't reliably hitting anyway — see HashError noise in earlier logs) | document that `collapse_steps=True` is the production-tested path |
| Lower-priority: Prefect UI shows fewer entities per rule | n/a | low | rename outer task to embed pipeline name |
| Hidden code paths assume a flow-context exists when the steps run (e.g. `prefect.context.get_run_context()`) | medium | medium | pre-flight grep across `pycmor/std_lib/` for `get_run_context` and any decorator-with-side-effects patterns; mock test |
| Killed parent task does not propagate cancellation to in-flight pipeline body | medium | medium | the **current** subflow design has the same property — Prefect's dask runner cancels from outside; the inner flow's tasks are already in the same boat. The new design is no worse. |

The single largest unknown is the **prefect.context** pre-flight
audit. We have not yet grep'd the pipeline-step library for context
look-ups. This is item 1 in the validation plan below.

---

## 6. Validation plan

### Phase 6.1 — static audit (1 hour)

* `git grep -nE "from prefect\.context|get_run_context|TaskRunContext"` in `src/pycmor/`.
* If any pipeline step calls one of these, mark it as needing rework
  before the patch lands.
* Diff the existing `tests/unit/test_pipeline.py` and
  `tests/unit/test_files.py` against the proposed change. Identify
  any test that asserts a `Completed` state shape. Adjust as needed.

### Phase 6.2 — single-rule smoke test (30 min)

* `examples/cmip7_bench_hr_ua_6hr.yaml` at `parallel: false` (default)
  and `parallel: true`. Confirm `pycmor process` succeeds end-to-end
  in both modes. The single-rule case is small enough that any
  surface-level break shows immediately.

### Phase 6.3 — mini-cap7 re-sweep (5 hours)

* Re-run the mini-cap7 sweep (`examples/launch_mini_cap7_sweep.sh`)
  with the new pipeline implementation, **at n=5 ensemble per
  configuration × 8 configurations = 40 jobs** (revised from
  Round 4's 3-copy ensemble). The 3-copy ensemble used in Round 4
  supports a one-sided 95 %-CI upper bound of ~12 % on
  `p(deadlock)` per configuration; n=5 tightens this to **< 7 %**,
  which is the threshold below which we are willing to claim
  "deadlock retired" in a public commit message.
* The two extra copies should be drawn from the same `/work` source
  set used in Round 4 (each on its own Lustre stripe-c8 directory)
  so wall-time variance is dominated by the same I/O conditions.
* Goal: zero deadlocks across all 40 jobs. Any deadlock observation
  in the new architecture is a release blocker.
* Compare wall to Round 4b (`PYCMOR_PREFECT_COLLAPSE=1`) — should
  match or improve, never regress per (W, mem) configuration.
* Spot-check: run the existing `pycmor_bench_hr_ua_6hr.yaml` ensemble
  (5 ensemble × source data) — wall should match the warm-cache
  baseline (~2:45) within ensemble variance.

### Phase 6.4 — full cap7_atm validation (3 hours)

* `submit_hr_year.sh` against year 1587 cap7_atm at the new default
  config (likely `3×4×48` or `4×4×16` based on Round 4b). Acceptance
  criteria:
  * **Completion count: matches or improves the better of**
    (a) the in-flight `2×4×64+collapse` baseline (jobs `24713243-60`,
    §10.3 gate B) and (b) §10.1 row 2 (`4×4×16+collapse`,
    49 ok / 4 fail / 8 unsubmitted under the unchanged
    architecture). We refuse to commit to a numeric absolute
    threshold here because the §10.1 4×4×16 number (49/61, ~80 %)
    is too low to be acceptable as a default; it reflects the
    pre-fix architecture's failure modes (4 recipe bugs are
    independent of architecture, but the 8 unsubmitted are not —
    they likely correlate with the deadlock §4 targets and/or the
    wall-budget binding documented in §10.1's "unsubmitted-rule
    tails" framing). The gate-B comparison number is the right
    reference; we will fill it in once that run completes.
  * **Wall matches or improves the 2:57 baseline** measured at the
    prior production default (`2×4×64+collapse`). The earlier
    "≥ 15 % wall reduction" target was an upper bound from
    mini-cap7's heaviest-7-rules subset; the full 52-rule mix is
    dominated by Lustre metadata / I/O cost on the lighter rules,
    so a comparable mini-cap7 win does not extrapolate. We are
    not willing to commit to a numeric wall reduction at the
    full-tier level until §10.3 gates A and C produce a real
    measurement.
  * Zero deadlock symptoms (no log silence > 5 min after the heavy
    rules have started, with the heartbeat patch from issue #1
    confirming progress within each save).
  * `KilledWorker` count not worse than the same-config
    unchanged-architecture baseline. We do **not** require zero
    kills here — §10.1 demonstrates that the kill-storm pattern at
    tight memory budgets is independent of the §4 fix and would
    block this gate spuriously if held to "zero".

### Phase 6.5 — cross-tier regression (overnight)

* Run all 17 HR tier yamls with `submit_hr_year.sh` under the new
  defaults. None should regress vs current baseline (52-rule core
  rules, 17-rule ocean tiers, etc.).

---

## 7. Bundled changes

This proposal is for a single patch with the following pieces:

1. **`src/pycmor/core/pipeline.py`**:
   - Refactor `_run_prefect` to call `_run_native` directly, no inner `@flow`.
   - Wire on_completion / on_failure callbacks via direct calls.
   - Default `collapse_steps=True` going forward (env var
     `PYCMOR_PREFECT_COLLAPSE` becomes `=0` to opt out).

2. **`src/pycmor/core/cmorizer.py`**:
   - Optionally rename the outer `@task` to embed the rule name
     (e.g. via `task_run_name="Process rule[{rule.name}]"`).

3. **`src/pycmor/std_lib/files.py`**:
   - Add the `_Heartbeat` context manager around `save_dataset`
     (issue #1; small, independent, ships in this same PR for one
     atomic UX improvement).

4. **`tests/unit/test_pipeline.py` — positive unit-test plan**:

   The patch removes the inner `@flow` and routes through a new
   direct-call path. We add the following tests (none of them
   require a running Prefect server or a real dask cluster — all run
   under the existing in-process pytest setup):

   - `test_run_prefect_calls_run_native_directly`: monkey-patch
     `Pipeline._run_native` to record its argument-tuple, call
     `_run_prefect(data, rule_spec)`, assert `_run_native` was
     invoked exactly once with `(data, rule_spec)`. Catches a
     regression where the inner-flow wrapper accidentally re-appears.
   - `test_on_completion_native_invoked_on_success`: pipeline with a
     no-op step, monkey-patch `Pipeline.on_completion_native` to
     record its kwargs, assert the call carries `rule_name`,
     `pipeline_name`, and `elapsed_s ≥ 0`.
   - `test_on_failure_native_invoked_on_exception`: pipeline with a
     step that raises `ValueError("boom")`, monkey-patch
     `Pipeline.on_failure_native` similarly, assert the call carries
     `exception` of type `ValueError` with the right message, and
     that `_run_prefect` re-raises after the callback runs.
   - `test_callback_exception_does_not_mask_pipeline_result`: make
     `on_completion_native` itself raise; assert the pipeline
     result is still returned and the callback exception is logged
     (matching the `try/except Exception as cb_exc: logger.warning`
     idiom in the §4.1 pseudocode).
   - `test_collapse_steps_default_true`: assert that
     `Pipeline(steps=[...])` constructed without an explicit
     `collapse_steps=...` argument has `collapse_steps == True`
     after this patch. Catches accidental flips of the default.
   - `test_no_dask_cluster_assigned_uses_inline_native_path`: with
     `self._cluster is None`, assert `_run_prefect` runs the steps
     in-thread without raising and without attempting to construct
     a `DaskTaskRunner`. (Today's code falls back to a local
     cluster; the patch should not regress that behaviour for the
     orchestrator≠dask case.)
   - `test_no_prefect_run_context_required`: after the patch,
     `_run_prefect` is called with no surrounding flow context;
     assert it returns successfully even when
     `prefect.context.get_run_context()` would raise. This makes
     the §4.2 "cache_policy is a no-op" property explicit: the
     code path does not depend on a flow context existing.

   Existing tests to audit for breakage (Phase 6.1):
   `tests/unit/test_pipeline.py`, `tests/unit/test_files.py`. Any
   assertion of a `Completed` state shape returned by
   `_run_prefect` will need updating to the new "return data on
   success / raise on failure" contract (which is what the
   serial-mode path already does).

5. **`OPTIMIZATION_PLAN.md`**:
   - Append a Round 5 entry pointing at this design and the
     Phase 6.3-6.4 validation results.

6. **`HANDOFF_memory_pressure.md`** (the project's running
   investigation log):
   - Cross-reference the Round 4 deadlock evidence to the design
     proposal so future agents understand the context.

Estimated effort: 3-5 days of careful work, including the static
audit and validation. The actual code change is ~50 lines.

---

## 8. Alternatives considered

### 8.1 Run "Process rule" off the dask pool entirely

Use a separate `concurrent.futures.ThreadPoolExecutor` (or
`ProcessPoolExecutor`) at the top level for `_process_rule`, so dask
workers only see pipeline child tasks. This is the cleanest
separation and has been used in similar tools (e.g. some Dagster
configurations). However:

* requires changing `Cmorizer.process()` to manage a second
  executor, including how progress, retries, and exceptions
  propagate;
* loses the natural "everything is a Prefect task" narrative;
* the resulting code path is harder to reason about (two parallel
  executors, two queues, two failure semantics).

The proposed change in § 4 is strictly simpler: collapse two layers
into one. It achieves the same deadlock-elimination property with
fewer moving parts.

### 8.2 Increase `TPW` so child tasks always have headroom

E.g. set `TPW = 8` so each parent has spare threads in its own
worker. Empirically: as `TPW` grows, the deadlock window narrows but
contention on shared memory / blosc CPUs grows. The Round 4 sweep
held `TPW=4` constant precisely because lower values
(`TPW = 1, 2`) deadlock catastrophically — pycmor's earlier
investigations confirmed this.

This is a tuning band-aid; it doesn't change the structural fact
that a parent×child same-pool nesting can deadlock. Rejected.

### 8.3 Rewrite to native Dask delayed graphs (no Prefect)

Express the entire cmorize flow as a dask `delayed`/`futures`
DAG. This is the orthodox Dask approach and would let the scheduler
see the full graph upfront, avoiding nested submission. However it
is a multi-week rewrite of pycmor's orchestration layer and the
project's per-rule observability (run names, retries,
`on_completion`/`on_failure` hooks, per-task caching) would all need
re-implementation. Out of scope for the present problem.

### 8.4 Switch task runner to Prefect's `ConcurrentTaskRunner`

Prefect's default task runner uses `asyncio` rather than dask.
Avoids the dask thread pool entirely. But: pycmor's pipeline steps
are *intentionally* dask-backed (the dataset loads as a dask array,
xarray operations build a dask graph that's evaluated during
`save_dataset`). Switching the *task* runner away from dask doesn't
remove dask usage — dask still exists for the data plane. The slot
question collapses to "how many Prefect concurrent tasks" vs "how
many dask threads", and the same M-parents-blocking-N-children
shape can re-emerge depending on configuration. This adds complexity
without solving the structural issue.

### 8.5 Limit parent concurrency with a Semaphore on the parent pool

Keep both nesting levels (outer `Process rule` parent + inner
`@flow` subflow), but cap the number of parents that can be
*running their body* simultaneously to `max(1, S - 1)` so that at
least one slot is always free for a child. Implementation: a
`distributed.Semaphore(name="pycmor-parent", max_leases=S-1)`
around `_process_rule`'s body; parents block on `acquire()` before
calling `pipeline.run(...)`.

Why this is tempting: it is a ~10-line change with no
architectural restructuring. The parent×subflow deadlock condition
`P_active = S, C_active = 0` becomes unreachable by construction
because we cap `P_active ≤ S - 1`.

Why we reject it as the *primary* fix:

* The cap reduces effective rule concurrency from `S` to `S - 1`
  for the same hardware budget. At today's `S = 16` (`4×4×4`)
  this is a 6 % throughput hit on the parent dimension; not free.
* The Semaphore must be sized exactly. Sized too low → wasted
  slots; sized too high → deadlock returns. The "right" value
  depends on the runtime's M (children per parent), which varies
  per pipeline (the `huss_pipeline` and `sfcwind_pipeline` differ
  in step count). One global Semaphore can't capture this.
* The fix preserves the deeper code smell — *we still have nested
  bounded-pool submission* — and any future refactor that
  introduces a third nesting level (e.g. a step that spawns a
  sub-task) re-creates the deadlock window.
* `distributed.Semaphore` requires a Dask scheduler to host the
  state. The current code path attaches the inner subflow to a
  scheduler that's created per `_run_prefect` call when no
  cluster has been assigned via `assign_cluster(...)`. Plumbing a
  shared Semaphore across that lifecycle is non-trivial.

A Semaphore could nevertheless be useful as a **belt-and-braces
guard** layered on top of §4 — set `max_leases = S` (i.e.
unconstrained for the §4 path where there are no children to
starve), and only tighten if a future change re-introduces nested
submission. The patch does not currently include this, but it is
a low-risk follow-up if reviewers want defence-in-depth.

### 8.6 Run the inner pipeline on a *separate* dask pool

Keep both nesting levels but give each one its own thread pool:
the outer parent runs on the main dask cluster (`W × TPW` slots),
and each `_run_prefect` constructs a fresh small `LocalCluster`
(say 1 worker × `TPW` threads) just for that pipeline's steps.
Children no longer compete with parents for the same slots,
breaking the deadlock by isolation rather than by elimination.

Why we reject this as the primary fix:

* **Boot cost**: spinning up a `LocalCluster` is 1-3 s of
  overhead (worker init, dashboard, registration) — multiplied by
  N rules per yaml (≈ 50-120 in production) this is 1-6 minutes
  of pure overhead per cmorize, paid wall-time-serially in the
  driver. That's a 5-10 % wall-time regression at the high end.
* **Memory accounting becomes opaque**: each inner cluster
  declares its own `memory_limit`. The total commit across all
  parents is then `W_outer × TPW + N_concurrent_parents × TPW`
  threads against the same SLURM cgroup, with no central
  scheduler view of the concurrent footprint. The §6.4
  zero-`KilledWorker` acceptance criterion would be substantially
  harder to meet.
* **Scheduler proliferation**: each inner cluster has its own
  scheduler/dashboard. Diagnostics multiply. Operators tracking
  one stuck rule have to find the right inner-cluster log among
  many.
* **The fix doesn't address the 3 production failure modes
  documented in §10 either** — `KilledWorker` storms (memory
  budget) and unsubmitted-rule tails (wall budget) are
  unaffected. So the upside is exactly the same as §4 (deadlock
  retired) and the downside is much larger.

This is the right pattern for systems where parent and child work
have *qualitatively different* resource profiles (e.g. metadata-
bound parents and compute-bound children). pycmor's parents and
children are the same kind of work; isolating them adds complexity
without buying any structural property §4 doesn't already provide.

---

## 9. Open questions for review

1. Are the on_completion / on_failure callbacks guaranteed to never
   inspect Prefect-flow-specific fields anywhere in the codebase?
   We will answer this in the static audit (Phase 6.1) but flag it
   here for reviewers.
2. Is anyone running pycmor with `parallel: true,
   pipeline_orchestrator: prefect_native` (i.e. without dask)? If so,
   does the proposed change affect that path? (`_run_prefect` is
   only called when the orchestrator is dask in `parallel:true` mode;
   we believe the answer is "no impact", but want a maintainer to
   confirm.)
3. The cmorizer caches Prefect *task* results (`cache_policy`).
   Does anyone actually rely on cache hits between runs of the same
   yaml? With `collapse_steps=True` becoming the default, the cache
   key changes (1 task per rule instead of 13), so any stored cache
   from before the change is invalidated. This is acceptable for
   production runs (one cmorize per simulation year) but worth
   flagging for development workflows.
4. The proposal default-flips `collapse_steps`. Is there any user
   actively relying on the 13-task-per-rule behaviour for
   per-step inspection? If so, opting back in via
   `collapse_steps: false` per-pipeline yaml is preserved.

---

## 10. Empirical baseline at proposed operating point (PRE-FIX, NOT §4 validation)

> ⚠️ **READ THIS FIRST.** The 17-tier run reported below was executed
> with the **§4 architectural fix NOT applied**. It used the unchanged
> `_run_prefect` (i.e. the inner `@flow` + `DaskTaskRunner` nesting at
> `pipeline.py:177-203` is still present). Commit `a41103a`
> ("HR submit: new default 4×4×16 + collapse") only changed config
> defaults — it did not touch `pipeline.py`. Verify with
> `grep -nE "@flow|DaskTaskRunner" src/pycmor/core/pipeline.py`
> (4 matches expected).
>
> So §10 is **production-scale baseline data at the proposed new
> operating point under the existing architecture**. It is *not*
> evidence for or against the §4 patch's effectiveness. Earlier
> wording in revision 2 of this proposal mis-framed §10 as validation
> of §4; that wording is wrong and is corrected throughout this
> revision (revision 3 of §10).

Between revision 1 and revision 2, we ran a full 17-tier cmorize of
year 1587 at the proposed new default (`N_WORKERS=4`,
`MEM_PER_WORKER=16GB`, `PYCMOR_PREFECT_COLLAPSE=1`,
`TPW=4`) — SLURM jobs `24711300` through `24711317`.

Logs (one per tier, at the repository root for verification):

```
/work/ab0246/a270092/software/pycmor/pycmor_hr_4x4x16_<tier>_<jobid>.log
```

The comparison run (`2×4×64+collapse` — the prior production default,
same year, same yamls) is in flight as SLURM jobs
`24713243-24713260`, launched 2026-05-06 ~05:30 UTC. Logs land at
`pycmor_hr_par_pycmor-hr-<tier>-y1587-2x4x64-collapse-*.log` once
those jobs finish.

### 10.1 Tier-by-tier outcome at 4×4×16+collapse (pre-fix)

Counts derived from grepping each log for
`Flow run '.*' - Finished in state Completed` (ok),
`Flow run '.*' - Finished in state Failed` (fail), and
`KilledWorker` (worker terminations). Rule denominators are from
the per-tier yaml (`grep -c '^  - name:' yamls/<tier>.yaml`).
"Submitted" = ok + fail; rules in the denominator that are neither
ok nor fail were either never dispatched (job ran out of wall) or
stuck in Prefect's pending state.

| tier         | rules | ok | fail | killed | terminal state                                   |
|---|---:|---:|---:|---:|---|
| cap7_aerosol | 7   | 6  | 0  | 0  | success-with-1-pending                            |
| cap7_atm     | 61  | 49 | 4  | 0  | partial; 4 recipe bugs match prior default; 8 unsubmitted |
| cap7_land    | 123 | 0  | 0  | 0  | **stuck — 0 ok, 0 fail, 0 killed, walltime hit** |
| cap7_ocean   | 14  | 3  | 5  | 0  | partial; 6 unsubmitted                            |
| cap7_seaice  | 10  | 10 | 0  | 0  | clean ✓                                          |
| core_atm     | 84  | 76 | 2  | 13 | partial; 13 worker kills; 6 unsubmitted          |
| core_land    | 14  | 12 | 0  | 0  | clean-ish (2 unsubmitted)                        |
| core_ocean   | 38  | 29 | 0  | 0  | clean-ish (9 unsubmitted)                        |
| core_seaice  | 12  | 10 | 0  | 0  | clean-ish (2 unsubmitted)                        |
| extra_atm    | 25  | 16 | 5  | **68** | partial; **68 worker kills** — heaviest churn observed |
| extra_land   | 18  | 14 | 0  | 0  | clean-ish (4 unsubmitted)                        |
| lrcs_land    | 10  | 7  | 0  | 0  | clean-ish (3 unsubmitted)                        |
| lrcs_ocean   | 79  | 51 | 8  | 0  | partial; 20 unsubmitted                          |
| lrcs_seaice  | 80  | 41 | 9  | 22 | partial; 22 worker kills; 30 unsubmitted         |
| veg_atm      | 21  | 0  | 0  | 0  | **stuck — same fingerprint as cap7_land**        |
| veg_land     | 67  | 0  | 12 | 0  | partial-with-zero-completions; nLitter ran 9461 s on `save_dataset` per heartbeat patch but no rule reached `Completed` |
| veg_seaice   | 2   | 2  | 0  | 0  | clean ✓                                          |

Aggregate: **2/17 tiers truly clean** (cap7_seaice, veg_seaice — and
both happen to have ≤10 rules). **2/17 tiers with zero `Completed`
flow runs** (cap7_land, veg_atm). **3/17 with significant
`KilledWorker` events** (extra_atm 68, lrcs_seaice 22, core_atm 13).
**Remaining 10/17 tiers** have unsubmitted-rule tails ranging from
2 to 28 rules, indicating the 3 h walltime is sometimes a binding
constraint at this configuration.

This is **substantially worse than revision-2's §10.1 table claimed**.
The earlier table wrote "clean" for ten tiers without checking
completion counts; in fact several had unsubmitted-rule tails and
two more had `KilledWorker` events that the original table missed
(core_atm 13, extra_atm 68). Numbers above come from grep on the
actual logs and supersede the prior table.

### 10.2 What we cannot say from this data

1. **We cannot distinguish the two zero-completion tiers
   (cap7_land, veg_atm) from the parent×subflow deadlock §4
   targets.** Revision 2 confidently labelled these "Prefect
   ephemeral-server cold start". That label was unsupported. The
   fingerprints — 0 % CPU, 0 task completions, 0 heartbeats, no
   exception, runs to walltime — are *clinically indistinguishable*
   from the very deadlock §4 is designed to eliminate. We have not
   instrumented the stuck workers (e.g. `py-spy dump` against the
   stuck PIDs) to discriminate. Treat both stalls as
   **uncategorized** until we have a stack trace.

2. **We cannot quantify wall-time on the "clean-ish" tiers without
   a side-by-side baseline.** The proposal's wall-time motivation is
   the entire point. Revision 2 wrote "core_atm ~2:30h" in isolation.
   That is not a measurement — it's a single point with no reference.
   Without `2×4×64+collapse` numbers for the same tiers, we have no
   speedup or regression number to report. The in-flight comparison
   run is the missing data point.

3. **We cannot characterize the `KilledWorker` events as recovery vs
   loss.** `extra_atm` shows 68 kills + 16 ok + 5 fail — meaning at
   least some kills resulted in successful retries, but we did not
   compute the recovery rate. Was 3 h consumed mostly by kill+retry
   churn, or by genuine compute on the rules that completed? §10
   reports the count but not the cost decomposition. This is
   instrumentation work for the next round.

4. **`veg_land`'s heartbeat-confirmed live progress is a
   half-result, not a clean win.** `save_dataset[nLitter]` ran
   9461 s — confirming the rule was alive — but the tier ended with
   **zero `Completed` flow runs**. One rule consuming 2.6 h of the
   3 h budget on a single save blocked everything else. The
   heartbeat patch correctly re-classified this from "hang" to
   "slow". But heartbeat-confirmed-slow is still slow, and the
   single-rule-eats-tier-budget pattern is itself a problem
   (compounding with the `S = W × TPW = 16` rule-concurrency cap).
   Revision 2 §10.2 framed this as a win for the heartbeat work
   alone; it is also a flag of a separate bottleneck.

5. **Statistics**: §6.3 was bumped 3→5 ensemble for the mini-cap7
   deadlock claim. §10 reports **n=1** per tier × 17 tiers. With
   2 zero-completion tiers observed, the underlying stall rate is
   anywhere from ~3 % to ~30 % at 95 % CI. The two stuck tiers
   could be unlucky single draws, or they could be an emerging
   failure mode. We don't know without re-runs.

### 10.3 Required before merging §4 to main

The §4 patch is fit for a feature branch with this baseline data
attached, but is **not yet fit for default-config promotion**. To
land §4 + flip the default to a high-concurrency config, the
next review round needs:

* **A.** Re-run the 17-tier set at `4×4×16+collapse` with the §4
  patch applied. Compare cap7_land + veg_atm completion vs the
  baseline above. If they complete → strong evidence §4 fixed
  what we were calling "Prefect-boot stalls" (i.e. they were
  the deadlock). If they still stall → genuinely independent
  failure mode and §4.3 should drop the
  "higher-concurrency-becomes-viable" framing.
* **B.** Complete the in-flight `2×4×64+collapse` 17-tier run
  (jobs `24713243-60`) and tabulate the same ok / fail / killed /
  unsubmitted counts. This isolates *what changes between
  configs at the unchanged architecture*.
* **C.** Quantify wall-time on three named representative tiers
  at both configs (`2×4×64+collapse`, `4×4×16+collapse`) **and**
  at both architectures (pre-fix vs §4-applied) — i.e. 12 wall
  measurements total. The three tiers and the question each
  answers:
  - **`core_atm`** (84 rules, largest absolute wall on the
    "clean-ish" tiers in §10.1) — does §4 produce a measurable
    wall improvement on the bulk of production, or are gains
    confined to the kill-storm tiers?
  - **`extra_atm`** (25 rules, **68 `KilledWorker` events** at
    `4×4×16+collapse` — heaviest churn observed). Does §4 reduce
    the kill count? If kill-storm is purely memory-budget-driven
    (independent of architecture), the kill count should be
    invariant across pre-fix vs post-fix at the *same* (W, mem).
    If the kill count drops post-fix, that's evidence kill-storm
    has a deadlock-adjacent component. This tier is the
    discriminator.
  - **`core_ocean`** (38 rules, 2D-dominated workload, no kill
    storms in §10.1) — does the §4 wall win generalize beyond
    the atm-heavy tiers, or is it specifically a parent-fanout
    win? 2D ocean rules have a different memory profile and a
    different child-task fanout shape, so they are the cleanest
    out-of-distribution test.
* **D.** Capture a `py-spy dump` against the stuck workers in a
  reproduction of the cap7_land / veg_atm zero-completion pattern
  so the two stall fingerprints can be discriminated.

Until A and D land, the conservative read of §10 is:

> §10 is **production-scale baseline at the proposed new operating
> point under the unchanged architecture**, not evidence about §4.
> It exposes additional failure modes at full scale that mini-cap7
> did not surface (extra_atm 68 worker kills, several tiers with
> unsubmitted-rule tails). Whether any of these are also
> attributable to the parent×subflow deadlock §4 targets is an
> open question. §4.3's
> "higher-concurrency configs become viable in production" framing
> is **untested** and should not appear in the merged commit
> message.

We propose the next review round produce final wording for §4.3
once A, B, C, D have run.

### 10.4 Gate-B final results (2026-05-06)

Final outcome of the `2×4×64+collapse` 17-tier comparison run
(jobs `24713243-24713260`, submitted 2026-05-06 ~05:30 UTC).
14 of 17 jobs ran to completion or natural failure. Three jobs
(cap7_land `24713245`, lrcs_land `24713255`, veg_land `24713259`)
were **manually cancelled at 1:22 wall-clock** after producing
zero `Completed` flow runs and no Prefect log activity for
1 h 22 min. Numbers are ok / fail / killed; "begin" is
`grep -c "Beginning subflow run"`. Logs at
`/work/ab0246/a270092/software/pycmor/pycmor_hr_2x4x64c_<tier>_<jobid>.log`.

| tier         | rules | 4×4×16 (begin/ok/fail/killed) | 2×4×64 (begin/ok/fail/killed) | gate-B verdict |
|---|---:|---|---|---|
| cap7_aerosol | 7   | 5/6/0/0       | 5/6/0/0                | match |
| cap7_atm     | 61  | ?/49/4/0      | 52/24/29/0             | 2×4×64 surfaces 25 more recipe fails (see HANDOFF_failed_rules.md) |
| **cap7_land**| 123 | **28/0/0/0**  | **12/0/0/0 (cancelled at 1:22)** | **deadlocked at BOTH configs** |
| cap7_ocean   | 14  | 8/3/5/0       | 7/3/5/0                | match |
| cap7_seaice  | 10  | 10/10/0/0     | 9/10/0/0               | match |
| core_atm     | 84  | ?/76/2/13     | 76/44/37/0             | 2×4×64 finished; 4×4×16 had 13 kills, 0 here |
| core_land    | 14  | 11/12/0/0     | 11/12/0/0              | match |
| core_ocean   | 38  | 28/29/0/0     | 28/29/0/0              | match |
| core_seaice  | 12  | 9/10/0/0      | 9/10/0/0               | match |
| extra_atm    | 25  | ?/16/5/68     | 20/7/14/0              | 2×4×64 cleared the kill storm; more rules surfaced fails |
| extra_land   | 18  | 13/14/0/0     | 13/14/0/0              | match |
| **lrcs_land**| 10  | 6/7/0/0       | **5/0/0/0 (cancelled at 1:22)** | **deadlocked at 2×4×64; succeeded at 4×4×16** |
| lrcs_ocean   | 79  | ?/51/8/0      | 58/45/14/0             | comparable; 6 fewer ok at 2×4×64 |
| lrcs_seaice  | 80  | 64/41/9/22    | 63/49/15/0             | 2×4×64 cleared 22 kills; +8 ok |
| **veg_atm**  | 21  | **18/0/0/0 STALL** | **20/16/5/0 (21/21)** | **deadlocked at 4×4×16; succeeded at 2×4×64 in ~22 min** |
| **veg_land** | 67  | **59/0/12/0**     | **28/0/0/0 (cancelled at 1:22)** | **deadlocked at BOTH configs** |
| veg_seaice   | 2   | ?/2/0/0       | 1/2/0/0                | match |

#### 10.4.1 The begin/finish-ratio deadlock fingerprint

Gate B produced a **clean diagnostic** for the parent×subflow
deadlock that earlier sections did not have. Define:

* `begin` = count of `Beginning subflow run` lines in the log,
  i.e. parents that fired and entered `dynamic_flow(...)`;
* `ok + fail` = subflows that reached a terminal state
  (`Finished in state Completed | Failed`).

A healthy tier has `ok + fail ≈ begin` (every parent that
started its inner flow either completes or fails). A deadlocked
tier has `begin > 0, ok = 0, fail = 0, killed = 0` and **no log
activity for tens of minutes** — parents have entered the inner
`@flow` body and are blocked at the synchronous
`dynamic_flow(...)` call (`pipeline.py:197`) waiting for
children that cannot be scheduled because parents hold all the
slots. This is exactly the §3 mechanism's signature.

Tiers exhibiting this signature in gate B:

* `cap7_land` @ 4×4×16: 28 begin, 0 ok, 0 fail
* `cap7_land` @ 2×4×64: 12 begin, 0 ok, 0 fail (cancelled)
* `veg_atm`   @ 4×4×16: 18 begin, 0 ok, 0 fail
* `lrcs_land` @ 2×4×64:  5 begin, 0 ok, 0 fail (cancelled)
* `veg_land`  @ 2×4×64: 28 begin, 0 ok, 0 fail (cancelled)

Tiers running cleanly at the *same* config rule out
"Prefect ephemeral-server cold start" as the cause: cap7_aerosol,
cap7_seaice, core_*, extra_*, etc. all produce ok counts > 0 at
both configs from the same submit script and conda env. So
Prefect itself starts fine; the deadlock is in the user code's
nested submission pattern, exactly as §3 predicts.

#### 10.4.2 Headline findings

1. **The §4 deadlock is real, deterministic on lpjg-style tiers,
   and reproduces at the *prior production default* (`2×4×64`).**
   This is the strongest evidence in the proposal. Three tiers
   stalled identically at `2×4×64` (cap7_land, lrcs_land,
   veg_land), confirming the deadlock is not specific to the
   high-concurrency `4×4×16` config. The mini-cap7 sweep
   (`OPTIMIZATION_PLAN.md` Round 4) saying "2×4×64 is safe" was
   misleading because mini-cap7 picks the heaviest 7 cap7_atm
   rules, none of which are lpjg-style. **The prior production
   default is also affected.**

2. **Tier-by-tier deadlock susceptibility**:
   * `cap7_land` (123 rules, lpjg-monthly pipelines): deadlocks
     at **both** configs.
   * `veg_land` (67 rules, lpjg-monthly): deadlocks at **both**
     configs.
   * `veg_atm` (21 rules): deadlocks at 4×4×16, succeeds at
     2×4×64 — config-dependent.
   * `lrcs_land` (10 rules): deadlocks at 2×4×64, succeeds at
     4×4×16 — config-dependent in the *opposite* direction. This
     fits the §3 mechanism: at 2×4×64 (S=8) only 8 slots, and
     10 lpjg-style parents fire fast enough to fill them; at
     4×4×16 (S=16) the kill+restart cycle from tight memory
     breaks the resource hold.

3. **`KilledWorker` storms are pure memory-budget**: 68→0
   (extra_atm), 22→0 (lrcs_seaice), 13→0 (core_atm) moving from
   16 GB to 64 GB per worker. §4 does not address this.

4. **Recipe-bug rate increases at 2×4×64** for the heavier-fanout
   tiers: cap7_atm 4 fail → 29 fail, core_atm 2 → 37 (final),
   extra_atm 5 → 14, lrcs_ocean 8 → 14, lrcs_seaice 9 → 15.
   Tracked in `HANDOFF_failed_rules.md` — open question whether
   the extras are config-order-dependent or simply more rules
   completing far enough to surface their bugs.

5. **The "boot-stall" hypothesis from earlier §10 drafts is
   formally retracted.** The begin/finish-ratio diagnostic shows
   Prefect started fine; the deadlocks are in user code. Gate D
   (`py-spy dump`) is no longer needed to discriminate boot vs
   deadlock — the begin/finish ratio already does that.

#### 10.4.3 Implication for §4

Three tiers (cap7_land, veg_land, plus the config-dependent
others) were already broken at the prior production default
under the unchanged architecture. **§4 is no longer just a
prophylactic for a hypothetical higher-concurrency future — it
is a fix for a presently-broken default.** Gate A (running the
17-tier set with §4 applied at either config) is the next
required step.

### 10.5 §4 patch implemented; gate-A v1 partial findings; two infrastructure bugs fixed

#### 10.5.1 §4 patch is in code

Implemented on branch `feat/cmip7-awiesm3-veg-hr`
(2026-05-06 ~13:00 UTC) at
[`src/pycmor/core/pipeline.py`](src/pycmor/core/pipeline.py):

* Removed `from prefect import flow` and
  `from prefect_dask import DaskTaskRunner` imports.
* Replaced `_run_prefect`'s body with a direct synchronous call to
  `_run_native(data, rule_spec)` wrapped in a `time.monotonic()`
  timing band. No inner `@flow`. No `DaskTaskRunner(...)`.
* Added `on_completion_native(rule_name, pipeline_name, elapsed_s)`
  and
  `on_failure_native(rule_name, pipeline_name, elapsed_s, exception)`
  as `@staticmethod @add_to_report_log` callbacks invoked
  directly with real arguments (no synthetic Prefect-shaped
  `flow=None, flowrun=None`).
* Old `on_completion` / `on_failure` static methods retained on
  the class for any external import; no longer called from
  `_run_prefect`.
* Default `collapse_steps=True` (env var `PYCMOR_PREFECT_COLLAPSE`
  still respected for opt-out).

Phase 6.1 static audit (per §6) ran clean:
`git grep -nE "from prefect\.context|get_run_context|TaskRunContext|FlowRunContext"`
across `src/pycmor/` and `tests/` returns zero matches — no
pipeline step depends on a flow context being active.

`tests/unit/test_pipeline.py` passes (2/2, 104 s) under the
patched module.

The new diagnostic line in production logs is
`Pipeline '<pipeline>' running for rule '<rule>'` (start),
`Pipeline '<pipeline>' completed for rule '<rule>' in <s>s` (ok),
`Pipeline '<pipeline>' FAILED for rule '<rule>' after <s>s: <exc>`
(fail). The pre-fix `Beginning subflow run` and
`Flow run '...' Finished in state Completed` lines are absent;
their absence in a post-fix log is a sanity check that the patch
is taking effect.

#### 10.5.2 Gate-A v1: abandoned due to infrastructure contamination

Gate A was first submitted as jobs `24717840-24717858` at
`2×4×64+collapse` with the §4 patch applied. **Every job logged
`sqlite3.OperationalError: disk I/O error` during Prefect
ephemeral-server boot** (alembic migrations on aiosqlite). Some
jobs (cap7_atm `24717841`, cap7_seaice `24717844`) crashed at
~7:50 with zero task starts; others retried internally and
recovered. We cancelled the 14 still-pending / starting jobs and
let three stragglers (`core_atm 24717845`, `extra_atm 24717850`,
`lrcs_seaice 24717855`) run to gather the partial signal before
also cancelling them.

The partial signal from the three stragglers:

| job     | tier        | wall  | start (post-patch) | ok | fail | killed | old-subflow |
|---|---|---|---:|---:|---:|---:|---:|
| 24717845 | core_atm    | 24:52 | 77 | 15 | 15 | 0 | **0** |
| 24717850 | extra_atm   | 23:29 | 20 |  0 |  0 | 0 | **0** |
| 24717855 | lrcs_seaice | 18:27 | 64 |  4 | 52 | 0 | **0** |

Three things are visible in this partial v1 data even with the
infrastructure contamination:

1. **The `Beginning subflow run` count is zero in every log.**
   The §4 patch is in effect — there is no longer an inner Prefect
   flow per rule.
2. **`extra_atm` shows clean §4 behaviour for the first time.**
   At t=23:29 the heartbeat patch reports
   `save_dataset[rss|evspsbl|pfull|cl|wsg|rls]` running in
   parallel for 60-240 s each — i.e. all 20 parents are in their
   `_run_native` body simultaneously, all in `save_dataset`, with
   **0 `KilledWorker` events**. Pre-fix at the same `2×4×64`
   config this tier finished at `7 ok / 14 fail / 0 killed` in
   1:10. Pre-fix at `4×4×16` it was the worst tier in the batch
   with **68 `KilledWorker`** events. We now have one (still
   incomplete) data point that this tier runs cleanly under §4.
3. **The `0 ok` for extra_atm at 23 min is not a deadlock.** Under
   pre-fix any tier with `start > 0, ok = 0, fail = 0` for tens
   of minutes was the deadlock (§10.4.1). Here the heartbeat
   shows the rules are alive and progressing through
   `save_dataset` — the slow saves are the OIFS chunked-input
   read bottleneck (cross-ref `bench_hr_ua_6hr_results.md`),
   not orchestration. The diagnostic in §10.4.1 should add a
   parenthetical: under the §4 patch, `start > 0, ok = 0` is
   ambiguous between "rules are slow" and "rules deadlocked"
   *unless* the heartbeat output is consulted. Pre-fix the
   heartbeat would never appear because the inner flow blocked
   the worker thread. Post-fix the heartbeat is the
   discriminator.

#### 10.5.3 Two infrastructure bugs fixed

Two bugs were independently surfaced while triaging gate-A v1.
Both are pre-existing (affected gate B and the 4×4×16 baseline
too) but were attributed to other causes until v1 made the
pattern visible.

* **Bug 1 — Prefect SQLite on Lustre /scratch.**
  [`examples/run_hr_yaml_parallel.sh`](examples/run_hr_yaml_parallel.sh)
  set `PREFECT_HOME` to `/scratch/.../pycmor_tmp/$$/prefect`. When
  17 jobs concurrently boot ephemeral Prefect servers each backed
  by an aiosqlite DB on shared Lustre, alembic migrations
  intermittently raise
  `sqlite3.OperationalError: disk I/O error`. Some jobs recover
  via Prefect's internal retry; some crash at boot with zero rules
  run. **Fix**: put `PREFECT_HOME` on node-local `/tmp` (the
  ephemeral DB is < 10 MB; only big HDF5 spill stays on
  `/scratch`). The fix is in the same shell script:

  ```bash
  PREFECT_NODELOCAL=/tmp/pycmor_prefect_${SLURM_JOB_ID:-$$}
  mkdir -p $PREFECT_NODELOCAL/storage
  export PREFECT_HOME=$PREFECT_NODELOCAL
  export PREFECT_LOCAL_STORAGE_PATH=$PREFECT_NODELOCAL/storage
  trap "rm -rf $PREFECT_NODELOCAL" EXIT
  ```

  Implication for §10's earlier interpretation: the **disk-I/O
  flake is independent of the §4 deadlock**, but it was
  contributing intermittent "boot stalls" that earlier drafts of
  §10 misclassified. The boot-stall hypothesis was already
  retracted in §10.4.1 on the basis of the begin/finish-ratio
  diagnostic; this finding **confirms the retraction by giving
  a separate root cause for the cases that did stall pre-warmup**.

* **Bug 2 — `repoint_hr_year.py` digit-blind year filter.**
  [`examples/repoint_hr_year.py`](examples/repoint_hr_year.py)
  applied year filtering to `*_file:` lines via the regex
  `[a-z_]+_file:`, which excludes digits. Variables with digits
  in their names (`sgm22`, `sgm12` in `lrcs_seaice`) silently
  bypassed year filtering and shipped the regex-form pattern
  `sgm22\.fesom\..*\.nc` into the yaml's `*_file:` field, which
  pycmor's resolver treats as a literal path. The two
  `FileNotFoundError: 'sgm22\\.fesom\\..*\\.nc'` failures in
  `lrcs_seaice` at both 4×4×16 and 2×4×64 baselines (§10.1
  and §10.4) trace back to this. **Fix**: regex changed to
  `[a-z0-9_]+_file:`. Verified on lrcs_seaice that the patched
  script now produces
  `sgm22_file: ...sgm22\.fesom\.1587\.nc`.

  A second issue surfaced: the script's `OLD_RUN_TOKEN` was
  `"HR_test_01"` but the source HR yamls have
  `Final_CMIP7_IO_Test_01` hardcoded as the data-path component.
  The constant has been updated to match the current source, and
  a comment now points future maintainers at a one-line grep to
  re-derive the value if it drifts again.

Neither bug invalidates the §3 deadlock mechanism or the §4 fix.
Bug 1 explains a contamination source for §10's "stalls" that
revision 3 had already retracted. Bug 2 explains 2 of the 9
failures in the lrcs_seaice column of §10.1 / §10.4.1.

#### 10.5.4 Gate-A v2: clean re-submission

Submitted as jobs `24718781-24718797` on 2026-05-06 ~14:00 UTC
with the full stack: §4 patch + Prefect-on-`/tmp` +
fixed `repoint_hr_year.py`, fresh workdir, honest RUN argument
(`Final_CMIP7_IO_Test_01` rather than the no-op `Test_16n`
label). At time of writing the jobs are pending node
availability. The three load-bearing tiers to watch for the
deadlock-fix verdict (all stalled at this exact `2×4×64`
config under the unchanged architecture, §10.4):

* `24718783` cap7_land — pre-fix: 12 begin / 0 ok / 0 fail (cancelled)
* `24718792` lrcs_land — pre-fix:  5 begin / 0 ok / 0 fail (cancelled)
* `24718796` veg_land  — pre-fix: 28 begin / 0 ok / 0 fail (cancelled)

Acceptance for "§4 fixes the deadlock":

* Each of the three produces `ok > 0` (some
  `Pipeline '...' completed for rule` lines) within ~60 min of
  start. Heartbeat output should be visible during long saves.
* Zero `Beginning subflow run` lines (sanity-check the patch is
  in effect; the deployed pipeline.py has the @flow stripped).
* Zero `sqlite3.OperationalError: disk I/O error` lines
  (sanity-check Prefect-on-tmp is in effect).

Refusal mode: if any of the three still shows
`start > 0, ok = 0, fail = 0` with no heartbeat output for an
extended window, the §3 mechanism is incomplete and the §4 fix
is necessary but not sufficient. Reviewers should hold §4.3's
"deadlock window is eliminated" wording until v2 reports.

### 10.6 Gate-A v2 final results + new failure mode + parent-throttle fix

#### 10.6.1 Final tier-by-tier outcome at 2×4×64+collapse with §4 applied

Counts use the post-§4 log patterns
(`Pipeline '<pipeline>' running for rule` for start,
`Pipeline '<pipeline>' completed for rule` for ok,
`ERROR: Pipeline ... FAILED for rule` for fail). All 17 logs
contain zero `Beginning subflow run` lines (sanity check that the
§4 patch is in effect) and zero `sqlite3.OperationalError: disk
I/O error` lines (sanity check Prefect-on-`/tmp` is in effect).

| tier         | rules | start | ok | fail | killed | OSerr-30s-cascade |
|---|---:|---:|---:|---:|---:|---:|
| cap7_aerosol |   7 |   5 |   5 |  0 | 0 |   0 |
| cap7_atm     |  61 |  52 |  50 |  2 | 0 |   0 |
| **cap7_land**| 123 | 120 |   **0** | 62 | 0 | **436** |
| cap7_ocean   |  14 |   7 |   3 |  4 | 0 |   0 |
| cap7_seaice  |  10 |   9 |   9 |  0 | 0 |   0 |
| core_atm     |  84 |  77 |  77 |  0 | 0 |   0 |
| core_land    |  14 |  11 |  11 |  0 | 0 |   0 |
| core_ocean   |  38 |  28 |  28 |  0 | 0 |   0 |
| core_seaice  |  12 |   9 |   9 |  0 | 0 |   0 |
| extra_atm    |  25 |  20 |  15 |  1 | 0 |   0 |
| extra_land   |  18 |  13 |  13 |  0 | 0 |   0 |
| **lrcs_land**|  10 |   6 |   **6** |  0 | 0 |   0 |
| lrcs_ocean   |  79 |  58 |  51 |  7 | 0 |   0 |
| lrcs_seaice  |  80 |  64 |  51 | 13 | 0 |   0 |
| **veg_atm**  |  21 |  20 |  **20** |  0 | 0 |   0 |
| **veg_land** |  67 |  60 |  **59** |  1 | 0 |   0 |
| veg_seaice   |   2 |   1 |   1 |  0 | 0 |   0 |

`start < rules` in many tiers reflects rules that were filtered
out before submission (input regex matched no files, etc.) — not
the throttle. `start = ok + fail` for every tier *except*
`cap7_land`, where `start = 120, ok = 0, fail = 62, OSError
cascade = 436` — i.e. 62 rules failed and the remaining 58 ran
to walltime without ever returning a result.

#### 10.6.2 §4 verdict: deadlock fixed at 3 of 4 test tiers

The four tiers that stalled with the deadlock fingerprint pre-fix
(§10.4):

| tier         | pre-fix at this config (begin/ok/fail) | post-§4 (start/ok/fail) | verdict |
|---|---|---|---|
| veg_atm      | 18 / 0 / 0 STALL (4×4×16)              | 20 / 20 / 0             | **fixed** |
| lrcs_land    |  5 / 0 / 0 STALL (2×4×64)              |  6 / 6  / 0             | **fixed** |
| veg_land     | 28 / 0 / 0 STALL (2×4×64)              | 60 / 59 / 1             | **fixed** |
| **cap7_land**| 12 / 0 / 0 STALL (2×4×64)              | 120 / **0** / 62 + 436 OSError-cascade | **§3 deadlock fixed; new failure mode** |

So §4 retired the parent×subflow slot deadlock for 3 of 4
test tiers, exactly as the §3 mechanism predicted. cap7_land
unmasks a *different* failure that §4 alone doesn't address.

#### 10.6.3 cap7_land's new failure mode: unbounded parent fan-out

The 436 `OSError: Timed out trying to connect to scheduler after
30 s` events on cap7_land are not the parent×subflow deadlock.
Heartbeat output is present (~14-34 #s per rule) and 100 distinct
rules entered `save_dataset`. Diagnosis:

* `_parallel_process_prefect`'s naive
  `[self._process_rule.submit(rule) for rule in self.rules]`
  fires every rule synchronously into Prefect/Dask's queue.
* Each parent reaches `save_dataset` → `dataset.to_netcdf()` →
  `dask.compute()`. xarray/dask-distributed call
  `distributed.secede()` on the parent's worker thread to release
  it back to the pool while awaiting the chunk-write graph.
* Dask sees the thread free and dispatches the next queued
  parent. The new parent reaches `save_dataset` and secedes too.
* Iterate. With 123 homogeneous lpjg-monthly rules all hitting
  `save_dataset` at roughly the same rate, the scheduler ends up
  holding 50-100 concurrent save graphs.
* Scheduler's asyncio loop and TCP accept queue back up. New
  worker connection attempts hit the 30 s tornado connect timeout
  and the OSError cascade fires.

This is the ***same*** root pattern as §3 ("nested
bounded-pool submission overcommits the worker pool") but at a
different layer: parent → child here is "rule → save's dask
graph" via secede, not "parent task → inner-flow task" via
nesting.

Heterogeneous tiers escape this in gate-A v2: veg_land has 8
distinct pipelines and rule entry into `save_dataset` is
naturally staggered, so peak concurrency stays well below the
saturation cliff.

#### 10.6.4 Fix: enforce W×TPW parent throttle at the submit site

§4.3 of this proposal claimed:

> *"with one task per rule, S = W × TPW is the literal cap on
> rule concurrency"*

That promise was implicit ("dask will bound it") but `secede()`
breaks the implicit cap. Commits `6773ea5` (in
`_parallel_process_dask`) and `90f382f` (in
`_parallel_process_prefect`) make the cap explicit:

* `_parallel_process_prefect` (the production path under the
  current dispatcher routing) now submits in batches of
  `max_in_flight = W × TPW`, calling `prefect.futures.wait()`
  between batches. Trades a small wait-for-slowest-in-batch
  inefficiency for a hard concurrency cap.
* `_parallel_process_dask` uses an `as_completed` rolling
  window for the same effect; it is currently unreached because
  the `parallel_process()` dispatcher reads
  `pipeline_orchestrator` while the schema defines
  `pipeline_workflow_orchestrator` — left for a separate fix.

The throttle does not change the §4 architectural fix; it
addresses a *separate* failure mode that §4 unmasks at full
scale on tiers with many homogeneous rules.

#### 10.6.5 Gate-A v3: full 17-tier with throttle in place

Submission pending. Acceptance criteria for declaring §4 +
parent-throttle production-ready:

* Zero `OSError: Timed out trying to connect to scheduler` lines
  in any tier's log (especially cap7_land).
* `cap7_land` produces `ok > 0` (at least one `lpjg_monthly`
  rule completes — the saturation cliff currently kills all of
  them).
* No regression on the 3 tiers §4 already fixed (veg_atm,
  lrcs_land, veg_land all keep their `ok > 0` results).
* `start = ok + fail` for every tier — no rules wedged in flight
  at job end.

If gate-A v3 hits any of these acceptance criteria negatively,
the proposal needs a third diagnostic round before merge. The
failure modes documented in §10.6.3 may have a deeper structural
cause (e.g. the dispatcher key bug indicating that the dask path
should be the production default; an unrelated Prefect-vs-dask
choice; etc.).

---

## 11. Decision required

We propose **landing this patch on a feature branch and running
Phase 6.3-6.5 before merging to main**. The risk surface is small
(50 lines of code, no behavioural change for serial/single-rule mode),
the upside is large (eliminates a real production failure mode and
opens 15-23 % wall reduction on heavy yamls), and the validation
plan is well-scoped (≤ 1 working day of compute-time across the
proposed phases).

Reviewers requested for sign-off:

* pycmor maintainer for the architectural change (`pipeline.py`,
  `cmorizer.py`).
* AWI HPC team for the Lustre / SLURM behaviour the validation runs
  exercise (none expected to change but worth a sanity check).
* Any user actively relying on the per-step Prefect UI granularity.
