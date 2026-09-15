# Running cmorization with pycmor on Levante

A short guide for a colleague running their first cmorization pass on their own model output.

## 1. Environment

```bash
source /work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh
conda activate pycmor_py312
```

That points at Jan's shared miniforge install. To build your own env instead, clone `/work/ab0246/a270092/software/pycmor` and run `pixi install`, then `pixi shell -e dev`.

## 2. Rules

The AWI-ESM3-veg-HR rule set lives under `awi-esm3-veg-hr-variables/`, split by tier:

- `core_*` core CMIP7 variables per component (atm, land, ocean, seaice)
- `cap7_*` CMIP7-added variables beyond the core
- `extra_*` optional extras
- `lrcs_*` land / regional CMIP scenario variables
- `veg_*` vegetation-specific

Each `.yaml` file has a fixed top block plus a list of per-variable rules. There is no need to copy or prune them by hand — the submitter processes all 17, and `TIER=<name>` restricts a run to a single one.

## 3. Point at your data

**Do not edit the tier yamls.** They ship pointing at a reference run, and `examples/repoint_hr_year.py` rewrites that path — along with the year in every input pattern — into copies under your workdir. The submitter in section 5 runs it for you, so passing your run path as an argument is all you need to do.

Hand-editing the `inherit:` block breaks this. The repointer searches for the reference path in order to replace it; if you have already replaced it yourself there is nothing to find, and the run stops with:

```
ERROR: <tier>.yaml does not contain the expected template path
```

If you want a yaml to smoke-test with in section 4, generate the repointed copies first and use one of those:

```bash
python3 examples/repoint_hr_year.py \
  /work/<proj>/<user>/<run> 1851 \
  /scratch/<init>/<user>/pycmor_hr/mytest/yamls
```

Rule-level `input.pattern` regexes stay as-is if your model uses the same output filenames as AWI-ESM3-veg-HR. If not, edit the patterns in the source tier yamls to match what your XIOS or component writes — but keep the `\d{4}` year placeholder, which the repointer substitutes with the year you ask for. A pattern written in an unrecognised shape is left untouched and will match every year on disk.

## 4. Smoke-test one tier locally

Use one of the repointed copies from section 3, and pick a cheap tier — `veg_seaice` has 2 rules, `cap7_aerosol` has 7:

```bash
pycmor process /scratch/<init>/<user>/pycmor_hr/mytest/yamls/veg_seaice.yaml \
  --output-directory /scratch/<init>/<user>/pycmor_hr/smoketest
```

Login nodes are shared and memory-limited, so keep this to the smallest tiers. Anything reading the TCo319 atmosphere grid, or any tier with more than a handful of rules, belongs on `compute` — use the `TIER=` form from section 7 for those.

Confirm the `.nc` files appear, then open one with `ncdump -h` and check the metadata looks right.

## 5. Full year on SLURM

From the pycmor repo root:

```bash
bash examples/submit_hr_year_shards.sh \
  /work/<proj>/<user>/<run> 1851 \
  /scratch/<init>/<user>/pycmor_hr/mytest_ab0246
```

Arguments:
1. Absolute path to your model run root
2. Year to process
3. Workdir for repointed yamls, shards, and cmorized output

That submits one SLURM array per tier (~17 arrays). Monitor with `squeue --me`. Output lands at `<workdir>/cmorized/MIP-DRS7/CMIP7/...`.

The submit log echoes `drs=on` for every tier. If it says `drs=off`, stop and resubmit with `SHARD_DRS=on`: the flat per-tier layout fails the FILE001, PATH001 and PATH002 checks on every single file, which looks like a catastrophic QC result but is purely a directory-layout artefact.

## 6. Aggregate QC

Every `.nc` gets a sibling `qc_*.json` with wcrp + cf-checker findings. Count HIGH findings and see the top offenders:

```python
import json, glob, collections
c = collections.Counter()
for f in glob.glob('<workdir>/cmorized/**/qc_*.json', recursive=True):
    for ncp in json.load(open(f)).values():
        for sc in ncp.values():
            for it in sc.get('high_priorities', []) or []:
                v = it.get('value')
                if isinstance(v, list) and len(v) == 2 and v[0] < v[1]:
                    c[it.get('name')] += 1
print(sum(c.values()), 'HIGH; top:', c.most_common(10))
```

## 7. Iterate

Typical HIGH-finding fixes:
- `cmor_variable` name mismatch in the rule
- Wrong `cell_methods` for the variable's temporal shape
- Bad `compound_name` (typo or wrong branding suffix)
- Missing `data_request_variable` mapping

Fix the rule, then rerun just that tier through the submitter. It reapplies the tier's memory, worker-count and throttle settings, and re-shards from the corrected yaml:

```bash
TIER=<tier> bash examples/submit_hr_year_shards.sh <run> <year> <workdir>
```

Give the full tier name (`core_seaice`, not `seaice`) — the match is on a substring, so `seaice` would pick up all four sea-ice tiers. The submit log says how many tiers it kept; check that line before walking away.

Avoid calling `run_hr_shard.sh` directly. It reruns a single shard, but on its own it inherits none of the settings the submitter normally passes: `SHARD_DRS` falls back to `off`, so output lands in the flat layout and every file collects FILE001/PATH001/PATH002 again, and the per-tier memory, worker and throttle overrides are lost with it. If you genuinely need one shard, pass them yourself:

```bash
sbatch --array=<N> \
  --export=ALL,SHARD_DRS=on,CGROUP_GB=256,N_WORKERS=4,MEM_PER_WORKER=48GB \
  examples/run_hr_shard.sh <shards_dir> <run> <year> .
```

where `<N>` is the shard index within that tier (from the submit log), and the trailing `.` is the output subdirectory that `SHARD_DRS=on` expects. Check `submit_hr_year_shards.sh` for the tier's actual memory and throttle values — several tiers differ from the defaults above.

## Gotchas

- SLURM account is `ab0246` (project-specific; ask your PI if you need a different one).
- Never single-node for TCo319 grid data. The driver needs multi-node memory on `compute` partition (default `--mem=0` for full-node memory).
- Each shard runs under an inactivity watchdog. If its log stops growing for 90 minutes (`WEDGE_TIMEOUT_SEC`, default 5400s) the job cancels itself, on the assumption it has wedged — the failure mode it exists for is a shard going silent mid-save with no error. A job killed this way shows `CANCELLED` in `sacct` and `[WATCHDOG]` in its log. Set `WEDGE_TIMEOUT_SEC=0` for a shard you know is legitimately slow.
- The watchdog finds its log via `SLURM_SUBMIT_DIR`, so it works wherever you submit from. Submitting from the repo root is still the habit worth keeping, since that is where the logs then collect.
- A HIGH finding is not automatically a rule bug. Several classes are checker-side and get fixed upstream in cc-plugin-wcrp; keep the local checkout on a recent `develop` before concluding your data is wrong.
- Findings that hit every file at once usually point at one systematic cause, not hundreds of separate problems. FILE001/PATH001/PATH002 across the whole run means the DRS layout (see step 5), not bad metadata.
- GPG signing on commits uses key `D763C0EA86718612` when unlocked; do not add `--no-gpg-sign`.

## Where to look for help

- `CLAUDE.md` at the repo root has the project overview and dev conventions.
- `examples/` has real working configs (e.g. `submit_hr_year_shards.sh`, `run_hr_shard.sh`).
- `doc/` has the full Sphinx docs (or read on ReadTheDocs at pycmor.readthedocs.io).
- Ping Jan for anything model-specific.
