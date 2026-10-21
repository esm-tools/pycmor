# AWI-ESM3 LR CMIP7 cmorization: handover

State on 2026-10-21. Branch `feat/cmip7-awiesm3-veg-lr` (esm-tools/pycmor). It contains everything of the HR branch `feat/cmip7-awiesm3-veg-hr` and is kept in step with it by merging HR into LR.

## Two rule sets in one tree

Each tier directory under `awi-esm3-lr-variables/` has two files.

- `cmip7_awiesm3-lr_*.yaml` is your raw-monthly workflow, listed in `profile.yaml` and run with `campaign.py` / `start_campaign.sh`. 195 products from the default (non-CMIP7) output.
- `cmip7xml_awiesm3-lr_*.yaml` is the full rule set for runs written with the CMIP7 XIOS output of v3.5.1 (`oifs.cmip7_cmor_output` and `fesom.cmip7_cmor_output`). It is the HR rule set adapted to TCo95/L91 and CORE3, plus the rules that were switched off for HR and can come back for LR (`notes/LR_reactivation_plan.md`). 556 compounds. It runs through the HR machinery with `MODEL=lr` (`examples/repoint_hr_year.py`, `examples/submit_hr_year_shards.sh`, `examples/campaign.py`).

`profile.yaml` does not list the `cmip7xml_` files, and the `MODEL=lr` glob only matches that prefix, so neither workflow loads the other's rules. `tests/unit/test_campaign.py` covers the `cmip7xml_` tree, `test_lr_campaign.py` and `test_awiesm3_lr.py` yours.

The LR source_id and grid labels are not in the EMD yet. They are placeholders, defined once in `examples/repoint_hr_year.py` (`MODELS["lr"]`): source_id `AWI-ESM3-veg-LR`, `g235` (TCo95), `g236`/`g238` (CORE3 nodes/elements), `g999` for the 400x192 regrid. QC errors from these are expected until the EMD entry lands.

## What was tested

One month (January 1850) of AWI-ESM3 v3.5.1-cc on TCo95/CORE3 with the CMIP7 FESOM XML, REcoM and the FESOM XIOS alias streams. Run: `/work/bb1469/a270092/runtime/awiesm3-v3.5.1-cc-xiosdraft/xiosdraft_m1`, runscript and checks in `/work/ab0246/a270092/runscripts/xiosdraft/`. OpenIFS wrote only its monthly set in that run, so the atmosphere and land rules could not be tested; LPJ-GUESS files are empty after one month.

Cmorized with `MODEL=lr` through `submit_hr_year_shards.sh`, output in `/scratch/a/a270092/pycmor_lr/xiosdraft_m1/cmorized`:

| tier | ok | not produced |
|---|---|---|
| core_ocean, core_seaice, cap7_ocean, cap7_seaice, veg_seaice | all | |
| lrcs_ocean | 44 | 9 decadal (no full decade) |
| lrcs_seaice | 53 | 11 that need OpenIFS input |
| atmosphere and land tiers | | missing input only |

Grids come out as 220509 nodes / 426403 elements with 47 (48) levels. Surface fields are missing at the 10047 cavity nodes.

## Fixes that change your workflow's numbers

- **Bottom level** (`src/pycmor/awiesm3_lr.py`, a2d3e18d). On CORE3 `depth_lev` is the number of wet layers: the last finite layer of LR `temp` is `depth_lev - 1` at all 220509 nodes. `lr_bottom` took `depth_lev - 2`, so tob and sob came from one layer above the sea floor. `validate_outputs.py` checked against the same wrong layer, which is why validation passed. Both use `depth_lev - 1` now; the test fixture counts wet layers like the mesh.
- **wfo** in the `cmip7xml_` rules is `-1025 * fw` (FESOM's fw is positive upward and divided by 1025), as in your workflow. The HR rule had `+1000` and is fixed on the HR branch too (3b59e810).

## Model and output configuration

- **esm_tools** `feat/awiesm3-v3.4-co2` (pushed):
  - OpenIFS pressure levels: plev3/plev6/plev7h were standalone XIOS axes and wrote the first n FullPos levels under their labels (HR plev3 labelled 850/500/250 held 1000/925/850 hPa). Now one 23-level FullPos axis, and plev19/plev3/plev6/plev7h are exact linear interpolations from it (59cc2bd98). `zoom_axis index` does not work in XIOS 2.5: it selects the min-max range.
  - LR-only streams at TCO95: 3-hourly plev6, 6-hourly model levels, instantaneous ts/sp (1dbf136ce).
  - Christopher's CMIP7 REcoM XML (5dceafc70).
- **FESOM XIOS averaging**: FESOM sends a field whenever any XIOS file wants it and each send is the mean since the previous send, so a field id in files of different freq_op leaves the coarser file with the last short mean (monthly m_ice was the last day). Christopher's XML sends such fields at the finest freq_op (correct, about 303 GB per simulated year of FESOM-to-XIOS traffic on CORE3). The alternative is FESOM alias streams, one stream per cadence: draft PR FESOM/fesom2#1216 (stacked on #1215), 84 GB per year, tested on the month above. Its XML is on the local esm_tools branch `draft/fesom-xios-aliases` (`~/esm_tools-draft-xios` of a270092, with `check_xios_cadence.py`) and goes up as a PR together with #1216.
- **Daily hfx/hfy**: the 3D daily `utemp_day`/`vtemp_day` cost about 15 GB per year each for two 2D daily variables. FESOM's CMOR diagnostics now form the column integrals `utemp_int`/`vtemp_int` every step (needs `ldiag_cmor` only, not `ldiag_trflx`), already rotated to geographic. Branch `feat/cmip7-heat-transport-int` (127c9c68, pushed, no PR yet). Tested in one month (`runtime/awiesm3-v3.5.1-cc-xiosdraft/xiosdraft_hti1`): 6.2e-8 against the offline integral of the 3D output, about 29 GB less output per year. XML switches `fesom.cmip7_daily_trflx_int` and `fesom.cmip7_daily_trflx_3d`.
- **Vector rotation**: in XIOS mode FESOM (2.7 and 2.8) forces `vec_autorotate=.FALSE.`, so every vector field (u, v, uice, vice, tx_sur, ...) is written in the rotated grid frame; pycmor does not rotate. At 80-88N, u/v agree with geostrophy -0.13 as written and 0.77 rotated.
  - LR: fixed in FESOM's XIOS path, so the raw output is geographic. Branch `fix/xios-vector-rotation` (cd02c26b, pushed, no PR), only `src/io_meandata.F90`: pairs are found by name (u/v, uice/vice, unod/vnod, unod_sfc/vnod_sfc, bolus_u/v, tx_sur/ty_sur, atmice_x/y, iceoce_x/y, utemp/vtemp, usalt/vsalt and their aliases), the running sums are rotated before the division, element centroids handle the cyclic seam. utemp_int/vtemp_int (already rotated) and the stress tensor sgm* are left alone; sistressave/sistressmax are invariants. Builds through esm_master.
    - **To finish:** test month `runtime/awiesm3-v3.5.1-cc-xiosdraft/xiosdraft_rot1` (job 28304304, runscript `runscripts/xiosdraft/xiosdraft_rot1.yaml`) ran an earlier build that differs only in a warning. Check it with the scripts in `/work/ab0246/a270092/runscripts/xiosdraft/checks/`: `compare_rot.py <hti1> <rot1> <core3 mesh.nc> <checks dir>` (each pair against the offline rotation of hti1, expect about 1e-7), `bitident.py <hti1> <rot1>` (everything that is not a vector component bit-identical), `geostrophy.py <rot1> <checks dir>` (raw agreement about 0.77 at 80-88N). Then one month with the committed build (staged `work/lib/fesom/libfesom.so` md5 `72b2adad...`).
    - `awi-esm3-lr-variables/discovery.py:102` insists on `vec_autorotate=true` in the namelist, which XIOS mode ignores, so that check proves nothing.
  - HR: years of output exist, so a pycmor step rotates the affected HR rules: `rotate_vector_to_geographic` in `examples/custom_steps.py`, a no-op unless a rule sets `vector_partner_pattern` (22116ab3, with tests), applied to 28 HR rules (3f144425): uo, vo, uos, vos, umo, vmo, tauuo, tauvo (mon, dec), hfx, hfxint, hfy, hfyint, sfx, sfy, siu, siv (mon, day), sistr*, sidmasstran*. hfbasin and sltbasin stay unrotated on purpose: tripyview multiplies by edge vectors from `fesom.mesh.diag.nc` that are in the rotated frame too, so they are already right. The LR `cmip7xml_` rules do not get the step. Both commits are on the LR branch only; they go to the HR branch after validation.
    - **To finish:** offline check job 28304828 (log `/scratch/a/a270092/pycmor_hr/rotfix_offline/offline_28304828.log`, geostrophy of the cmorized uo/vo should match 0.77 / 0.92 / 0.997 at 80-88N / 65-80N / 40-65S), production year 1852 for the 26 rules (jobs 28304846-28304856, output `/scratch/a/a270092/pycmor_hr/rotfix_y1852/cmorized`), then `python /scratch/a/a270092/pycmor_hr/rotfix_offline/scripts/compare_cli123.py <rotfix cmorized> <cli123 cmorized> <dars2 mesh.nc>` (magnitudes unchanged, directions corrected), then cherry-pick 22116ab3 and 3f144425 onto `feat/cmip7-awiesm3-veg-hr`. All cmorized HR uo/vo/tauuo/... so far are in the rotated frame.
- **PR stack** on FESOM/fesom2: #1215 (Christopher, REcoM CMIP7 output) <- #1216 (alias streams, draft) <- `feat/cmip7-heat-transport-int` <- `fix/xios-vector-rotation`; the last two are pushed branches without PRs. The esm_tools side is branch `feat/fesom-xios-aliases` (7bc1509e3, pushed, no PR, on top of `feat/awiesm3-v3.4-co2`; `check_xios_cadence.py` passes) and goes up together with them: FESOM does not send the alias and integral ids it references without the FESOM side.
- **Builds** go through esm_master: `model_codes/awiesm3-v3.5.1-cc` (a270092), fesom-2.8 on the stack above, REcoM `db7de3a`. The tip of REcoM `feat/cmip7-output` (4b44e7a) does not compile (`recom_modules.F90:2817`, missing `, &`). Test runscripts in `/work/ab0246/a270092/runscripts/xiosdraft/`. Short tests run fastest under account bb1469 with QoS `devel` (30 minutes at most).
- Runs shorter than a year need `general.icb_from_budget: false` and `fesom.ini_iceberg_dir: .../input/fesom2/icebergs_coldstart`: the iceberg seed step refuses legs under a year.

## Data volume

`awi-esm3-lr-variables/volume/estimate_lr_campaign_volume.py` regenerates `volume/lr_campaign_volume.json` and `examples/lr_cmip7_volume.png`. For piControl 550 y, historical 172, 1pctCO2 150, abrupt-4xCO2 300 and seven scen7 to 2299 (3118 model years):

- model output about 840 TB, cmorized about 530 TB if every experiment wrote every variable;
- cmorized **about 280 TB** if each experiment gets only what Data Request v1.2.2.5 requests for it (the extensions 2101-2299 request a fifth of the variables);
- the model output does not need to be kept: cmorize each leg, check it, delete it. A rolling buffer of about 20 TB is enough.

The largest items: 6-hourly model-level ta/ua/va/hus/zg (82 TB as requested, 35 of it piControl), all ocean BGC (76 TB), of which the 7 daily 3D variables at Low/Medium priority 53 TB. DReq time subsets (e.g. piControl years matching the last 40 of historical) are not applied yet and would reduce this further.

## Open

- A per-experiment filter in pycmor: run only the rules whose compound name the DReq requests for the experiment (needed for the 280 TB). Then the DReq time subsets.
- Rules for the new FESOM streams, listed with their inputs in the test report: 3-hourly hfds/wfo/tauuo/tauvo/rsntds/friver/tos, daily surface BGC (o2, ph, co3, chl, ...), daily 3D BGC, daily so/wo/rsdo. sfdsi 3-hourly needs a decision on its recipe.
- The REcoM filter of the 178 ocean BGC variables (Christopher), lists in `obgc/`, `co2/`, `icebergs_cavities/`.
- Decisions: `cmip7_daily_3d_output` for LR, 3-hourly flandice.
- siconc/sitimefrac fill cavity nodes with 0 % ice in the `cmip7xml_` rules, your workflow keeps them missing; sidragtop/sidragbot are constant and not masked (HR as well).
- The atmosphere and land `cmip7xml_` rules are untested: they need a run with the full OpenIFS CMIP7 XML (`oifs.cmip7_cmor_output: true`, not `monthly`).
- pycmor rules for `utemp_int`/`vtemp_int`: `hfx_int_day`/`hfy_int_day` read them, drop the hnode inputs and the vertical sum, keep `scale_factor` and `grid_file`; `compute_heat_transport` needs a branch for column-integrated input. Monthly `hfxint`/`hfyint` can use the same daily files. The monthly 3D hfx/hfy (`tavg-ol`) still need 3D utemp/vtemp, which keeps `ldiag_trflx` on; sltbasin would need `usalt_int`/`vsalt_int` (not implemented).
- Questions to settle: rho0*cp for hfx/hfy (pycmor 4095900, FESOM 4.2e6, tripyview 1030*3850); the XIOS path sets every 2D field except fw/fh to missing under ice shelves, including integrals such as pbo and opottemptend.
