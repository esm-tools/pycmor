# Rules to reactivate once XIOS writes the new streams

Rules deactivated in the AWI-ESM3-veg-HR recipes because the model output they
need does not exist in the current HR run. They are commented out in place, so
reactivating is uncommenting plus repointing the input pattern.

The matching XIOS changes are in `esm_tools`:

- `namelists/oifs/48r1/xios/cmip7/file_def_oifs_cmip7_spinup.xml.j2` (mrsol)
- `namelists/fesom2/xios_xml_cmip7/file_def_fesom.xml.j2` (sea ice)

Both apply to any run started after those file_defs land, LR included.

There is also one rule that is **not** deactivated but is running on an
approximation, and gets better with the new FESOM streams: see
[Daily sivol](#daily-sivol-approximation-in-use) below.

---

# Part 1 — OIFS soil moisture (mrsol)

## Why they were deactivated

A single 3-hourly stream (`atmos_3h_land_mrsol`, `operation="instant"`, carrying
the **100 cm** field) was feeding three DReq entries that disagree on both depth
and temporal shape. Two consequences:

- `tavg-d100cm` received instantaneous samples, so it could not be a mean.
- Both `d10cm` entries silently read the 100 cm field, publishing 1 m soil
  moisture as 10 cm.

The depth error was confirmed on cli108 output: 3hr `d10cm` mean 72.6 kg m-2
against 6.9 for the genuinely-10 cm monthly rule, a 10.5x ratio matching the
1.00 m / 0.10 m depth ratio. It is a wrong-values problem, not a metadata one,
and no QC check catches it.

## The three rules

| Rule | File | DReq entry | New stream |
|---|---|---|---|
| `mrsol_3hr_100cm` | `veg_land/cmip7_awiesm3-veg-hr_land.yaml` | `land.mrsol.tavg-d100cm-hxy-lnd.3hr.glb` | `atmos_3h_land_mrsol100_avg` |
| `mrsol_3hr_10cm` | `veg_land/cmip7_awiesm3-veg-hr_land.yaml` | `land.mrsol.tpt-d10cm-hxy-lnd.3hr.glb` | `atmos_3h_land_mrsol10_inst` |
| `mrsol_day_10cm` | `cap7_land/cmip7_awiesm3-veg-hr_cap7_land.yaml` | `land.mrsol.tavg-d10cm-hxy-lnd.day.glb` | `atmos_day_land_mrsol10_avg` |

Input patterns to use when uncommenting:

- `atmos_3h_land_mrsol100_avg_mrsol_.*\.nc`
- `atmos_3h_land_mrsol10_inst_mrsol_.*\.nc`
- `atmos_day_land_mrsol10_avg_mrsol_.*\.nc`

Confirm the exact filenames against the first year of output before trusting
these: XIOS composes them from the `<file name=...>` plus the field name under
`timeseries="only"`, and the composition has not yet been observed on disk.

## Checks before trusting the reactivated output

- **Depth.** `d10cm` values should be roughly a tenth of `d100cm`. If a 3-hourly
  `d10cm` file reports means near 70 kg m-2 rather than near 7, it is reading the
  100 cm field again.
- **Operation.** `ncdump -h` the source; `online_operation` must be `average` for
  the two `tavg` entries and `instant` for the `tpt` one.
- **Timestamps.** An averaged 3h stream should be stamped at window midpoints
  (01:30, 04:30, ...). Boundary stamps (03:00, 06:00, ...) with a trailing sample
  in the following year mean it is still `instant`.

## Not affected

These stay enabled and read correct sources:

- `mrsol` (`core_land`) `tavg-d10cm` monthly, reads `atmos_mon_land`
- `mrsol_mon` (`cap7_land`) `tavg-sl` monthly, reads LPJ-GUESS `.out`
- `mrsolLut_mon` (`veg_land`), reads LPJ-GUESS `.out` (now `mrsosLut_monthly.out`,
  which is the genuine 0-10 cm tile output; it previously read the whole-column
  `mrsoLut_monthly.out` under the same `d10cm` brand)
- `tslsi_3hr`, already `tpt` and correctly fed by an `instant` stream

---

# Part 2 — FESOM sea ice

The matching XIOS change is
`namelists/fesom2/xios_xml_cmip7/file_def_fesom.xml.j2`, which adds four
streams. All four are new `<file>` entries rather than `operation` flips on the
existing ones, so nothing that currently wants a mean can silently start
getting a snapshot.

| New stream | Field | Cadence | Operation | Serves |
|---|---|---|---|---|
| `sgm11_inst.fesom` | `sgm11` | 1mo | `instant` | `sistressave`, `sistressmax` |
| `sgm12_inst.fesom` | `sgm12` | 1mo | `instant` | `sistressmax` |
| `sgm22_inst.fesom` | `sgm22` | 1mo | `instant` | `sistressave`, `sistressmax` |
| `m_ice_day.fesom` | `m_ice` | 1d | average | `sivol_north_day`, `sivol_south_day` |

## sistressave / sistressmax — deactivated

In `lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml`, commented out in place
with the reactivated input patterns already written in.

Both DReq entries are branded `tpt-u-hxy-si` and ask for "an instantaneous
value at the center of the month". FESOM writes `sgm11`/`sgm12`/`sgm22` as
monthly means (12 records, `cell_methods = "time: mean"`), so publishing these
meant labelling a monthly mean as a snapshot.

For `sistressmax` it is worse than a labelling problem. The invariant is
`sqrt(((sgm11-sgm22)/2)^2 + sgm12^2)`, a nonlinear function of the components,
so deriving it from monthly means is not even the monthly mean of the
invariant. No QC check catches either of these.

Per-timestep stress output was the only alternative route and is not viable at
HR: `sgm11` alone is ~206 MB for 12 records, so roughly 1.5 TB per year per
component.

To reactivate: uncomment both rules. The patterns
(`sgm11_inst\.fesom\.\d{4}\.nc` and siblings) are already in place, and the
`sistressave_pipeline` / `sistressmax_pipeline` definitions were left in the
yaml untouched.

**Check before trusting the output.** The sample lands at the end of the output
interval, not on the 15th as the DReq text prefers. It is a genuine
instantaneous value either way, but confirm `online_operation = instant` in the
source header, and confirm the values are noticeably rougher in time than the
old monthly means — a snapshot field should not look smoothed.

## Daily sivol — approximation in use

`sivol_north_day` and `sivol_south_day` are **enabled**, not deactivated, but
they run on a reconstruction that the new `m_ice_day` stream makes unnecessary.

They used to read the `sivoln`/`sivols` ldiag_cmor scalars, the same source as
the monthly rules. Those scalars are monthly in this run, so cli108 published a
335-step daily axis carrying 12 distinct values and 323 NaNs.

`m_ice` ("sea-ice mass per area", in metres, so volume per unit area) is the
right input but is monthly-only here. `h_ice` ("ice thickness over ice-covered
fraction") and `a_ice` are both daily, and `m_ice = h_ice * a_ice` by
construction, so the rules now rebuild the daily field via
`compute_ice_volume_per_area` + `integrate_over_hemisphere`
(`ice_volume_hemisphere_pipeline`).

The approximation: both inputs are daily means, so the product drops the
sub-daily covariance between thickness and concentration,
`mean(h) * mean(a) != mean(h * a)`.

It has been measured, not assumed. The Final_CMIP7_IO_Test_01 run writes
`sivoln` every model timestep (87600 samples for 1586), which is FESOM's own
hemispheric ice-volume diagnostic. Resampled to daily and compared against the
reconstruction:

```
FESOM sivoln daily-mean (1e3 km3): min 10.911 max 29.497 mean 20.585
ours h_ice*a_ice                 : min 10.921 max 29.524 mean 20.605
relative difference: mean +0.094%  max|.| 0.107%   correlation 1.000000
```

So the reconstruction runs about a tenth of a percent high. That is well
inside anything the variable is used for, but the exact field is preferable
where it exists.

To switch to the exact field once `m_ice_day.fesom` exists, in both rules:

```yaml
    inputs:
      - path: *dp
        pattern: m_ice_day\.fesom\.\d{4}\.nc
    model_variable: m_ice
    pipelines:
      - hemisphere_integral_pipeline    # not ice_volume_hemisphere_pipeline
```

and drop the `aice_path` / `aice_pattern` / `aice_variable` attributes. Keep
`model_unit: "m3"` and `hemisphere`. The values should barely move; a large
jump means the wrong field or the wrong unit chain.

---

## Open question

Only `mrsol` was audited by hand, because the DKRZ review pointed at it. The
later source-vs-DReq audit caught `mrsolLut`, `sivol` daily and the two
`sistress` rules, but it covered a sample, not everything. Other variables
sharing one stream across several DReq entries could have the same class of
problem, and it is invisible to QC. A sweep comparing each rule's
`compound_name` against the `operation` and field definition of the stream it
reads would be worth doing before publication.
