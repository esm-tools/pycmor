"""
Set time bounds for datasets based on time method and approximate interval.

Time bounds represent the start and end of the time interval associated with
each time point. For climate data, these are required for CMIP compliance.

The main function is :func:`time_bounds` which creates appropriate bounds
based on the time method (mean, instantaneous, or climatology) and the
data's temporal frequency.
"""

import json as _json
from importlib.resources import files as _pkg_files

import numpy as np
import xarray as xr

from ..core.logging import logger
from ..core.rule import Rule
from .dataset_helpers import get_time_label


def _load_temporal_shape_to_must_have_bounds():
    """Build a mapping from CMIP7 ``temporal_shape`` (as reported by the
    data request variable) to the ``must_have_bounds`` field of the
    corresponding time axis in the vendored ``CMIP7_coordinate.json``.

    The DReq's ``temporal_shape`` values (``time-intv``, ``time-point``,
    ``time-fxc``, ``climatology``, ``diurnal-cycle``, ``None``) correspond
    to axis entries (``time`` / ``time1`` / ``timefxc`` / etc.) in the
    coordinate JSON. Only ``time-point`` maps to an axis with
    ``must_have_bounds="no"`` (namely ``time1``); everything else wants
    bounds. Vendored from WCRP-CMIP/cmip7-cmor-tables (main branch,
    cmor_version 3.15).
    """
    try:
        _path = _pkg_files("pycmor.data.cmip7").joinpath("CMIP7_coordinate.json")
        _tbl = _json.loads(_path.read_text())
    except Exception as exc:  # pragma: no cover - vendored file always present
        logger.warning(f"could not load CMIP7_coordinate.json: {exc}; defaulting to must_have_bounds=yes")
        return {}
    axis_entry = _tbl.get("axis_entry", {})
    # Direct axis lookup by out_name for temporal_shape mapping. We consult
    # the JSON so downstream table updates propagate without code changes.
    _map = {
        "time-intv": axis_entry.get("time", {}).get("must_have_bounds", "yes"),
        "time-point": axis_entry.get("time1", {}).get("must_have_bounds", "no"),
        "time-fxc": axis_entry.get("timefxc", {}).get("must_have_bounds", "yes"),
        "climatology": "yes",
        "diurnal-cycle": "yes",
    }
    return _map


_TEMPORAL_SHAPE_TO_MHB = _load_temporal_shape_to_must_have_bounds()


def _axis_must_have_bounds(rule: Rule) -> bool:
    """Return True if the axis for this rule's variable requires bounds
    per CMIP7_coordinate.json (via the DReq's ``temporal_shape``).
    Defaults to True on any lookup failure so we err on the side of
    preserving bnds.
    """
    drv = getattr(rule, "data_request_variable", None)
    tshape = (getattr(drv, "temporal_shape", None) or "").strip() if drv else ""
    if not tshape or tshape.lower() == "none":
        return True
    val = _TEMPORAL_SHAPE_TO_MHB.get(tshape, "yes")
    return val != "no"


def time_bounds(ds: xr.Dataset, rule: Rule) -> xr.Dataset:
    """
    Set time bounds for a variable based on time method and approx_interval.

    Time bounds are set according to the following logic:

    - For instantaneous time method: bounds equal the time values (delta = 0)
    - For mean time method: bounds span the averaging interval (delta > 0)
    - For climatology: no bounds are created
    - Uses approx_interval from rule to determine appropriate bounds

    Parameters
    ----------
    ds : xr.Dataset
        The input dataset.
    rule : Rule
        Rule object. Uses ``approx_interval`` (float, days) and
        ``time_method`` (str) attributes if present.

    Returns
    -------
    xr.Dataset
        The output dataset with time bounds information.

    Raises
    ------
    ValueError
        If the input is not a Dataset, has no time coordinate, or has
        insufficient time points for the requested bounds type.
    """
    dataset_name = ds.attrs.get("name", "unnamed_dataset")
    logger.info(f"[Time bounds] {dataset_name}")

    if not isinstance(ds, xr.Dataset):
        raise ValueError("The input is not a dataset.")

    time_label = get_time_label(ds)
    if time_label is None:
        # fx / ofx and other time-invariant outputs have no time coord.
        # set_time_bounds is a no-op for those; skip silently rather than
        # raising so the same DefaultPipeline can be reused for fx rules.
        logger.info("  no time coordinate; skipping (fx-style dataset)")
        return ds

    bounds_dim_label = "bnds"
    time_bounds_label = f"{time_label}_{bounds_dim_label}"

    approx_interval = getattr(rule, "approx_interval", None)
    time_method = getattr(rule, "time_method", None) or ds.attrs.get("time_method", "mean")

    # Auto-detect tpt (time:point) variants from the data request's cell_methods.
    # wcrp_cmip7 TIME001 reads cell_methods directly: if it contains "time: point",
    # use_midpoint=False and the expected time is the filename start, not the bnds
    # midpoint. Without this override, a default-mean rule on a tpt yearly LUT
    # compound (e.g. land.cLitterLut.tpt-u-hxy-multi.yr.glb) builds year-snap
    # bnds and writes time at year-midpoint, tripping TIME001 with "expected
    # -182.0 (year_start in days since year_mid epoch), got 0.5 (mid-year)".
    if time_method == "mean":
        drv = getattr(rule, "data_request_variable", None)
        cm = (getattr(drv, "cell_methods", "") or "").lower() if drv else ""
        if "time: point" in cm:
            time_method = "instantaneous"
            logger.info("  cell_methods has 'time: point'; overriding time_method to instantaneous")

    logger.info(f"  time label: {time_label}, approx_interval: {approx_interval} days")
    logger.info(f"  time method: {time_method}")

    # fx / ofx variables are time-invariant. The saved file will have no
    # time coord at all (collapsed to scalar in the fx save branch). Don't
    # build time_bnds for them: doing so trips the wcrp_treats_as_instant
    # branch below (because "fx" is not in _WCRP_AVG_FREQS), which builds
    # period-snap bnds against the source's multi-month time dim. The
    # save path then can't reset_coords the resulting indexed dim coord.
    drv_freq = ""
    drv = getattr(rule, "data_request_variable", None)
    if drv is not None:
        drv_freq = (getattr(drv, "frequency", "") or "").strip()
    if drv_freq in ("fx", "ofx"):
        logger.info(f"  freq={drv_freq!r}: skipping bnds creation for time-invariant variable")
        return ds

    # If the source carries a non-canonical XIOS-style ``<time>_bounds``
    # (FESOM convention) but not the CMIP-canonical ``<time>_bnds``,
    # rename it now so the rest of the function (and downstream tools)
    # sees the canonical name. Avoids the wcrp TIME003 month-precision
    # fallback that kicks in when no canonical bnds variable is found.
    noncanonical = f"{time_label}_bounds"
    if noncanonical in ds.variables and time_bounds_label not in ds.variables:
        ds = ds.rename({noncanonical: time_bounds_label})
        ds[time_label].attrs["bounds"] = time_bounds_label
        logger.info(f"  renamed {noncanonical} -> {time_bounds_label}")

    if time_method not in ("mean", "instantaneous", "climatology"):
        logger.warning(f"  Unknown time method '{time_method}', defaulting to 'mean'")
        time_method = "mean"

    if time_method == "climatology":
        logger.info("  skipping bounds creation for climatology data")
        return ds

    # NOTE: instantaneous (tpt / ``cell_methods = "time: point"``)
    # variables still go through the full flow below so the wcrp_treats_
    # as_instantaneous branch can realign time to period_start. The
    # actual decision to WRITE time_bnds happens at the end of this
    # function via _axis_must_have_bounds(rule): if the CMIP7 axis for
    # this variable's temporal_shape has ``must_have_bounds="no"`` (only
    # time1 for now), the freshly-built bnds are dropped just before
    # return. Skipping the flow entirely (as f7cbcda2 did) also skipped
    # the time realignment, which broke LPJ-GUESS LUT yearly tpt files
    # by leaving them at mid-year midpoints instead of period_start.

    time_var = ds[time_label]
    time_values = time_var.values

    if len(time_values) < 1:
        raise ValueError("Cannot create time bounds: no time points found")

    # Single-stamp ``mean``-default rules (fx / ofx that didn't explicitly set
    # ``time_method=instantaneous``) would crash in ``_create_mean_bounds``,
    # and even if they didn't, manufacturing bnds for a time-invariant field
    # is wrong — the file shouldn't carry time_bnds at all. Skip silently.
    #
    # Yearly files are a real exception: a 1-year LPJ-GUESS or FESOM yearly
    # output has exactly one stamp per file but is NOT fx-like. It needs
    # proper (year_start, next_year_start) bnds so wcrp TIME001 can verify
    # the midpoint. Detect via approx_interval or rule.data_request_variable
    # .frequency and let the call fall through to _create_mean_bounds, where
    # the yearly branch builds them.
    if len(time_values) < 2 and time_method != "instantaneous":
        if not (_looks_yearly(rule, approx_interval) or _looks_decadal(rule, approx_interval)):
            logger.info(
                f"  {len(time_values)} time point(s) with method='{time_method}'; "
                "treating as fx-like, no bounds created"
            )
            _force_canonical_time_encoding(ds, time_label)
            return ds
        logger.info(
            f"  {len(time_values)} time point(s) with method='{time_method}'; "
            "yearly or decadal frequency detected, building period-snapped bounds"
        )

    # If the source already shipped time_bnds (e.g. FESOM daily files), don't
    # recompute them, but still realign time = midpoint(bnds) so the wcrp
    # TIME001 check passes. FESOM daily output writes bnds = (day_start,
    # next_day_start) but time at day_start (midnight); the cf/CMIP
    # convention is time at the bnds midpoint (noon). The realignment is
    # skipped for instantaneous time (the point IS the value).
    if time_bounds_label in ds.variables:
        logger.info(f"  using existing bounds: {time_bounds_label}")
        if time_method != "instantaneous" and len(time_values) >= 1:
            existing_bnds = ds[time_bounds_label].values
            if existing_bnds.ndim == 2 and existing_bnds.shape[1] == 2:
                try:
                    new_time_values = _midpoint_bounds(existing_bnds)
                except Exception as exc:
                    logger.warning(f"  could not realign {time_label} from existing bnds: {exc}")
                    new_time_values = None
                if new_time_values is not None:
                    new_time = time_var.copy(data=new_time_values)
                    ds = ds.assign_coords({time_label: new_time})
                    logger.info(f"  realigned {time_label} to midpoint of existing {time_bounds_label}")
        if "bounds" not in ds[time_label].attrs:
            ds[time_label].attrs["bounds"] = time_bounds_label
        # Strip stale source-side bnds attrs (LPJ-GUESS daily ships
        # time_bnds with a long_name that doesn't match the parent),
        # then re-attach long_name to MATCH the parent. cf §7.1 wants
        # bnds attrs to match parent (mismatch is the failure); cf §3.3
        # wants every aux-coord variable to carry a long_name.
        _strip_bnds_inheritable_attrs(ds[time_bounds_label])
        _parent_long_name = ds[time_label].attrs.get("long_name")
        if _parent_long_name:
            ds[time_bounds_label].attrs["long_name"] = _parent_long_name
        _force_canonical_time_encoding(ds, time_label)
        return ds

    logger.info(f"  {len(time_values)} points from {time_values[0]} to {time_values[-1]}")

    # wcrp_cmip7 TIME001 uses two signals to decide whether the file's
    # time stamps should be at the period midpoint or the period start:
    #
    #   use_midpoint = (not instantaneous) and (freq in AVERAGE_CORRECTION_FREQ)
    #   instantaneous = "time: point" in cell_methods  OR  freq NOT in AVG list
    #
    # AVG list (from cc-plugin-wcrp time_constants.py) AS OF cc-plugin-wcrp#67:
    #   {"day", "mon", "monPt", "yr", "yrPt", "1hrCM", "sem",
    #    "1hr", "3hr", "6hr", "dec"}
    #
    # Pre-#52 the sub-daily frequencies were excluded, so the original
    # comment ("non-AVG frequency (dec, 3hr, 6hr, 1hr, ...) even if
    # cell_methods says 'time: mean'") is now stale: with the wcrp
    # plugin upgraded (commit beb00ce on master, merged 2026-06-25)
    # sub-daily tavg files DO want use_midpoint=True. Mirror that.
    #
    # Two groups want time = period_start (not midpoint):
    #   1) any rule with cell_methods "time: point" (tpt-style)
    #   2) any rule with a non-AVG frequency (none left: cc-plugin-wcrp#67 added dec, so dec now wants the midpoint)
    drv = getattr(rule, "data_request_variable", None)
    freq = (getattr(drv, "frequency", "") or "").strip() if drv else ""
    _WCRP_AVG_FREQS = {
        "day",
        "mon",
        "monPt",
        "yr",
        "yrPt",
        "1hrCM",
        "sem",
        "1hr",
        "3hr",
        "6hr",
        "dec",
    }
    wcrp_treats_as_instantaneous = time_method == "instantaneous" or (freq and freq not in _WCRP_AVG_FREQS)

    if wcrp_treats_as_instantaneous and len(time_values) >= 1:
        # Build period bnds via _create_mean_bounds (it snaps to
        # frequency boundaries for yearly via the dedicated helper, and
        # for monthly / decadal / hourly via approx_interval) and pin
        # time to bnds[:, 0] = period_start.
        try:
            bounds_data = _create_mean_bounds(time_values, approx_interval, rule=rule)
            new_time_values = bounds_data[:, 0]
            logger.info(
                f"  freq={freq!r} method={time_method!r}: wcrp treats as instantaneous; "
                "bnds = period-snap, time = period_start"
            )
        except Exception as exc:
            logger.warning(f"  could not build period-snap bnds ({exc}); " f"falling back to zero-width bnds")
            bounds_data = np.column_stack([time_values, time_values])
            new_time_values = None
    elif time_method == "instantaneous":
        # Fallback for genuinely zero-length tpt arrays.
        bounds_data = np.column_stack([time_values, time_values])
        new_time_values = None
    else:
        bounds_data = _create_mean_bounds(time_values, approx_interval, rule=rule)
        # CF/CMIP and wcrp TIME001 require coord == midpoint(bnds). Source
        # files often write fixed-day-of-month timestamps (e.g. always the
        # 16th at 12:00) that are off by 1-2 days for non-31-day months.
        # Replace time with the bnds midpoint so the check passes. Skipped
        # for instantaneous time (the point IS the value).
        new_time_values = _midpoint_bounds(bounds_data)

    # IMPORTANT ordering: swap the time coord to the midpoint FIRST, then
    # attach time_bnds. xarray reindexes on assign_coords; if we attach a
    # bnds DataArray whose ``time`` axis is the midpoint while ds still
    # holds the original (off-midpoint) time, every bnds entry silently
    # becomes NaN. Daily / monthly paths only get away with the reverse
    # ordering because their source stamps happen to already sit at the
    # midpoint (FESOM daily at noon, monthly at the 16th roughly the
    # month-midpoint). Yearly broadcast stamps are at Jul 1 but the
    # year-snapped midpoint lands at Jul 2 12:00, so the mismatch is
    # visible. Fix it for everyone.
    if new_time_values is not None:
        new_time = time_var.copy(data=new_time_values)
        ds = ds.assign_coords({time_label: new_time})

    # cf §7.1 wants bnds attrs to MATCH the parent coord's attrs (mismatch
    # is the actual failure). cf §3.3 wants every aux-coord variable to
    # carry at least a long_name. The combination requires time_bnds to
    # carry the same long_name as parent time. Reading from the parent
    # rather than hard-coding makes the bnds track any later parent
    # changes. Note: the previous "strip everything" approach satisfied
    # §7.1 alone but tripped §3.3 because time_bnds becomes an aux-coord
    # of any data var that references it via ``coordinates``.
    _bnds_attrs = {}
    _parent_long_name = ds[time_label].attrs.get("long_name") if time_label in ds.variables else None
    if _parent_long_name:
        _bnds_attrs["long_name"] = _parent_long_name
    # No index/coord variable on the ``bnds`` dim: files historically
    # carried an ``int64 bnds(bnds)`` helper with ``long_name = "bounds
    # index"`` to give cf §3.3 something to point at, but that's unusual
    # for CMIP output (DKRZ review, 2026-06-30) and adds no value.
    # Downstream tools treat ``bnds`` as an anonymous size-2 dim.
    bounds = xr.DataArray(
        data=bounds_data,
        dims=(time_label, bounds_dim_label),
        coords={
            time_label: new_time_values if new_time_values is not None else time_values,
        },
        attrs=_bnds_attrs,
    )

    # ``bounds`` carries its own bare ``time`` coord built from raw values,
    # so it has no ``encoding``. ``assign_coords`` propagates that bare coord
    # over the dataset's own and wipes ``units``/``calendar``, after which
    # _force_canonical_time_encoding below sees no units and falls back to
    # deriving a date-only epoch from the first timestamp. That produced a
    # different epoch per file (monthly ``days since 1851-01-16``, daily
    # ``days since 1851-01-01``, yearly ``days since 1851-07-02``) even
    # though every input ships ``seconds since 1850-01-01``. Carry the
    # encoding across the assignment so the source epoch survives.
    _saved_time_enc = dict(ds[time_label].encoding) if time_label in ds.variables else {}

    ds = ds.assign_coords({time_bounds_label: bounds})

    if _saved_time_enc:
        for _k, _v in _saved_time_enc.items():
            ds[time_label].encoding.setdefault(_k, _v)

    if "bounds" not in ds[time_label].attrs:
        ds[time_label].attrs["bounds"] = time_bounds_label

    _force_canonical_time_encoding(ds, time_label)

    logger.info(f"  set {time_bounds_label}{bounds.shape}, " f"range: {bounds.values[0][0]} to {bounds.values[-1][-1]}")

    # If the CMIP7 axis for this variable has must_have_bounds="no"
    # (i.e. time1 for temporal_shape="time-point"), drop the freshly-built
    # bnds and clear the parent's ``bounds`` attribute now. Keeping the
    # flow up to here means time got realigned to period_start via the
    # wcrp_treats_as_instantaneous branch; only the on-disk bnds are
    # elided. See _axis_must_have_bounds docstring.
    if not _axis_must_have_bounds(rule):
        logger.info(
            f"  temporal_shape maps to axis with must_have_bounds='no'; "
            f"dropping {time_bounds_label} after time realignment"
        )
        if time_bounds_label in ds.variables:
            ds = ds.drop_vars(time_bounds_label)
        ds[time_label].attrs.pop("bounds", None)

    return ds


def _strip_bnds_inheritable_attrs(bnds_var):
    """Drop attrs that bounds variables MUST inherit from the parent coord
    per CF §7.1, rather than declare themselves. Mirrors the empty-attrs
    construction on the freshly-built bnds path so the existing-bnds
    path (passed through from a source file, e.g. LPJ-GUESS daily) ends
    up with the same surface.

    The CF spec forbids bnds vars from carrying their own boundary-
    related attrs; if any are present, cf §7.1 flags a mismatch against
    the parent coord. Stripping them is the canonical fix.
    """
    for attr in (
        "long_name",
        "standard_name",
        "units",
        "calendar",
        "axis",
        "positive",
        "leap_month",
        "leap_year",
        "month_lengths",
        "climatology",
        "bounds",
        "comment",
    ):
        bnds_var.attrs.pop(attr, None)


def _force_canonical_time_encoding(ds, time_label):
    """Normalise the encoded ``time`` (and ``time_bnds``) attributes:

    - calendar: promote ``standard``/``gregorian`` to ``proleptic_gregorian``
      (wcrp_cmip7 TIME003a recommendation; semantically identical for any
      date after 1582-10-15).
    - units: rewrite ``seconds since`` to ``days since`` (CMIP convention;
      xarray re-encodes the same cftime objects at the new unit, no data
      mutation needed). Leaves the reference date unchanged. Also strips
      fractional seconds and ISO ``T``/``Z`` separators from the reference
      date so the CMIP grammar ``days since YYYY-M-D( HH:MM:SS)?`` accepts
      it (wcrp_cmip7 ATTR004 / cchecker units-regex). When the source has
      no units string at all, derives a date-only ``days since YYYY-MM-DD``
      from the first time value so xarray's default encoder doesn't invent
      a ``.000000`` fractional-seconds reference.
    - dtype: force ``float64`` (CMIP cmor-tables specify ``type=double`` for
      time; FESOM/XIOS sometimes ships ``int64`` and writes seconds, which
      then trips wcrp_cmip7 VAR005 ``time dtype int64 (expected float)``).
    - Propagate the same encoding to ``time_bnds`` so its units / dtype /
      calendar match.

    Idempotent: re-calling on already-canonical state is a no-op.
    """
    if time_label not in ds.variables:
        return
    coord = ds[time_label]
    enc = coord.encoding

    cal_enc = enc.get("calendar")
    cal_attr = coord.attrs.get("calendar")
    if (cal_enc in (None, "standard", "gregorian")) and (cal_attr in (None, "standard", "gregorian")):
        enc["calendar"] = "proleptic_gregorian"
    # Always strip a stale attrs["calendar"]: xarray's CF encoder refuses to
    # write encoding["calendar"] if attrs already carries the key, and a
    # non-canonical value there would also leak straight onto disk regardless
    # of what encoding says.
    coord.attrs.pop("calendar", None)

    units = enc.get("units") or coord.attrs.get("units")
    if isinstance(units, str):
        units = _canonicalize_time_units_str(units)
        enc["units"] = units
        coord.attrs.pop("units", None)
    else:
        # No units set anywhere: derive a date-only epoch from the first
        # time value. Otherwise xarray's default CF encoder will mint a
        # ``days since YYYY-MM-DD HH:MM:SS.000000`` reference at write time,
        # which trips the wcrp_cmip7 / cchecker units regex.
        derived = _derive_date_only_units(coord)
        if derived:
            enc["units"] = derived

    enc["dtype"] = "float64"
    # cf §3.3 / §5.1 / wcrp ATTR001: parent time coord MUST carry
    # standard_name='time' and axis='T'. Some sources (FESOM monthly via
    # timeavg) drop these when the time coord is rebuilt; restore them
    # unconditionally so the checker is happy. long_name is advisory but
    # cf §3.3 flags its absence on the time coord at HIGH; keep it set.
    coord.attrs.setdefault("standard_name", "time")
    coord.attrs.setdefault("long_name", "time")
    coord.attrs.setdefault("axis", "T")
    # cf §7.1 forbids _FillValue (and any other non-bounds attr) on the
    # time coord itself, but xarray's default encoder ALWAYS emits a
    # _FillValue for float dtypes unless explicitly suppressed. Set None
    # in encoding to disable it.
    enc["_FillValue"] = None

    bnds_label = f"{time_label}_bnds"
    if bnds_label in ds.variables:
        b_enc = ds[bnds_label].encoding
        if "calendar" in enc:
            b_enc["calendar"] = enc["calendar"]
        if "units" in enc:
            b_enc["units"] = enc["units"]
        b_enc["dtype"] = "float64"
        # Same _FillValue suppression as the parent coord: cf §7.1
        # explicitly bans `_FillValue` on bounds variables ("The Boundary
        # variables 'time_bnds' should not have the attributes:
        # ['_FillValue']"). xarray adds it by default for float dtype.
        b_enc["_FillValue"] = None
        # time_bnds inherits the parent coord's units/calendar at write time;
        # keep its own attrs empty (cf §7.1).
        ds[bnds_label].attrs.pop("units", None)
        ds[bnds_label].attrs.pop("calendar", None)


def _derive_date_only_units(time_coord):
    """Return a CMIP-canonical ``days since YYYY-MM-DD`` derived from the
    first value of ``time_coord``. Returns ``None`` if the coord is empty
    or the value cannot be coerced into a date.
    """
    try:
        v = time_coord.values
    except Exception:
        return None
    if v is None or getattr(v, "size", len(v) if hasattr(v, "__len__") else 0) == 0:
        return None
    first = v[0] if hasattr(v, "__getitem__") else v
    # cftime objects expose year/month/day attributes directly.
    try:
        return f"days since {first.year:04d}-{first.month:02d}-{first.day:02d}"
    except AttributeError:
        pass
    # numpy datetime64 or python datetime: format via pandas for portability.
    try:
        import pandas as pd

        ts = pd.Timestamp(str(first))
        return f"days since {ts:%Y-%m-%d}"
    except Exception:
        return None


def _canonicalize_time_units_str(units):
    """Rewrite a ``time:units`` string to the CMIP-canonical form:

    - ``seconds since ...`` -> ``days since ...``
    - strip fractional seconds from the reference date
    - strip ISO ``T`` separator and trailing ``Z`` / ``z``
    - collapse any reference-date time component (``HH:MM:SS``) to date-only

    Returns the cleaned string. Safe to call repeatedly: already-canonical
    inputs round-trip unchanged.

    The date-only collapse is load-bearing: xarray's CF encoder
    ``encode_cf_datetime`` reformats any ``days since YYYY-M-D HH:MM:SS``
    units string with an ISO ``T`` separator on write (``days since
    YYYY-M-DTHH:MM:SS``), which the wcrp_cmip7 ATTR004 / cchecker units
    regex ``days since YYYY-M-D( HH:MM:SS)?`` rejects. Reducing the
    reference to date-only preserves absolute time (the encoded values
    are recomputed against the new epoch) and dodges the ``T`` rewrite.
    """
    if not isinstance(units, str):
        return units
    if units.startswith("seconds since"):
        units = "days since" + units[len("seconds since") :]
    parts = units.split(" ", 2)
    # parts[0]="days", parts[1]="since", parts[2]=<reference-date-and-time>
    if len(parts) >= 3:
        ref = parts[2]
        # ISO `T` between date and clock-time -> single space; drop trailing Z.
        ref = ref.replace("T", " ").rstrip("Z").rstrip("z")
        # Date-only: keep just the first whitespace-separated token. This
        # also drops fractional seconds for free, and avoids xarray
        # rewriting `HH:MM:SS` back to `THH:MM:SS` at encode time.
        ref = ref.split(" ", 1)[0]
        units = f"{parts[0]} {parts[1]} {ref}"
    return units


def canonicalize_time_in_encoding_dict(encoding, time_label, ds=None):
    """Force canonical time encoding into the per-variable ``encoding`` dict
    that gets passed to ``xr.Dataset.to_netcdf`` / ``xr.save_mfdataset``.

    When pycmor builds a ``final_encoding`` dict and passes it to the writer,
    that dict overrides whatever ``ds[time_label].encoding`` says, so
    ``_force_canonical_time_encoding`` on the dataset alone is not enough.
    This helper patches the dict so:

    - ``time`` carries ``calendar='proleptic_gregorian'`` (wcrp_cmip7 TIME003a)
    - ``time:units`` matches ``days since YYYY-M-D( HH:MM:SS)?`` (no
      ``seconds since``, no fractional seconds, no ISO ``T``, no trailing ``Z``)
    - ``time:dtype = float64`` (wcrp_cmip7 VAR005)
    - ``time_bnds`` inherits the same calendar / units / dtype

    Idempotent. ``ds`` is optional and only used to discover the source
    units / calendar when the encoding dict doesn't already carry them.

    Parameters
    ----------
    encoding : dict
        Per-variable encoding dict (``{var_name: {key: value, ...}, ...}``).
        Mutated in place; also returned for chaining.
    time_label : str
        Name of the time coordinate, e.g. ``"time"`` or ``"time1"``.
    ds : xr.Dataset, optional
        The dataset being written. Used as a fallback source for the
        current ``units`` / ``calendar`` when the encoding dict doesn't
        already carry them.

    Returns
    -------
    dict
        The (mutated) encoding dict.
    """
    if not isinstance(encoding, dict):
        return encoding
    t_enc = encoding.setdefault(time_label, {})

    # Calendar: promote any "standard"/"gregorian"/None to proleptic_gregorian.
    cal = t_enc.get("calendar")
    if cal is None and ds is not None and time_label in ds.variables:
        cal = ds[time_label].encoding.get("calendar") or ds[time_label].attrs.get("calendar")
    if cal in (None, "standard", "gregorian"):
        t_enc["calendar"] = "proleptic_gregorian"
    else:
        t_enc["calendar"] = cal

    # Units: pull current units from the encoding dict, then the dataset,
    # then rewrite to canonical "days since ..." with no fractional seconds.
    units = t_enc.get("units")
    if units is None and ds is not None and time_label in ds.variables:
        units = ds[time_label].encoding.get("units") or ds[time_label].attrs.get("units")
    if isinstance(units, str):
        t_enc["units"] = _canonicalize_time_units_str(units)
    elif ds is not None and time_label in ds.variables:
        derived = _derive_date_only_units(ds[time_label])
        if derived:
            t_enc["units"] = derived

    # Dtype: force float64 (CMIP cmor-tables: time is type=double).
    t_enc["dtype"] = "float64"

    # Propagate to time_bnds if present in the encoding dict or the dataset.
    bnds_label = f"{time_label}_bnds"
    has_bnds = bnds_label in encoding or (ds is not None and bnds_label in ds.variables)
    if has_bnds:
        b_enc = encoding.setdefault(bnds_label, {})
        if "calendar" in t_enc:
            b_enc["calendar"] = t_enc["calendar"]
        if "units" in t_enc:
            b_enc["units"] = t_enc["units"]
        b_enc["dtype"] = "float64"

    return encoding


def _midpoint_bounds(bounds_data):
    """Per-row midpoint of a (n, 2) time-bounds array.

    Handles both numpy ``datetime64`` and ``cftime`` object arrays. The
    midpoint is computed as ``b0 + (b1 - b0) / 2`` rather than
    ``np.mean(axis=1)`` so that cftime calendars (proleptic_gregorian,
    noleap, 360_day, ...) round-trip correctly — ``np.mean`` doesn't
    know how to average cftime objects.
    """
    if np.issubdtype(bounds_data.dtype, np.datetime64):
        return bounds_data[:, 0] + (bounds_data[:, 1] - bounds_data[:, 0]) / 2
    return np.array(
        [b0 + (b1 - b0) / 2 for b0, b1 in bounds_data],
        dtype=object,
    )


def _create_mean_bounds(time_values, approx_interval, rule=None):
    """Create bounds for mean time method.

    Parameters
    ----------
    time_values : np.ndarray
        Array of time coordinate values.
    approx_interval : float or None
        Approximate interval in days from the CMIP table.
    rule : Rule or None
        Optional rule, only consulted for the single-time-point yearly path
        where we can't infer cadence from ``np.diff`` and have to fall back
        on ``rule.data_request_variable.frequency`` / ``rule.frequency``.

    Returns
    -------
    np.ndarray
        Array of shape (n, 2) with start/end bounds per time point.
    """
    # Single-stamp yearly files (LPJ-GUESS one-year output, FESOM 1-year
    # chunks) need bnds even though np.diff returns empty. The top-level
    # ``time_bounds`` already gated this with _looks_yearly, so trust the
    # signal and snap to (year_start, next_year_start).
    if len(time_values) < 2:
        if _looks_decadal(rule, approx_interval):
            logger.info("  single-stamp decadal data, using decade-start bounds")
            return _create_decadal_bounds(time_values)
        if _looks_yearly(rule, approx_interval):
            logger.info("  single-stamp yearly data, using year-start bounds")
            return _create_yearly_bounds(time_values)
        raise ValueError("Cannot create mean time bounds: need at least 2 time points")

    # Several decadal stamps snap to calendar decades as well. The spacing
    # branch below derives cells from the stamps themselves, which lands them
    # a few days off the grid (1849-12-28 .. 1860-01-02), and cc-plugin-wcrp#81
    # checks that the cells are regular calendar decades.
    if _looks_decadal(rule, approx_interval):
        logger.info("  decadal data, using decade-start bounds")
        return _create_decadal_bounds(time_values)

    # For numpy datetime64 we can cast directly; cftime objects need
    # date2num via their own calendar to land in a numeric space.
    if np.issubdtype(time_values.dtype, np.datetime64):
        time_diff_seconds = np.median(np.diff(time_values.astype("datetime64[s]").astype(float)))
    else:
        import cftime

        cal = getattr(time_values[0], "calendar", "standard")
        nums = cftime.date2num(time_values, units="seconds since 1970-01-01", calendar=cal)
        time_diff_seconds = float(np.median(np.diff(np.asarray(nums, dtype=float))))
    data_freq_days = time_diff_seconds / (24 * 3600)

    # Use month-aware bounds when the data spacing IS monthly. approx_interval
    # is an optional sanity check; if set, it must also indicate monthly,
    # otherwise we fall through to consecutive-step bounds. The previous
    # logic required approx_interval to be set, which silently degraded
    # broadcast-style pipelines (cfc11, ch4, ...) where the rule doesn't
    # carry a DReq-derived interval.
    data_looks_monthly = 27 <= data_freq_days <= 32
    approx_disagrees = approx_interval is not None and not (28 <= approx_interval <= 32)
    if data_looks_monthly and not approx_disagrees:
        logger.info("  detected monthly data, using month-start bounds")
        return _create_monthly_bounds(time_values)

    # Daily data: align bnds to midnight-to-midnight rather than using the
    # raw input timestamps. FESOM/XIOS writes daily stamps at noon; raw
    # consecutive bnds = (noon, next_noon) would produce midnight midpoints,
    # which wcrp TIME001 then flags as off by 0.5 against the canonical
    # (midnight, next_midnight, midpoint=noon) convention.
    data_looks_daily = 0.9 <= data_freq_days <= 1.1
    approx_disagrees_daily = approx_interval is not None and not (0.9 <= approx_interval <= 1.1)
    if data_looks_daily and not approx_disagrees_daily:
        logger.info("  detected daily data, using day-start bounds")
        return _create_daily_bounds(time_values)

    # Yearly data: snap each stamp's bnds to (Jan 1, next Jan 1). LPJ-GUESS
    # yearly broadcast writes mid-year (Jul 1) stamps; FESOM yearly chunks
    # can land anywhere in the year. Either way, the canonical CMIP yearly
    # bnds span the calendar year and the midpoint is ~Jul 2. Without this,
    # wcrp TIME001 expects midpoint = (Jan 1, next Jan 1) / 2 but the file
    # ships the raw stamp, off by however far the stamp is from Jul 2.
    # 360_day calendars also fall in this band hence the [360, 370] range.
    data_looks_yearly = 360 <= data_freq_days <= 370
    approx_disagrees_yearly = approx_interval is not None and not (360 <= approx_interval <= 370)
    if data_looks_yearly and not approx_disagrees_yearly:
        logger.info("  detected yearly data, using year-start bounds")
        return _create_yearly_bounds(time_values)

    # Default: bnds centered on each timestamp, half-step on each side.
    # CMIP convention is time at the midpoint of its averaging interval, so
    # for input at HH:30 with dt=1h the canonical bnds is (HH:00, HH+1:00)
    # and the recomputed midpoint == input (no time shift). The previous
    # consecutive-step extension (bnds[i] = (t[i], t[i+1])) made bnds
    # straddle the source stamp, so the downstream
    # ``new_time = midpoint(bnds)`` step shifted time forward by dt/2.
    # That bug accounted for every cli66-cli91 sub-daily TIME001 failure.
    time_diff = np.median(np.diff(time_values))
    half = time_diff / 2
    return np.column_stack([time_values - half, time_values + half])


def _create_monthly_bounds(time_values):
    """Create monthly bounds as (month_start, next_month_start).

    Handles both numpy ``datetime64`` and ``cftime`` object arrays.
    cftime is the common case for FESOM/XIOS output (calendar=standard
    or proleptic_gregorian), where ``pd.Timestamp(cftime_obj)`` raises.

    Parameters
    ----------
    time_values : np.ndarray
        Array of time values (datetime64 or cftime objects).

    Returns
    -------
    np.ndarray
        Array of shape (n, 2) with monthly bounds.
    """
    if np.issubdtype(time_values.dtype, np.datetime64):
        import pandas as pd

        bounds_data = []
        for time_val in time_values:
            ts = pd.Timestamp(time_val)
            month_start = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if ts.month == 12:
                next_month_start = month_start.replace(year=ts.year + 1, month=1)
            else:
                next_month_start = month_start.replace(month=ts.month + 1)
            bounds_data.append([month_start.to_numpy(), next_month_start.to_numpy()])
        return np.array(bounds_data)

    bounds_data = []
    for time_val in time_values:
        month_start = time_val.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if time_val.month == 12:
            next_month_start = month_start.replace(year=time_val.year + 1, month=1)
        else:
            next_month_start = month_start.replace(month=time_val.month + 1)
        bounds_data.append([month_start, next_month_start])
    return np.array(bounds_data, dtype=object)


def _create_daily_bounds(time_values):
    """Create daily bounds as (day_start, next_day_start).

    Snaps the per-cell bnds to midnight regardless of where the input
    timestamp lies within the day. FESOM/XIOS daily output is typically
    at noon, so consecutive-step bnds = (noon, next_noon) would put the
    midpoint at midnight instead of the canonical noon. Snapping to day
    boundaries fixes wcrp TIME001 on every daily file. Handles both
    numpy ``datetime64`` and ``cftime`` object arrays.
    """
    import datetime as _dt

    if np.issubdtype(time_values.dtype, np.datetime64):
        starts = time_values.astype("datetime64[D]").astype("datetime64[ns]")
        nexts = starts + np.timedelta64(1, "D")
        return np.column_stack([starts, nexts])

    one_day = _dt.timedelta(days=1)
    bounds_data = []
    for time_val in time_values:
        day_start = time_val.replace(hour=0, minute=0, second=0, microsecond=0)
        next_day_start = day_start + one_day
        bounds_data.append([day_start, next_day_start])
    return np.array(bounds_data, dtype=object)


def _create_yearly_bounds(time_values):
    """Create yearly bounds as (year_start, next_year_start).

    Snaps the per-cell bnds to Jan 1 of the same year and Jan 1 of the next
    year, regardless of where the input timestamp lies within the year.
    LPJ-GUESS yearly broadcast writes mid-year (Jul 1) stamps, FESOM yearly
    chunks can land anywhere — neither is the canonical CMIP yearly cell
    boundary. wcrp TIME001 builds its expected midpoint from (Jan 1, next
    Jan 1) using the FREQ_INC mapping; without these snapped bnds the time
    coordinate is off by however far the source stamp lies from Jul 2.
    Handles both numpy ``datetime64`` and ``cftime`` object arrays.
    """
    if np.issubdtype(time_values.dtype, np.datetime64):
        starts = time_values.astype("datetime64[Y]").astype("datetime64[ns]")
        # ``datetime64[Y] + 1`` is "next Jan 1" exactly.
        nexts = (time_values.astype("datetime64[Y]") + np.timedelta64(1, "Y")).astype("datetime64[ns]")
        return np.column_stack([starts, nexts])

    bounds_data = []
    for time_val in time_values:
        year_start = time_val.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        next_year_start = year_start.replace(year=time_val.year + 1)
        bounds_data.append([year_start, next_year_start])
    return np.array(bounds_data, dtype=object)


def _create_decadal_bounds(time_values):
    """Create decadal bounds as (decade_start, next_decade_start).

    Snaps to the calendar decade containing the stamp, so 1854-12-31 becomes
    (1850-01-01, 1860-01-01). ``dec`` used to be folded into
    :func:`_create_yearly_bounds`, which gave a decadal cell one year of
    bnds: a ten-year mean that claimed to cover twelve months. Nothing
    caught it until wcrp added ``dec`` to its TIME001 frequency list, at
    which point every single-stamp decadal file failed the squareness check.

    Only the single-stamp path needs this. With two or more stamps the
    spacing-based branch already derives ten-year cells correctly.

    Handles both numpy ``datetime64`` and ``cftime`` object arrays.
    """
    if np.issubdtype(time_values.dtype, np.datetime64):
        years = time_values.astype("datetime64[Y]").astype(int) + 1970
        starts = (years // 10) * 10
        return np.column_stack(
            [
                np.array([f"{y:04d}-01-01" for y in starts], dtype="datetime64[ns]"),
                np.array([f"{y + 10:04d}-01-01" for y in starts], dtype="datetime64[ns]"),
            ]
        )

    bounds_data = []
    for time_val in time_values:
        decade_start = time_val.replace(
            year=(time_val.year // 10) * 10, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        next_decade_start = decade_start.replace(year=decade_start.year + 10)
        bounds_data.append([decade_start, next_decade_start])
    return np.array(bounds_data, dtype=object)


def _looks_decadal(rule, approx_interval):
    """Heuristic: does this rule describe a decadal variable?

    Kept apart from :func:`_looks_yearly` on purpose. Both feed the same
    single-stamp branch, and folding them together is what produced
    one-year bnds on ten-year cells.
    """
    if approx_interval is not None:
        try:
            if 3600 <= float(approx_interval) <= 3700:
                return True
        except (TypeError, ValueError):
            pass
    drv = getattr(rule, "data_request_variable", None) if rule is not None else None
    if drv is not None and getattr(drv, "frequency", None) == "dec":
        return True
    if rule is not None and getattr(rule, "frequency", None) == "dec":
        return True
    return False


def _looks_yearly(rule, approx_interval):
    """Heuristic: does this rule describe a yearly variable?

    Used by the single-time-point branch of ``time_bounds`` and by the
    cadence-blind branch of ``_create_mean_bounds`` to decide whether to
    snap bnds to whole calendar years. Three signals, any of them is
    sufficient:

    * ``approx_interval`` in the [360, 370] day band (covers 365 standard,
      360 for 360_day calendars, and 365.25 julian-ish edge cases).
    * ``rule.data_request_variable.frequency`` is one of ``yr``, ``yrPt``,
      ``dec``.
    * ``rule.frequency`` (the rule-level convenience alias) is one of the
      same set.
    """
    if approx_interval is not None:
        try:
            if 360 <= float(approx_interval) <= 370:
                return True
        except (TypeError, ValueError):
            pass
    yearly_freqs = ("yr", "yrPt")
    drv = getattr(rule, "data_request_variable", None) if rule is not None else None
    if drv is not None and getattr(drv, "frequency", None) in yearly_freqs:
        return True
    if rule is not None and getattr(rule, "frequency", None) in yearly_freqs:
        return True
    return False
