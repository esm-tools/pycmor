#!/usr/bin/env python
"""Full CMIP7 DReq list + compressed output-size estimate for AWI-ESM3 LR (carbon-cycle variant).

Three variable sets, one engine:
  obgc  : every variable whose CMIP7 compound name has realm ocnBgchem
  co2   : carbon-cycle / CO2 variables outside ocnBgchem (CO2 amounts, atmosphere-surface CO2 fluxes)
  ice   : iceberg and ice-shelf-cavity variables (ficeberg, hfibthermds, flandice, libmassbf, sftflf,
          iareafl, licalvf, lifmassbf, tendlicalvf, litempbot, all variants)

Scope: list + size only. No mapping to REcoM / OIFS / LPJ-GUESS output, no feasibility judgement
(the optional 'peer_precheck' column is carried over unchanged from notes/lr_peer_session/flux_table.md,
and the co2 set has a light 'hr_writes' / 'lr_source_hint' pair as requested).

Inputs (read only):
  - DReq content v1.2.2.5 from the data_request_api cache (offline)
  - LR mesh core3: nod2d.out, nlvls.out, cavity_nlvls.out, aux3d.out, elem2d.out
  - HR cmorized year (cli123) bytes/value: hr_bytes_per_value_measured.csv (made by measure_hr_bytes_per_value.py)
  - LR raw REcoM/FESOM output (h5py storage sizes only) for the BGC-vs-physics compression ratio
  - HR yamls (grep compound_name) for the co2 'hr_writes' column

Usage:
  python estimate_lr_volume.py            # all three sets
  python estimate_lr_volume.py obgc co2   # a subset
Outputs (relative to awi-esm3-lr-variables/):
  obgc/cmip7_obgc_variables.csv, obgc/cmip7_obgc_todo.md
  co2/cmip7_co2_variables.csv,   co2/cmip7_co2_todo.md
  icebergs_cavities/cmip7_icebergs_cavities_variables.csv, icebergs_cavities/cmip7_icebergs_cavities_todo.md
"""
import collections
import csv
import glob
import io
import os
import re
import sys
import contextlib

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
LRVAR = os.path.dirname(HERE)
REPO = os.path.dirname(LRVAR)
DREQ_VERSION = "v1.2.2.5"
MESH = "/work/ab0246/a270092/input/fesom2/core3"
HR_MESH = "/work/ab0246/a270092/input/fesom2/dars2"
HR_YAML_DIR = os.path.join(REPO, "awi-esm3-veg-hr-variables")
HR_MEASURED = os.path.join(HERE, "hr_bytes_per_value_measured.csv")
LR_RUN = "/work/ab1095/a270073/out/awiesm3-v3.5.0-cc/pi/outdata"
PEER_TABLE = os.path.join(REPO, "notes", "lr_peer_session", "flux_table.md")

ATM_CELLS = 40320          # TCo95 -> octahedral O96 reduced Gaussian grid, native output
ATM_REMAP = 400 * 192      # what the LR test run's atm_remapped_* files use (sensitivity only)
ATM_LEVELS = 91            # L91
OIFS_DT = 3600             # TSTEP in the LR test run fort.4 -> subhr = 8760 steps / yr
N_SITES = 126              # DReq 'site' coordinate size (CFMIP stations)
N_ZONAL_LAT = 180          # basin / zonal-mean diagnostics, 1 deg latitude, as in HR (hfbasin 3x180)

STEPS = {"mon": 12, "day": 365, "6hr": 1460, "3hr": 2920, "1hr": 8760, "subhr": 365 * 86400 // OIFS_DT,
         "yr": 1, "dec": 0.1, "fx": 0}
PRIO_ORDER = ["Core", "High", "Medium", "Low"]

# Jan's decisions that take a variable out of the totals (listed, not counted)
DROPPED = {
    "ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb": "dropped (Jan, 2026-10-19)",
}

ICE_ROOTS = {"ficeberg", "hfibthermds", "flandice", "libmassbf", "sftflf", "iareafl", "licalvf", "lifmassbf",
             "tendlicalvf", "litempbot"}
# land carbon fluxes that are CO2 exchange with the atmosphere but whose standard name lacks 'carbon_dioxide'
CO2_EXTRA_ROOTS = {"gpp", "npp", "gppLut", "nppLut", "gppVgt", "nppVgt", "gppc13", "gppc14", "rhLitter", "rhSoil"}


# ----------------------------------------------------------------------------------------------
# DReq
# ----------------------------------------------------------------------------------------------
def load_dreq():
    from data_request_api.content import dreq_content as dc
    from data_request_api.query import dreq_query as dq
    with contextlib.redirect_stdout(io.StringIO()):
        content = dc.load(DREQ_VERSION, offline=True)
        base = dq.create_dreq_tables_for_request(content, DREQ_VERSION)
        meta = dq.get_variables_metadata(base, DREQ_VERSION, verbose=False)
        req = dq.get_requested_variables(base, DREQ_VERSION, use_opps="all", priority_cutoff="Low",
                                         verbose=False, check_core_variables=False)
    meta = {k: v for k, v in meta.items() if k != "Header"}
    # variable -> opportunities / groups via the tables (all 46 opportunities, v1.2.2.5 has no status field)
    V, G, O, P = base["Variables"], base["Variable Group"], base["Opportunity"], base["Priority Level"]
    var_info = collections.defaultdict(lambda: {"opps": {}, "groups": {}})
    opp_title = {}
    for oid, o in O.records.items():
        onum = int(o.opportunity_id)
        opp_title[onum] = o.title
        for gl in getattr(o, "variable_groups", []):
            g = G.records[gl.record_id]
            pr = P.records[g.priority_level[0].record_id].name if isinstance(g.priority_level, list) else g.priority_level
            for vl in getattr(g, "variables", []):
                cn = V.records[vl.record_id].cmip7_compound_name
                d = var_info[cn]
                if onum not in d["opps"] or PRIO_ORDER.index(pr) < PRIO_ORDER.index(d["opps"][onum]):
                    d["opps"][onum] = pr
                d["groups"][g.name] = pr
    # variable -> experiments (best priority per experiment)
    expts = collections.defaultdict(dict)
    for ex, byp in req["experiment"].items():
        for pr, names in byp.items():
            for cn in names:
                old = expts[cn].get(ex)
                if old is None or PRIO_ORDER.index(pr) < PRIO_ORDER.index(old):
                    expts[cn][ex] = pr
    return meta, var_info, expts, opp_title


def select(meta, which):
    out = []
    for cn, m in meta.items():
        realm, var = cn.split(".")[:2]
        sn = m.get("standard_name", "") or ""
        if which == "obgc" and realm == "ocnBgchem":
            out.append(cn)
        elif which == "co2" and realm != "ocnBgchem":
            if ("co2" in var.lower() and "4co2" not in var) or "carbon_dioxide" in sn or var in CO2_EXTRA_ROOTS:
                out.append(cn)
        elif which == "ice" and var in ICE_ROOTS:
            out.append(cn)
    return sorted(out)


# ----------------------------------------------------------------------------------------------
# Grids
# ----------------------------------------------------------------------------------------------
def read_mesh(path):
    with open(os.path.join(path, "nod2d.out")) as fh:
        n = int(fh.readline())
        xy = np.loadtxt(fh, usecols=(1, 2), max_rows=n)
    lon, lat = xy[:, 0], xy[:, 1]
    lon = np.where(lon > 180, lon - 360, lon)
    nl = np.loadtxt(os.path.join(path, "nlvls.out"), dtype=int)
    cav = os.path.join(path, "cavity_nlvls.out")
    ul = np.loadtxt(cav, dtype=int) if os.path.exists(cav) else np.ones_like(nl)
    nz = int(open(os.path.join(path, "aux3d.out")).readline()) - 1  # layers = interfaces - 1
    nelem = int(open(os.path.join(path, "elem2d.out")).readline())
    wet_layers = nl - ul                 # wet layers per node (cavity nodes start below the ice-shelf base)
    open_ocean = ul == 1                 # surface fields are fill under ice shelves
    return dict(n=n, lon=lon, lat=lat, nz=nz, nelem=nelem, wet_layers=wet_layers, open=open_ocean,
                cavity=~open_ocean)


def o96_cells():
    """Latitude of every cell of the octahedral reduced Gaussian grid O96 (TCo95)."""
    x, _ = np.polynomial.legendre.leggauss(192)
    lats = np.degrees(np.arcsin(x))[::-1]           # north -> south
    npts = [20 + 4 * i for i in range(96)]
    npts = npts + npts[::-1]
    return np.repeat(lats, npts)


class Grids:
    def __init__(self):
        self.lr = read_mesh(MESH)
        self.hr = read_mesh(HR_MESH)
        self.atm_lat = o96_cells()
        assert self.atm_lat.size == ATM_CELLS
        lr = self.lr
        self.masks = {
            "glb": np.ones(lr["n"], bool),
            "30S-90S": lr["lat"] <= -30.0,
            "ata": lr["lat"] <= -60.0,                                    # Antarctic ice-sheet / shelf region
            "grl": (lr["lat"] >= 58) & (lr["lon"] >= -75) & (lr["lon"] <= -10),  # Greenland box
        }
        self.atm_masks = {"glb": np.ones(ATM_CELLS, bool), "30S-90S": self.atm_lat <= -30.0}
        hr = self.hr
        self.hr_wet3d_frac = hr["wet_layers"].sum() / (hr["n"] * hr["nz"])

    def ocean(self, region):
        m = self.masks[region]
        lr = self.lr
        return dict(nodes=int(m.sum()), open=int((m & lr["open"]).sum()), cavity=int((m & lr["cavity"]).sum()),
                    wet3d=int(lr["wet_layers"][m].sum()), nz=lr["nz"])

    def atm(self, region):
        return int(self.atm_masks.get(region, self.atm_masks["glb"]).sum())


# ----------------------------------------------------------------------------------------------
# Bytes per value, measured
# ----------------------------------------------------------------------------------------------
CLASS_DEFS = {
    # name: (description, selector on HR measured rows)
    # The three ocean physics classes are paired with the LR raw physics fields used for the BGC/physics
    # compression ratio: thetao uo vo wo <-> temp unod vnod w, tos sos zos <-> sst sss ssh.
    "ocn3d_mon": ("HR cmorized ocean 3D monthly on nodes (thetao uo vo wo), per wet value",
                  lambda r: r["grid_label"] == "g130" and r["frequency"] == "mon" and r["region"] == "glb"
                  and r["var"] in {"thetao", "uo", "vo", "wo"} and r["shape"].count("x") == 2),
    "ocn2d_mon": ("HR cmorized ocean 2D monthly on nodes (tos sos zos)",
                  lambda r: r["grid_label"] == "g130" and r["frequency"] == "mon" and r["shape"].count("x") == 1
                  and r["var"] in {"tos", "sos", "zos"}),
    "ocn2d_day": ("HR cmorized ocean 2D daily on nodes (tos sos zos)",
                  lambda r: r["grid_label"] == "g130" and r["frequency"] == "day" and r["var"] in {"tos", "sos", "zos"}),
    "sparse2d_mon": ("HR cmorized sea-ice 2D monthly on nodes (fields that are zero/fill over most of the ocean)",
                     lambda r: r["grid_label"] == "g130" and r["frequency"] == "mon" and r["realm"].startswith("seaIce")
                     and r["shape"].count("x") == 1 and r["var"] not in {"sidragbot", "sidragtop", "sbl"}),
    "sparse2d_day": ("HR cmorized sea-ice 2D daily on nodes",
                     lambda r: r["grid_label"] == "g130" and r["frequency"] == "day" and r["realm"].startswith("seaIce")
                     and r["var"] not in {"sitimefrac"}),
    "zeros": ("HR cmorized all-zero / constant fields (vsfcorr sidragbot sidragtop)",
              lambda r: r["grid_label"] == "g130" and r["var"] in {"vsfcorr", "sidragbot", "sidragtop"}),
    "ocn_fx": ("HR cmorized ocean fx on nodes (areacello deptho volcello)",
               lambda r: r["grid_label"] == "g130" and r["frequency"] == "fx" and r["var"] in
               {"areacello", "deptho", "volcello"}),
    "ocn_mask_fx": ("HR cmorized ocean mask fx (sftof basin)",
                    lambda r: r["grid_label"] == "g130" and r["frequency"] == "fx" and r["var"] in {"sftof", "basin"}),
    "scalar": ("HR cmorized global-mean / hemispheric time series (thetao so tos sos volo zostoga siarea ... hm)",
               lambda r: "-hm-" in r["branding"]),
    "zonal": ("HR cmorized basin/zonal ocean diagnostics (hfbasin sltbasin msftm)",
              lambda r: r["grid_label"] == "g239"),
    "atm2d_mon": ("HR cmorized atmosphere/land 2D monthly on native TCo319 cells, atmosphere fields (g122)",
                  lambda r: r["grid_label"] == "g122" and r["frequency"] == "mon" and r["realm"].startswith("atmos")
                  and r["shape"].count("x") == 1),
    "atm3d_mon": ("HR cmorized atmosphere 3D monthly on native cells (ta hus hur on model levels, hur zg on plev19)",
                  lambda r: r["grid_label"] == "g122" and r["frequency"] == "mon" and r["var"] in
                  {"ta", "hus", "hur", "zg"} and r["shape"].count("x") == 2),
    "atm2d_1hr": ("HR cmorized atmosphere 2D hourly on native cells (proxy for subhr site output)",
                  lambda r: r["grid_label"] == "g122" and r["frequency"] == "1hr" and r["region"] == "glb"),
    "lnd2d_mon": ("HR cmorized land 2D monthly on native cells (LPJ-GUESS / HTESSEL, ocean is fill)",
                  lambda r: r["grid_label"] == "g122" and r["frequency"] == "mon" and r["realm"].startswith("land")
                  and r["shape"].count("x") == 1),
    "lnd2d_day": ("HR cmorized land 2D daily on native cells",
                  lambda r: r["grid_label"] == "g122" and r["frequency"] == "day" and r["realm"].startswith("land")
                  and r["shape"].count("x") == 1),
    "lnd_multi_mon": ("HR cmorized land monthly with a land-use / pft axis (gppLut fracLut cSoilPools ...)",
                      lambda r: r["grid_label"] == "g122" and r["frequency"] == "mon" and r["realm"].startswith("land")
                      and r["shape"].count("x") == 2),
    "lnd2d_yr": ("HR cmorized land 2D yearly on native cells",
                 lambda r: r["grid_label"] == "g122" and r["frequency"] == "yr" and r["realm"].startswith("land")),
}


def measured_classes(grids):
    rows = list(csv.DictReader(open(HR_MEASURED)))
    for r in rows:
        r["bpv"] = float(r["bytes_per_value"])
        n_h = int(r["shape"].split("x")[-1])
        r["ovh_per_point"] = int(r["overhead_bytes"]) / n_h
    cls = {}
    for name, (desc, sel) in CLASS_DEFS.items():
        rs = [r for r in rows if sel(r)]
        if not rs:
            raise SystemExit(f"class {name}: no HR files matched")
        b = np.array([r["bpv"] for r in rs])
        if name == "ocn3d_mon":
            b = b / grids.hr_wet3d_frac          # bytes per wet value
        ovh = float(np.median([r["ovh_per_point"] for r in rs]))
        cls[name] = dict(desc=desc, n=len(rs), median=float(np.median(b)), low=float(b.min()), high=float(b.max()),
                         p10=float(np.percentile(b, 10)), p90=float(np.percentile(b, 90)),
                         ovh_per_point=ovh, files=" ".join(sorted({r["var"] for r in rs})),
                         ovh_median=float(np.median([int(r["overhead_bytes"]) for r in rs])))
    return cls


def lr_raw_recom_ratio():
    """Compression of LR raw REcoM fields relative to raw FESOM physics in the same run (lossless output).
    Returned per class: median(bgc)/median(physics)."""
    import h5py
    d = os.path.join(LR_RUN, "fesom")
    phys3d, phys2d = {"temp", "unod", "vnod", "w"}, {"sst", "sss", "ssh"}
    bgc_skip = phys3d | phys2d | {"u", "v", "salt", "Av", "Kv", "N2", "bolus_u", "bolus_v", "bolus_w", "density_dMOC",
                                  "a_ice", "m_ice", "m_snow", "uice", "vice", "alb", "apnd", "hpnd", "ipnd", "fh",
                                  "fw", "tx_sur", "ty_sur", "MLD1", "MLD2", "MLD3", "ibfwb", "ibfwbv", "ibfwe",
                                  "ibfwl", "ibhf", "xCO2atm"}
    res = collections.defaultdict(list)
    for f in sorted(glob.glob(os.path.join(d, "*.fesom.1850.nc"))):
        v = os.path.basename(f).split(".")[0]
        if v.startswith("std_"):
            continue
        try:
            with h5py.File(f, "r") as h:
                ds = h[v]
                if ds.shape[1] != 220509:
                    continue
                bpv = ds.id.get_storage_size() / np.prod(ds.shape) * (4 / ds.dtype.itemsize)
                kind = "3d" if ds.ndim == 3 else "2d"
        except Exception:
            continue
        if v in phys3d or v in phys2d:
            res["phys" + kind].append((v, bpv))
        elif v not in bgc_skip:
            res["bgc" + kind].append((v, bpv))
    out = {}
    for k in ("3d", "2d"):
        b = np.median([x[1] for x in res["bgc" + k]]) if res["bgc" + k] else np.nan
        p = np.median([x[1] for x in res["phys" + k]]) if res["phys" + k] else np.nan
        bb = np.array([x[1] for x in res["bgc" + k]])
        out[k] = dict(bgc=b, phys=p, ratio=b / p, n_bgc=len(res["bgc" + k]),
                      ratio_p10=float(np.percentile(bb, 10)) / p, ratio_p90=float(np.percentile(bb, 90)) / p,
                      phys_vars=" ".join(x[0] for x in res["phys" + k]),
                      bgc_low=min(x[1] for x in res["bgc" + k]), bgc_high=max(x[1] for x in res["bgc" + k]))
    return out


# ----------------------------------------------------------------------------------------------
# Sizing
# ----------------------------------------------------------------------------------------------
SPARSE_HINT = re.compile(r"-(si|fis|gis|is)$")


def size_one(cn, m, grids, cls, raw=None):
    realm, var, branding, freq, region = cn.split(".")
    dims = m["dimensions"].split()
    note = []
    steps = STEPS[freq]
    per_run = freq == "fx"
    if "time2" in dims:            # climatology: one 12-step field per climatology period, count it per run
        per_run = True
        note.append("climatology (time2): counted once per run with 12 steps")
    t_vals = 12 if ("time2" in dims) else (1 if per_run else steps)
    ocean_like = realm in ("ocean", "ocnBgchem", "seaIce", "landIce")
    horiz = "longitude" in dims and "latitude" in dims
    area = branding.split("-")[-1]
    lev = 1
    vert = None
    for d in dims:
        if d in ("olevel", "olevhalf"):
            vert, lev = d, grids.lr["nz"] + (1 if d == "olevhalf" else 0)
        elif d in ("alevel", "alevhalf"):
            vert, lev = d, ATM_LEVELS + (1 if d == "alevhalf" else 0)
        elif re.match(r"plev(\d+)", d):
            vert, lev = d, int(re.match(r"plev(\d+)", d).group(1))
    ovh = 0.0
    bclass = None
    if not horiz and "site" in dims:
        h = N_SITES
        bclass = "atm2d_1hr"
        note.append(f"{N_SITES} CFMIP sites, subhr = OIFS step {OIFS_DT}s")
        values = h * t_vals
        wet = values
        ovh = 2e5
    elif not horiz and "latitude" in dims:          # zonal / basin
        nb = 3 if "basin" in dims else 1
        values = nb * N_ZONAL_LAT * lev * t_vals
        wet = values
        bclass = "zonal"
        ovh = 4e4
    elif not horiz:                                  # global / regional mean time series
        values = t_vals * lev
        wet = values
        bclass = "scalar"
        ovh = 2.6e4
    elif ocean_like:
        reg = region if region in grids.masks else "glb"
        g = grids.ocean(reg)
        h = g["nodes"]
        if vert in ("olevel", "olevhalf"):
            values = h * lev * t_vals
            wet = g["wet3d"] * t_vals
            surface_only = var in ("ficeberg",)
            if surface_only:
                # FESOM puts iceberg melt into the top layer only: one sparse 2D layer + zeros below
                bclass = "sparse2d_mon" if freq != "day" else "sparse2d_day"
                wet = g["open"] * t_vals
                note.append("3D but only top layer non-zero (iceberg melt enters the surface layer); "
                            "sized as sparse 2D + zero layers")
            elif var in ICE_ROOTS:
                # iceberg heat flux: non-zero only where icebergs are; sparse class on the wet count
                bclass = "sparse2d_mon"
                note.append("3D iceberg field, zero away from icebergs; sparse class per wet value")
            else:
                bclass = "ocn3d_mon"
                if freq != "mon":
                    note.append(f"no HR 3D {freq} file; monthly 3D bytes/value used")
            ovh = cls["ocn3d_mon"]["ovh_per_point"] * h
        else:
            values = h * t_vals
            # surface / depth-integrated fields are fill under ice shelves, except ice-shelf quantities
            wet = (g["cavity"] if area == "fis" else g["open"]) * t_vals
            if var in ("sftflf", "basin", "sftof"):
                bclass = "ocn_mask_fx"           # 0/100 mask, constant in time; compresses like HR sftof/basin
                wet = h * t_vals
                note.append("mask field, sized with HR sftof/basin bytes/value")
            elif freq == "fx":
                bclass = "ocn_fx"
            elif area in ("si", "fis", "gis", "is") or var in ("ficeberg", "flandice", "hfibthermds",
                                                               "libmassbf", "litempbot", "licalvf", "lifmassbf"):
                bclass = "sparse2d_day" if freq in ("day", "3hr", "1hr") else "sparse2d_mon"
                note.append("sparse class: field zero/fill over most of the domain")
            elif freq in ("day", "3hr", "1hr", "6hr"):
                bclass = "ocn2d_day"
            else:
                bclass = "ocn2d_mon"
            if reg != region:
                note.append(f"region {region} sized as {reg}")
            ovh = cls["ocn2d_mon"]["ovh_per_point"] * h
    else:                                            # atmosphere / land on native TCo95 cells
        h = grids.atm(region)
        values = h * lev * t_vals
        wet = values
        if realm == "land":
            nlut = 7 if "vegtype" in dims else 4 if area == "multi" else 1   # DReq vegtype 7, landuse 4
            values *= nlut
            wet = values
            bclass = {"day": "lnd2d_day", "yr": "lnd2d_yr"}.get(freq, "lnd_multi_mon" if nlut > 1 else "lnd2d_mon")
            if nlut > 1:
                note.append(f"{'vegtype' if nlut == 7 else 'landuse'} axis: {nlut} tiles")
        elif lev > 1:
            bclass = "atm3d_mon"
        else:
            bclass = "atm2d_mon" if freq != "1hr" else "atm2d_1hr"
        ovh = 19.1 * h                                # lat/lon + cell vertices, HR 8.04 MB / 421120 cells
        if vert in ("alevel", "alevhalf"):
            ovh += h * t_vals * 2.5                   # ps formula term written with model-level fields
    c = cls[bclass]
    # 3D ocean class is per wet value; the 2D ocean classes are per full-array value of a mesh with no
    # land nodes (HR), so they act on the open-ocean / cavity count as well.
    if bclass == "ocn3d_mon" or ocean_like and horiz:
        base_n = wet
    else:
        base_n = values
    if bclass == "zeros":
        base_n = values
    fac = {"median": 1.0, "low": 1.0, "high": 1.0}
    if realm == "ocnBgchem" and bclass in ("ocn3d_mon", "ocn2d_mon", "ocn2d_day") and raw:
        rr = raw["3d" if bclass == "ocn3d_mon" else "2d"]
        fac = {"median": rr["ratio"], "low": rr["ratio_p10"], "high": rr["ratio_p90"]}
        note.append(f"BGC/physics compression ratio {rr['ratio']:.2f} (LR raw output) applied")
    data_bytes = {k: c[k] * fac[k] * base_n for k in ("median", "low", "high")}
    if ocean_like and horiz and vert and var == "ficeberg":
        zeros = cls["zeros"]["median"] * (values - wet)
        data_bytes = {k: v + zeros for k, v in data_bytes.items()}
    tot = {k: v + ovh for k, v in data_bytes.items()}
    files = 0 if per_run else (0.1 if freq == "dec" else 1)
    return dict(size_class=bclass, horiz_points=values // max(t_vals * lev, 1) if values else 0,
                levels=lev, steps_per_year=0 if per_run else steps, values=values, wet_values=wet,
                bytes_per_value=c["median"] * fac["median"], bpv_low=c["low"] * fac["low"],
                bpv_high=c["high"] * fac["high"],
                overhead_MB=ovh / 1e6, files_per_year=files, per_run=per_run,
                MB=tot["median"] / 1e6, MB_low=tot["low"] / 1e6, MB_high=tot["high"] / 1e6,
                raw_MB=values * 4 / 1e6, note="; ".join(note))


# ----------------------------------------------------------------------------------------------
# Extra columns
# ----------------------------------------------------------------------------------------------
def hr_yaml_compound_names():
    found = collections.defaultdict(list)
    for y in glob.glob(os.path.join(HR_YAML_DIR, "*", "*.yaml")):
        tier = os.path.basename(os.path.dirname(y))
        for line in open(y):
            mm = re.match(r"^(\s*#?\s*)compound_name:\s*([\w.\-]+)", line)
            if mm:
                found[mm.group(2)].append(tier + (" (commented)" if "#" in mm.group(1) else ""))
    return found


def hr_written():
    s = set()
    for r in csv.DictReader(open(HR_MEASURED)):
        s.add((r["var"], r["branding"], r["frequency"], r["region"]))
    return s


def peer_tags():
    tags = {}
    if not os.path.exists(PEER_TABLE):
        return tags
    for line in open(PEER_TABLE):
        mm = re.match(r"^\|\s*`([\w.\-]+)`\s*\|[^|]*\|\s*([WCSN])\s*\|", line)
        if mm:
            tags[mm.group(1)] = mm.group(2)
    return tags


PEER_TODO = os.path.join(REPO, "awi-esm3-veg-hr-variables", "lr_fluxes_bgc_co2_ice_todo.md")


def peer_todo_sections():
    """Section of the parallel session's lr_fluxes_bgc_co2_ice_todo.md that names a compound name
    (1 = W pycmor only, 2 = X XML/namelist, 3 = S source change). Informational only."""
    out = {}
    if not os.path.exists(PEER_TODO):
        return out
    sec = None
    for line in open(PEER_TODO):
        mm = re.match(r"^## (\d)\.", line)
        if mm:
            sec = {"1": "W", "2": "X", "3": "S"}.get(mm.group(1))
            continue
        if sec:
            for cn in re.findall(r"`((?:ocnBgchem|ocean|atmos|land|landIce)\.[\w]+\.[\w\-]+\.\w+\.[\w\-]+)`", line):
                out.setdefault(cn, sec)
    return out


def lr_source_hint(cn):
    """co2 set only, light: which LR test-run field could feed it (no feasibility judgement)."""
    realm, var, branding, freq, region = cn.split(".")
    oifs = {
        "co2": "OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run",
        "co23D": "OIFS CO2 tracer (model levels not in test run; atm_remapped_1m_pl_co2 is plev19)",
        "co2mass": "OIFS CO2 tracer integrated with air mass (needs 3D co2 + ps)",
        "fco2fos": "OIFS co2apf (atm_remapped_1m_co2apf)",
        "fco2antt": "OIFS co2apf (+ LPJ-GUESS fLuc); LPJ-GUESS fco2antt_monthly.out exists",
        "fco2nat": "OIFS co2nbf + co2of; LPJ-GUESS fco2nat_monthly.out exists",
        "fco2all": "OIFS co2apf + co2nbf + co2of",
    }
    if freq == "subhr":
        return ""
    if var in oifs:
        return oifs[var]
    lpj = os.path.join(LR_RUN, "lpj_guess", "18500101-18501231")
    cands = [f"{var}_monthly.out", f"{var}.out", f"{var}_yearly.out", f"{var}_daily.out"]
    if branding.endswith("-ng"):
        cands.insert(0, f"{var}Grass_monthly.out")
    if branding.endswith("-shb"):
        cands.insert(0, f"{var}Shrub_monthly.out")
    if branding.endswith("-tree"):
        cands.insert(0, f"{var}Tree_monthly.out")
    for c in cands:
        if os.path.exists(os.path.join(lpj, c)):
            return f"LPJ-GUESS {c}"
    extra = {"nbp": "LPJ-GUESS nbp_monthly.out", "nep": "LPJ-GUESS nep_monthly.out / cflux.out NEE"}
    return extra.get(var, "")


# ----------------------------------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------------------------------
META_COLS = ["frequency", "modeling_realm", "standard_name", "units", "cell_methods", "cell_measures", "long_name",
             "comment", "processing_note", "dimensions", "out_name", "type", "positive", "spatial_shape",
             "temporal_shape", "cmip6_table", "physical_parameter_name", "variableRootDD", "branding_label",
             "branded_variable_name", "region", "cmip6_compound_name", "cmip7_compound_name", "uid",
             "flag_values", "flag_meanings"]


def best_prio(opps):
    return min(opps.values(), key=PRIO_ORDER.index) if opps else "none"


def fmt_opps(opps):
    return " ".join(f"{o}{p[0]}" for o, p in sorted(opps.items()))


def build(which, ctx):
    meta, var_info, expts, opp_title, grids, cls, peer = (ctx[k] for k in
                                                         ("meta", "var_info", "expts", "opp_title", "grids", "cls", "peer"))
    names = select(meta, which)
    rows = []
    for cn in names:
        m = meta[cn]
        vi = var_info.get(cn, {"opps": {}, "groups": {}})
        ex = expts.get(cn, {})
        s = size_one(cn, m, grids, cls, ctx["raw"])
        r = {"name": cn}
        r.update({k: m.get(k, "") for k in META_COLS})
        r.update(dict(
            priority=best_prio(vi["opps"]),
            opportunities=fmt_opps(vi["opps"]),
            opportunity_titles="; ".join(f"{o}: {opp_title[o]}" for o in sorted(vi["opps"])),
            variable_groups=" ".join(f"{g}({p[0]})" for g, p in sorted(vi["groups"].items())),
            n_experiments=len(ex),
            experiments=" ".join(sorted(ex)),
            status=DROPPED.get(cn, ""),
            peer_precheck=peer.get(cn, ""),
            peer_todo_section=ctx["peer_todo"].get(cn, ""),
        ))
        r.update(s)
        if which == "co2":
            r["hr_writes"] = "; ".join(ctx["hr_yaml"].get(cn, [])) or "no"
            realm, var, branding, freq, region = cn.split(".")
            r["hr_cli123_file"] = "yes" if (var, branding, freq, region) in ctx["hr_files"] else "no"
            r["lr_source_hint"] = lr_source_hint(cn)
            r["co2_subset"] = ("co2-named" if "co2" in var.lower() else
                               "isotope" if re.search(r"c1[34]|C1[34]", var) else "land-atm carbon flux")
            r["MB_400x192"] = (r["MB"] * ATM_REMAP / ATM_CELLS) if realm in ("atmos", "land") and \
                "longitude" in m["dimensions"] else r["MB"]
        rows.append(r)
    return rows


def write_csv(rows, path):
    keys = list(rows[0].keys())
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["index"] + keys)
        w.writeheader()
        for i, r in enumerate(rows):
            rr = {k: (f"{v:.4g}" if isinstance(v, float) else v) for k, v in r.items()}
            w.writerow({"index": i, **rr})


def gb(x):
    return f"{x / 1e3:.2f}"


def summarize(rows):
    """Totals over rows that are counted (not dropped, not per-run)."""
    cnt = [r for r in rows if not r["status"]]
    yr = [r for r in cnt if not r["per_run"]]
    return cnt, yr


def table(rows, key, keys_order=None):
    agg = collections.OrderedDict()
    for r in rows:
        k = key(r)
        a = agg.setdefault(k, dict(n=0, files=0.0, MB=0.0, MB_low=0.0, MB_high=0.0, raw=0.0))
        a["n"] += 1
        a["files"] += r["files_per_year"]
        a["MB"] += r["MB"]
        a["MB_low"] += r["MB_low"]
        a["MB_high"] += r["MB_high"]
        a["raw"] += r["raw_MB"]
    ks = list(agg)
    if keys_order:
        ks = sorted(ks, key=lambda k: keys_order.index(k) if k in keys_order else 99)
    return [(k, agg[k]) for k in ks]


FREQ_ORDER = ["fx", "dec", "yr", "mon", "day", "6hr", "3hr", "1hr", "subhr"]


def md_totals(rows, title_extra=""):
    cnt, yr = summarize(rows)
    L = []
    pr_tab = table(yr, lambda r: r["priority"], PRIO_ORDER + ["none"])
    L.append("| Priority | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) | GB/yr raw float32 |")
    L.append("|---|---|---|---|---|---|")
    tot = dict(n=0, files=0, MB=0, MB_low=0, MB_high=0, raw=0)
    for k, a in pr_tab:
        L.append(f"| {k} | {a['n']} | {a['files']:.0f} | {gb(a['MB'])} | {gb(a['MB_low'])}-{gb(a['MB_high'])} | {gb(a['raw'])} |")
        for kk in tot:
            tot[kk] += a[kk]
    L.append(f"| **Total** | **{tot['n']}** | **{tot['files']:.0f}** | **{gb(tot['MB'])}** | "
             f"{gb(tot['MB_low'])}-{gb(tot['MB_high'])} | {gb(tot['raw'])} |")
    L.append("")
    fr_tab = table(yr, lambda r: r["frequency"], FREQ_ORDER)
    L.append("| Frequency | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) |")
    L.append("|---|---|---|---|---|")
    for k, a in fr_tab:
        L.append(f"| {k} | {a['n']} | {a['files']:.0f} | {gb(a['MB'])} | {gb(a['MB_low'])}-{gb(a['MB_high'])} |")
    L.append("")
    # priority x frequency matrix (GB)
    pf = collections.defaultdict(float)
    pn = collections.defaultdict(int)
    for r in yr:
        pf[(r["priority"], r["frequency"])] += r["MB"]
        pn[(r["priority"], r["frequency"])] += 1
    freqs = [f for f in FREQ_ORDER if any(k[1] == f for k in pf)]
    prios = [p for p in PRIO_ORDER + ["none"] if any(k[0] == p for k in pf)]
    L.append("| Priority \\ freq | " + " | ".join(freqs) + " |")
    L.append("|---|" + "---|" * len(freqs))
    for p in prios:
        L.append(f"| {p} | " + " | ".join(f"{pn[(p, f)]} / {gb(pf[(p, f)])}" if pn[(p, f)] else "-"
                                            for f in freqs) + " |")
    L.append("")
    L.append("(cells: variables / GB per year)")
    L.append("")
    return L, tot


def variants(rows):
    cnt, yr = summarize(rows)
    def s(f):
        sel = [r for r in yr if f(r)]
        return len(sel), sum(r["files_per_year"] for r in sel), sum(r["MB"] for r in sel), \
            sum(r["MB_low"] for r in sel), sum(r["MB_high"] for r in sel)
    is3dday = lambda r: r["frequency"] in ("day", "3hr", "1hr", "6hr") and r["levels"] > 1
    so = lambda r: r["region"] == "30S-90S"
    out = [
        ("All counted variables", s(lambda r: True)),
        ("Without sub-monthly 3D fields", s(lambda r: not is3dday(r))),
        ("Without the 30S-90S subsets", s(lambda r: not so(r))),
        ("Without sub-monthly 3D and without 30S-90S", s(lambda r: not is3dday(r) and not so(r))),
        ("Core + High only", s(lambda r: r["priority"] in ("Core", "High"))),
        ("Core + High, without sub-monthly 3D", s(lambda r: r["priority"] in ("Core", "High") and not is3dday(r))),
    ]
    L = ["| Selection | Variables | Files/yr | GB/yr | GB/yr low-high | TB per 100 model years |",
         "|---|---|---|---|---|---|"]
    for name, (n, f, mb, lo, hi) in out:
        L.append(f"| {name} | {n} | {f:.0f} | {gb(mb)} | {gb(lo)}-{gb(hi)} | {mb * 100 / 1e6:.2f} |")
    pr = [r for r in cnt if r["per_run"]]
    if pr:
        L.append("")
        L.append(f"Once per run (fx, climatologies, not in the per-year numbers): {len(pr)} variables, "
                 f"{sum(r['MB'] for r in pr):.1f} MB.")
    return L


def todo_line(r):
    extra = []
    if r.get("status"):
        extra.append(f"**{r['status']}**, not in totals")
    if r["per_run"]:
        size = f"{r['MB']:.1f} MB per run"
    else:
        size = f"{r['MB']:.1f} MB/yr"
    if r.get("peer_precheck"):
        extra.append(f"peer pre-check {r['peer_precheck']}"
                     + (f"/{r['peer_todo_section']}" if r.get("peer_todo_section") and
                        r["peer_todo_section"] != r["peer_precheck"] else ""))
    if r.get("hr_writes") and r["hr_writes"] != "no":
        extra.append(f"HR: {r['hr_writes']}")
    if r.get("lr_source_hint"):
        extra.append(f"LR hint: {r['lr_source_hint']}")
    dims = r["dimensions"]
    return (f"- [ ] **{r['out_name']}** ({r['frequency']}, {r['region']}) `{r['name']}`: {r['long_name']} "
            f"[{r['units']}]; dims `{dims}`; {size} ({r['size_class']}); opps {r['opportunities']}; "
            f"{r['n_experiments']} expts" + ("; " + "; ".join(extra) if extra else ""))


def opp_cover(rows, opp_title):
    by = collections.defaultdict(set)
    for r in rows:
        for tok in r["opportunities"].split():
            by[int(tok[:-1])].add(r["name"])
    allv = set(r["name"] for r in rows)
    L = ["| Opportunity | Variables | H / M / L | Only in this one |", "|---|---|---|---|"]
    keys = sorted(by, key=lambda k: -len(by[k]))
    for k in keys:
        others = set().union(*[by[j] for j in keys if j != k]) if len(keys) > 1 else set()
        hml = collections.Counter()
        for r in rows:
            for tok in r["opportunities"].split():
                if int(tok[:-1]) == k:
                    hml[tok[-1]] += 1
        L.append(f"| {k} {opp_title[k][:55]} | {len(by[k])} | {hml['H']} / {hml['M']} / {hml['L']} | "
                 f"{len(by[k] - others)} |")
    L.append("")
    L.append("Greedy cover (add the opportunity that brings the most new compound names):")
    L.append("")
    L.append("| Step | Opportunity | New | Running total |")
    L.append("|---|---|---|---|")
    cov = set()
    step = 0
    while cov != allv:
        k = max(keys, key=lambda j: len(by[j] - cov))
        g = len(by[k] - cov)
        if g == 0:
            break
        cov |= by[k]
        step += 1
        L.append(f"| {step} | {k} {opp_title[k][:50]} | {g} | {len(cov)} |")
    rest = [k for k in keys if k not in []]
    L.append("")
    return L


def southern_ocean_note(rows):
    so = [r for r in rows if r["region"] == "30S-90S"]
    if not so:
        return []
    glb_roots = {(r["out_name"], r["frequency"]) for r in rows if r["region"] == "glb"}
    glb_names = {r["out_name"] for r in rows if r["region"] == "glb"}
    same_freq = sum(1 for r in so if (r["out_name"], r["frequency"]) in glb_roots)
    any_glb = sum(1 for r in so if r["out_name"] in glb_names)
    opps = collections.Counter(tok for r in so for tok in r["opportunities"].split())
    mb = sum(r["MB"] for r in so if not r["status"])
    return [f"- {len(so)} entries are 30S-90S subsets (opportunities: "
            + ", ".join(f"{k} x{v}" for k, v in opps.most_common()) + f"), {gb(mb)} GB/yr together.",
            f"- {any_glb} of them have the same variable name requested globally in this set "
            f"({same_freq} at the same frequency), so they could be cut from the global field.",
            f"- LR 30S-90S share: {ctx_glob['so_nodes']} of {ctx_glob['lr_nodes']} ocean nodes "
            f"({100 * ctx_glob['so_nodes'] / ctx_glob['lr_nodes']:.1f} %), "
            f"{ctx_glob['so_atm']} of {ATM_CELLS} atmosphere cells."]


ctx_glob = {}


def method_section(ctx):
    g, cls, raw = ctx["grids"], ctx["cls"], ctx["raw"]
    lr = g.lr
    oc = g.ocean("glb")
    so = g.ocean("30S-90S")
    L = ["## Method", "",
         f"- DReq **{DREQ_VERSION}** (released 2026-07-17, newest tag of CMIP7_DReq_Content; read offline from the "
         "data_request_api cache). All 46 opportunities, all priorities. Priority = highest priority any "
         "opportunity gives the variable. Experiments = union over all requesting opportunities.",
         f"- LR ocean grid core3: {lr['n']} surface nodes, {lr['nelem']} elements, {lr['nz'] + 1} levels = "
         f"**{lr['nz']} layers** (aux3d.out), cavity nodes {int(lr['cavity'].sum())}. Wet 3D node-layers "
         f"{oc['wet3d']} of {lr['n'] * lr['nz']} ({100 * oc['wet3d'] / (lr['n'] * lr['nz']):.1f} %). 30S-90S: "
         f"{so['nodes']} nodes, {so['wet3d']} wet node-layers.",
         f"- LR atmosphere/land: TCo95 native O96 grid, {ATM_CELLS} cells, L{ATM_LEVELS}; 30S-90S {g.atm('30S-90S')} "
         f"cells. The LR test run remaps to 400x192 = {ATM_REMAP}; writing on that grid scales atmosphere/land "
         f"sizes by {ATM_REMAP / ATM_CELLS:.2f}.",
         "- Steps per year: mon 12, day 365, 3hr 2920, 1hr 8760, subhr 8760 (OIFS TSTEP 3600 s), yr 1. fx and "
         "climatologies count once per run, outside the per-year totals. One file per variable per year.",
         "- Compressed size = bytes/value measured on the cmorized HR piControl year 1852 "
         "(`/scratch/a/a270092/pycmor_hr/cli123/cmorized/MIP-DRS7`, BitGroom nsd=5 + zlib, HDF5 storage size of "
         "the data variable only), times the LR value count, plus a per-file coordinate overhead (lat/lon and "
         "cell vertices) scaled from HR per horizontal point. Ocean 3D bytes/value is taken per wet value "
         f"(HR DARS2 wet fraction {100 * g.hr_wet3d_frac:.1f} %) and applied to the LR wet count; ocean 2D "
         "classes are applied to open-ocean nodes (fill under ice shelves), HR has no land nodes either.",
         "- Low/high = smallest/largest bytes/value among the HR files of the same class.",
         "", "### Bytes per value used (HR cli123, measured)", "",
         "| Class | HR files | median B/value | min-max | overhead per horiz. point (B) | measured on |",
         "|---|---|---|---|---|---|"]
    for k, c in cls.items():
        ovh = f"{c['ovh_per_point']:.1f}" if k not in ("scalar", "zonal") else f"{c['ovh_median'] / 1e3:.0f} kB/file"
        L.append(f"| {k} | {c['n']} | {c['median']:.3f} | {c['low']:.3f}-{c['high']:.3f} | "
                 f"{ovh} | {c['files'][:90]} |")
    L += ["", "ocn3d_mon is per wet value (HR full-array median "
          f"{cls['ocn3d_mon']['median'] * g.hr_wet3d_frac:.3f} B/value).", "",
          "### Does BGC compress like physics?", "",
          "LR raw REcoM vs FESOM physics output of the same run (awiesm3-v3.5.0-cc pi, 1850, lossless netCDF, "
          "HDF5 storage size / values, float32-equivalent):", "",
          "| | BGC median B/value | BGC range | physics median | ratio BGC/physics |", "|---|---|---|---|---|"]
    for k in ("3d", "2d"):
        r = raw[k]
        L.append(f"| {k} ({r['n_bgc']} BGC fields; physics: {r['phys_vars']}) | {r['bgc']:.2f} | "
                 f"{r['bgc_low']:.2f}-{r['bgc_high']:.2f} | {r['phys']:.2f} | {r['ratio']:.2f} |")
    hr3 = cls["ocn3d_mon"]["median"]
    L += ["", "For ocnBgchem the HR cmorized physics class (thetao uo vo wo / tos sos zos) is multiplied by this "
          f"BGC/physics ratio: 3D {hr3:.2f} x {raw['3d']['ratio']:.2f} = {hr3 * raw['3d']['ratio']:.2f} B per wet "
          f"value, 2D {cls['ocn2d_mon']['median']:.2f} x {raw['2d']['ratio']:.2f} = "
          f"{cls['ocn2d_mon']['median'] * raw['2d']['ratio']:.2f} B/value. Low/high combine the HR class min/max "
          f"with the 10th/90th percentile of the BGC ratio (3D {raw['3d']['ratio_p10']:.2f}-"
          f"{raw['3d']['ratio_p90']:.2f}, 2D {raw['2d']['ratio_p10']:.2f}-{raw['2d']['ratio_p90']:.2f}).",
          "",
          "Uncertainty: fields that are zero over large areas (sea-ice algae, bottom-only fluxes, ice-shelf "
          "quantities, iceberg melt, fields of switched-off REcoM components such as coccolithophores) compress "
          "far better than this (raw LR aN, all zero: 0.01 B/value); 2D fields flagged `sparse` use the HR sea-ice "
          "class (~0.75 B/value instead of ~2.2). The central number is therefore an upper-side estimate for "
          "such fields. BitGroom (nsd=5) is applied in cmorization only, so the ratio from lossless raw output "
          "is a relative measure, not an absolute one. Peer data point: raw LR DetC 3D monthly is 249 MB on "
          "disk vs 497 MB raw float32.",
          "- The ocean per-file overhead is large: HR 3D ocean files carry ~206 B/node of coordinates "
          "(vertices chunked (N/10,1) compress badly), 2D files ~55 B/node. On core3 that is ~45 MB per 3D file "
          "and ~12 MB per 2D file. Fixing the vertex chunking in pycmor would cut most of it.", ""]
    return L


def write_todo(which, rows, ctx, path, title, intro):
    L = [f"# {title}", "", *intro, ""]
    tot_lines, tot = md_totals(rows)
    L += ["## Summary (per model year, counted variables)", ""] + tot_lines
    L += ["## Totals with and without the expensive parts", ""] + variants(rows) + [""]
    dropped = [r for r in rows if r["status"]]
    if dropped:
        L += ["Dropped (listed, not counted):", ""]
        for r in dropped:
            L.append(f"- `{r['name']}`: {r['status']} ({r['MB']:.0f} MB/yr compressed, {r['raw_MB']:.0f} MB/yr raw)")
        L.append("")
    L += ["## Opportunities", ""] + opp_cover(rows, ctx["opp_title"])
    son = southern_ocean_note(rows)
    if son:
        L += ["## Southern Ocean 30S-90S subsets", ""] + son + [""]
    if which == "obgc":
        L += ["## REcoM filter: pending", "",
              "Not done in this phase. Next step: mark each entry below as written / derivable / needs output "
              "change / not in REcoM, then recompute the totals.", ""]
    elif which == "co2":
        L += ["## Feasibility filter: pending", "", "Only the light `HR:` and `LR hint:` notes are given; no judgement yet.", ""]
    else:
        L += ["## Feasibility filter: pending", "", "Not done in this phase.", ""]
    npeer = sum(1 for r in rows if r.get("peer_precheck"))
    L += [f"Peer pre-check (informational, {npeer} of {len(rows)} entries, flux variables only): W/C/S/N from "
          "`notes/lr_peer_session/flux_table.md` (W run writes the inputs, C namelist change, S source change, "
          "N not in the model). Where `awi-esm3-veg-hr-variables/lr_fluxes_bgc_co2_ice_todo.md` (the parallel "
          "session's newer plan) puts the variable in a different section, its code follows after a slash "
          "(W pycmor only, X XML + namelist.io, S source change). That file also lists exclusions it calls "
          "decided (coccolithophores, carbon isotopes); they are not applied here, only the 3-hourly 3D "
          "ficeberg drop by Jan is.", ""]
    L += ["## Variables", ""]
    for p in PRIO_ORDER + ["none"]:
        sel = [r for r in rows if r["priority"] == p]
        if not sel:
            continue
        mb = sum(r["MB"] for r in sel if not r["status"] and not r["per_run"])
        L += [f"### {p} ({len(sel)} entries, {gb(mb)} GB/yr)", ""]
        for r in sorted(sel, key=lambda r: (FREQ_ORDER.index(r["frequency"]), r["out_name"], r["name"])):
            L.append(todo_line(r))
        L.append("")
    L += method_section(ctx)
    L += ["## Columns in the CSV", "",
          "DReq metadata columns as in the HR CSVs, then: priority, opportunities (id + H/M/L), "
          "opportunity_titles, variable_groups, n_experiments, experiments, status (dropped), peer_precheck "
          "(W/C/S/N from notes/lr_peer_session/flux_table.md, informational), size_class, horiz_points, levels, "
          "steps_per_year, values (per year, full array), wet_values, bytes_per_value (+low/high), overhead_MB "
          "(per file), files_per_year, per_run, MB (per year compressed, central), MB_low, MB_high, raw_MB "
          "(float32 uncompressed), note."
          + (" co2 set also: hr_writes (HR yaml tier), hr_cli123_file, lr_source_hint, co2_subset, MB_400x192."
             if which == "co2" else ""), "",
          f"Regenerate: `python {os.path.relpath(os.path.join(HERE, 'estimate_lr_volume.py'), REPO)} {which}` "
          "(needs `hr_bytes_per_value_measured.csv` from `measure_hr_bytes_per_value.py`).", ""]
    with open(path, "w") as fh:
        fh.write("\n".join(L))


def main(sets):
    meta, var_info, expts, opp_title = load_dreq()
    grids = Grids()
    cls = measured_classes(grids)
    raw = lr_raw_recom_ratio()
    ctx = dict(meta=meta, var_info=var_info, expts=expts, opp_title=opp_title, grids=grids, cls=cls, raw=raw,
               peer=peer_tags(), peer_todo=peer_todo_sections(), hr_yaml=hr_yaml_compound_names(), hr_files=hr_written())
    ctx_glob.update(so_nodes=grids.ocean("30S-90S")["nodes"], lr_nodes=grids.lr["n"], so_atm=grids.atm("30S-90S"))
    spec = {
        "obgc": ("obgc", "cmip7_obgc", "CMIP7 ocean biogeochemistry (ocnBgchem) for AWI-ESM3 LR: full list and size",
                 ["All variables whose CMIP7 compound name has realm `ocnBgchem` in DReq " + DREQ_VERSION + ".",
                  "Scope of this phase: the complete list and a compressed size estimate on the LR grids. "
                  "No REcoM mapping or feasibility yet (see 'REcoM filter: pending').",
                  "No ocnBgchem variable is requested at Core priority or by the Baseline opportunity (69)."]),
        "co2": ("co2", "cmip7_co2", "CMIP7 CO2 / carbon-cycle variables outside ocnBgchem for AWI-ESM3 LR",
                ["Selection: realm is not ocnBgchem and (variable name contains `co2`, excluding the `*4co2` "
                 "radiation diagnostics of opportunity 72, or the CF standard name contains `carbon_dioxide`, or "
                 "the variable is a land CO2 exchange flux without that word: gpp, npp, gppLut, nppLut, gppVgt, "
                 "nppVgt, gppc13, gppc14, rhLitter, rhSoil). Carbon pools (cLand, cVeg, ...) and internal land "
                 "transfers (fVegLitter, nppLeaf, ...) are left to the land tier.",
                 "ocnBgchem CO2 variables (fgco2, spco2 glb + 30S-90S, dpco2, fg13co2) are in `../obgc/`, not here.",
                 "`HR:` = HR yaml tier that has a rule with this compound name; `LR hint:` = a field in the LR "
                 "test run that could feed it (light check, file names only)."]),
        "ice": ("icebergs_cavities", "cmip7_icebergs_cavities",
                "CMIP7 iceberg and ice-shelf cavity variables for AWI-ESM3 LR",
                ["LR v3.5.1-cc on core3 runs with `use_icebergs` and `use_cavity` on (HR coverage notes assume "
                 "both off). Roots: ficeberg, hfibthermds, flandice, libmassbf, sftflf, iareafl, licalvf, "
                 "lifmassbf, tendlicalvf, litempbot, all variants in DReq " + DREQ_VERSION + ".",
                 "Decision by Jan, 2026-10-19: `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb` is not produced; it is "
                 "listed and left out of the totals. 3-hourly flandice is undecided and counted.",
                 "Regional landIce grids sized on FESOM nodes: `ata` = nodes south of 60S, `grl` = nodes in "
                 "58N-90N, 75W-10W. `fis` fields count only cavity nodes as non-fill."]),
    }
    for which in sets:
        d, stem, title, intro = spec[which]
        od = os.path.join(LRVAR, d)
        os.makedirs(od, exist_ok=True)
        rows = build(which, ctx)
        write_csv(rows, os.path.join(od, f"{stem}_variables.csv"))
        write_todo(which, rows, ctx, os.path.join(od, f"{stem}_todo.md"), title, intro)
        cnt, yr = summarize(rows)
        print(f"{which}: {len(rows)} entries, counted per-year {len(yr)}, "
              f"{sum(r['MB'] for r in yr) / 1e3:.2f} GB/yr (low {sum(r['MB_low'] for r in yr) / 1e3:.2f}, "
              f"high {sum(r['MB_high'] for r in yr) / 1e3:.2f}), raw {sum(r['raw_MB'] for r in yr) / 1e3:.2f} GB/yr")


if __name__ == "__main__":
    main(sys.argv[1:] or ["obgc", "co2", "ice"])
