#!/usr/bin/env python3
"""LR CMIP7 campaign data volume: model output and cmorized, by sphere, frequency and experiment.

Per model year:
  CMIP7  = the HR rule set, measured on the cmorized HR year cli123 and scaled
           file by file to TCo95/L91, 400x192 and CORE3 (cells, levels)
         + the rules LR switches back on (notes/LR_reactivation_plan.md),
           values x measured bytes per value
         + the ocean BGC, CO2 and iceberg lists in ../obgc, ../co2,
           ../icebergs_cavities (MB per year columns)
  output = OpenIFS: HR piControl 1852 raw output scaled to LR, plus the
           streams LR switches on (6-hourly model levels, 3-hourly plev6)
         + FESOM/REcoM: one month of a v3.5.1-cc run with the CMIP7 XML and
           the daily 3D switch on, scaled to a year
         + LPJ-GUESS: one year of the LR run

Campaign: the experiments below. Two totals for CMIP7: every variable in
every experiment, and only what the Data Request v1.2.2.5 requests for each
experiment (the extensions 2101-2299 are the DReq's scen7-<x>-ext). DReq time
subsets (e.g. piControl years matching the last 40 of historical) are not
applied, so the second total is still an upper bound.

Usage: estimate_lr_campaign_volume.py [--out DIR]   (writes JSON tables and a PNG)
"""
import argparse
import collections
import glob
import json
import os
import re

import netCDF4
import pandas as pd
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
LRV = os.path.dirname(HERE)
REPO = os.path.dirname(LRV)
CLI123 = "/scratch/a/a270092/pycmor_hr/cli123/cmorized/MIP-DRS7"
HR_RAW = "/work/bb1469/a270089/runtime/awiesm3-v3.4.2/AWI-ESM3-VEG-HR-CMIP7-piControl/outdata"
LR_FESOM_MONTH = "/work/bb1469/a270092/runtime/awiesm3-v3.5.1-cc-xiosdraft/xiosdraft_m1/outdata/fesom"
LR_LPJG_YEAR = "/work/ab1095/a270073/out/awiesm3-v3.5.0-cc/pi/outdata/lpj_guess/18500101-18501231"
FESOM_FIELD_DEF = "/home/a/a270092/esm_tools-draft-xios/namelists/fesom2/xios_xml_cmip7/field_def_fesom.xml"
DREQ = os.path.expanduser("~/.CMIP7_data_request_api_cache/v1.2.2.5/dreq_release_consolidate.json")

EXPERIMENTS = [
    ("piControl", "piControl", 550),
    ("historical", "historical", 172),
    ("1pctCO2", "1pctCO2", 150),
    ("abrupt-4xCO2", "abrupt-4xCO2", 300),
]
# plan name -> DReq name; scen7-vllo/vlho are assumed to be the DReq vl/ln
SCENARIOS = {
    "scen7-vllo": "scen7-vl",
    "scen7-vlho": "scen7-ln",
    "scen7-l": "scen7-l",
    "scen7-ml": "scen7-ml",
    "scen7-m": "scen7-m",
    "scen7-hl": "scen7-hl",
    "scen7-h": "scen7-h",
}
for s, d in SCENARIOS.items():
    EXPERIMENTS += [(f"{s} to 2100", d, 79), (f"{s} 2101-2299", d + "-ext", 199)]

STEPS = {"1hr": 8760, "3hr": 2920, "6hr": 1460, "day": 365, "mon": 12, "yr": 1}
FREQ = {"1hrPt": "1hr", "3hrPt": "3hr", "6hrPt": "6hr", "monC": "mon", "1hrCM": "1hr", "subhr": "1hr"}
SPHERE = {
    "atmos": "Atmosphere",
    "aerosol": "Atmosphere",
    "atmosChem": "Atmosphere",
    "land": "Land",
    "landIce": "Land ice",
    "ocean": "Ocean",
    "seaIce": "Sea ice",
    "ocnBgchem": "Ocean biogeochemistry",
}
# measured bytes per value, cli123: instantaneous 6-hourly 3D (plev3) 2.40, HR raw 6h pressure levels 2.15
BPV_TPT_3D, BPV_RAW_3D = 2.40, 2.15
NAT, NOD, ELEM = 40320, 220509, 426403


def scale_hr_to_lr(dims):
    f = 1.0
    for k, n in dims.items():
        if k == "cell":
            f *= NAT / 421120
        elif k == "ncells":
            f *= NAT / 421120 if n == 421120 else (NOD / 3146761 if n == 3146761 else 1)
        elif k == "nod2":
            f *= NOD / 3146761
        elif k == "elem":
            f *= ELEM / 6229020
        elif k == "lat" and n == 640:
            f *= 192 / 640
        elif k == "lon" and n == 1296:
            f *= 400 / 1296
        elif k == "lev":
            f *= {137: 91 / 137, 56: 47 / 56, 57: 48 / 57}.get(n, 1)
    return f


def cmip7_items():
    """{compound: (sphere, freq, GB per model year)}"""
    items = {}

    def add(c, realm, fr, gb):
        s, f0, g0 = items.get(c, (SPHERE.get(realm, realm), FREQ.get(fr, fr), 0.0))
        items[c] = (s, f0, g0 + gb)

    for f in glob.glob(CLI123 + "/**/*.nc", recursive=True):
        name = os.path.basename(f)
        var, brand = name.split("_")[:2]
        with netCDF4.Dataset(f) as ds:
            realm = ds.getncattr("realm").split()[0]
            freq, region = ds.getncattr("frequency"), ds.getncattr("region")
            dims = {d: len(ds.dimensions[d]) for d in ds[var].dimensions} if var in ds.variables else {}
        add(f"{realm}.{var}.{brand}.{freq}.{region}", realm, freq, os.path.getsize(f) * scale_hr_to_lr(dims) / 1e9)
    for v in ("ta", "ua", "va", "hus", "zg"):
        add(f"atmos.{v}.tpt-al-hxy-u.6hr.glb", "atmos", "6hr", NAT * 91 * 1460 * BPV_TPT_3D / 1e9)
        add(f"atmos.{v}.tpt-p7h-hxy-air.6hr.glb", "atmos", "6hr", NAT * 7 * 1460 * BPV_TPT_3D / 1e9)
    for v in ("ta", "ua", "va", "wap", "hus"):
        add(f"atmos.{v}.tpt-p6-hxy-air.3hr.glb", "atmos", "3hr", NAT * 6 * 2920 * BPV_TPT_3D / 1e9)
    for c, gb in (
        ("atmos.ps.tpt-u-hxy-u.1hr.30S-90S", 0.74),
        ("atmos.ts.tpt-u-hxy-u.3hr.glb", 0.25),
        ("ocean.hfx.tavg-u-hxy-sea.day.glb", 0.17),
        ("ocean.hfy.tavg-u-hxy-sea.day.glb", 0.17),
        ("land.mrsol.tavg-d100cm-hxy-lnd.3hr.glb", 0.08),
        ("land.mrsol.tpt-d10cm-hxy-lnd.3hr.glb", 0.08),
        ("land.mrsol.tavg-d10cm-hxy-lnd.day.glb", 0.01),
    ):
        add(c, c.split(".")[0], c.split(".")[3], gb)
    for fn in (
        "obgc/cmip7_obgc_variables.csv",
        "co2/cmip7_co2_variables.csv",
        "icebergs_cavities/cmip7_icebergs_cavities_variables.csv",
    ):
        x = pd.read_csv(os.path.join(LRV, fn))
        if "hr_writes" in x:
            x = x[~x["hr_writes"].astype(str).str.lower().isin(["true", "yes", "1"])]
        x = x[~x["status"].astype(str).str.contains("dropped", case=False)]
        x = x[~x["per_run"].astype(str).str.lower().isin(["true", "yes", "1"])]
        for _, r in x.iterrows():
            add(r["cmip7_compound_name"], str(r["modeling_realm"]).split()[0], r["frequency"], r["MB"] / 1e3)
    return items


def raw_items():
    """{(source, stream): (sphere, freq, GB per model year)}"""
    pats = []
    for f in glob.glob(os.path.join(REPO, "awi-esm3-veg-hr-variables/*/*.yaml")):
        for r in yaml.safe_load(open(f)).get("rules") or []:
            for i in r.get("inputs", []):
                try:
                    pats.append((re.compile(i["pattern"]), r.get("compound_name", "atmos.").split(".")[0]))
                except (KeyError, re.error):
                    pass

    def realm_of(fn):
        return next((r for p, r in pats if p.match(fn)), "atmos")

    out = {}
    fmap = {"1h": "1hr", "3h": "3hr", "6h": "6hr", "day": "day", "mon": "mon", "1d": "day", "1m": "mon"}
    odir = os.path.join(HR_RAW, "oifs")
    for fn in os.listdir(odir):
        if "_1852-1852" not in fn:
            continue
        p = fn.split("_")
        fr, f = (fmap[p[2]], 400 * 192 / (1296 * 640)) if fn.startswith("atm_remapped") else (fmap[p[1]], NAT / 421120)
        if "_ml_" in fn:
            f *= 91 / 137
        out[("OpenIFS", fn)] = (
            SPHERE.get(realm_of(fn), "Atmosphere"),
            fr,
            os.path.getsize(os.path.join(odir, fn)) * f / 1e9,
        )
    for v in ("ta", "ua", "va", "hus", "zg"):
        out[("OpenIFS, LR adds", f"atmos_6h_ml_{v}")] = ("Atmosphere", "6hr", 91 * NAT * 1460 * BPV_RAW_3D / 1e9)
    for v in ("ta", "ua", "va", "wap", "hus"):
        out[("OpenIFS, LR adds", f"atmos_3h_pl6_{v}")] = ("Atmosphere", "3hr", 6 * NAT * 2920 * BPV_RAW_3D / 1e9)
    fd = open(FESOM_FIELD_DEF).read()
    bgc = set()
    for g in re.finditer(r'<field_group id="([^"]+)"[^>]*>(.*?)</field_group>', fd, re.S):
        if "bgc" in g.group(1):
            bgc |= set(re.findall(r'<field\s+id="([^"]+)"', g.group(2)))
    bgc_alias = re.compile(
        r"^(O2|pH|CO3|CO3satA|PhyC|DiaC|PhyChl|DiaChl)_sfc$"
        r"|^(NPPn|NPPd|intPOC|bflxC|O2min|DOC|DetC|HetC|idetz2c|Zoo2C|Zoo3C)_day$"
    )
    ice = re.compile(
        r"^(a_ice|m_ice|m_ice_day|m_snow|h_ice|h_snow|ist|alb|uice|vice|[ahi]pnd|thdgr\w+|dyngr\w+"
        r"|fw_ice|fw_snw|atmice_\w|iceoce_\w|strength_ice|sgm\d\d(_inst)?|qcon|si\w+[ns])$"
    )
    for fn in os.listdir(LR_FESOM_MONTH):
        if not fn.endswith(".nc"):
            continue
        name = fn.split(".fesom")[0]
        with netCDF4.Dataset(os.path.join(LR_FESOM_MONTH, fn)) as ds:
            nt = len(ds.dimensions["time"]) if "time" in ds.dimensions else 1
        fr, sc = ("3hr", 365 / 31) if nt >= 200 else (("day", 365 / 31) if nt >= 28 else ("mon", 12))
        sph = (
            "Ocean biogeochemistry"
            if (name in bgc or bgc_alias.match(name))
            else ("Sea ice" if ice.match(name) else "Ocean")
        )
        out[("FESOM/REcoM", fn)] = (sph, fr, os.path.getsize(os.path.join(LR_FESOM_MONTH, fn)) * sc / 1e9)
    for fn in os.listdir(LR_LPJG_YEAR):
        fr = "day" if "daily" in fn else ("mon" if "monthly" in fn else "yr")
        out[("LPJ-GUESS", fn)] = ("Land", fr, os.path.getsize(os.path.join(LR_LPJG_YEAR, fn)) / 1e9)
    return out


def dreq_requests():
    D = json.load(open(DREQ))["Data Request"]

    def link(x):
        return [i.split("link::")[-1] for i in (x if isinstance(x, list) else ([x] if x else []))]

    vid = {k: v["CMIP7 Compound Name"] for k, v in D["Variables"]["records"].items()}
    req = {
        e["Experiment"]: {vid[i] for i in link(e.get("Variables")) if i in vid}
        for e in D["Experiments"]["records"].values()
    }
    return req, set(vid.values())


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=HERE)
    a = ap.parse_args()
    cm, raw = cmip7_items(), raw_items()
    req, known = dreq_requests()
    years = sum(y for _, _, y in EXPERIMENTS)
    rows = []
    for kind, src in (("raw", raw.values()), ("cmip7", cm.values())):
        agg = collections.defaultdict(float)
        for s, f, g in src:
            agg[(s, f)] += g
        rows += [
            dict(
                kind=kind, sphere=s, frequency=f, gb_per_year=round(g, 3), tb_all_experiments=round(g * years / 1e3, 2)
            )
            for (s, f), g in sorted(agg.items())
            if g > 0.001
        ]
    per_exp = []
    for lab, de, y in EXPERIMENTS:
        g = sum(v[2] for c, v in cm.items() if c in req.get(de, set()) or c not in known)
        per_exp.append(
            dict(experiment=lab, dreq_experiment=de, years=y, gb_per_year=round(g, 1), tb=round(g * y / 1e3, 1))
        )
    summary = dict(
        model_years=years,
        raw_gb_per_year=round(sum(v[2] for v in raw.values()), 1),
        cmip7_gb_per_year=round(sum(v[2] for v in cm.values()), 1),
        raw_tb_campaign=round(sum(v[2] for v in raw.values()) * years / 1e3),
        cmip7_tb_every_variable_everywhere=round(sum(v[2] for v in cm.values()) * years / 1e3),
        cmip7_tb_as_requested_per_experiment=round(sum(e["tb"] for e in per_exp)),
    )
    json.dump(
        dict(
            summary=summary,
            by_sphere_and_frequency=rows,
            per_experiment=per_exp,
            cmip7_items_gb_per_year={c: round(v[2], 4) for c, v in sorted(cm.items(), key=lambda z: -z[1][2])},
        ),
        open(os.path.join(a.out, "lr_campaign_volume.json"), "w"),
        indent=1,
    )
    print(json.dumps(summary, indent=1))
    plot(rows, years, os.path.join(REPO, "examples", "lr_cmip7_volume.png"))


def plot(rows, years, out):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sph = ["Atmosphere", "Ocean", "Ocean biogeochemistry", "Sea ice", "Land", "Land ice"]
    frq = ["1hr", "3hr", "6hr", "day", "mon", "yr"]
    sc = {
        "Atmosphere": "#4c78a8",
        "Ocean": "#1f4e79",
        "Ocean biogeochemistry": "#2a9d8f",
        "Sea ice": "#a6cee3",
        "Land": "#8c6d31",
        "Land ice": "#cccccc",
    }
    fc = {"1hr": "#7f2704", "3hr": "#d94801", "6hr": "#fd8d3c", "day": "#fdbe85", "mon": "#6baed6", "yr": "#2171b5"}

    def agg(kind, by):
        d = collections.defaultdict(float)
        for r in rows:
            if r["kind"] == kind:
                d[r[by]] += r["tb_all_experiments"]
        return d

    fig, axes = plt.subplots(1, 2, figsize=(12, 6.2), sharey=True)
    kinds = [("raw", "model output\n(as written, compressed)"), ("cmip7", "CMIP7\n(cmorized)")]
    for ax, (by, cats, cols, title) in zip(
        axes, [("sphere", sph, sc, "by sphere"), ("frequency", frq, fc, "by frequency")]
    ):
        handles = {}
        for i, (k, _) in enumerate(kinds):
            a, bottom = agg(k, by), 0
            for c in cats:
                v = a.get(c, 0)
                if v <= 0:
                    continue
                handles[c] = ax.bar(i, v, bottom=bottom, color=cols[c], width=0.6, edgecolor="white", linewidth=0.8)
                if v > 25:
                    ax.text(
                        i,
                        bottom + v / 2,
                        f"{v:.0f}",
                        ha="center",
                        va="center",
                        fontsize=9,
                        color="black" if c in ("day", "Sea ice", "Land ice") else "white",
                    )
                bottom += v
            ax.text(i, bottom + 12, f"{bottom:.0f} TB", ha="center", va="bottom", fontsize=11, fontweight="bold")
        ax.set_xticks([0, 1])
        ax.set_xticklabels([lab for _, lab in kinds])
        ax.set_title(title, fontsize=12)
        order = [c for c in cats if c in handles][::-1]
        ax.legend([handles[c] for c in order], order, frameon=False, loc="upper right", fontsize=9)
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_xlim(-0.6, 2.1)
    axes[0].set_ylabel("TB for the whole campaign")
    top = max(sum(agg(k, "sphere").values()) for k, _ in kinds)
    for ax in axes:
        ax.set_ylim(0, top * 1.12)
    fig.suptitle(
        f"AWI-ESM3 LR data volume, {years} model years, every variable in every experiment", fontsize=13, y=0.99
    )
    fig.text(
        0.5,
        0.005,
        "Experiments: piControl 550, historical 172, 1pctCO2 150, abrupt-4xCO2 300 y, seven scen7 scenarios"
        " 2022-2299 (278 y each); spin-up not included.\n"
        "Model output: FESOM/REcoM measured on one CMIP7-XML month (daily 3D switch on), OpenIFS scaled from HR 1852"
        " plus the streams LR adds, LPJ-GUESS from the LR run.\n"
        "CMIP7: HR cli123 scaled to TCo95/CORE3, plus reactivated rules and the ocean BGC, CO2 and iceberg lists."
        " Only what the DReq requests per experiment: see lr_campaign_volume.json.",
        ha="center",
        va="bottom",
        fontsize=8,
        color="#444",
    )
    plt.tight_layout(rect=(0, 0.09, 1, 0.96))
    plt.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
