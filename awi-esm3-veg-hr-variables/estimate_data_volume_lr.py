#!/usr/bin/env python3
"""
Estimate annual data volume for AWI-ESM3-VEG-LR CMIP7 CMORization.

Identical logic to estimate_data_volume.py but with LR grid sizes:
  - atm (OIFS TCo95 reduced Gauss → 192 x 400): 76,800 gridpoints
  - atm_ml: x91 levels
  - ocean/seaice (FESOM CORE2): 126,858 surface nodes
  - ocean 3D: x47 levels
  - LPJ-GUESS: same ~420,000 land cells (resolution independent here)
"""

import glob
import os
import re
import csv

GRID_POINTS = {
    "atm_sfc": 192 * 400,                 # 76,800
    "atm_ml": 192 * 400 * 91,
    "atm_pl": 192 * 400 * 19,
    "atm_pl3": 192 * 400 * 3,
    "atm_pl6": 192 * 400 * 6,
    "oce_sfc": 126_858,
    "oce_3d": 126_858 * 47,
    "lpjg": 420_000,
}

TIMESTEPS = {
    "fx": 1, "yr": 1, "dec": 0.1,
    "mon": 12, "Amon": 12, "Lmon": 12, "Omon": 12, "SImon": 12,
    "AERmon": 12, "Emon": 12, "LImon": 12, "CFmon": 12,
    "day": 365, "Eday": 365, "SIday": 365, "Oday": 365, "CFday": 365,
    "6hr": 1460, "6hrPt": 1460,
    "3hr": 2920, "3hrPt": 2920, "CF3hr": 2920, "E3hrPt": 2920,
    "1hr": 8760, "E1hr": 8760, "AERhr": 8760,
}

BYTES_PER_VALUE = 4


def guess_grid(rule_name, compound_name, realm, model_variable, is_3d=False):
    cn = compound_name.lower() if compound_name else ""
    rn = rule_name.lower()
    if realm in ("ocean", "seaice", "seaIce", "landIce"):
        if is_3d or any(k in cn for k in ("-al-", "-ol-", "3d", "mlev")):
            return "oce_3d"
        return "oce_sfc"
    if "-al-" in cn or "ml" in rn or "pfull" in rn:
        return "atm_ml"
    if "plev19" in cn or "-p19-" in cn or "_pl_" in rn:
        return "atm_pl"
    if "plev3" in cn or "-p3-" in cn or "_pl3" in rn:
        return "atm_pl3"
    if "plev6" in cn or "-p6-" in cn or "_pl6" in rn:
        return "atm_pl6"
    if "lpjg" in rn or "lpj" in rn or "Lut" in rn:
        return "lpjg"
    return "atm_sfc"


def guess_frequency(rule_name, compound_name):
    cn = compound_name if compound_name else ""
    parts = cn.split(".")
    if len(parts) >= 4:
        freq = parts[-2]
        if freq in TIMESTEPS:
            return freq
    rn = rule_name.lower()
    for freq_key in ["1hr", "3hr", "6hr", "day", "mon", "yr", "fx", "dec"]:
        if freq_key in rn:
            return freq_key
    return "mon"


def is_3d_rule(rule_name, compound_name, model_variable):
    cn = (compound_name or "").lower()
    rn = rule_name.lower()
    return any(k in cn for k in ("-al-", "-ol-", "-p19-", "-p3-", "-p6-")) or any(
        k in rn for k in ("_ml", "_pl", "pfull", "plev")
    )


def parse_yaml_rules(yaml_path):
    rules = []
    with open(yaml_path) as f:
        content = f.read()
    dirname = os.path.basename(os.path.dirname(yaml_path))
    if "ocean" in dirname:
        realm = "ocean"
    elif "seaice" in dirname:
        realm = "seaice"
    elif "land" in dirname:
        realm = "land"
    else:
        realm = "atmos"
    rule_blocks = re.split(r"\n\s*- name:", content)
    for i, block in enumerate(rule_blocks):
        if i == 0:
            continue
        lines = block.strip().split("\n")
        name = lines[0].strip()
        compound = ""
        model_var = ""
        has_lpjg = "lpjg" in block.lower() or "lpj_guess" in block.lower()
        for line in lines:
            line = line.strip()
            if line.startswith("compound_name:"):
                compound = line.split(":", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("model_variable:"):
                model_var = line.split(":", 1)[1].strip().strip('"').strip("'")
        if compound:
            cr = compound.split(".")[0].lower()
            if cr in ("ocean", "omon"):
                realm_r = "ocean"
            elif cr in ("seaice", "simon", "siday"):
                realm_r = "seaice"
            elif cr in ("landice",):
                realm_r = "land"
            elif cr in ("atmos", "aerosol", "atmoschem"):
                realm_r = "atmos"
            elif cr in ("land",):
                realm_r = "land"
            else:
                realm_r = realm
        else:
            realm_r = realm
        threed = is_3d_rule(name, compound, model_var)
        grid = guess_grid(name, compound, realm_r, model_var, threed)
        if has_lpjg or "lpjg" in name.lower():
            grid = "lpjg"
        freq = guess_frequency(name, compound)
        rules.append({
            "config": os.path.basename(yaml_path),
            "dir": dirname, "name": name, "compound": compound,
            "realm": realm_r, "grid": grid, "freq": freq, "model_var": model_var,
        })
    return rules


def human_size(nbytes):
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} EB"


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    all_rules = []
    yaml_files = sorted(glob.glob(os.path.join(base, "*/cmip7_awiesm3-veg-hr_*.yaml")))
    for yf in yaml_files:
        all_rules.extend(parse_yaml_rules(yf))

    realm_map = {"atmos": "Atmosphere", "land": "Land", "ocean": "Ocean", "seaice": "Sea Ice"}

    print("=" * 90)
    print("AWI-ESM3-VEG-LR CMIP7 — Annual Data Volume Estimate (TCo95 / CORE2)")
    print("=" * 90)
    print()

    realm_rules, realm_bytes, freq_bytes = {}, {}, {}
    for r in all_rules:
        realm_label = realm_map.get(r["realm"], r["realm"])
        gp = GRID_POINTS.get(r["grid"], GRID_POINTS["atm_sfc"])
        ts = TIMESTEPS.get(r["freq"], 12)
        annual_bytes = gp * ts * BYTES_PER_VALUE
        realm_rules[realm_label] = realm_rules.get(realm_label, 0) + 1
        realm_bytes[realm_label] = realm_bytes.get(realm_label, 0) + annual_bytes
        freq_bytes[r["freq"]] = freq_bytes.get(r["freq"], 0) + annual_bytes

    print(f"{'Realm':<15} {'Rules':>6} {'Annual size':>14}")
    print("-" * 40)
    total_rules = 0
    total_bytes = 0
    for realm_label in ["Atmosphere", "Land", "Ocean", "Sea Ice"]:
        nr = realm_rules.get(realm_label, 0)
        nb = realm_bytes.get(realm_label, 0)
        print(f"{realm_label:<15} {nr:>6} {human_size(nb):>14}")
        total_rules += nr
        total_bytes += nb
    print("-" * 40)
    print(f"{'TOTAL':<15} {total_rules:>6} {human_size(total_bytes):>14}")
    print()

    print(f"{'Frequency':<10} {'Rules':>6} {'Annual size':>14} {'Fraction':>9}")
    print("-" * 45)
    for freq in sorted(freq_bytes, key=lambda f: freq_bytes[f], reverse=True):
        nb = freq_bytes[freq]
        nrules = sum(1 for r in all_rules if r["freq"] == freq)
        frac = nb / total_bytes * 100 if total_bytes > 0 else 0
        print(f"{freq:<10} {nrules:>6} {human_size(nb):>14} {frac:>8.1f}%")
    print()

    grid_bytes, grid_rules = {}, {}
    for r in all_rules:
        gp = GRID_POINTS.get(r["grid"], GRID_POINTS["atm_sfc"])
        ts = TIMESTEPS.get(r["freq"], 12)
        ab = gp * ts * BYTES_PER_VALUE
        grid_bytes[r["grid"]] = grid_bytes.get(r["grid"], 0) + ab
        grid_rules[r["grid"]] = grid_rules.get(r["grid"], 0) + 1

    print(f"{'Grid':<12} {'Rules':>6} {'Annual size':>14} {'Fraction':>9}")
    print("-" * 47)
    for g in sorted(grid_bytes, key=lambda g: grid_bytes[g], reverse=True):
        nb = grid_bytes[g]
        frac = nb / total_bytes * 100 if total_bytes > 0 else 0
        print(f"{g:<12} {grid_rules[g]:>6} {human_size(nb):>14} {frac:>8.1f}%")
    print()
    print(f"Total estimated annual volume: {human_size(total_bytes)}")
    print(f"  (uncompressed float32, before NetCDF compression)")
    print(f"  With typical 2-3x NetCDF4/zlib compression: ~{human_size(total_bytes/2.5)}")


if __name__ == "__main__":
    main()
